use std::{str::FromStr, sync::{Arc}};

use anyhow::{Ok, Result};
use futures::{StreamExt, future::join_all};
use solana_client::{
    nonblocking::{
        pubsub_client::PubsubClient,
        rpc_client::RpcClient,
    }, rpc_config::{RpcTransactionConfig, RpcTransactionLogsConfig}, rpc_request::Address, rpc_response::{
        OptionSerializer, RpcLogsResponse, transaction::{Signature, VersionedMessage}
    }
};
use spl_token_interface::{instruction::TokenInstruction};
use sqlx::{PgPool, Postgres};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

pub const USDC_MINT: &str = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v";

type TxnRecord = (String, i64, String, String, i64, String);

#[tokio::main]
async fn main() -> Result<()> {
    println!("Welcome to Whale Indexer");

    //load .env into process environment
    dotenv::dotenv()?;

    let pg_pool = PgPool::connect(
        std::env::var("DATABASE_URL")
            .expect("Required 'DATABASE_URL'!")
            .as_str(),
    )
    .await?;

    // lets use only the token accounts for now, we can add the owners later if necessary
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS whale_txns (
            id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            signature TEXT, 
            slot BIGINT NOT NULL,
            source_token_acc TEXT NOT NULL,
            dest_token_acc TEXT NOT NULL,
            amount BIGINT NOT NULL, 
            mint TEXT NOT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );",
    )
    .execute(&pg_pool)
    .await?;
    println!("Debug log - Table created");

    // create rpc client
    let rpc_url = std::env::var("RPC_URL").expect("Required Rpc endpoint Url!");
    let arc_rpc_client = Arc::new(RpcClient::new(rpc_url));

    // async communication channel
    let (log_tx, log_rx) = tokio::sync::mpsc::channel::<RpcLogsResponse>(100); 

    //create websocket connection
    let ws_rpc_url = std::env::var("WEBSOCKET_RPC_URL").expect("Required 'WEBSOCKET_RPC_URL'!");
    let ws_client = PubsubClient::new(ws_rpc_url).await?;
    let (mut log_stream, _log_unsubscribe) = ws_client
        .logs_subscribe(
            solana_client::rpc_config::RpcTransactionLogsFilter::Mentions(vec![USDC_MINT.to_string()]),
            RpcTransactionLogsConfig { commitment: None },
        )
        .await?;
    println!("Debug log - Websocket connected");

    // spawn the filter cum dispatcher, and keep it ready to receive messages
    let dispatcher_handle = tokio::spawn(filter_logs_and_dispatch(log_rx, arc_rpc_client, pg_pool));

    println!("Debug log - Streaming logs from websocket");
    while let Some(log_response) = log_stream.next().await {
        println!("Debug log - Received Response");

        // deligate the msg to process to filter cum dispatcher
        log_tx.send(log_response.value).await?;
    };

    println!("Debug log - Waiting on dispatcher to complete");
    dispatcher_handle.await??;

    Ok(())
}

/// Filter logs for successful transactions and pass down to workers
async fn filter_logs_and_dispatch(mut receiver: tokio::sync::mpsc::Receiver<RpcLogsResponse>, rpc_client: Arc<solana_client::nonblocking::rpc_client::RpcClient>, pg_pool: PgPool)->Result<()>{
    println!("Debug log - Welcome to Dispatcher");
    
    // worker permits
    let workers_semaphor = Arc::new(Semaphore::new(10));

    // record collection channel for batch insertion
    let (record_tx, record_rx) = tokio::sync::mpsc::channel::<TxnRecord>(50);
    // batch insertion worker
    println!("Debug log - spawning Db Pusher for batch insertions");
    let db_pusher_handle = tokio::spawn(db_pusher(record_rx, pg_pool));

    let mut worker_handles = vec![];

    while let Some(log_response) = receiver.recv().await {
        if log_response.err.is_some() {
            println!("Debug log - Transaction has error");
            // failed transactions
            continue;
        }
        
        // successful transactions

        // get permission or wait for permission and spawn a worker task
        println!("Debug log - Waiting for a worker permission");
        let permit = workers_semaphor.clone().acquire_owned().await?;
        let rpc_client_clone = rpc_client.clone();
        let txn_record_sender = record_tx.clone();

        println!("Debug log - spawning a new worker");
        let worker_handle = tokio::spawn(worker(permit, log_response, txn_record_sender, rpc_client_clone));
        worker_handles.push(worker_handle);
    };

    println!("Debug log - Waiting on workers to complete current in hand processings...");
    join_all(worker_handles).await;

    println!("Debug log - Waiting on db_pusher to finish");
    db_pusher_handle.await??;

    Ok(())
}

/// Worker task/function to process the logs, find whale txns 
async fn worker(permit: OwnedSemaphorePermit, log_response: RpcLogsResponse, record_sender: tokio::sync::mpsc::Sender<TxnRecord>, rpc_client: Arc<solana_client::nonblocking::rpc_client::RpcClient>)->Result<()>{
        // filter for usdc transfers
        // since legacy token doesn't log the name of instruction being invoked,
        // we'll just qualify every transaction thats coming from the websocket
        // because transaction already mentions USDC mint and
        // there is a high chance it is a transfer, we'll do the final check later
        if !log_response
            .logs
            .iter()
            .any(|lg| lg.contains(spl_token_interface::ID.to_string().as_str()))
        {
            println!("Debug log - Transaction logs doesn't contain token program");
            return Ok(());
        }

        // process transactions for qualified transactions

        // fetch transaction from rpc node
        let txn_signature = Signature::from_str(log_response.signature.as_str())?;
        let encoded_confirmed_txn = rpc_client
            .get_transaction_with_config(
                &txn_signature,
                RpcTransactionConfig {
                    encoding: Some(solana_client::rpc_config::UiTransactionEncoding::Base58),
                    max_supported_transaction_version: Some(0),
                    ..Default::default()
                },
            )
            .await?;
        println!("Debug log - Fetched transaction from rpc");

        let usdc_mint = USDC_MINT.to_string();

        // decode and process each transaction
        if let Some(txn) = encoded_confirmed_txn.transaction.transaction.decode() {
            let (account_keys, insns) = match txn.message {
                VersionedMessage::Legacy(msg) => (msg.account_keys, msg.instructions),
                VersionedMessage::V0(msg) => {
                    let mut account_keys = msg.account_keys;
                    if let Some(txn_meta) = &encoded_confirmed_txn.transaction.meta {
                        if let OptionSerializer::Some(loaded_addresses) = &txn_meta.loaded_addresses {
                            println!("Debug log - Extending account keys with loaded addresses");
                            account_keys.extend(loaded_addresses.writable.iter().map(|s| Address::from_str(s.as_str()).unwrap()));
                            account_keys.extend(loaded_addresses.readonly.iter().map(|s| Address::from_str(s.as_str()).unwrap()));
                        }
                    }
                    (account_keys, msg.instructions)
                },
            };

            let signature = log_response.signature;
            let slot = encoded_confirmed_txn.slot as i64;

            // process outer instructions
            println!("Debug log - Processing instructions...");
            let insn_futures = insns
                .iter()
                .filter(|insn| {
                    account_keys[insn.program_id_index as usize] == spl_token_interface::ID
                })
                .map( |insn| {
                    let account_keys = account_keys.clone();
                    let usdc_mint = usdc_mint.clone();
                    let txn_meta = encoded_confirmed_txn.transaction.meta.clone();
                    let signature = signature.clone();
                    let record_sender = record_sender.clone();

                    println!("Debug log - Passed the program id filter, and checking futher...");

                    async move {
                        let token_insn = TokenInstruction::unpack(&insn.data).unwrap();
                        println!("Debug log - Unpacked token instruction.");
                        match token_insn {
                            TokenInstruction::Transfer { amount } => {
                                println!("Debug log - Transfer Instruction");
                                // whale amount validation
                                if amount < 10_000_000_000 {
                                    println!("Debug log - amount less than 10_000 USD");
                                    return ();
                                } 
                                // the transfer now consists of whale amount
                                let source_acc = account_keys[insn.accounts[0] as usize];
                                let dest_acc = account_keys[insn.accounts[1] as usize];
                                if let Some(meta) = txn_meta.as_ref() {
                                    if let OptionSerializer::Some(pre_token_balances) =
                                        meta.pre_token_balances.as_ref()
                                    {
                                        if let Some(source_pre_token_bal) =
                                            pre_token_balances.iter().find(|token_acc| {
                                                token_acc.account_index == insn.accounts[0]
                                            })
                                        {
                                            println!("Debug log - Validating mint against the information in transaction meta.");
                                            if source_pre_token_bal.mint == usdc_mint {
                                                println!("Debug log - Waiting for insert_tuples lock");
                                                // verified that the transfer is usdc transfer
                                                println!("Debug log - ******************* Found a whale record *******************");
                                                record_sender.send((
                                                    signature.clone(),
                                                    slot,
                                                    source_acc.to_string(),
                                                    dest_acc.to_string(),
                                                    amount as i64,
                                                    usdc_mint.clone(),
                                                )).await.unwrap();
                                            }
                                        }
                                    }else {
                                        println!("Debug log - No pre token balance found!");
                                    }
                                } else {
                                    println!("Debug log - No transaction meta found");
                                };
                            }
                            TokenInstruction::TransferChecked { amount, decimals: _ } => {
                                println!("Debug log - TransferChecked Instruction");
                                // whale amount validation
                                if amount < 10_000_000_000 {
                                    return ();
                                } 
                                // the transfer now consists of whale amount
                                let source_acc = account_keys[insn.accounts[0] as usize];
                                let mint = account_keys[insn.accounts[1] as usize];
                                let dest_acc = account_keys[insn.accounts[2] as usize];
                                // check if the mint is usdc
                                if mint.to_string() == usdc_mint {
                                    println!("Debug log - *************** Found a whale record **************");
                                    record_sender.send((
                                        signature.clone(),
                                        slot,
                                        source_acc.to_string(),
                                        dest_acc.to_string(),
                                        amount as i64,
                                        usdc_mint.clone(),
                                    )).await.unwrap();
                                };
                            },
                            _ => {
                                println!("{}! - Not a transfer instruction!", token_instruction_name(&token_insn));
                            },
                        }
                    }
                });

            println!("Debug log - Processing insn_processing futures ..."); 
            futures::future::join_all(insn_futures).await;
            
            // check and process inner instructions as well
            println!("Debug log - Processing inner instruction if present any transaction meta...");
            if let Some(meta) = &encoded_confirmed_txn.transaction.meta {
                if let OptionSerializer::Some(inner_insns) = &meta.inner_instructions {
                    println!("Debug log - Processing inner instructions...");
                    for inna_insns in inner_insns {
                        for inna_insn in &inna_insns.instructions {
                            match inna_insn {
                                solana_client::rpc_response::UiInstruction::Compiled(ui_compiled_instruction) => {
                                    // check if the program is spl token program
                                    if account_keys[ui_compiled_instruction.program_id_index as usize] == spl_token_interface::ID {
                                        println!("Debug log - Found token program in inner instruction");
                                        //decode the instruction data
                                        let raw_data = bs58::decode(&ui_compiled_instruction.data).into_vec()?;
                                        let sanitized_raw_data = sanitize_token_data(&raw_data.as_slice());
                                        match TokenInstruction::unpack(&sanitized_raw_data) {
                                            std::result::Result::Ok(token_insn) => {
                                                println!("Debug log - unpacked inner instruction into TokenInstruction");
                                                match token_insn {
                                                    TokenInstruction::Transfer { amount }=>{
                                                        println!("Debug log - Found transfer as inner instruction.");
                                                        if amount < 10_000_000_000 {
                                                            println!("Debug log - The amount is less than the whale threshold");
                                                            continue;
                                                        }
                                                        // the transfer now consist of whale amount
                                                        let source_acc = account_keys[ui_compiled_instruction.accounts[0] as usize];
                                                        let dest_acc = account_keys[ui_compiled_instruction.accounts[1] as usize];
                                                        if let OptionSerializer::Some(pre_token_balances) = &meta.pre_token_balances {
                                                            if let Some(source_pre_token_bal) = pre_token_balances.iter().find(|p|p.account_index == ui_compiled_instruction.accounts[0]) {
                                                                if source_pre_token_bal.mint != usdc_mint {
                                                                    println!("Debug log - Source acc doesn't have usdc mint");
                                                                    continue;
                                                                }
                                                            }else{
                                                                println!("Debug log - Failed to find the source account in pre token balances");
                                                            }
                                                        };
                                                        // usdc mint transfer
                                                        println!("Debug log - ************* Adding a whale record ************ ");
                                                        record_sender.send((
                                                            signature.clone(),
                                                            slot,
                                                            source_acc.to_string(),
                                                            dest_acc.to_string(),
                                                            amount as i64,
                                                            usdc_mint.clone(),
                                                        )).await?;
                                                    }
                                                    TokenInstruction::TransferChecked { amount, decimals: _ }=>{
                                                        println!("Debug log - Found TransferChecked as inner instruction");
                                                        // whale amount validation
                                                        if amount < 10_000_000_000 {
                                                            println!("Debug log - The amount isn't enough to be whale");
                                                            continue;
                                                        } 
                                                        // the transfer now consists of whale amount
                                                        let source_acc = account_keys[ui_compiled_instruction.accounts[0] as usize];
                                                        let mint = account_keys[ui_compiled_instruction.accounts[1] as usize];
                                                        let dest_acc = account_keys[ui_compiled_instruction.accounts[2] as usize];
                                                        // check if the mint is usdc
                                                        println!("Debug log - Validating mint against usdc mint.");
                                                        if mint.to_string() == usdc_mint {
                                                            println!("Debug log - *************** Adding a whale record *******************");
                                                            record_sender.send((
                                                                signature.clone(),
                                                                slot,
                                                                source_acc.to_string(),
                                                                dest_acc.to_string(),
                                                                amount as i64,
                                                                usdc_mint.clone(),
                                                            )).await?;
                                                        };
                                                    },
                                                    _=>{
                                                        println!("{} - Not a transfer instruction!", token_instruction_name(&token_insn));
                                                    }
                                                };
                                            },
                                            Err(e)=>{
                                                println!("Failed to unpack raw data into TokenInstruction:\n{}", e.to_string());
                                            }
                                        };
                                    };
                                },
                                solana_client::rpc_response::UiInstruction::Parsed(ui_parsed_instruction) => {
                                    println!("Debug log - Found Parse Instruction in Inner instruction");
                                    match ui_parsed_instruction {
                                        solana_client::rpc_response::UiParsedInstruction::Parsed(parsed_instruction) => {
                                            println!("Debug log - Checking program id in Parsed instruction...");
                                            if parsed_instruction.program_id == spl_token_interface::ID.to_string() {
                                                if let (Some(instruction_type), Some(info)) = (parsed_instruction.parsed.get("type").and_then(|t|t.as_str()), parsed_instruction.parsed.get("info")){
                                                    match instruction_type {
                                                        "transfer" => {
                                                            println!("Debug log - Found transfer instruction in parsed-inner instruction");
                                                            // Standard transfer instruction
                                                            let source_acc = info.get("source").and_then(|s| s.as_str()).unwrap_or("");
                                                            let dest_acc = info.get("destination").and_then(|d|d.as_str()).unwrap_or("");
                                                            let amount = info.get("amount").and_then(|a|a.as_str()).unwrap_or("");
                                                            let amount_i64 = i64::from_str(amount).unwrap_or(0);

                                                            // validate the mint of source account if non empty, and check if its a whale transfer
                                                            if !source_acc.is_empty() && amount_i64 > 10_000_000_000 {
                                                                println!("Debug log - Validating mint");
                                                                if let OptionSerializer::Some(pre_token_balances) = &meta.pre_token_balances {
                                                                    if let Some(source_pre_token_bal) = pre_token_balances.iter().find(|p| account_keys[p.account_index as usize].to_string() == source_acc.to_string()){
                                                                        if source_pre_token_bal.mint != usdc_mint {
                                                                            println!("Debug log - Found the source token mint is not usdc mint");
                                                                            continue;
                                                                        }
                                                                    }else{
                                                                        println!("Debug log - Unable to find the source pre token balance");
                                                                    }
                                                                }
                                                                println!("Debug log - **************** Adding a whale record ************************");
                                                                record_sender.send((
                                                                    signature.clone(),
                                                                    slot,
                                                                    source_acc.to_string(),
                                                                    dest_acc.to_string(),
                                                                    amount_i64,
                                                                    usdc_mint.clone(),
                                                                )).await?;
                                                            }else {
                                                                println!("Debug log - The amount is not enough to be considered a whale transfer");
                                                            }
                                                        }
                                                        "TransferChecked" => {
                                                            println!("Debug log - Found TransferChecked instruction in parsed-inner instruction");
                                                            let source_acc = info.get("source").and_then(|s| s.as_str()).unwrap_or("");
                                                            let dest_acc = info.get("destination").and_then(|d|d.as_str()).unwrap_or("");
                                                            let amount = info.get("amount").and_then(|a|a.as_str()).unwrap_or("");
                                                            let mint = info.get("mint").and_then(|m| m.as_str()).unwrap_or("");
                                                            let amount_i64 = i64::from_str(amount).unwrap_or(0);

                                                            // validate the mint of source account if non empty, and check if its a whale
                                                            println!("Debug log - Validation mint and amount for whale usdc transfer");
                                                            if !mint.is_empty() && amount_i64 > 10_000_000_000 && mint.to_string() == usdc_mint {
                                                                println!("Debug log - ***************** Adding a whale transfer *********************");
                                                                record_sender.send((
                                                                    signature.clone(),
                                                                    slot,
                                                                    source_acc.to_string(),
                                                                    dest_acc.to_string(),
                                                                    amount_i64,
                                                                    usdc_mint.clone(),
                                                                )).await?;
                                                            }
                                                        }
                                                        _ => {
                                                            println!("{} - Not a transfer instruction in inner instruction!", instruction_type);
                                                        }
                                                    }
                                                }
                                            }
                                        },
                                        solana_client::rpc_response::UiParsedInstruction::PartiallyDecoded(ui_partially_decoded_instruction) => {
                                            if ui_partially_decoded_instruction.program_id == spl_token_interface::ID.to_string() {
                                                println!("Unexpected Instruction format found!");
                                            }
                                        },
                                    }
                                },
                            }
                        }
                    }
                }
            };
            
        };

        drop(permit);

        Ok(())
}

/// Collects records to do batch push into db
async fn db_pusher(mut receiver: tokio::sync::mpsc::Receiver<TxnRecord>, pg_pool: PgPool)->Result<()>{
    let mut tuples = vec![];

    while let Some(tuple) = receiver.recv().await {
        tuples.push(tuple); //collect into a temporary list

        // condition for triggering batch insertion
        if tuples.len() > 19 {
            // build query to do batch insertion
            let mut qb: sqlx::QueryBuilder<Postgres> = sqlx::QueryBuilder::new(
                "INSERT INTO whale_txns
                ( signature, slot, source_token_acc, dest_token_acc, amount, mint ) ",
            );

            // let tuples_copy = tuples.clone();
            //tuples.clear();
            let tuples = std::mem::take(&mut tuples);
            qb.push_values(tuples, |mut b, (sig, slot, source, dest, amnt, mint) | {
                b.push_bind(sig).push_bind(slot).push_bind(source).push_bind(dest).push_bind(amnt).push_bind(mint);
            });

            let pg_result = qb.build().execute(&pg_pool).await?;
            println!("*******************************************************************************");
            println!("********************            inserted {} row            ********************", pg_result.rows_affected());
            println!("*******************************************************************************");
        } else {
            println!("Batch size not reached.");
        }
    }

    Ok(())
}

/// Returns the name of the Instruction from the format such as
/// Transfer {
///     amount: 000001
/// }
fn token_instruction_name(token_insn: &TokenInstruction)-> String {
    format!("{:?}", token_insn).split_whitespace().next().unwrap().trim_end_matches('{').to_string()
}

/// Strips trailing padding from token transfer instructions so they can be successfully
/// unpacked by the strict `spl_token` crate.
fn sanitize_token_data(raw_data: &[u8])->&[u8]{
    if raw_data.is_empty() {
        return raw_data;
    }

    match raw_data[0] {
        // Tag 3 = Transfer. Requires exactly 9 bytes (1 tag + 8 amount).
        3 if raw_data.len() >= 9 => &raw_data[..9],

        // Tag 12 = TransferChecked. Requires exactly 10 bytes (1 tag + 8 amount + 1 decimals)
        12 if raw_data.len() >= 10 => &raw_data[..10],

        // If it's not a transfer, or if the array is dangerously short,
        // return it as-is and let the offical unpacker deal with it
        _ => raw_data
    }
}
