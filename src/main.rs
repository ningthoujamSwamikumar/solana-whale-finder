use std::{str::FromStr, sync::{Arc, Mutex}};

use anyhow::{Ok, Result};
use futures::StreamExt;
use solana_client::{
    nonblocking::{
        pubsub_client::PubsubClient,
        rpc_client::RpcClient,
    }, rpc_config::{RpcTransactionConfig, RpcTransactionLogsConfig}, rpc_request::Address, rpc_response::{
        OptionSerializer,
        transaction::{Signature, VersionedMessage},
    }
};
use spl_token_interface::{instruction::TokenInstruction, state::GenericTokenAccount};
use sqlx::{PgPool, Postgres};

pub const USDC_MINT: &str = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v";

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

    let usdc_mint = USDC_MINT.to_string();

    //create websocket connection
    let ws_rpc_url = std::env::var("WEBSOCKET_RPC_URL").expect("Required 'WEBSOCKET_RPC_URL'!");
    let ws_client = PubsubClient::new(ws_rpc_url).await?;
    let (mut log_stream, _log_unsubscribe) = ws_client
        .logs_subscribe(
            solana_client::rpc_config::RpcTransactionLogsFilter::Mentions(vec![usdc_mint.clone()]),
            RpcTransactionLogsConfig { commitment: None },
        )
        .await?;
    println!("Debug log - Websocket connected");

    while let Some(log_response) = log_stream.next().await {
        println!("Debug log - Received Response");

        if log_response.value.err.is_some() {
            println!("Debug log - Transaction has error");
            // failed transactions
            continue;
        }
        // successful transactions

        // filter for usdc transfers
        // since legacy token doesn't log the name of instruction being invoked,
        // we'll just qualify every transaction thats coming from the websocket
        // because transaction already mentions USDC mint and
        // there is a high chance it is a transfer, we'll do the final check later
        if !log_response
            .value
            .logs
            .iter()
            .any(|lg| lg.contains(spl_token_interface::ID.to_string().as_str()))
        {
            println!("Debug log - Transaction logs doesn't contain token program");
            continue;
        }

        // process transactions for qualified transactions

        // fetch transaction from rpc node
        let txn_signature = Signature::from_str(log_response.value.signature.as_str())?;
        let rpc_client = arc_rpc_client.clone();
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

            // build query to do batch insertion
            let mut qb: sqlx::QueryBuilder<Postgres> = sqlx::QueryBuilder::new(
                "INSERT INTO whale_txns
                ( signature, slot, source_token_acc, dest_token_acc, amount, mint ) ",
            );

            let signature = log_response.value.signature;
            let slot = encoded_confirmed_txn.slot as i64;

            let insert_tuples = Arc::new(Mutex::new(vec![]));
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
                    let insert_tuples = insert_tuples.clone();
                    let signature = signature.clone();
                    let rpc_client = arc_rpc_client.clone();

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
                                                let mut tuples = insert_tuples.lock().unwrap();
                                                println!("Debug log - ******************* Adding a tuple into insert_tuples *******************");
                                                tuples.push((
                                                    signature.clone(),
                                                    slot,
                                                    source_acc.to_string(),
                                                    dest_acc.to_string(),
                                                    amount as i64,
                                                    usdc_mint.clone(),
                                                ));
                                            }
                                        }
                                    }
                                } else {
                                    println!("Debug log - Validating mint by fetching account data from rpc");
                                    if let Some(source_token_acc_data) =
                                        rpc_client.get_account_data(&source_acc).await.ok()
                                    {
                                        if let Some(source_token_acc_mint) =
                                            spl_token_interface::state::Account::unpack_account_mint(
                                                &source_token_acc_data.as_slice(),
                                            )
                                        {
                                            if source_token_acc_mint.to_string() == usdc_mint {
                                                println!("Debug log - Waiting for insert_tuples lock");
                                                let mut tuples = insert_tuples.lock().unwrap();
                                                println!("Debug log - *************** Adding a new tuple into insert_tuples *******************");
                                                tuples.push((
                                                    signature.clone(),
                                                    slot,
                                                    source_acc.to_string(),
                                                    dest_acc.to_string(),
                                                    amount as i64,
                                                    usdc_mint.clone(),
                                                ));
                                            };
                                        };
                                    } else {
                                        println!("Failed to fetch source token account data! Assuming {} as usdc token account. So ignoring the transfer!", source_acc.to_string());
                                    }
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
                                    println!("Debug log - *************** Adding a whale record **************");
                                    let mut tuples = insert_tuples.lock().unwrap();
                                    tuples.push((
                                        signature.clone(),
                                        slot,
                                        source_acc.to_string(),
                                        dest_acc.to_string(),
                                        amount as i64,
                                        usdc_mint.clone(),
                                    ));
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
            if let Some(meta) = encoded_confirmed_txn.transaction.meta {
                if let OptionSerializer::Some(inner_insns) = meta.inner_instructions {
                    println!("Debug log - Processing inner instructions...");
                    for inna_insns in inner_insns {
                        for inna_insn in inna_insns.instructions {
                            match inna_insn {
                                solana_client::rpc_response::UiInstruction::Compiled(ui_compiled_instruction) => {
                                    // check if the program is spl token program
                                    if account_keys[ui_compiled_instruction.program_id_index as usize] == spl_token_interface::ID {
                                        println!("Debug log - Found token program in inner instruction");
                                        //decode the instruction data
                                        let raw_data = bs58::decode(ui_compiled_instruction.data).into_vec()?;
                                        println!("raw_data length: {}", raw_data.len());
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
                                                        let source_acc_data = rpc_client.get_account_data(&source_acc).await?;
                                                        println!("Debug log - Unpacking account mint to validate the mint");
                                                        if let Some(mint) = spl_token_interface::state::Account::unpack_account_mint(&source_acc_data) {
                                                            if mint.to_string() == usdc_mint {
                                                                println!("Debug log - ************* Adding a whale record ************ ");
                                                                let mut tuples = insert_tuples.lock().unwrap();
                                                                tuples.push((
                                                                    signature.clone(),
                                                                    slot,
                                                                    source_acc.to_string(),
                                                                    dest_acc.to_string(),
                                                                    amount as i64,
                                                                    usdc_mint.clone(),
                                                                ));
                                                            }
                                                        }
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
                                                            let mut tuples = insert_tuples.lock().unwrap();
                                                            tuples.push((
                                                                signature.clone(),
                                                                slot,
                                                                source_acc.to_string(),
                                                                dest_acc.to_string(),
                                                                amount as i64,
                                                                usdc_mint.clone(),
                                                            ));
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
                                                                let source_acc_data = rpc_client.get_account_data(&Address::from_str(source_acc)?).await?;
                                                                println!("Debug log - Validating mint");
                                                                if let Some(source_acc_mint) = spl_token_interface::state::Account::unpack_account_mint(source_acc_data.as_slice()) {
                                                                    if source_acc_mint.to_string() == usdc_mint {
                                                                        println!("Debug log - **************** Adding a whale record ************************");
                                                                        let mut tuples = insert_tuples.lock().unwrap();
                                                                        tuples.push((
                                                                            signature.clone(),
                                                                            slot,
                                                                            source_acc.to_string(),
                                                                            dest_acc.to_string(),
                                                                            amount_i64,
                                                                            usdc_mint.clone(),
                                                                        ));
                                                                    }
                                                                }
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
                                                                let mut tuples = insert_tuples.lock().unwrap();
                                                                tuples.push((
                                                                    signature.clone(),
                                                                    slot,
                                                                    source_acc.to_string(),
                                                                    dest_acc.to_string(),
                                                                    amount_i64,
                                                                    usdc_mint.clone(),
                                                                ));
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

            // we will do this after inner instructions are handled
            let tuples =  insert_tuples.lock().unwrap();
            if tuples.len() > 0 {
                qb.push_values(tuples.iter(), |mut b, (sig, slot, source, dest, amnt, mint) | {
                    b.push_bind(sig).push_bind(slot).push_bind(source).push_bind(dest).push_bind(amnt).push_bind(mint);
                });

                let pg_result = qb.build().execute(&pg_pool).await?;
                println!("inserted {} rows", pg_result.rows_affected());
            } else {
                println!("There are no tuples found to insert into db!");
            }

        };
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
