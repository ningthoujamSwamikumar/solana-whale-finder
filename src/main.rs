use std::{str::FromStr, sync::Arc, time::Duration};

use anyhow::{Context, Ok, Result};
use futures::StreamExt;
use solana_client::{
    nonblocking::{pubsub_client::PubsubClient, rpc_client::RpcClient},
    rpc_config::{RpcTransactionConfig, RpcTransactionLogsConfig},
    rpc_request::Address,
    rpc_response::{
        OptionSerializer, RpcLogsResponse,
        transaction::{Signature, VersionedMessage},
    },
};
use spl_token_interface::instruction::TokenInstruction;
use sqlx::{PgPool, Postgres};
use tokio::time::timeout;

pub const USDC_MINT: &str = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v";
pub const USDC_MINT_ADDRESS: Address = Address::from_str_const(USDC_MINT);
pub const SPL_TOKEN: &str = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA";

struct TxnRecord {
    signature: Signature,
    slot: i64,
    source_token_acc: Address,
    dest_token_acc: Address,
    amount: i64,
    mint: Address,
}

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
    // below fields are added after the initial table structure
    // signature_bytes, source_token_bytes, dest_token_bytes, mint_bytes
    // NOT NULL has been dropped from column signature, source_token_acc, dest_token_acc, mint
    // later - drop the original TEXT versions of the respective columns
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS whale_txns (
            id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            signature TEXT, 
            signature_bytes BYTEA,
            slot BIGINT NOT NULL,
            source_token_acc TEXT NOT NULL,
            source_token_bytes BYTEA,
            dest_token_acc TEXT NOT NULL,
            dest_token_bytes BYTEA,
            amount BIGINT NOT NULL, 
            mint TEXT NOT NULL,
            mint_bytes BYTEA,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        );",
    )
    .execute(&pg_pool)
    .await?;
    println!("Debug log - Table created");

    // create rpc client
    let rpc_url = std::env::var("RPC_URL").expect("Required Rpc endpoint Url!");
    let arc_rpc_client = Arc::new(RpcClient::new(rpc_url));

    // record collection channel for batch insertion
    let (record_tx, record_rx) = tokio::sync::mpsc::channel::<TxnRecord>(50);
    // batch insertion worker
    println!("Debug log - spawning Db Pusher for batch insertions");
    let db_pusher_handle = tokio::spawn(db_pusher(record_rx, pg_pool));

    // spawn static task pool
    let (orchestrator_log_tx, orchestrator_log_rx) =
        tokio::sync::mpsc::channel::<RpcLogsResponse>(100);
    println!("Debug log - spawning Worker Orchestrator.");
    let orchestrator_handle = tokio::spawn(run_worker_orchestrator(
        orchestrator_log_rx,
        arc_rpc_client,
        record_tx,
    ));

    //create websocket connection
    let ws_rpc_url = std::env::var("WEBSOCKET_RPC_URL").expect("Required 'WEBSOCKET_RPC_URL'!");
    let ws_client = PubsubClient::new(ws_rpc_url).await?;
    let (mut log_stream, _log_unsubscribe) = ws_client
        .logs_subscribe(
            solana_client::rpc_config::RpcTransactionLogsFilter::Mentions(vec![
                USDC_MINT.to_string(),
            ]),
            RpcTransactionLogsConfig { commitment: None },
        )
        .await?;
    println!("Debug log - Websocket connected");

    println!("Debug log - Streaming logs from websocket");
    tokio::select! {
        _ = async {
            // Log/Data Ingestion loop
            while let Some(log_response) = log_stream.next().await {
                println!("Debug log - Received Response");

                // deligate the msg to process to filter cum dispatcher
                if let Err(e) = orchestrator_log_tx.send(log_response.value).await {
                    eprintln!("Failed to send log to workers. Error: {:?}", e);
                    // This allows the loop to skip the current packet block and continue even when an error due
                    // to channel full, from slow worker processing happens
                    continue;
                }
            }
        } => {},
        _ = tokio::signal::ctrl_c() => {
            println!("Shutdown signal intercepted. Draining pipeline...");
        }
    }

    // Explicit drop to notify workers that no new logs are coming.
    // This will only allow the rx to recv all the already buffered messages
    drop(orchestrator_log_tx);
    if let Err(e) = orchestrator_handle.await? {
        eprintln!("While waiting on orchestrator_handle, got Error: {:?}", e);
    }

    // Wait for the db pusher taskt to complete
    if let Err(e) = db_pusher_handle.await {
        println!("Db pusher has stop. {:?}", e);
    }

    println!("Pipeline gracefully shutdown.");

    Ok(())
}

async fn run_worker_orchestrator(
    mut orchestrator_log_reciever: tokio::sync::mpsc::Receiver<RpcLogsResponse>,
    arc_rpc_client: Arc<solana_client::nonblocking::rpc_client::RpcClient>,
    record_tx: tokio::sync::mpsc::Sender<TxnRecord>,
) -> Result<()> {
    let worker_pool_size = std::env::var("WORKER_POOL_SIZE").map_or(10, |s| {
        i32::from_str(&s).expect("Failed to convert POOL SIZE env var!")
    });
    let (workers_log_tx, workers_log_rx) = async_channel::bounded::<RpcLogsResponse>(1000);

    println!("Debug log - initializing worker pool");
    let mut worker_handles = Vec::with_capacity(worker_pool_size as usize);
    for _ in 0..worker_pool_size {
        let workers_log_rx = workers_log_rx.clone();
        let rpc_client = arc_rpc_client.clone();
        let record_tx = record_tx.clone();
        let handle = tokio::spawn(worker(workers_log_rx, record_tx, rpc_client));
        println!("Debug log - worker id: {} spawned", handle.id());
        worker_handles.push(handle);
    }

    // Prevents the thread's own variable from staying alive forever
    drop(workers_log_rx);

    // pass down logs to workers
    while let Some(log_response) = orchestrator_log_reciever.recv().await {
        if let Err(e) = workers_log_tx.send(log_response).await {
            eprintln!("Failed to send log to workers channel. Error: {:?}", e);
        }
    }

    println!("Debug log - Orchestrator log channel has been closed!");
    // notify receiver for no logs are going to send in, more than already buffered ones
    println!("Debug log - Dropping workers log trasmitter");
    drop(workers_log_tx);

    println!("Debug log - waiting for workers to complete the in hand works");
    for worker in worker_handles {
        match worker.await {
            std::result::Result::Ok(join_result) => {
                if let Err(e) = join_result {
                    eprintln!("Task return error. Error: {:?}", e);
                }
            }
            Err(e) => {
                eprintln!("Failed to join worker task! Error: {:?}", e);
            }
        }
    }
    println!("Debug log - all workers are terminated");

    // A component writing to a data lier must outlive the components generating the work
    // So, we are dropping this after all the workers are confirmed terminated,
    // so to not close the channel totally before the whale data are written
    drop(record_tx);

    Ok(())
}

// Don't use kind of function, because it calls an async function, and
// when the data receiving loop exits because the trasmitter has been closed,
// the async part might not be ready, and it hangs in the air.

/// Filter logs for successful transactions and pass down to workers
// async fn filter_logs_and_dispatch(
//     worker_log_receiver: async_channel::Receiver<RpcLogsResponse>,
//     record_tx: tokio::sync::mpsc::Sender<TxnRecord>,
//     rpc_client: Arc<solana_client::nonblocking::rpc_client::RpcClient>,
// ) -> Result<()> {
//     println!("Debug log - Welcome to Dispatcher");
//     while let std::result::Result::Ok(log_response) = worker_log_receiver.recv().await {
//         if log_response.err.is_some() {
//             println!("Debug log - Transaction has error");
//             // failed transactions
//             continue;
//         }
//         // successful transactions
//         let rpc_client = rpc_client.clone();
//         let record_sender = record_tx.clone();
//         if let Err(e) = worker(log_response, record_sender, rpc_client).await {
//             eprintln!("Failed to process a log. Error: {:?}", e);
//             continue;
//         }
//     }
//     Ok(())
// }

/// Worker task/function to process the logs, find whale txns
async fn worker(
    log_receiver: async_channel::Receiver<RpcLogsResponse>,
    record_sender: tokio::sync::mpsc::Sender<TxnRecord>,
    rpc_client: Arc<solana_client::nonblocking::rpc_client::RpcClient>,
) -> Result<()> {
    while let std::result::Result::Ok(log_response) = log_receiver.recv().await {
        if log_response.err.is_some() {
            println!("Debug log - Transaction has error");
            // failed transactions
            continue;
        }

        // filter for usdc transfers
        // since legacy token doesn't log the name of instruction being invoked,
        // we'll just qualify every transaction thats coming from the websocket
        // because transaction already mentions USDC mint and
        // there is a high chance it is a transfer, we'll do the final check later
        if !log_response.logs.iter().any(|lg| lg.contains(SPL_TOKEN)) {
            println!("Debug log - Transaction logs doesn't contain token program");
            continue;
        }

        // process transactions for qualified transactions
        let res: Result<()> = {
            let mut result: Result<()> = Ok(());
            // fetch transaction from rpc node
            let txn_signature = Signature::from_str(log_response.signature.as_str())?;
            // RPC guardrail with manual retries and exponential backoff
            let mut attempts = 0;
            let max_attempts = 5;
            let mut delay = Duration::from_millis(150);
            let mut encoded_confirmed_txn = None;

            while attempts < max_attempts {
                match rpc_client
                    .get_transaction_with_config(
                        &txn_signature,
                        RpcTransactionConfig {
                            encoding: Some(
                                solana_client::rpc_config::UiTransactionEncoding::Base58,
                            ),
                            max_supported_transaction_version: Some(0),
                            ..Default::default()
                        },
                    )
                    .await
                {
                    std::result::Result::Ok(txn) => {
                        encoded_confirmed_txn = Some(txn);
                        break;
                    }
                    Err(e) => {
                        attempts += 1;
                        eprintln!(
                            "RPC Network warning: Attempt {}/{} failed for signature {}. Error: {:?}",
                            attempts, max_attempts, txn_signature, e
                        );
                        if attempts >= max_attempts {
                            result = Err(anyhow::anyhow!(
                                "RPC execution max retries exhausted for txn: {}",
                                txn_signature
                            ));
                        }
                        delay *= 2; // exponential backoff
                    }
                }
            }
            println!("Debug log - Fetched transaction from rpc");

            let encoded_confirmed_txn = encoded_confirmed_txn
                .context("Failed to safely secure transaction data payload.")?;
            let slot = encoded_confirmed_txn.slot as i64;

            // decode and process each transaction
            if let Some(txn) = encoded_confirmed_txn.transaction.transaction.decode() {
                let (account_keys, insns) = match txn.message {
                    VersionedMessage::Legacy(msg) => (msg.account_keys, msg.instructions),
                    VersionedMessage::V0(msg) => {
                        let mut account_keys = msg.account_keys;
                        if let Some(txn_meta) = &encoded_confirmed_txn.transaction.meta {
                            if let OptionSerializer::Some(loaded_addresses) =
                                &txn_meta.loaded_addresses
                            {
                                println!(
                                    "Debug log - Extending account keys with loaded addresses"
                                );
                                account_keys.reserve(
                                    loaded_addresses.writable.len()
                                        + loaded_addresses.readonly.len(),
                                ); // prevents from frequent reallocation
                                for addr in loaded_addresses
                                    .writable
                                    .iter()
                                    .chain(loaded_addresses.readonly.iter())
                                {
                                    account_keys.push(Address::from_str(addr.as_str())?);
                                }
                            }
                        }
                        (account_keys, msg.instructions)
                    }
                };

                let txn_meta = encoded_confirmed_txn.transaction.meta;
                let signature = txn.signatures[0];

                // process outer instructions
                println!("Debug log - Processing instructions...");
                for insn in insns {
                    if account_keys[insn.program_id_index as usize] != spl_token_interface::ID {
                        continue;
                    };

                    println!("Debug log - Passed the program id filter, and checking futher...");

                    let token_insn = TokenInstruction::unpack(&insn.data)?;
                    println!("Debug log - Unpacked token instruction.");
                    match token_insn {
                        TokenInstruction::Transfer { amount } => {
                            println!("Debug log - Transfer Instruction");
                            // whale amount validation
                            if amount < 10_000_000_000 {
                                println!("Debug log - amount less than 10_000 USD");
                                continue;
                            };
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
                                        println!(
                                            "Debug log - Validating mint against the information in transaction meta."
                                        );
                                        if &source_pre_token_bal.mint == USDC_MINT {
                                            println!("Debug log - Waiting for insert_tuples lock");
                                            // verified that the transfer is usdc transfer
                                            println!(
                                                "Debug log - ******************* Found a whale record *******************"
                                            );
                                            record_sender
                                                .send(TxnRecord {
                                                    signature,
                                                    slot,
                                                    source_token_acc: source_acc,
                                                    dest_token_acc: dest_acc,
                                                    amount: amount as i64,
                                                    mint: USDC_MINT_ADDRESS,
                                                })
                                                .await?;
                                        }
                                    }
                                } else {
                                    println!("Debug log - No pre token balance found!");
                                }
                            } else {
                                println!("Debug log - No transaction meta found");
                            };
                        }
                        TokenInstruction::TransferChecked {
                            amount,
                            decimals: _,
                        } => {
                            println!("Debug log - TransferChecked Instruction");
                            // whale amount validation
                            if amount < 10_000_000_000 {
                                continue;
                            }
                            // the transfer now consists of whale amount
                            let source_acc = account_keys[insn.accounts[0] as usize];
                            let mint = account_keys[insn.accounts[1] as usize];
                            let dest_acc = account_keys[insn.accounts[2] as usize];
                            // check if the mint is usdc
                            if mint == USDC_MINT_ADDRESS {
                                println!(
                                    "Debug log - *************** Found a whale record **************"
                                );
                                record_sender
                                    .send(TxnRecord {
                                        signature,
                                        slot,
                                        source_token_acc: source_acc,
                                        dest_token_acc: dest_acc,
                                        amount: amount as i64,
                                        mint: USDC_MINT_ADDRESS,
                                    })
                                    .await?;
                            };
                        }
                        _ => {
                            println!("Not a transfer instruction!");
                        }
                    };
                }

                // check and process inner instructions as well
                println!(
                    "Debug log - Processing inner instruction if present any transaction meta..."
                );
                if let Some(meta) = txn_meta.as_ref() {
                    if let OptionSerializer::Some(inner_insns) = &meta.inner_instructions {
                        println!("Debug log - Processing inner instructions...");
                        for inna_insns in inner_insns {
                            for inna_insn in &inna_insns.instructions {
                                match inna_insn {
                                    solana_client::rpc_response::UiInstruction::Compiled(
                                        ui_compiled_instruction,
                                    ) => {
                                        // check if the program is spl token program
                                        if account_keys
                                            [ui_compiled_instruction.program_id_index as usize]
                                            == spl_token_interface::ID
                                        {
                                            println!(
                                                "Debug log - Found token program in inner instruction"
                                            );
                                            //decode the instruction data
                                            let mut raw_sanitized_data = [0; 10];
                                            bs58::decode(&ui_compiled_instruction.data)
                                                .onto(&mut raw_sanitized_data)?;
                                            match TokenInstruction::unpack(&raw_sanitized_data) {
                                                std::result::Result::Ok(token_insn) => {
                                                    println!(
                                                        "Debug log - unpacked inner instruction into TokenInstruction"
                                                    );
                                                    match token_insn {
                                                        TokenInstruction::Transfer { amount } => {
                                                            println!(
                                                                "Debug log - Found transfer as inner instruction."
                                                            );
                                                            if amount < 10_000_000_000 {
                                                                println!(
                                                                    "Debug log - The amount is less than the whale threshold"
                                                                );
                                                                continue;
                                                            }
                                                            // the transfer now consist of whale amount
                                                            let source_acc = account_keys
                                                                [ui_compiled_instruction.accounts[0]
                                                                    as usize];
                                                            let dest_acc = account_keys
                                                                [ui_compiled_instruction.accounts[1]
                                                                    as usize];
                                                            if let OptionSerializer::Some(
                                                                pre_token_balances,
                                                            ) = &meta.pre_token_balances
                                                            {
                                                                if let Some(source_pre_token_bal) =
                                                                    pre_token_balances.iter().find(
                                                                        |p| {
                                                                            p.account_index
                                                                    == ui_compiled_instruction
                                                                        .accounts[0]
                                                                        },
                                                                    )
                                                                {
                                                                    if &source_pre_token_bal.mint
                                                                        != USDC_MINT
                                                                    {
                                                                        println!(
                                                                            "Debug log - Source acc doesn't have usdc mint"
                                                                        );
                                                                        continue;
                                                                    }
                                                                } else {
                                                                    println!(
                                                                        "Debug log - Failed to find the source account in pre token balances"
                                                                    );
                                                                }
                                                            };
                                                            // usdc mint transfer
                                                            println!(
                                                                "Debug log - ************* Adding a whale record ************ "
                                                            );
                                                            record_sender
                                                                .send(TxnRecord {
                                                                    signature,
                                                                    slot,
                                                                    source_token_acc: source_acc,
                                                                    dest_token_acc: dest_acc,
                                                                    amount: amount as i64,
                                                                    mint: USDC_MINT_ADDRESS,
                                                                })
                                                                .await?;
                                                        }
                                                        TokenInstruction::TransferChecked {
                                                            amount,
                                                            decimals: _,
                                                        } => {
                                                            println!(
                                                                "Debug log - Found TransferChecked as inner instruction"
                                                            );
                                                            // whale amount validation
                                                            if amount < 10_000_000_000 {
                                                                println!(
                                                                    "Debug log - The amount isn't enough to be whale"
                                                                );
                                                                continue;
                                                            }
                                                            // the transfer now consists of whale amount
                                                            let source_acc = account_keys
                                                                [ui_compiled_instruction.accounts[0]
                                                                    as usize];
                                                            let mint = account_keys
                                                                [ui_compiled_instruction.accounts[1]
                                                                    as usize];
                                                            let dest_acc = account_keys
                                                                [ui_compiled_instruction.accounts[2]
                                                                    as usize];
                                                            // check if the mint is usdc
                                                            println!(
                                                                "Debug log - Validating mint against usdc mint."
                                                            );
                                                            if mint == USDC_MINT_ADDRESS {
                                                                println!(
                                                                    "Debug log - *************** Adding a whale record *******************"
                                                                );
                                                                record_sender
                                                                    .send(TxnRecord {
                                                                        signature,
                                                                        slot,
                                                                        source_token_acc:
                                                                            source_acc,
                                                                        dest_token_acc: dest_acc,
                                                                        amount: amount as i64,
                                                                        mint: USDC_MINT_ADDRESS,
                                                                    })
                                                                    .await?;
                                                            };
                                                        }
                                                        _ => {
                                                            println!("Not a transfer instruction.");
                                                        }
                                                    };
                                                }
                                                Err(e) => {
                                                    println!(
                                                        "Failed to unpack raw data into TokenInstruction:\n{}",
                                                        e.to_string()
                                                    );
                                                }
                                            };
                                        };
                                    }
                                    _ => {
                                        println!(
                                            "Debug log - Unexpected !! Found Parse Instruction in Inner instruction."
                                        );
                                    }
                                }
                            }
                        }
                    }
                };
            }

            result
        };

        // handle each processing result gracefully
        if let Err(e) = res {
            eprintln!("Failed processing log in worker. Error: {:?}", e);
        }
    }

    println!("Debug log - Worker loop exited");

    // drop all the record tx, to notify the receiver that work is done
    drop(record_sender);

    Ok(())
}

/// Collects records to do batch push into db
/// pushes at a set time or when reach batch limit reached or at system interrupt
async fn db_pusher(
    mut receiver: tokio::sync::mpsc::Receiver<TxnRecord>,
    pg_pool: PgPool,
) -> Result<()> {
    let batch_size: usize = 100;
    let mut txn_records: Vec<TxnRecord> = Vec::with_capacity(batch_size);

    loop {
        // Timeout the waiting for records from the upstream tasks
        let result = timeout(Duration::new(2, 0), receiver.recv()).await;

        match result {
            // When receiver recieves a record before timeout
            std::result::Result::Ok(Some(record)) => {
                txn_records.push(record);
                if txn_records.len() > batch_size {
                    flush_batch(&mut txn_records, &pg_pool).await?;
                }
            }
            // When the channel is broken, maybe because of system interrupt
            std::result::Result::Ok(None) => {
                // flush the records present in the buffer
                if !txn_records.is_empty() {
                    flush_batch(&mut txn_records, &pg_pool).await?;
                }
                eprintln!("Debug log - TxnRecord channel is broken!");
                break;
            }
            // Timeout: no records came within the set time
            Err(_) => {
                // flush the waiting records
                if !txn_records.is_empty() {
                    flush_batch(&mut txn_records, &pg_pool).await?;
                }
                eprintln!("Debug log - Txn record waiting timeout!");
            }
        };
    }

    Ok(())
}

async fn flush_batch(txn_records: &mut Vec<TxnRecord>, pg_pool: &PgPool) -> Result<()> {
    let mut qb: sqlx::QueryBuilder<Postgres> = sqlx::QueryBuilder::new(
        "INSERT INTO whale_txns
        ( signature_bytes, slot, source_token_bytes, dest_token_bytes, amount, mint_bytes ) ",
    );

    let record_buffer = std::mem::take(txn_records);
    qb.push_values(
        record_buffer,
        |mut b,
         TxnRecord {
             signature,
             slot,
             source_token_acc,
             dest_token_acc,
             amount,
             mint,
         }| {
            b
                //  .push_bind(signature.to_string())
                .push_bind(<[u8; 64]>::from(signature))
                .push_bind(slot)
                //    .push_bind(source_token_acc.to_string())
                .push_bind(source_token_acc.to_bytes())
                //    .push_bind(dest_token_acc.to_string())
                .push_bind(dest_token_acc.to_bytes())
                .push_bind(amount)
                //    .push_bind(mint.to_string())
                .push_bind(mint.to_bytes());
        },
    );

    let qry = qb.build();
    let pg_result = qry.execute(pg_pool).await?;
    println!("*******************************************************************************");
    println!(
        "********************            inserted {} row            ********************",
        pg_result.rows_affected()
    );
    println!("*******************************************************************************");

    Ok(())
}
