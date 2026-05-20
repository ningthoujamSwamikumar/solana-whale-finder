use std::str::FromStr;

use anyhow::{Ok, Result};
use futures::StreamExt;
use solana_client::{
    nonblocking::{
        pubsub_client::{PubsubClient, PubsubClientResult},
        rpc_client::RpcClient,
    },
    rpc_config::{CommitmentConfig, RpcTransactionConfig, RpcTransactionLogsConfig},
    rpc_response::transaction::{Signature, VersionedMessage},
};
use spl_token_interface::instruction::TokenInstruction;
use sqlx::{PgPool, Pool, Postgres};

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

    let pg_result = sqlx::query(
        "CREATE TABLE IF NOT EXISTS whale_txns (
            signature TEXT PRIMARY_KEY, 
            slot BIGINT NOT NULL, 
            source_owner TEXT NOT NULL, 
            destination_owner TEXT NOT NULL, 
            amount BIGINT NOT NULL, 
            mint TEXT NOT NULL
        );",
    )
    .execute(&pg_pool)
    .await?;

    // create rpc client
    let rpc_url = std::env::var("RPC_URL").expect("Required Rpc endpoint Url!");
    let rpc_client = RpcClient::new(rpc_url);

    //create websocket connection
    let ws_rpc_url = std::env::var("WEBSOCKET_RPC_URL").expect("Required 'WEBSOCKET_RPC_URL'!");
    let ws_client = PubsubClient::new(ws_rpc_url).await?;
    let (mut log_stream, _log_unsubscribe) = ws_client
        .logs_subscribe(
            solana_client::rpc_config::RpcTransactionLogsFilter::Mentions(vec![
                //spl_token_interface::ID.to_string(),
                "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v".to_string(), // usdc mint
            ]),
            RpcTransactionLogsConfig { commitment: None },
        )
        .await?;

    while let Some(log_response) = log_stream.next().await {
        if log_response.value.err.is_some() {
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
            continue;
        }

        // process transactions for qualified transactions

        // fetch transaction from rpc node
        let txn_signature = Signature::from_str(log_response.value.signature.as_str())?;
        let encoded_txn = rpc_client
            .get_transaction_with_config(
                &txn_signature,
                RpcTransactionConfig {
                    encoding: Some(solana_client::rpc_config::UiTransactionEncoding::Base58),
                    ..Default::default()
                },
            )
            .await?;
        // decode and process each transaction
        if let Some(txn) = encoded_txn.transaction.transaction.decode() {
            let (account_keys, insns) = match txn.message {
                VersionedMessage::Legacy(msg) => (msg.account_keys, msg.instructions),
                VersionedMessage::V0(msg) => (msg.account_keys, msg.instructions),
            };
            insns
                .iter()
                .filter(|insn| {
                    account_keys[insn.program_id_index as usize] == spl_token_interface::ID
                })
                .for_each(|insn| {
                    let token_insn = TokenInstruction::unpack(&insn.data).unwrap();
                    let source_acc = account_keys[insn.accounts[0] as usize];
                    let dest_acc = account_keys[insn.accounts[1] as usize];
                    let signer_acc = account_keys[insn.accounts[2] as usize];
                    match token_insn {
                        TokenInstruction::Transfer { amount } => {
                            //sqlx::query("INSERT") 
                            todo!()
                        },
                        TokenInstruction::TransferChecked { amount, decimals } => todo!(),
                        _ => todo!(),
                    };
                });
        };
    }

    Ok(())
}
