use std::{str::FromStr, sync::Arc, time::Duration};

use anyhow::{Ok, Result};
use metrics::{counter, histogram};
use solana_client::{
    rpc_config::RpcTransactionConfig,
    rpc_request::Address,
    rpc_response::{
        OptionSerializer,
        transaction::{Signature, VersionedMessage},
    },
};
use spl_token_interface::instruction::TokenInstruction;
use tracing::{error, info, instrument, warn};

use crate::{
    InboundTxnSource, RedactExt, SPL_TOKEN, TxnRecord, USDC_MINT, USDC_MINT_ADDRESS,
    UnifiedTxnPayload,
};

// Automatically creates heirarchical logging spans tied exclusively to the unique worker instances
#[instrument(skip_all, fields(worker_id = _id))]
/// Worker task/function to process the logs, find whale txns
pub(crate) async fn worker(
    _id: i32,
    log_receiver: async_channel::Receiver<InboundTxnSource>,
    record_sender: tokio::sync::mpsc::Sender<TxnRecord>,
    rpc_client: Arc<solana_client::nonblocking::rpc_client::RpcClient>,
    rpc_url: Arc<String>,
) -> Result<()> {
    while let std::result::Result::Ok(inbound_txn) = log_receiver.recv().await {
        // convert inbound txn to unified txn
        let txn_payload: UnifiedTxnPayload = match inbound_txn {
            InboundTxnSource::RpcLogGate(log_response) => {
                if log_response.err.is_some() {
                    // failed transactions
                    continue;
                }

                // filter for usdc transfers
                // since legacy token doesn't log the name of instruction being invoked,
                // we'll just qualify every transaction thats coming from the websocket
                // because transaction already mentions USDC mint and
                // there is a high chance it is a transfer, we'll do the final check later
                if !log_response.logs.iter().any(|lg| lg.contains(SPL_TOKEN)) {
                    continue;
                }

                let txn_signature = match Signature::from_str(&log_response.signature) {
                    std::result::Result::Ok(sig) => sig,
                    Err(_) => continue,
                };

                // RPC guardrail with manual retries and exponential backoff
                let mut attempts = 0;
                let max_attempts = 5;
                let mut delay = Duration::from_millis(150);
                let mut encoded_confirmed_txn = None;

                let start = std::time::Instant::now();
                while attempts < max_attempts {
                    counter!("pipeline.rpc_lookups_total").increment(1);
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
                            let sanitized_e = e.redact_key(&*rpc_url);
                            counter!("pipeline.rpc_errors_total").increment(1);
                            warn!(
                                signature = %txn_signature,
                                attempt = attempts,
                                error=sanitized_e,
                                "Solana RPC transaction retrieval transient network failure warning."
                            );
                            tokio::time::sleep(delay).await;
                            delay *= 2; // exponential backoff
                        }
                    }
                }
                // Records latency breakdown of JSON-RPC requests across clustering endpoints
                histogram!("pipeline.rpc_lookup_latency_seconds")
                    .record(start.elapsed().as_secs_f64());

                let encoded_confirmed_txn = match encoded_confirmed_txn {
                    Some(txn) => txn,
                    None => {
                        error!(signature = %txn_signature, "Exhausted all available retry paths for target block payload. Dropping sequence.");
                        continue;
                    }
                };

                let Some(txn) = encoded_confirmed_txn.transaction.transaction.decode() else {
                    continue;
                };

                let Some(meta) = encoded_confirmed_txn.transaction.meta else {
                    continue;
                };

                UnifiedTxnPayload {
                    signature: txn_signature,
                    slot: encoded_confirmed_txn.slot,
                    message: txn.message,
                    pre_token_balances: meta.pre_token_balances,
                    inner_instructions: meta.inner_instructions,
                    loaded_addresses: meta.loaded_addresses,
                }
            }
            InboundTxnSource::DirechPushStream(unified_txn_payload) => unified_txn_payload,
        };

        let slot = txn_payload.slot as i64;

        // decode and process each transaction
        let (account_keys, insns) = match txn_payload.message {
            VersionedMessage::Legacy(msg) => (msg.account_keys, msg.instructions),
            VersionedMessage::V0(msg) => {
                let mut account_keys = msg.account_keys;
                if let OptionSerializer::Some(loaded_addresses) = &txn_payload.loaded_addresses {
                    account_keys
                        .reserve(loaded_addresses.writable.len() + loaded_addresses.readonly.len()); // prevents from frequent reallocation
                    for addr in loaded_addresses
                        .writable
                        .iter()
                        .chain(loaded_addresses.readonly.iter())
                    {
                        account_keys.push(Address::from_str(addr.as_str())?);
                    }
                }
                (account_keys, msg.instructions)
            }
        };

        let signature = txn_payload.signature;

        // process outer instructions
        for insn in insns {
            if account_keys[insn.program_id_index as usize] != spl_token_interface::ID {
                continue;
            };

            if let std::result::Result::Ok(token_insn) = TokenInstruction::unpack(&insn.data) {
                match token_insn {
                    TokenInstruction::Transfer { amount } => {
                        // whale amount validation
                        if amount < 10_000_000_000 {
                            continue;
                        };
                        // the transfer now consists of whale amount
                        let source_acc = account_keys[insn.accounts[0] as usize];
                        let dest_acc = account_keys[insn.accounts[1] as usize];
                        if let OptionSerializer::Some(pre_token_balances) =
                            txn_payload.pre_token_balances.as_ref()
                        {
                            if let Some(source_pre_token_bal) = pre_token_balances
                                .iter()
                                .find(|token_acc| token_acc.account_index == insn.accounts[0])
                            {
                                if &source_pre_token_bal.mint == USDC_MINT {
                                    info!(signature = %signature, amount = amount, "Whale record intercepted across outer SPL Transfer pipeline.");
                                    counter!("pipeline.whale_txns_tracked_total").increment(1);
                                    // verified that the transfer is usdc transfer
                                    let _ = record_sender
                                        .send(TxnRecord {
                                            signature,
                                            slot,
                                            source_token_acc: source_acc,
                                            dest_token_acc: dest_acc,
                                            amount: amount as i64,
                                            mint: USDC_MINT_ADDRESS,
                                        })
                                        .await;
                                }
                            }
                        }
                    }
                    TokenInstruction::TransferChecked {
                        amount,
                        decimals: _,
                    } => {
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
                            info!(signature = %signature, amount = amount, "Whale record intercepted across outer SPL TransferChecked pipeline.");
                            counter!("pipeline.whale_txns_tracked_total").increment(1);
                            let _ = record_sender
                                .send(TxnRecord {
                                    signature,
                                    slot,
                                    source_token_acc: source_acc,
                                    dest_token_acc: dest_acc,
                                    amount: amount as i64,
                                    mint: USDC_MINT_ADDRESS,
                                })
                                .await;
                        };
                    }
                    _ => {}
                };
            }
        }

        // check and process inner instructions as well
        if let OptionSerializer::Some(inner_insns) = &txn_payload.inner_instructions {
            for inner_set in inner_insns {
                for inner_insn in &inner_set.instructions {
                    if let solana_client::rpc_response::UiInstruction::Compiled(
                        ui_compiled_instruction,
                    ) = inner_insn
                    {
                        // check if the program is spl token program
                        if account_keys[ui_compiled_instruction.program_id_index as usize]
                            == spl_token_interface::ID
                        {
                            //decode the instruction data
                            let mut raw_sanitized_data = [0u8; 128];
                            if let std::result::Result::Ok(decoded_bytes) =
                                bs58::decode(&ui_compiled_instruction.data)
                                    .onto(&mut raw_sanitized_data)
                            {
                                if let std::result::Result::Ok(token_insn) =
                                    TokenInstruction::unpack(&raw_sanitized_data[..decoded_bytes])
                                {
                                    match token_insn {
                                        TokenInstruction::Transfer { amount } => {
                                            if amount < 10_000_000_000 {
                                                continue;
                                            }
                                            // the transfer now consist of whale amount
                                            let source_acc = account_keys
                                                [ui_compiled_instruction.accounts[0] as usize];
                                            let dest_acc = account_keys
                                                [ui_compiled_instruction.accounts[1] as usize];
                                            if let OptionSerializer::Some(pre_token_balances) =
                                                &txn_payload.pre_token_balances
                                            {
                                                if let Some(source_pre_token_bal) =
                                                    pre_token_balances.iter().find(|p| {
                                                        p.account_index
                                                            == ui_compiled_instruction.accounts[0]
                                                    })
                                                {
                                                    if &source_pre_token_bal.mint == USDC_MINT {
                                                        info!(signature = %signature, amount = amount, "Whale record intercepted across nested CPI Inner Transfer pipeline.");
                                                        counter!(
                                                            "pipeline.whale_txns_tracked_total"
                                                        )
                                                        .increment(1);
                                                        let _ = record_sender
                                                            .send(TxnRecord {
                                                                signature,
                                                                slot,
                                                                source_token_acc: source_acc,
                                                                dest_token_acc: dest_acc,
                                                                amount: amount as i64,
                                                                mint: USDC_MINT_ADDRESS,
                                                            })
                                                            .await;
                                                    }
                                                }
                                            };
                                        }
                                        TokenInstruction::TransferChecked {
                                            amount,
                                            decimals: _,
                                        } => {
                                            // whale amount validation
                                            if amount < 10_000_000_000 {
                                                continue;
                                            }
                                            // the transfer now consists of whale amount
                                            let source_acc = account_keys
                                                [ui_compiled_instruction.accounts[0] as usize];
                                            let mint = account_keys
                                                [ui_compiled_instruction.accounts[1] as usize];
                                            let dest_acc = account_keys
                                                [ui_compiled_instruction.accounts[2] as usize];
                                            // check if the mint is usdc
                                            if mint == USDC_MINT_ADDRESS {
                                                info!(signature=%signature, amount =amount, "Whale record intercepted across nested CPI Inner TransferChecked pipeline.");
                                                counter!("pipeline.whale_txns_tracked_total")
                                                    .increment(1);
                                                let _ = record_sender
                                                    .send(TxnRecord {
                                                        signature,
                                                        slot,
                                                        source_token_acc: source_acc,
                                                        dest_token_acc: dest_acc,
                                                        amount: amount as i64,
                                                        mint: USDC_MINT_ADDRESS,
                                                    })
                                                    .await;
                                            };
                                        }
                                        _ => {}
                                    };
                                }
                            };
                        }
                    };
                }
            }
        }
    }

    // drop all the record tx, to notify the receiver that work is done
    drop(record_sender);

    Ok(())
}
