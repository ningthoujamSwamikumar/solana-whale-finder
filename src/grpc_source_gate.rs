use std::{collections::HashMap, str::FromStr};

use anyhow::{Ok, Result};
use futures::{SinkExt, StreamExt};
use solana_client::rpc_config::CommitmentLevel;
use solana_sdk::{message::VersionedMessage, signature::Signature};
use tokio_util::sync::CancellationToken;
use tracing::instrument;

use crate::{InboundTxnSource, USDC_MINT, UnifiedTxnPayload};

#[instrument(skip_all)]
pub(crate) async fn run_grpc_subscription(
    orchestrator_msg_tx: tokio::sync::mpsc::Sender<InboundTxnSource>,
    cancel_token: CancellationToken,
) -> Result<()> {
    let mut grpc_url = None;
    {
        // Preventing args to stay unnecessarily alive
        let args = std::env::args().collect::<Vec<_>>();
        for i in 0..args.len() {
            if args[i] == "--grpc-url" || args[i] == "-g" {
                grpc_url = Some(args[i + 1].clone());
                break;
            }
        }
    }
    // fallback to env variables
    let grpc_url = grpc_url
        .or_else(|| std::env::var("GRPC_URL").ok())
        .expect("No Grpc Url provided!");

    let mut grpc_client = yellowstone_grpc_client::GeyserGrpcBuilder::from_shared(grpc_url)?
        .connect()
        .await?;

    let (mut sink, mut stream) = grpc_client.subscribe().await?;

    // transaction filter to apply on the grpc stream
    let mut tx_filters = HashMap::new();
    tx_filters.insert(
        "all".to_string(),
        yellowstone_grpc_proto::geyser::SubscribeRequestFilterTransactions {
            vote: Some(false),
            failed: Some(false),
            signature: None,
            account_include: vec![USDC_MINT.to_string()],
            account_exclude: vec![],
            account_required: vec![],
        },
    );

    // make request
    sink.send(yellowstone_grpc_proto::geyser::SubscribeRequest {
        transactions: tx_filters,
        commitment: Some(CommitmentLevel::Confirmed as i32),
        ..Default::default()
    })
    .await?;

    while let Some(msg) = stream.next().await {
        let std::result::Result::Ok(msg_update) = msg else {
            continue;
        };
        let Some(update_oneof) = msg_update.update_oneof else {
            continue;
        };
        match update_oneof {
            yellowstone_grpc_proto::geyser::subscribe_update::UpdateOneof::Transaction(
                subscribe_update_txn,
            ) => {
                // let payload = UnifiedTxnPayload {
                //     signature: todo!(),
                //     slot: todo!(),
                //     message: todo!(),
                //     pre_token_balances: todo!(),
                //     inner_instructions: todo!(),
                //     loaded_addresses: todo!(),
                // };

                // let slot = subscribe_update_txn.slot;
                // let Some(txn_info) = subscribe_update_txn.transaction else {
                //     continue;
                // };
                // let signature = Signature::try_from(txn_info.signature.as_slice()).unwrap_or_else(op);
                // let Some(txn) = txn_info.transaction else {
                //     continue;
                // };
                // let Some(message) = txn.message else {
                //     continue;
                // };
                // let versioned_message: VersionedMessage = VersionedMessage::V0(())

                todo!()
            }
            _ => continue,
        }
    }

    Ok(())
}
