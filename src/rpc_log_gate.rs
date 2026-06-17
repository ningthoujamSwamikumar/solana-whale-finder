use anyhow::{Ok, Result};
use futures::StreamExt;
use metrics::{counter, gauge};
use solana_client::{
    nonblocking::pubsub_client::PubsubClient, rpc_config::RpcTransactionLogsConfig,
};
use tokio_util::sync::CancellationToken;
use tracing::{error, info, instrument};

use crate::{InboundTxnSource, USDC_MINT};

#[instrument(skip_all)]
pub(crate) async fn run_rpc_websocket_subscription(
    orchestrator_msg_tx: tokio::sync::mpsc::Sender<InboundTxnSource>,
    cancel_token: CancellationToken,
) -> Result<()> {
    //create websocket connection
    let ws_rpc_url = std::env::var("WEBSOCKET_RPC_URL").expect("Required 'WEBSOCKET_RPC_URL'!");
    let ws_client = PubsubClient::new(ws_rpc_url)
        .await
        .expect("Failed to establish PubsubClient!");

    let (mut log_stream, _) = ws_client
        .logs_subscribe(
            solana_client::rpc_config::RpcTransactionLogsFilter::Mentions(vec![
                USDC_MINT.to_string(),
            ]),
            RpcTransactionLogsConfig { commitment: None },
        )
        .await
        .expect("Failed to subscribe to Rpc logs");
    info!("Solana Cluster Websocket pipeline established. Streamin live blocks...");

    // Log/Data Ingestion loop
    while let Some(log_response) = log_stream.next().await {
        // Increment total logs ingested metrics counter
        counter!("pipeline.logs_ingested_total").increment(1);

        // Dynamically trace length of the orchestrator inbound channel to observe structural backpressure
        gauge!("pipeline.orchestrator_channel_depth")
            .set((orchestrator_msg_tx.max_capacity() - orchestrator_msg_tx.capacity()) as f64);

        // deligate the msg to process to filter cum dispatcher
        if let Err(_) = orchestrator_msg_tx
            .send(InboundTxnSource::RpcLogGate(log_response.value))
            .await
        {
            error!("Orchestrator channel disconnected. Closing network streaming pipeline.");
            // send returns error only when the receiver is dropped
            break;
        }

        // see if the task has been cancelled
        if cancel_token.is_cancelled() {
            info!("Rpc Websocket Task has been cancelled.");
            break;
        }
    }

    // Explicit drop to notify workers that no new logs are coming.
    // This will only allow the rx to recv all the already buffered messages
    drop(orchestrator_msg_tx);

    Ok(())
}
