use std::{str::FromStr, sync::Arc};

use anyhow::{Ok, Result};
use metrics::gauge;
use solana_client::nonblocking::rpc_client::RpcClient;
use tracing::error;

use crate::{InboundTxnSource, TxnRecord, worker::worker};

pub(crate) async fn run_worker_orchestrator(
    mut orchestrator_log_reciever: tokio::sync::mpsc::Receiver<InboundTxnSource>,
    record_tx: tokio::sync::mpsc::Sender<TxnRecord>,
) -> Result<()> {
    let worker_pool_size = std::env::var("WORKER_POOL_SIZE").map_or(10, |s| {
        i32::from_str(&s).expect("Failed to convert POOL SIZE env var!")
    });
    let (workers_log_tx, workers_log_rx) = async_channel::bounded::<InboundTxnSource>(1000);

    // create rpc client
    let arc_rpc_url = Arc::new(std::env::var("RPC_URL").expect("Required Rpc endpoint Url!"));
    let arc_rpc_client = Arc::new(RpcClient::new(arc_rpc_url.to_string()));

    let mut worker_handles = Vec::with_capacity(worker_pool_size as usize);
    for id in 0..worker_pool_size {
        let workers_log_rx = workers_log_rx.clone();
        let rpc_client = arc_rpc_client.clone();
        let record_tx = record_tx.clone();
        let rpc_url = arc_rpc_url.clone();
        let handle = tokio::spawn(worker(id, workers_log_rx, record_tx, rpc_client, rpc_url));
        worker_handles.push(handle);
    }
    // Prevents this thread's own variable from staying alive forever
    drop(workers_log_rx);

    // pass down logs to workers
    while let Some(log_response) = orchestrator_log_reciever.recv().await {
        gauge!("pipeline.worker_channel_depth").set(workers_log_tx.len() as f64);
        if let Err(e) = workers_log_tx.send(log_response).await {
            error!(error = ?e, "Failed passing incoming log packet down to inner async worker pool.");
        }
    }

    // notify receiver for no logs are going to send in, more than already buffered ones
    drop(workers_log_tx);
    for worker in worker_handles {
        match worker.await {
            std::result::Result::Ok(join_result) => {
                if let Err(e) = join_result {
                    error!("Worker Error: {:?}", e);
                }
            }
            Err(e) => {
                error!("Worer Join Error: {:?}", e);
            }
        }
    }

    // A component writing to a data lier must outlive the components generating the work
    // So, we are dropping this after all the workers are confirmed terminated,
    // so to not close the channel totally before the whale data are written
    drop(record_tx);

    Ok(())
}
