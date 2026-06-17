use anyhow::{Context, Ok, Result};
use futures::StreamExt;
use metrics::{counter, gauge};
use metrics_exporter_prometheus::PrometheusBuilder;
use solana_client::{
    nonblocking::pubsub_client::PubsubClient,
    rpc_config::RpcTransactionLogsConfig,
    rpc_request::Address,
    rpc_response::{
        OptionSerializer, RpcLogsResponse,
        transaction::{Signature, VersionedMessage},
    },
};
use sqlx::PgPool;

use rustls::crypto::ring::default_provider;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};
use tracing_subscriber::EnvFilter;

use crate::{
    grpc_source_gate::run_grpc_subscription, orchestrator::run_worker_orchestrator,
    rpc_log_gate::run_rpc_websocket_subscription, storage::db_pusher,
};

mod grpc_source_gate;
mod orchestrator;
mod rpc_log_gate;
mod storage;
mod worker;

// --- ALTERNATIVE ALLOCATOR CONFIGURATION ---
// Replaces the fragmented glibc allocator with Jemalloc.
// This forces aggressive memory reclamation back to the OS inside multi-threaded async tasks.
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

pub(crate) const USDC_MINT: &str = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v";
pub(crate) const USDC_MINT_ADDRESS: Address = Address::from_str_const(USDC_MINT);
pub(crate) const SPL_TOKEN: &str = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA";

/// Domain Model: Decoupled entirely from the transport mechanism (JSON-RPC or Proto)
pub(crate) struct UnifiedTxnPayload {
    pub signature: Signature,
    pub slot: u64,
    pub message: VersionedMessage,
    pub pre_token_balances:
        OptionSerializer<Vec<solana_client::rpc_response::UiTransactionTokenBalance>>,
    pub inner_instructions: OptionSerializer<Vec<solana_client::rpc_response::UiInnerInstructions>>,
    pub loaded_addresses: OptionSerializer<solana_client::rpc_response::UiLoadedAddresses>,
}

/// Abstract source strategy supporting both RPC polling and GRPC/Geyser pushes
pub(crate) enum InboundTxnSource {
    /// Raw logs that demand an external http pull lookup roundtrip
    RpcLogGate(RpcLogsResponse),
    /// Zero-roundtrip push payloads (Geyser/Yellowstone GRPC)
    DirechPushStream(UnifiedTxnPayload),
}

pub(crate) struct TxnRecord {
    signature: Signature,
    slot: i64,
    source_token_acc: Address,
    dest_token_acc: Address,
    amount: i64,
    mint: Address,
}

#[tokio::main]
async fn main() -> Result<()> {
    default_provider()
        .install_default()
        .expect("failed to install rustls provider");

    // Initialized Structured Logging Subsystem
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().or_else(|_| EnvFilter::try_new("info"))?)
        .with_target(false)
        .init();

    info!("Initializing Whale Indexer Engine...");

    // Initialized Internal Metrics Exporter (Exposes Prometheus metrics on http://127.0.0.1:9000/metrics)
    PrometheusBuilder::new()
        .with_http_listener(([127, 0, 0, 1], 9000))
        .install()
        .context("Failed to install Prometheus metrics exorter subsystem")?;
    info!("Prometheus Telemetry Scrapper listening on http://127.0.0.1:9000/metrics");

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
    // later - drop the original TEXT versions of the respective columns, after moving the string/text values to bytea values
    // later - also, set the bytea columns to not null
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
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );",
    )
    .execute(&pg_pool)
    .await?;
    info!("Database core transaction tables verified.");

    // record collection channel for batch insertion
    let (record_tx, record_rx) = tokio::sync::mpsc::channel::<TxnRecord>(50);
    // batch insertion worker
    let db_pusher_handle = tokio::spawn(db_pusher(record_rx, pg_pool));

    // spawn static task pool
    let (orchestrator_msg_tx, orchestrator_msg_rx) =
        tokio::sync::mpsc::channel::<InboundTxnSource>(100);
    let orchestrator_handle = tokio::spawn(run_worker_orchestrator(orchestrator_msg_rx, record_tx));

    let mut grpc_opted = false;
    for arg in std::env::args() {
        if &arg == "--use-grpc" {
            grpc_opted = true;
            break;
        }
    }

    let cancel_token = CancellationToken::new();
    tokio::select! {
        _ = async {
            // start the data source based on the user input through env var
            if grpc_opted {
                // call grpc client
                tokio::spawn(run_grpc_subscription(orchestrator_msg_tx, cancel_token.clone()));
            }else {
                // default
                // run websocket rpc client
                tokio::spawn(run_rpc_websocket_subscription(orchestrator_msg_tx, cancel_token.clone()));
            }
        } => {},
        _ = tokio::signal::ctrl_c() => {
            warn!("System shutdown signal captured. Terminating pipeline execution chains gracefully...");
            cancel_token.cancel();
        }
    }

    if let Err(e) = orchestrator_handle.await? {
        error!("Orchestrator Error: {:?}", e);
    }

    // Wait for the db pusher taskt to complete
    if let Err(e) = db_pusher_handle.await {
        error!("Db Pusher Error: {:?}", e);
    }

    info!("Pipeline safely shutdown. Memory pools cleanly recycled.");
    Ok(())
}

pub(crate) trait RedactExt<T> {
    /// Sanitizes error string variants to strip away sensitive Solana Solana RPC private key paths.
    fn redact_key(self, sensitive_url: &str) -> String;
}

impl<E: std::fmt::Debug> RedactExt<E> for E {
    fn redact_key(self, sensitive_url: &str) -> String {
        let error_string = format!("{:?}", self);

        // Extract the raw API Key token from your environment URL configuration
        // e.g. If URL is "https://mainnet.helius-rpc.com/?api-key=abc123xyz"
        // we isolate "abc123xyz" or simply search for the entire URL base sequence.
        if let Some(key_index) = sensitive_url.find("api-key=") {
            let token = &sensitive_url[key_index..];
            if !token.is_empty() {
                return error_string.replace(token, "api-key=[REDACTED]");
            }
        }

        // Alternative fallback: Blindly mask the entire custom RPC URL if it's found inside the debug error dump
        if error_string.contains(sensitive_url) {
            return error_string.replace(sensitive_url, "https://[RPC_URL_REDACTED]");
        }

        error_string
    }
}
