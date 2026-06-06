use std::time::Duration;

use anyhow::{Ok, Result};
use metrics::{counter, gauge, histogram};
use sqlx::{PgPool, Postgres};
use tokio::time::timeout;
use tracing::{debug, error};

use crate::TxnRecord;

/// Collects records to do batch push into db
/// pushes at a set time or when reach batch limit reached or at system interrupt
pub(crate) async fn db_pusher(
    mut receiver: tokio::sync::mpsc::Receiver<TxnRecord>,
    pg_pool: PgPool,
) -> Result<()> {
    let batch_size: usize = 100;
    let mut txn_records: Vec<TxnRecord> = Vec::with_capacity(batch_size);

    loop {
        gauge!("pipeline.database_channel_depth")
            .set((receiver.max_capacity() - receiver.capacity()) as f64);
        // Timeout the waiting for records from the upstream tasks
        let result = timeout(Duration::new(2, 0), receiver.recv()).await;

        match result {
            // When receiver recieves a record before timeout
            std::result::Result::Ok(Some(record)) => {
                txn_records.push(record);
                // hit batch size limit
                if txn_records.len() >= batch_size {
                    if let Err(e) = flush_batch(&mut txn_records, &pg_pool).await {
                        error!(error = ?e, "Fatal bulk flush compilation dump execution error inside core database worker.");
                    }
                }
            }
            // When the channel is broken, maybe because of system interrupt
            std::result::Result::Ok(None) => {
                // flush the records present in the buffer
                if !txn_records.is_empty() {
                    if let Err(e) = flush_batch(&mut txn_records, &pg_pool).await {
                        error!(error = ?e, "Fatal bulk flush compilation dump execution error inside core database worker.");
                    }
                }
                break;
            }
            // Timeout: no records came within the set time
            Err(_) => {
                // flush the waiting records
                if !txn_records.is_empty() {
                    debug!(
                        "Batch window commit interval timeout reached. Flushing database buffer segments early."
                    );
                    if let Err(e) = flush_batch(&mut txn_records, &pg_pool).await {
                        error!(error = ?e, "Fatal bulk flush compilation dump execution error inside core database worker.");
                    }
                }
            }
        };
    }

    Ok(())
}

async fn flush_batch(txn_records: &mut Vec<TxnRecord>, pg_pool: &PgPool) -> Result<()> {
    let rows_count = txn_records.len();
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

    let start_time = std::time::Instant::now();
    qb.build().execute(pg_pool).await?;

    // Telemetry metric bindings
    histogram!("pipeline.database_flush_latency_seconds")
        .record(start_time.elapsed().as_secs_f64());
    counter!("pipeline.database_rows_written_total").increment(rows_count as u64);

    debug!(
        rows = rows_count,
        latency_ms = start_time.elapsed().as_millis(),
        "Successfully committed bulk batch records directly to PostgreSQL."
    );
    Ok(())
}
