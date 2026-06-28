# Solana Data Pipeline: A High-Througput Asynchronous Ingestion Engine
A concurrent, low-latency blockchain indexing engine built with Rust. Designed to monitor, parsed, and route high-volume Solana tansactions (e.g. Whale movements) without memory fragmentation or async task starvation. Utilizing a protocol agnostic abstraction layer (InboundTxnSource), the pipeline unifies both RPC and geyser based streams into a single, decoupled execution flow managed by bounded backpressure channels and zero copy data decoding.

## Architecture and Concurrency Model
- **Decoupled Orchestration**: Separated network ingestion from Database IO. Uses a supervisor loop that feeds a fixed pool of 20 persistent async worker tasks via a `tokio::sync::mpsc` channels.
- **Protocol Agnostic Ingestion**: Allows the engine to consume faster geyser grpc stream, as well as RPC websocket stream by abstracting away the network ingestion from the processings.
- **Database Batching & Non-blocking I/O**: Offloaded PostgreSQL operations to a dedicated background actor thread. Implemented a custom batch accumulation routine (`flush_batch`) utilizing `sqlx::QueryBuilder` to aggregate hundreds of incoming transaction records in-memory, dynamically compiling them into unified multi-row insert statement to protect the database connection pool from exhaustion.

## Resilience and Backpressure Management
- **Systematic Backpressure**: Utilized bounded channels which creates a natural consumption throttling at the network interface layer; if the database lags during a traffic spike, the orchestrator safely blocks instead of causing Out-of-Memory explosion.
- **Zero Data-Loss Graceful Teardown**: Integrated Tokio's `TaskTracker` to manage thread scoping boundaries. During system interrupts (SIGINT), the engine executes an ordered teardown sequence - closing network channels, waiting for all independent worker futures to natively exhaust their queus, and flushing the final database batch before process termination.

## Low-Allocation Mastery
- **Zero-Copy Deserialization**: Minimized heap allocations on the critical hot-path by decoding base58 byte streams directly into stack-allocated arrays (`bs58::decode().onto(...)`).

## Production Observability and Security

## Local Stress Testing and Benchmarks



