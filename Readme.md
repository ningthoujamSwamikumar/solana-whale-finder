# Solana Data Pipeline: A High-Througput Asynchronous Ingestion Engine
A concurrent, low-latency blockchain indexing engine built with Rust. Designed to monitor, parsed, and route high-volume Solana tansactions (e.g. Whale movements) without memory fragmentation or async task starvation. Utilizing a protocol agnostic abstraction layer (InboundTxnSource), the pipeline unifies both RPC and geyser based streams into a single, decoupled execution flow managed by bounded backpressure channels and zero copy data decoding.

## Architecture and Concurrency Model
- **Decoupled Orchestration**: Separated network ingestion from Database IO. Uses a supervisor loop that feeds a fixed pool of 20 persistent async worker tasks via a `tokio::sync::mpsc` channels.
- **Protocol Agnostic Ingestion**: Allows the engine to consume faster geyser grpc stream, as well as RPC websocket stream by abstracting away the network ingestion from the other components.
- **Database Batching & Non-blocking I/O**: Offloaded PostgreSQL operations to a dedicated background actor thread. Implemented a custom batch accumulation routine (`flush_batch`) utilizing `sqlx::QueryBuilder` to aggregate hundreds of incoming transaction records in-memory, dynamically compiling them into unified multi-row insert statement to protect the database connection pool from exhaustion.

## Resilience and Backpressure Management


## Memory Profiling and Low-Allocation Mastery

## Production Observability and Security

## Local Stress Testing and Benchmarks

## Features cum Todo
- Real time data stream through websocket
    - Connect to helius websocket endpoint using a Pubsub client from solana-client
    - subscribe to logs using logSubscribe and filter for transactions that mention USDC mint
    > - Filtering by the USDC mint might miss us out on transactions that uses transfer instruction because transfer instruction doesn't require token mint. But modern transactions usually uses transfer checked instruction as historically using transfer instruction led to transfer to wrong address and lost lots of money.
    > - This is a tradeoff between getting 100% transactions and hitting rate limit or exhaustion of our limited compute resources, and getting 99% transaction.
    - fetch filter passed transaction from rpc node.
        - kdkkd
        - kdkdk
- Filter and extract whales
- fetch whale transaction and store them


