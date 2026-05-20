# Indexer
Indexing whale transfers in Solana

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


