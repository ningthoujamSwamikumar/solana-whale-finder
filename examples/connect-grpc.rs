use futures::{SinkExt, StreamExt};
use std::{collections::HashMap, error::Error};
use yellowstone_grpc_proto::geyser::{SubscribeRequest, SubscribeRequestFilterTransactions, subscribe_update::UpdateOneof};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    println!("Hello World");
    let client_builder =
        yellowstone_grpc_client::GeyserGrpcClient::build_from_static("http://127.0.0.1:10000");
    let mut client = client_builder.connect().await?;

    let (mut sink, mut geyser_stream) = client.subscribe().await?;

    let mut tx_filters = HashMap::new();
    tx_filters.insert(
        "all".to_string(),
        SubscribeRequestFilterTransactions {
            vote: Some(false),
            failed: Some(false),
            signature: None,
            account_include: vec![],
            account_exclude: vec![],
            account_required: vec![],
        },
    );

    // sends the subscription request for transactions
    sink.send(SubscribeRequest {
        transactions: tx_filters,
        ..Default::default()
    })
    .await?;

    // Read updates for the subscription made above
    while let Some(msg) = geyser_stream.next().await {
        match msg {
            Ok(update) => {
                if let Some( one_of)= update.update_oneof {
                    if let UpdateOneof::Transaction(txn_update) = one_of {
                        println!("Transaction Update: \n{:#?}", txn_update)
                    }
                }
            },
            Err(e) => {
                eprintln!("Stream Error: {e}");
                break;
            }
        }
    }

    Ok(())
}
