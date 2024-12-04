use rdkafka::consumer::Consumer; // This will give access to methods like `subscribe()`
use rdkafka::consumer::{StreamConsumer};
use rdkafka::config::ClientConfig;
use rdkafka::message::Message;
use serde::{Deserialize, Serialize};
use tokio_postgres::NoTls;

// Define the structure of trade data (same as before)
#[derive(Debug, Serialize, Deserialize)]
struct BinanceTrade {
    e: String,    // Event type
    #[serde(rename = "E")]
    event_time: u64, // Event time
    s: String,    // Symbol
    p: String,    // Price
    q: String,    // Quantity
    #[serde(rename = "T")]
    trade_time: u64, // Trade time
    m: bool,      // Is buyer the market maker?
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Connect to QuestDB
    let (client, connection) =
        tokio_postgres::connect("host=localhost port=8812 user=admin password=quest dbname=qdb", NoTls)
            .await?;
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("Connection error: {}", e);
        }
    });

    println!("Connected to QuestDB!");

    // Initialize Kafka Consumer
    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", "localhost:9092")
        .set("group.id", "btc-consumer-group")
        .set("enable.partition.eof", "false")
        .set("session.timeout.ms", "6000")
        .set("enable.auto.commit", "true")
        .create()?;

    consumer.subscribe(&["btc-usdt"])?;

    println!("Kafka consumer started. Listening for messages on topic 'btc-usdt'...");

    loop {
        // Use recv to fetch messages from Kafka
        match consumer.recv().await {
            Ok(msg) => {
                if let Some(payload) = msg.payload_view::<str>() {
                    match payload {
                        Ok(json) => {
                            if let Ok(trade) = serde_json::from_str::<BinanceTrade>(json) {
                                println!("Received trade: {:?}", trade);

                                // Insert trade into QuestDB
                                let insert_query = format!(
                                    "INSERT INTO btc_usdt_trades (event_time, symbol, price, quantity, trade_time, is_market_maker) \
                                     VALUES ({}, '{}', {}, {}, {}, {});",
                                    trade.event_time * 1000, // Convert milliseconds to microseconds
                                    trade.s,
                                    trade.p,
                                    trade.q,
                                    trade.trade_time * 1000, // Convert milliseconds to microseconds
                                    trade.m
                                );

                                // Log the query to ensure it's being generated correctly
                                println!("Executing Query: {}", insert_query);
                                
                                // Execute the query to insert into QuestDB
                                if let Err(e) = client.execute(&insert_query, &[]).await {
                                    eprintln!("Failed to insert into QuestDB: {}", e);
                                } else {
                                    println!("Inserted trade into QuestDB: {:?}", trade);
                                }
                            } else {
                                eprintln!("Failed to deserialize message: {}", json);
                            }
                        }
                        Err(e) => eprintln!("Error while deserializing payload: {:?}", e),
                    }
                }
            }
            Err(e) => {
                eprintln!("Error receiving message: {:?}", e);
            }
        }
    }
}

