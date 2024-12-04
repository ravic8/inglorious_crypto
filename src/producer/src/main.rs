use rdkafka::config::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord};
use serde::{Deserialize, Serialize};
use std::time::Duration;

#[derive(Debug, Serialize, Deserialize)]
struct BitcoinData {
    e: String, // Event type
    #[serde(rename = "E")]
    event_time: u64, // Event time
    s: String, // Symbol
    p: String, // Price
    q: String, // Quantity
    #[serde(rename = "T")]
    trade_time: u64, // Trade time
    m: bool,  // Is the buyer the market maker?
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Kafka broker and topic
    let kafka_broker = "localhost:9092";
    let kafka_topic = "btc-usdt";

    // Create a Kafka producer
    let producer: FutureProducer = ClientConfig::new()
    .set("bootstrap.servers", kafka_broker)
    .set("message.timeout.ms", "15000") // Increased timeout
    .set("debug", "all") // Enable debug logging
    .create()?;

    println!("Kafka producer created. Publishing messages to topic '{}'.", kafka_topic);

    // Simulated Bitcoin data (replace with actual data from WebSocket)
    let bitcoin_data = BitcoinData {
        e: "trade".to_string(),
        event_time: 1672515782136,
        s: "BTCUSDT".to_string(),
        p: "30000.50".to_string(),
        q: "0.001".to_string(),
        trade_time: 1672515782140,
        m: true,
    };

    // Serialize the Bitcoin data to JSON
    let payload = serde_json::to_string(&bitcoin_data)?;

    // Publish to Kafka
    let delivery_status = producer
        .send(
            FutureRecord::to(kafka_topic)
                .payload(&payload)
                .key("btc-trade"),
            Duration::from_secs(0),
        )
        .await;

    match delivery_status {
        Ok(delivery) => println!("Message delivered: {:?}", delivery),
        Err((e, _)) => eprintln!("Failed to deliver message: {:?}", e),
    }

    Ok(())
}

