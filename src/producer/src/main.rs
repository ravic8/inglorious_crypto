use rdkafka::consumer::{StreamConsumer, Consumer};
use rdkafka::config::ClientConfig;
use rdkafka::message::Message;
use serde::{Deserialize, Serialize};
use tokio_postgres::NoTls;
use chrono::{NaiveDateTime, Utc};

const KAFKA_BROKER: &str = "localhost:9092";
const KAFKA_TOPIC: &str = "btc-usdt-kline";
const POSTGRES_URI: &str = "host=localhost port=8812 user=admin password=quest dbname=qdb";

#[derive(Debug, Serialize, Deserialize)]
struct KlinePayload {
    e: String,  // Event type
    #[serde(rename = "E")]
    event_time: u64, // Event time
    s: String,  // Symbol
    k: KlineData, // Kline data
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct KlineData {
    t: u64,  // Kline start time
    T: u64,  // Kline close time
    s: String,  // Symbol
    i: String,  // Interval
    f: u64,  // First trade ID
    L: u64,  // Last trade ID
    o: String,  // Open price
    c: String,  // Close price
    h: String,  // High price
    l: String,  // Low price
    v: String,  // Base asset volume
    n: u64,  // Number of trades
    x: bool,  // Is this kline closed?
    q: String,  // Quote asset volume
    V: String,  // Taker buy base asset volume
    Q: String,  // Taker buy quote asset volume
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Connect to QuestDB
    let (client, connection) = tokio_postgres::connect(POSTGRES_URI, NoTls).await?;
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("PostgreSQL connection error: {}", e);
        }
    });
    println!("Connected to QuestDB!");

    // Initialize Kafka Consumer
    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", KAFKA_BROKER)
        .set("group.id", "kline-producer-group")
        .set("enable.partition.eof", "false")
        .set("session.timeout.ms", "6000")
        .set("enable.auto.commit", "true")
        .create()?;

    consumer.subscribe(&[KAFKA_TOPIC])?;
    println!("Kafka consumer started. Listening to topic '{}'.", KAFKA_TOPIC);

    loop {
        match consumer.recv().await {
            Ok(msg) => {
                if let Some(payload) = msg.payload_view::<str>() {
                    match payload {
                        Ok(raw_json) => {
                            // Parse the Kafka message into a KlinePayload struct
                            if let Ok(kline_payload) = serde_json::from_str::<KlinePayload>(raw_json) {
                                let k = kline_payload.k.clone(); // Clone the 'k' field
                                
                                let formatted_timestamp = NaiveDateTime::from_timestamp_millis(kline_payload.event_time as i64)
                                    .unwrap_or_else(|| Utc::now().naive_utc())
                                    .format("%Y-%m-%d %H:%M:%S")
                                    .to_string();

                                // Insert the structured data into QuestDB
                                let insert_query = format!(
                                    "INSERT INTO btc_usdt_kline (event_time, event_type, symbol, kline_start_time, kline_close_time, formatted_time, interval, first_trade_id, last_trade_id, open_price, close_price, high_price, low_price, base_asset_volume, number_of_trades, is_closed, quote_asset_volume, taker_buy_base_volume, taker_buy_quote_volume) \
    VALUES ({}, '{}', '{}', {}, {}, '{}', '{}', {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {});",
                                    kline_payload.event_time,  
                                    kline_payload.e,
                                    kline_payload.s,
                                    k.t,  // Kline start time
                                    k.T,  // Kline close time
                                    formatted_timestamp, // Formatted timestamp
                                    k.i,
                                    k.f,
                                    k.L,
                                    k.o.parse::<f64>().unwrap_or(0.0),
                                    k.c.parse::<f64>().unwrap_or(0.0),
                                    k.h.parse::<f64>().unwrap_or(0.0),
                                    k.l.parse::<f64>().unwrap_or(0.0),
                                    k.v.parse::<f64>().unwrap_or(0.0),
                                    k.n,
                                    k.x,
                                    k.q.parse::<f64>().unwrap_or(0.0),
                                    k.V.parse::<f64>().unwrap_or(0.0),
                                    k.Q.parse::<f64>().unwrap_or(0.0),
                                );

                                if let Err(e) = client.execute(&insert_query, &[]).await {
                                    eprintln!("Failed to insert into QuestDB: {}", e);
                                } else {
                                    println!("Inserted into QuestDB: {:?}", kline_payload);
                                }
                            } else {
                                eprintln!("Failed to parse Kafka message: {}", raw_json);
                            }
                        }
                        Err(e) => eprintln!("Error deserializing Kafka payload: {}", e),
                    }
                }
            }
            Err(e) => eprintln!("Kafka error: {}", e),
        }
    }
}

