use futures_util::{StreamExt, TryStreamExt};
use rdkafka::config::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord};
use serde::{Deserialize, Serialize};
use tokio::net::TcpStream;
use tokio_rustls::{rustls, TlsConnector};
use tokio_tungstenite::{client_async, tungstenite::Error};
use url::Url;
use std::sync::Arc;
use webpki_roots::TLS_SERVER_ROOTS;
use chrono::Utc;

// WebSocket URL for Binance Kline
const BINANCE_WS_URL: &str = "wss://stream.binance.com:9443/ws/btcusdt@kline_1s";

// Kafka Configuration
const KAFKA_BROKER: &str = "localhost:9092";
const KAFKA_TOPIC: &str = "btc-usdt-kline";

#[derive(Debug, Serialize, Deserialize)]
struct KlinePayload {
    e: String,  // Event type
    #[serde(rename = "E")]
    event_time: u64, // Event time
    s: String,  // Symbol
    k: KlineData, // Kline data
}

#[derive(Debug, Serialize, Deserialize)]
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
    // Initialize Kafka Producer
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", KAFKA_BROKER)
        .set("message.timeout.ms", "5000")
        .create()?;

    println!("Kafka producer created. Connecting to Binance WebSocket...");
    

    // Set up TLS configuration for WebSocket
    let mut root_cert_store = rustls::RootCertStore::empty();
    root_cert_store.add_server_trust_anchors(TLS_SERVER_ROOTS.0.iter().map(|ta| {
        rustls::OwnedTrustAnchor::from_subject_spki_name_constraints(
            ta.subject,
            ta.spki,
            ta.name_constraints,
        )
    }));

    let tls_config = rustls::ClientConfig::builder()
        .with_safe_defaults()
        .with_root_certificates(root_cert_store)
        .with_no_client_auth();
    let connector = TlsConnector::from(Arc::new(tls_config));

    // Connect to WebSocket
    let url = Url::parse(BINANCE_WS_URL)?;
    let domain = url.host_str().ok_or("Invalid WebSocket URL")?;
    let stream = TcpStream::connect((domain, 443)).await?;
    let tls_stream = connector.connect(domain.try_into()?, stream).await?;
    let (ws_stream, _) = client_async(url, tls_stream).await?;
    println!("Connected to Binance WebSocket.");

    let (_, mut read) = ws_stream.split();

    // Listen for messages from the WebSocket
    while let Some(msg) = read.try_next().await? {
        if let Ok(text) = msg.into_text() {
            // Parse the JSON message into the KlinePayload struct
            if let Ok(kline_payload) = serde_json::from_str::<KlinePayload>(&text) {
                println!("Received Kline data: {:?}", kline_payload);
                
                // Add fetcher_processed_time (current timestamp)
                let fetcher_processed_time = Utc::now().timestamp_millis();
                
                // Prepare payload with latency metadata
                let payload_with_latency = serde_json::json!({
                    "event_time": kline_payload.event_time, // Original event time
                    "fetcher_processed_time": fetcher_processed_time, // Fetcher processing time
                    "kline_data": kline_payload.k // Include the kline data
                });
                

                // Serialize the Kline payload into JSON and send it to Kafka
                if let Ok(payload) = serde_json::to_string(&kline_payload) {
                    let delivery_status = producer
                        .send(
                            FutureRecord::to(KAFKA_TOPIC)
                                .payload(&payload)
                                .key(&kline_payload.s),
                            std::time::Duration::from_secs(0),
                        )
                        .await;

                    match delivery_status {
                        Ok(delivery) => println!("Message delivered: {:?}", delivery),
                        Err((e, _)) => eprintln!("Failed to deliver message: {:?}", e),
                    }
                }
            } else {
                eprintln!("Failed to parse Kline message: {}", text);
            }
        }
    }

    Ok(())
}

