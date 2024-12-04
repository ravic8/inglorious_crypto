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

// WebSocket URL for Binance trades
const BINANCE_WS_URL: &str = "wss://stream.binance.com:9443/ws/btcusdt@trade";

// Kafka Configuration
const KAFKA_BROKER: &str = "localhost:9092";
const KAFKA_TOPIC: &str = "btc-usdt";

// Define the structure of trade data
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
            // Parse the JSON message into the BinanceTrade struct
            if let Ok(trade) = serde_json::from_str::<BinanceTrade>(&text) {
                println!("Received trade: {:?}", trade);

                // Serialize trade data into JSON
                if let Ok(payload) = serde_json::to_string(&trade) {
                    // Publish the JSON payload to Kafka
                    let delivery_status = producer
                        .send(
                            FutureRecord::to(KAFKA_TOPIC)
                                .payload(&payload)
                                .key("btc-trade"),
                            std::time::Duration::from_secs(0),
                        )
                        .await;

                    match delivery_status {
                        Ok(delivery) => println!("Message delivered: {:?}", delivery),
                        Err((e, _)) => eprintln!("Failed to deliver message: {:?}", e),
                    }
                }
            } else {
                eprintln!("Failed to parse trade message: {}", text);
            }
        }
    }

    Ok(())
}

