use std::time::Duration;

use anyhow::Result;
use chrono::Utc;
use futures_util::StreamExt;
use metrics::{counter, histogram};
use obsv::{init_metrics, init_tracing, measure_ms_async};
use rdkafka::config::ClientConfig;
use rdkafka::message::{Header, OwnedHeaders};
use rdkafka::producer::{FutureProducer, FutureRecord};
use tokio_tungstenite::connect_async;
use uuid::Uuid;

fn env<T: AsRef<str>>(k: T, default: &str) -> String {
    std::env::var(k.as_ref()).unwrap_or_else(|_| default.to_string())
}

#[tokio::main]
async fn main() -> Result<()> {
    init_metrics(9464);
    init_tracing();

    let brokers   = env("KAFKA_BROKERS", "localhost:29092");
    let topic_out = env("TOPIC_OUT", "ticks.raw");
    let symbol    = env("SYMBOL", "btcusdt"); // lower-case for Binance
    let ws_url    = format!("wss://stream.binance.com:9443/ws/{}@trade", symbol);

    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", &brokers)
        .set("message.timeout.ms", "5000")
        .set("socket.keepalive.enable", "true")
        .set("request.timeout.ms", "20000")
        .create()?;

    let (ws_stream, _) = connect_async(&ws_url).await?;
    tracing::info!(target: "fetcher", "connected to {}", ws_url);
    let (_w, mut r) = ws_stream.split();

    while let Some(msg) = r.next().await {
        let msg = match msg {
            Ok(m) => m,
            Err(e) => { tracing::error!(target:"fetcher", error=?e, "websocket error"); continue; }
        };
        if !msg.is_text() { continue; }

        let payload = msg.into_text().unwrap_or_default();
        let msg_id = Uuid::new_v4().to_string();
        let ts_produce_ns = Utc::now().timestamp_nanos_opt().unwrap().to_string();

        counter!("produced_total").increment(1);

        // Await the send so delivery failures are logged
        let (delivery, ms) = measure_ms_async(
            producer.send(
                FutureRecord::to(&topic_out)
                    .payload(&payload)
                    .key(&symbol)
                    .headers(
                        OwnedHeaders::new()
                            .insert(Header { key: "msg_id", value: Some(msg_id.as_bytes()) })
                            .insert(Header { key: "ts_produce_ns", value: Some(ts_produce_ns.as_bytes()) })
                    ),
                Duration::from_secs(5),
            )
        ).await;
        histogram!("produce_latency_ms").record(ms);

        if let Err((e, _)) = delivery {
            tracing::error!(target="fetcher", error=?e, "kafka delivery failed");
        }
    }

    Ok(())
}
