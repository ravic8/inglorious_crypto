use std::time::Duration;

use anyhow::Result;
use chrono::Utc;
use futures_util::StreamExt;
use metrics::{counter, histogram};
use obsv::{init_metrics, init_tracing, measure_ms_async};
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::message::{BorrowedMessage, Header, Headers, OwnedHeaders};
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::Message;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

fn env<T: AsRef<str>>(k: T, default: &str) -> String {
    std::env::var(k.as_ref()).unwrap_or_else(|_| default.to_string())
}

#[derive(Debug, Deserialize)]
struct RawTrade {
    #[serde(rename = "s")] symbol: String,
    #[serde(rename = "t")] trade_id: i64,
    #[serde(rename = "p")] price: String,
    #[serde(rename = "q")] qty: String,
    #[serde(rename = "T")] ts_trade: i64,  // ms
    #[serde(rename = "m")] is_bm: bool,
}

#[derive(Debug, Serialize)]
struct NormTrade {
    ts_ms: i64,
    symbol: String,
    price: f64,
    qty: f64,
    trade_id: i64,
    is_bm: bool,
}

fn header_str<'a>(m: &'a BorrowedMessage<'a>, key: &str) -> Option<&'a str> {
    m.headers()?.iter().find(|h| h.key == key)
        .and_then(|h| std::str::from_utf8(h.value?).ok())
}

#[tokio::main]
async fn main() -> Result<()> {
    init_metrics(9465);
    init_tracing();

    let brokers   = env("KAFKA_BROKERS", "localhost:29092");
    let topic_in  = env("TOPIC_IN", "ticks.raw");
    let topic_out = env("TOPIC_OUT", "ticks.norm");
    let group_id  = env("GROUP_ID", "producer-stage");

    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", &brokers)
        .set("group.id", &group_id)
        .set("enable.partition.eof", "false")
        .set("auto.offset.reset", "latest")
        .set("socket.keepalive.enable", "true")
        .set("request.timeout.ms", "20000")
        .create()?;
    consumer.subscribe(&[&topic_in])?;

    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", &brokers)
        .set("socket.keepalive.enable", "true")
        .set("request.timeout.ms", "20000")
        .create()?;

    let mut stream = consumer.stream();

    while let Some(result) = stream.next().await {
        let msg = match result {
            Ok(m) => m,
            Err(e) => { tracing::error!(target="producer", error=?e, "poll error"); continue; }
        };

        let payload = match msg.payload_view::<str>() {
            Some(Ok(s)) => s,
            _ => { tracing::warn!(target="producer", "empty/invalid payload"); continue; }
        };

        counter!("consumed_total").increment(1);

        let raw: RawTrade = match serde_json::from_str(payload) {
            Ok(v) => v,
            Err(e) => { tracing::error!(target="producer", error=?e, "parse error"); counter!("dropped_total").increment(1); continue; }
        };

        let norm = NormTrade {
            ts_ms: raw.ts_trade,
            symbol: raw.symbol,
            price: raw.price.parse().unwrap_or(0.0),
            qty: raw.qty.parse().unwrap_or(0.0),
            trade_id: raw.trade_id,
            is_bm: raw.is_bm,
        };
        let out_json = serde_json::to_string(&norm)?;

        let orig_ts_ns = header_str(&msg, "ts_produce_ns")
            .map(|s| s.to_string())
            .unwrap_or_else(|| Utc::now().timestamp_nanos_opt().unwrap().to_string());
        let msg_id = header_str(&msg, "msg_id")
            .map(|s| s.to_string())
            .unwrap_or_else(|| Uuid::new_v4().to_string());

        counter!("produced_total").increment(1);

        // Await the send and time it
        let (delivery, send_ms) = measure_ms_async(
            producer.send(
                FutureRecord::to(&topic_out)
                    .payload(&out_json)
                    .key(&norm.symbol)
                    .headers(
                        OwnedHeaders::new()
                            .insert(Header { key: "msg_id", value: Some(msg_id.as_bytes()) })
                            .insert(Header { key: "ts_produce_ns", value: Some(orig_ts_ns.as_bytes()) })
                    ),
                Duration::from_secs(5),
            )
        ).await;
        histogram!("produce_latency_ms").record(send_ms);

        if let Err((e, _)) = delivery {
            tracing::error!(target="producer", error=?e, "kafka delivery failed");
        }

        let _ = consumer.commit_message(&msg, rdkafka::consumer::CommitMode::Async);
    }

    Ok(())
}
