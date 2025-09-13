use std::time::{Duration, Instant};

use anyhow::Result;
use chrono::Utc;
use futures_util::StreamExt;
use metrics::{counter, gauge, histogram};
use obsv::{init_metrics, init_tracing, measure_ms, measure_ms_async};
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::message::{BorrowedMessage, Headers};
use rdkafka::Message;
use serde::Deserialize;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;

fn env<T: AsRef<str>>(k: T, default: &str) -> String {
    std::env::var(k.as_ref()).unwrap_or_else(|_| default.to_string())
}

fn header_str<'a>(m: &'a BorrowedMessage<'a>, key: &str) -> Option<&'a str> {
    m.headers()?.iter().find(|h| h.key == key)
        .and_then(|h| std::str::from_utf8(h.value?).ok())
}

#[derive(Debug, Deserialize)]
struct NormTrade {
    ts_ms: i64,
    symbol: String,
    price: f64,
    qty: f64,
    trade_id: i64,
    is_bm: bool,
}

// ---- ILP helpers ----
async fn ilp_connect(host: &str, port: u16) -> Result<TcpStream> {
    let addr = format!("{}:{}", host, port);
    let stream = TcpStream::connect(addr).await?;
    Ok(stream)
}

fn to_ilp_line(t: &NormTrade, msg_id: &str) -> String {
    format!(
        "trades,symbol={} price={},qty={},trade_id={}i,is_bm={},msg_id=\"{}\",ts_ms={}i {}",
        t.symbol,
        t.price,
        t.qty,
        t.trade_id,
        t.is_bm,
        msg_id.replace('\"', "\\\""),
        t.ts_ms,
        (t.ts_ms as i128) * 1_000_000i128 // ms -> ns
    )
}

#[tokio::main]
async fn main() -> Result<()> {
    init_metrics(9466);
    init_tracing();

    let brokers  = env("KAFKA_BROKERS", "localhost:29092");
    let topic_in = env("TOPIC_IN", "ticks.norm");
    let group_id = env("GROUP_ID", "consumer-stage");
    let ilp_host = env("QDB_HOST", "localhost");
    let ilp_port: u16 = env("QDB_ILP_PORT", "9009").parse().unwrap_or(9009);

    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", &brokers)
        .set("group.id", &group_id)
        .set("enable.partition.eof", "false")
        .set("auto.offset.reset", "latest")
        // soften control-plane timeouts & keepalive to cut req timeouts:
        .set("socket.keepalive.enable", "true")
        .set("request.timeout.ms", "20000")
        .create()?;
    consumer.subscribe(&[&topic_in])?;

    let mut ilp = ilp_connect(&ilp_host, ilp_port).await?;
    let mut last_lag_update = Instant::now();

    let mut stream = consumer.stream();
    while let Some(result) = stream.next().await {
        let msg = match result {
            Ok(m) => m,
            Err(e) => { tracing::error!(target="consumer", error=?e, "poll error"); continue; }
        };

        let payload = match msg.payload_view::<str>() {
            Some(Ok(s)) => s,
            _ => { tracing::warn!(target="consumer", "empty/invalid payload"); continue; }
        };

        counter!("consumed_total").increment(1);

        // E2E latency
        let now_ns = Utc::now().timestamp_nanos_opt().unwrap();
        let ts_produce_ns: i64 = header_str(&msg, "ts_produce_ns")
            .and_then(|s| s.parse::<i64>().ok())
            .unwrap_or(now_ns);
        let e2e_ms = (now_ns - ts_produce_ns) as f64 / 1e6;
        histogram!("e2e_latency_ms").record(e2e_ms);

        // Parse and write via ILP
        let t: NormTrade = match serde_json::from_str(payload) {
            Ok(v) => v,
            Err(e) => { tracing::error!(target="consumer", error=?e, "parse error"); continue; }
        };
        let msg_id = header_str(&msg, "msg_id").unwrap_or("");

        let line = to_ilp_line(&t, msg_id);
        let payload = format!("{}\n", line);

        let write_res = {
            let (res, write_ms) = measure_ms_async(ilp.write_all(payload.as_bytes())).await;
            histogram!("questdb_write_ms").record(write_ms);
            res
        };

        if let Err(e) = write_res {
            tracing::warn!(target="consumer", error=?e, "ILP write failed; reconnecting once");
            ilp = match ilp_connect(&ilp_host, ilp_port).await {
                Ok(s) => s,
                Err(e) => { tracing::error!(target="consumer", error=?e, "ILP reconnect failed"); continue; }
            };
            if let Err(e2) = ilp.write_all(payload.as_bytes()).await {
                tracing::error!(target="consumer", error=?e2, "ILP write still failing after reconnect");
                continue;
            }
        }

        // Commit offset (timed)
        let (_, commit_ms) = measure_ms(|| {
            let _ = consumer.commit_message(&msg, rdkafka::consumer::CommitMode::Async);
        });
        histogram!("commit_latency_ms").record(commit_ms);

        // --- Lag gauge: update at most every 5s, with a 2s call timeout ---
        if last_lag_update.elapsed() >= Duration::from_secs(5) {
            if let Ok((_, high)) = consumer.fetch_watermarks(
                msg.topic(), msg.partition(), Duration::from_secs(2)
            ) {
                let pos = msg.offset();
                let lag = (high - (pos + 1)).max(0);
                gauge!("consumer_lag").set(lag as f64);
            }
            last_lag_update = Instant::now();
        }
    }

    Ok(())
}
