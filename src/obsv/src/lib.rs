use std::time::Instant;
use metrics::{self, Unit};
use metrics_exporter_prometheus::PrometheusBuilder;
use tracing_subscriber::{fmt, EnvFilter};

/// Initialize JSON tracing with RFC3339 timestamps.
pub fn init_tracing() {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .json()
        .with_timer(fmt::time::UtcTime::rfc_3339())
        .with_target(true)
        .with_file(true)
        .with_line_number(true)
        .init();
}

/// Expose Prometheus `/metrics` on 0.0.0.0:<port>.
pub fn init_metrics(port: u16) {
    PrometheusBuilder::new()
        .with_http_listener(([0, 0, 0, 0], port))
        .install()
        .expect("install prometheus exporter");

    // Describe key metrics (optional, adds units/help)
    metrics::describe_histogram!("e2e_latency_ms", Unit::Milliseconds, "E2E latency producer->consumer");
    metrics::describe_histogram!("produce_latency_ms", Unit::Milliseconds, "Kafka produce latency");
    metrics::describe_histogram!("commit_latency_ms", Unit::Milliseconds, "Kafka commit latency");
    metrics::describe_histogram!("questdb_write_ms", Unit::Milliseconds, "QuestDB write latency");
    metrics::describe_gauge!("consumer_lag", Unit::Count, "Kafka consumer lag");
    metrics::describe_counter!("produced_total", Unit::Count, "Messages produced");
    metrics::describe_counter!("consumed_total", Unit::Count, "Messages consumed");
    metrics::describe_counter!("dropped_total", Unit::Count, "Messages dropped");
    metrics::describe_counter!("dupes_total", Unit::Count, "Duplicate messages");
}

/// Measure a synchronous operation and return ((), elapsed_ms).
pub fn measure_ms<F: FnOnce()>(f: F) -> ((), f64) {
    let t0 = Instant::now();
    let out = f();
    let ms = t0.elapsed().as_secs_f64() * 1000.0;
    (out, ms)
}

/// Measure an async future and return (output, elapsed_ms).
pub async fn measure_ms_async<Fut, T>(fut: Fut) -> (T, f64)
where
    Fut: std::future::Future<Output = T>,
{
    let t0 = Instant::now();
    let out = fut.await;
    let ms = t0.elapsed().as_secs_f64() * 1000.0;
    (out, ms)
}
