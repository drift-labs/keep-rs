//! HTTP and metrics server
use std::sync::Arc;

use axum::{
    body::Body,
    extract::State,
    http::{header::CONTENT_TYPE, Response, StatusCode},
    response::IntoResponse,
};
use prometheus::{
    Encoder, HistogramVec, IntCounter, IntCounterVec, IntGauge, Registry, TextEncoder,
};

#[derive(Debug)]
pub struct Metrics {
    pub tx_sent: IntCounterVec,
    pub tx_confirmed: IntCounterVec,
    pub tx_failed: IntCounterVec,
    pub trigger_expected: IntCounter,
    pub trigger_actual: IntCounter,
    pub fill_expected: IntCounterVec,
    pub fill_actual: IntCounterVec,
    pub liquidation_attempts: IntCounterVec,
    pub liquidation_success: IntCounterVec,
    pub liquidation_failed: IntCounterVec,
    pub swap_quote_latency_ms: IntGauge,
    pub jupiter_quote_failures: IntCounter,
    pub titan_quote_failures: IntCounter,
    pub confirmation_slots: HistogramVec,
    pub cu_spent: HistogramVec,
    pub registry: Registry,
}

impl Metrics {
    pub fn new() -> Self {
        let registry = Registry::new();

        let tx_sent = IntCounterVec::new(
            prometheus::Opts::new("rfb_tx_sent_total", "Number of transactions sent"),
            &["intent"],
        )
        .unwrap();
        registry.register(Box::new(tx_sent.clone())).unwrap();

        let tx_confirmed = IntCounterVec::new(
            prometheus::Opts::new("rfb_tx_confirmed_total", "Number of transactions confirmed"),
            &["intent", "result"],
        )
        .unwrap();
        registry.register(Box::new(tx_confirmed.clone())).unwrap();

        let tx_failed = IntCounterVec::new(
            prometheus::Opts::new("rfb_tx_failed_total", "Number of transactions failed"),
            &["intent", "reason"],
        )
        .unwrap();
        registry.register(Box::new(tx_failed.clone())).unwrap();

        let fill_expected = IntCounterVec::new(
            prometheus::Opts::new("rfb_fill_expected_total", "Number of expected fills"),
            &["intent"],
        )
        .unwrap();
        registry.register(Box::new(fill_expected.clone())).unwrap();

        let fill_actual = IntCounterVec::new(
            prometheus::Opts::new("rfb_fill_actual_total", "Number of actual fills"),
            &["intent"],
        )
        .unwrap();
        registry.register(Box::new(fill_actual.clone())).unwrap();

        let trigger_expected = IntCounter::new(
            "rfb_trigger_expected_total",
            "Number of expected triggered orders",
        )
        .unwrap();
        registry
            .register(Box::new(trigger_expected.clone()))
            .unwrap();

        let trigger_actual = IntCounter::new(
            "rfb_trigger_actual_total",
            "Number of actual triggered orders",
        )
        .unwrap();
        registry.register(Box::new(trigger_actual.clone())).unwrap();

        let liquidation_attempts = IntCounterVec::new(
            prometheus::Opts::new(
                "rfb_liquidation_attempts_total",
                "Number of liquidation attempts",
            ),
            &["type"],
        )
        .unwrap();
        registry
            .register(Box::new(liquidation_attempts.clone()))
            .unwrap();

        let liquidation_success = IntCounterVec::new(
            prometheus::Opts::new(
                "rfb_liquidation_success_total",
                "Number of successful liquidations",
            ),
            &["type"],
        )
        .unwrap();
        registry
            .register(Box::new(liquidation_success.clone()))
            .unwrap();

        let liquidation_failed = IntCounterVec::new(
            prometheus::Opts::new(
                "rfb_liquidation_failed_total",
                "Number of failed liquidations",
            ),
            &["type"],
        )
        .unwrap();
        registry
            .register(Box::new(liquidation_failed.clone()))
            .unwrap();

        let swap_quote_latency_ms = IntGauge::new(
            "rfb_swap_quote_latency_ms",
            "Swap quote request latency in milliseconds",
        )
        .unwrap();
        registry
            .register(Box::new(swap_quote_latency_ms.clone()))
            .unwrap();

        let jupiter_quote_failures = IntCounter::new(
            "rfb_jupiter_quote_failures_total",
            "Number of Jupiter quote failures",
        )
        .unwrap();
        registry
            .register(Box::new(jupiter_quote_failures.clone()))
            .unwrap();

        let titan_quote_failures = IntCounter::new(
            "rfb_titan_quote_failures_total",
            "Number of Titan quote failures",
        )
        .unwrap();
        registry
            .register(Box::new(titan_quote_failures.clone()))
            .unwrap();

        let confirmation_slots = HistogramVec::new(
            prometheus::HistogramOpts::new(
                "rfb_tx_confirmation_slots",
                "Slots taken to confirm tx",
            ),
            &["intent"],
        )
        .unwrap();
        registry
            .register(Box::new(confirmation_slots.clone()))
            .unwrap();

        let cu_spent = HistogramVec::new(
            prometheus::HistogramOpts::new("rfb_tx_cu_spent", "Compute units spent per tx"),
            &["intent"],
        )
        .unwrap();
        registry.register(Box::new(cu_spent.clone())).unwrap();

        Self {
            tx_sent,
            tx_confirmed,
            tx_failed,
            fill_expected,
            fill_actual,
            liquidation_attempts,
            liquidation_success,
            liquidation_failed,
            swap_quote_latency_ms,
            jupiter_quote_failures,
            titan_quote_failures,
            confirmation_slots,
            cu_spent,
            registry,
            trigger_expected,
            trigger_actual,
        }
    }
}

pub async fn metrics_handler(State(metrics): State<Arc<Metrics>>) -> impl IntoResponse {
    let metric_families = metrics.registry.gather();
    let mut buffer = Vec::new();
    let encoder = TextEncoder::new();
    encoder.encode(&metric_families, &mut buffer).unwrap();

    let response = String::from_utf8(buffer).unwrap();
    Response::builder()
        .header(CONTENT_TYPE, "text/plain;version=1.0.0;charset=utf-8")
        .body(response)
        .unwrap()
}

pub async fn health_handler() -> impl IntoResponse {
    Response::builder()
        .status(StatusCode::OK)
        .body(Body::empty())
        .unwrap()
}
