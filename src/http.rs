//! HTTP and metrics server
use std::sync::Arc;

use axum::{
    body::Body,
    extract::State,
    http::{header::CONTENT_TYPE, Response, StatusCode},
    response::IntoResponse,
};
use prometheus::{Encoder, HistogramVec, IntCounterVec, Registry, TextEncoder};

#[derive(Debug)]
pub struct Metrics {
    pub tx_sent: IntCounterVec,
    pub tx_confirmed: IntCounterVec,
    pub tx_failed: IntCounterVec,
    pub fill_expected: IntCounterVec,
    pub fill_actual: IntCounterVec,
    pub confirmation_slots: HistogramVec,
    pub cu_spent: HistogramVec,
    pub registry: Registry,
}

impl Metrics {
    pub fn new() -> Self {
        let registry = Registry::new();

        let tx_sent = IntCounterVec::new(
            prometheus::Opts::new("drift_tx_sent_total", "Number of transactions sent"),
            &["intent"],
        )
        .unwrap();
        registry.register(Box::new(tx_sent.clone())).unwrap();

        let tx_confirmed = IntCounterVec::new(
            prometheus::Opts::new(
                "drift_tx_confirmed_total",
                "Number of transactions confirmed",
            ),
            &["intent", "result"],
        )
        .unwrap();
        registry.register(Box::new(tx_confirmed.clone())).unwrap();

        let tx_failed = IntCounterVec::new(
            prometheus::Opts::new("drift_tx_failed_total", "Number of transactions failed"),
            &["intent", "reason"],
        )
        .unwrap();
        registry.register(Box::new(tx_failed.clone())).unwrap();

        let fill_expected = IntCounterVec::new(
            prometheus::Opts::new("drift_fill_expected_total", "Number of expected fills"),
            &["intent"],
        )
        .unwrap();
        registry.register(Box::new(fill_expected.clone())).unwrap();

        let fill_actual = IntCounterVec::new(
            prometheus::Opts::new("drift_fill_actual_total", "Number of actual fills"),
            &["intent", "amm"],
        )
        .unwrap();
        registry.register(Box::new(fill_actual.clone())).unwrap();

        let confirmation_slots = HistogramVec::new(
            prometheus::HistogramOpts::new(
                "drift_tx_confirmation_slots",
                "Slots taken to confirm tx",
            ),
            &["intent"],
        )
        .unwrap();
        registry
            .register(Box::new(confirmation_slots.clone()))
            .unwrap();

        let cu_spent = HistogramVec::new(
            prometheus::HistogramOpts::new("drift_tx_cu_spent", "Compute units spent per tx"),
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
            confirmation_slots,
            cu_spent,
            registry,
        }
    }
}

pub async fn metrics_handler(State(metrics): State<Arc<Metrics>>) -> impl IntoResponse {
    let metric_families = metrics.registry.gather();
    let mut buffer = Vec::new();
    let encoder = TextEncoder::new();
    encoder.encode(&metric_families, &mut buffer).unwrap();

    Response::builder()
        .header(
            CONTENT_TYPE,
            "application/openmetrics-text; version=1.0.0; charset=utf-8",
        )
        .body(Body::from(buffer))
        .unwrap()
}

pub async fn health_handler() -> impl IntoResponse {
    Response::builder()
        .status(StatusCode::OK)
        .body(Body::empty())
        .unwrap()
}
