use crate::exchange_connectors::{
    binance::BinanceExchange, coinbase::CoinbaseExchange, Exchange, PriceUpdate,
};
use crate::Config;
use anyhow::Result;
use drift_rs::DriftClient;
use log::{error, info};
use std::collections::HashMap;
use tokio::sync::mpsc;
use tokio::time::{sleep, Duration};

pub struct MmOracleCrankerBot {
    drift: DriftClient,
    config: Config,
}

impl MmOracleCrankerBot {
    pub async fn new(config: Config, drift: DriftClient) -> Self {
        Self { drift, config }
    }

    pub async fn run(&self) {
        info!("Starting MM Oracle Cranker Bot");

        let (tx, mut rx) = mpsc::channel(100);

        // Keep exchanges alive to maintain connections
        let mut _exchanges: Vec<Box<dyn Exchange>> = Vec::new();

        // Binance
        let binance_symbols: Vec<String> = self
            .config
            .binance_symbols
            .split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect();

        if !binance_symbols.is_empty() {
            let mut binance = BinanceExchange::new();
            if let Err(e) = binance.connect(tx.clone()).await {
                error!("Failed to connect to Binance: {}", e);
            } else {
                if let Err(e) = binance.subscribe(&binance_symbols).await {
                    error!("Failed to subscribe to Binance: {}", e);
                } else {
                    _exchanges.push(Box::new(binance));
                }
            }
        }

        // Coinbase
        let coinbase_symbols: Vec<String> = self
            .config
            .coinbase_symbols
            .split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect();

        if !coinbase_symbols.is_empty() {
            let mut coinbase = CoinbaseExchange::new();
            if let Err(e) = coinbase.connect(tx.clone()).await {
                error!("Failed to connect to Coinbase: {}", e);
            } else {
                if let Err(e) = coinbase.subscribe(&coinbase_symbols).await {
                    error!("Failed to subscribe to Coinbase: {}", e);
                } else {
                    _exchanges.push(Box::new(coinbase));
                }
            }
        }

        let mut prices: HashMap<String, f64> = HashMap::new();

        while let Some(update) = rx.recv().await {
            prices.insert(update.symbol.clone(), update.mid_price);

            let btc_usdt = prices.get("BTCUSDT").cloned();
            let usdt_usd = prices.get("USDT-USD").cloned();

            if let (Some(p1), Some(p2)) = (btc_usdt, usdt_usd) {
                let btc_usd = p1 * p2;
                info!(
                    "Calculated BTC/USD: {:.2} (BTC/USDT: {:.2}, USDT/USD: {:.4})",
                    btc_usd, p1, p2
                );
            } else {
                // Just log what we have
                info!("Update: {} = {:.4}", update.symbol, update.mid_price);
            }
        }
    }
}
