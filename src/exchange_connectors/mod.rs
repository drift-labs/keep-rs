use anyhow::Result;
use async_trait::async_trait;
use tokio::sync::mpsc::Sender;

#[derive(Debug, Clone)]
pub struct PriceUpdate {
    pub symbol: String,
    pub mid_price: f64,
    pub exchange: String,
}

pub enum ExchangeCommand {
    Subscribe(Vec<String>),
    Unsubscribe(Vec<String>),
}

#[async_trait]
pub trait Exchange: Send + Sync {
    async fn connect(&mut self, sender: Sender<PriceUpdate>) -> Result<()>;
    async fn subscribe(&self, symbols: &[String]) -> Result<()>;
    async fn unsubscribe(&self, symbols: &[String]) -> Result<()>;
}

pub mod binance;
pub mod coinbase;
