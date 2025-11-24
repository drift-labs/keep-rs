use super::{Exchange, ExchangeCommand, PriceUpdate};
use anyhow::Result;
use async_trait::async_trait;
use futures_util::{SinkExt, StreamExt};
use log::{error, info, warn};
use serde::Deserialize;
use serde_json::json;
use std::time::Duration;
use tokio::sync::mpsc::{self, Sender};
use tokio_tungstenite::{connect_async, tungstenite::Message};
use url::Url;

pub struct BinanceExchange {
    command_tx: Option<Sender<ExchangeCommand>>,
}

impl BinanceExchange {
    pub fn new() -> Self {
        Self { command_tx: None }
    }
}

#[derive(Deserialize, Debug)]
struct BinanceBookTicker {
    s: String, // symbol
    b: String, // best bid
    a: String, // best ask
}

#[async_trait]
impl Exchange for BinanceExchange {
    async fn connect(&mut self, sender: Sender<PriceUpdate>) -> Result<()> {
        let (cmd_tx, mut cmd_rx) = mpsc::channel(100);
        self.command_tx = Some(cmd_tx);

        tokio::spawn(async move {
            let mut active_symbols: Vec<String> = Vec::new();

            loop {
                let url = Url::parse("wss://fstream.binance.com/ws").unwrap();
                info!("Connecting to Binance Futures WS: {}", url);

                match connect_async(url.to_string()).await {
                    Ok((mut ws_stream, _)) => {
                        info!("Connected to Binance");

                        // Resubscribe if we have active symbols
                        if !active_symbols.is_empty() {
                            let params: Vec<String> = active_symbols
                                .iter()
                                .map(|s| format!("{}@bookTicker", s.to_lowercase()))
                                .collect();
                            let subscribe_msg = json!({
                                "method": "SUBSCRIBE",
                                "params": params,
                                "id": 1
                            });
                            if let Err(e) = ws_stream
                                .send(Message::Text(subscribe_msg.to_string().into()))
                                .await
                            {
                                error!("Failed to resubscribe: {}", e);
                            } else {
                                info!("Resubscribed to Binance symbols: {:?}", active_symbols);
                            }
                        }

                        loop {
                            tokio::select! {
                                Some(cmd) = cmd_rx.recv() => {
                                    match cmd {
                                        ExchangeCommand::Subscribe(symbols) => {
                                            active_symbols.extend(symbols.clone());
                                            // Dedup logic could go here

                                            let params: Vec<String> = symbols.iter().map(|s| format!("{}@bookTicker", s.to_lowercase())).collect();
                                            let subscribe_msg = json!({
                                                "method": "SUBSCRIBE",
                                                "params": params,
                                                "id": 1
                                            });
                                            if let Err(e) = ws_stream.send(Message::Text(subscribe_msg.to_string().into())).await {
                                                error!("Failed to subscribe: {}", e);
                                                break;
                                            }
                                            info!("Subscribed to Binance symbols: {:?}", symbols);
                                        }
                                        ExchangeCommand::Unsubscribe(symbols) => {
                                            active_symbols.retain(|s| !symbols.contains(s));

                                            let params: Vec<String> = symbols.iter().map(|s| format!("{}@bookTicker", s.to_lowercase())).collect();
                                            let unsubscribe_msg = json!({
                                                "method": "UNSUBSCRIBE",
                                                "params": params,
                                                "id": 1
                                            });
                                            if let Err(e) = ws_stream.send(Message::Text(unsubscribe_msg.to_string().into())).await {
                                                error!("Failed to unsubscribe: {}", e);
                                                break;
                                            }
                                            info!("Unsubscribed from Binance symbols: {:?}", symbols);
                                        }
                                    }
                                }
                                Some(msg) = ws_stream.next() => {
                                    match msg {
                                        Ok(Message::Text(text)) => {
                                            if let Ok(ticker) = serde_json::from_str::<BinanceBookTicker>(&text) {
                                                let bid: f64 = ticker.b.parse().unwrap_or(0.0);
                                                let ask: f64 = ticker.a.parse().unwrap_or(0.0);
                                                if bid > 0.0 && ask > 0.0 {
                                                    let mid_price = (bid + ask) / 2.0;
                                                    let update = PriceUpdate {
                                                        symbol: ticker.s.to_uppercase(),
                                                        mid_price,
                                                        exchange: "binance".to_string(),
                                                    };
                                                    if let Err(_) = sender.send(update).await {
                                                        break;
                                                    }
                                                }
                                            }
                                        }
                                        Ok(Message::Ping(payload)) => {
                                            if let Err(_) = ws_stream.send(Message::Pong(payload)).await {
                                                break;
                                            }
                                        }
                                        Err(e) => {
                                            error!("Binance WS error: {}", e);
                                            break;
                                        }
                                        _ => {}
                                    }
                                }
                                else => break, // Stream ended
                            }
                        }
                    }
                    Err(e) => {
                        error!("Failed to connect to Binance: {}", e);
                    }
                }
                warn!("Binance connection lost, reconnecting in 5s...");
                tokio::time::sleep(Duration::from_secs(5)).await;
            }
        });
        Ok(())
    }

    async fn subscribe(&self, symbols: &[String]) -> Result<()> {
        if let Some(tx) = &self.command_tx {
            tx.send(ExchangeCommand::Subscribe(symbols.to_vec()))
                .await?;
        }
        Ok(())
    }

    async fn unsubscribe(&self, symbols: &[String]) -> Result<()> {
        if let Some(tx) = &self.command_tx {
            tx.send(ExchangeCommand::Unsubscribe(symbols.to_vec()))
                .await?;
        }
        Ok(())
    }
}
