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

pub struct CoinbaseExchange {
    command_tx: Option<Sender<ExchangeCommand>>,
}

impl CoinbaseExchange {
    pub fn new() -> Self {
        Self { command_tx: None }
    }
}

#[derive(Deserialize, Debug)]
struct CoinbaseTickerMessage {
    channel: String,
    events: Vec<CoinbaseEvent>,
}

#[derive(Deserialize, Debug)]
struct CoinbaseEvent {
    tickers: Vec<CoinbaseTicker>,
}

#[derive(Deserialize, Debug)]
struct CoinbaseTicker {
    product_id: String,
    best_bid: String,
    best_ask: String,
}

#[async_trait]
impl Exchange for CoinbaseExchange {
    async fn connect(&mut self, sender: Sender<PriceUpdate>) -> Result<()> {
        let (cmd_tx, mut cmd_rx) = mpsc::channel(100);
        self.command_tx = Some(cmd_tx);

        tokio::spawn(async move {
            let mut active_symbols: Vec<String> = Vec::new();

            loop {
                let url = Url::parse("wss://advanced-trade-ws.coinbase.com").unwrap();
                info!("Connecting to Coinbase Advanced Trade WS: {}", url);

                match connect_async(url.to_string()).await {
                    Ok((mut ws_stream, _)) => {
                        info!("Connected to Coinbase");

                        // Resubscribe
                        if !active_symbols.is_empty() {
                            let subscribe_msg = json!({
                                "type": "subscribe",
                                "product_ids": active_symbols,
                                "channel": "ticker"
                            });
                            if let Err(e) = ws_stream
                                .send(Message::Text(subscribe_msg.to_string().into()))
                                .await
                            {
                                error!("Failed to resubscribe: {}", e);
                            } else {
                                info!("Resubscribed to Coinbase symbols: {:?}", active_symbols);
                            }
                        }

                        loop {
                            tokio::select! {
                                Some(cmd) = cmd_rx.recv() => {
                                    match cmd {
                                        ExchangeCommand::Subscribe(symbols) => {
                                            active_symbols.extend(symbols.clone());
                                            let subscribe_msg = json!({
                                                "type": "subscribe",
                                                "product_ids": symbols,
                                                "channel": "ticker"
                                            });
                                            if let Err(e) = ws_stream.send(Message::Text(subscribe_msg.to_string().into())).await {
                                                error!("Failed to subscribe: {}", e);
                                                break;
                                            }
                                            info!("Subscribed to Coinbase symbols: {:?}", symbols);
                                        }
                                        ExchangeCommand::Unsubscribe(symbols) => {
                                            active_symbols.retain(|s| !symbols.contains(s));
                                            let unsubscribe_msg = json!({
                                                "type": "unsubscribe",
                                                "product_ids": symbols,
                                                "channel": "ticker"
                                            });
                                            if let Err(e) = ws_stream.send(Message::Text(unsubscribe_msg.to_string().into())).await {
                                                error!("Failed to unsubscribe: {}", e);
                                                break;
                                            }
                                            info!("Unsubscribed from Coinbase symbols: {:?}", symbols);
                                        }
                                    }
                                }
                                Some(msg) = ws_stream.next() => {
                                    match msg {
                                        Ok(Message::Text(text)) => {
                                            if let Ok(message) = serde_json::from_str::<CoinbaseTickerMessage>(&text) {
                                                if message.channel == "ticker" {
                                                    for event in message.events {
                                                        for ticker in event.tickers {
                                                            let bid: f64 = ticker.best_bid.parse().unwrap_or(0.0);
                                                            let ask: f64 = ticker.best_ask.parse().unwrap_or(0.0);
                                                            if bid > 0.0 && ask > 0.0 {
                                                                let mid_price = (bid + ask) / 2.0;
                                                                let update = PriceUpdate {
                                                                    symbol: ticker.product_id.clone(),
                                                                    mid_price,
                                                                    exchange: "coinbase".to_string(),
                                                                };
                                                                if let Err(_) = sender.send(update).await {
                                                                    break;
                                                                }
                                                            }
                                                        }
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
                                            error!("Coinbase WS error: {}", e);
                                            break;
                                        }
                                        _ => {}
                                    }
                                }
                                else => break,
                            }
                        }
                    }
                    Err(e) => {
                        error!("Failed to connect to Coinbase: {}", e);
                    }
                }
                warn!("Coinbase connection lost, reconnecting in 5s...");
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
