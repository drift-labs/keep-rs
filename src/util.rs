use std::time::{Duration, SystemTime, UNIX_EPOCH};

use drift_rs::{dlob::MakerCrosses, types::MarketId};
use futures_util::StreamExt;
use pyth_lazer_client::AnyResponse;
use pyth_lazer_protocol::{
    message::Message,
    payload::{PayloadData, PayloadPropertyValue},
    router::{
        Channel, DeliveryFormat, FixedRate, Format, JsonBinaryEncoding, PriceFeedId,
        PriceFeedProperty, SubscriptionParams, SubscriptionParamsRepr,
    },
    subscription::{SubscribeRequest, SubscriptionId},
};
use solana_sdk::signature::Signature;

pub struct OrderSlotLimiter<const N: usize> {
    slots: [Vec<u32>; N],
    generations: [u64; N],
}

impl<const N: usize> OrderSlotLimiter<N> {
    pub fn new() -> Self {
        let slots = std::array::from_fn(|_| Vec::new());
        let generations = [0; N];
        Self { slots, generations }
    }

    pub fn allow_event(&mut self, g: u64, id: u32) -> bool {
        let idx = (g % N as u64) as usize;

        // Replace old generation
        if self.generations[idx] != g {
            self.slots[idx].clear();
            self.generations[idx] = g;
        }

        // Count occurrences of id in generations g - 1 to g - 4
        let mut count = 0;
        for i in 2..=4 {
            let past_g = g.saturating_sub(i);
            let past_idx = (past_g % N as u64) as usize;

            if self.generations[past_idx] == past_g {
                if self.slots[past_idx].binary_search(&id).is_ok() {
                    count += 1;
                    if count >= 1 {
                        // Already appeared once, so this would be the second time
                        return false;
                    }
                }
            }
        }

        // Insert in sorted order
        let slot = &mut self.slots[idx];
        match slot.binary_search(&id) {
            Ok(_) => false, // Already present — shouldn't happen
            Err(pos) => {
                slot.insert(pos, id);
                true
            }
        }
    }

    pub fn check_event(&self, g: u64, id: u32) -> bool {
        // Check generations g - 1 and g - 4
        for i in 1..=4 {
            let past_g = g.saturating_sub(i);
            let past_idx = (past_g % N as u64) as usize;

            if self.generations[past_idx] == past_g {
                if self.slots[past_idx].binary_search(&id).is_ok() {
                    return false;
                }
            }
        }

        return true;
    }
}

#[derive(Clone, Debug, Default)]
pub enum TxIntent {
    #[default]
    None,
    AuctionFill {
        taker_order_id: u32,
        has_trigger: bool,
        maker_crosses: MakerCrosses,
    },
    SwiftFill {
        maker_crosses: MakerCrosses,
    },
    VAMMTakerFill {
        slot: u64,
        market_index: u16,
        maker_order_id: u32,
    },
    /// limit orders crossed
    LimitUncross {
        slot: u64,
        market_index: u16,
        taker_order_id: u32,
        maker_order_id: u32,
    },
}

impl TxIntent {
    pub fn label(&self) -> &'static str {
        match self {
            TxIntent::None => "none",
            TxIntent::AuctionFill { maker_crosses, .. } => {
                if maker_crosses.has_vamm_cross {
                    "auction_fill_vamm"
                } else {
                    "auction_fill"
                }
            }
            TxIntent::SwiftFill { maker_crosses, .. } => {
                if maker_crosses.has_vamm_cross {
                    "swift_fill"
                } else {
                    "swift_fill_vamm"
                }
            }
            TxIntent::LimitUncross { .. } => "limit_uncross",
            TxIntent::VAMMTakerFill { .. } => "vamm_taker",
        }
    }

    pub fn expected_fill_count(&self) -> usize {
        match self {
            TxIntent::None => 0,
            TxIntent::AuctionFill { maker_crosses, .. } => {
                maker_crosses.orders.len() + if maker_crosses.has_vamm_cross { 1 } else { 0 }
            }
            TxIntent::SwiftFill { maker_crosses, .. } => {
                maker_crosses.orders.len() + if maker_crosses.has_vamm_cross { 1 } else { 0 }
            }
            TxIntent::VAMMTakerFill { .. } => 1,
            TxIntent::LimitUncross { .. } => 1,
        }
    }

    /// true if tx was expected to trigger the taker order
    pub fn expected_trigger(&self) -> bool {
        match self {
            TxIntent::AuctionFill { has_trigger, .. } => *has_trigger,
            _ => false,
        }
    }

    pub fn crosses_and_slot(&self) -> (Vec<(drift_rs::dlob::types::OrderMetadata, u64, u64)>, u64) {
        match self {
            TxIntent::None => (vec![], 0),
            TxIntent::AuctionFill { maker_crosses, .. } => (
                maker_crosses
                    .orders
                    .iter()
                    .map(|(meta, price, size)| (*meta, *price, *size))
                    .collect(),
                maker_crosses.slot,
            ),
            TxIntent::SwiftFill { maker_crosses, .. } => (
                maker_crosses
                    .orders
                    .iter()
                    .map(|(meta, price, size)| (*meta, *price, *size))
                    .collect(),
                maker_crosses.slot,
            ),
            Self::VAMMTakerFill { slot, .. } => (vec![], *slot),
            Self::LimitUncross { slot, .. } => (vec![], *slot),
        }
    }
}

#[derive(Clone, Default, Debug)]
pub struct PendingTxMeta {
    pub signature: Signature,
    pub intent: TxIntent,
    pub cu_limit: u64,
    pub ts: u64,
}

impl PendingTxMeta {
    pub fn new(sig: Signature, intent: TxIntent, cu_limit: u64) -> Self {
        Self {
            signature: sig,
            ts: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64,
            intent,
            cu_limit,
        }
    }
}

/// Circular buffer for pending transactions or similar FIFO workloads.
///
/// Usage example:
/// ```
/// let mut buf: PendingTxs<1024> = PendingTxs::new();
/// buf.insert(meta);
/// let confirmed = buf.confirm(|m| m.signature == sig);
/// ```
pub struct PendingTxs<const N: usize> {
    buffer: [PendingTxMeta; N],
    head: usize,
    tail: usize,
    size: usize,
}

impl<const N: usize> PendingTxs<N> {
    pub fn new() -> Self {
        Self {
            buffer: [(); N].map(|_| PendingTxMeta::default()),
            head: 0,
            tail: 0,
            size: 0,
        }
    }

    /// Insert a new item, overwriting the oldest if full.
    pub fn insert(&mut self, item: PendingTxMeta) {
        self.buffer[self.tail] = item;
        self.tail = (self.tail + 1) % N;
        if self.size == N {
            self.head = (self.head + 1) % N;
        } else {
            self.size += 1;
        }
    }

    /// Confirm and return the first item with matching signature.
    ///
    /// Returns Some(item) if found, else None.
    pub fn confirm(&mut self, sig: &Signature) -> Option<PendingTxMeta> {
        for i in 0..self.size {
            let idx = (self.head + i) % N;
            // TODO: check if overwritten entry is confirmed or not
            if self.buffer[idx].signature == *sig {
                return Some(self.buffer[idx].clone());
            }
        }
        None
    }
}

pub struct PythPriceUpdate {
    pub market_id: u16,
    pub feed_id: u32,
    pub price: u64,
}

pub fn subscribe_price_feeds(
    mut cli: pyth_lazer_client::LazerClient,
    market_ids: &[MarketId],
) -> tokio::sync::mpsc::Receiver<PythPriceUpdate> {
    let feed_ids: Vec<PriceFeedId> = market_ids
        .iter()
        .filter_map(|m| {
            drift_rs::constants::perp_market_index_to_pyth_lazer_feed_id(m.index()).map(PriceFeedId)
        })
        .collect();

    let (price_tx, price_rx) = tokio::sync::mpsc::channel(512);
    tokio::spawn(async move {
        loop {
            let pyth_lazer_stream = match cli.start().await {
                Ok(stream) => stream,
                Err(err) => {
                    log::error!(target: "filler", "pyth feed start failed: {err:?}");
                    tokio::time::sleep(Duration::from_secs(1)).await;
                    continue;
                }
            };

            let subscribe_request = SubscribeRequest {
                subscription_id: SubscriptionId(23),
                params: SubscriptionParams::new(SubscriptionParamsRepr {
                    price_feed_ids: feed_ids.clone(),
                    properties: vec![PriceFeedProperty::Price],
                    delivery_format: DeliveryFormat::Binary,
                    json_binary_encoding: JsonBinaryEncoding::Base64,
                    parsed: false,
                    channel: Channel::FixedRate(FixedRate::MIN),
                    formats: vec![Format::Solana],
                    ignore_invalid_feed_ids: false,
                })
                .expect("invalid subscription params"),
            };
            if let Err(err) = cli
                .subscribe(pyth_lazer_protocol::subscription::Request::Subscribe(
                    subscribe_request,
                ))
                .await
            {
                log::error!(target: "filler", "pyth feed subscribe failed: {err:?}");
                tokio::time::sleep(Duration::from_secs(1)).await;
                continue;
            }

            let mut stream = pyth_lazer_stream.boxed();
            while let Some(Ok(update)) = stream.next().await {
                match update {
                    AnyResponse::Binary(outer) => {
                        for message in outer.messages {
                            match message {
                                Message::Solana(message) => {
                                    let data = PayloadData::deserialize_slice_le(&message.payload)
                                        .unwrap();

                                    for f in data.feeds {
                                        for p in f.properties {
                                            if let PayloadPropertyValue::Price(Some(price)) = p {
                                                price_tx.try_send(PythPriceUpdate {
                                                    market_id: drift_rs::constants::pyth_lazer_feed_id_to_perp_market_index(f.feed_id.0).expect("feed maps to market"),
                                                    feed_id: f.feed_id.0,
                                                    price: price.0.unsigned_abs().into(),
                                                });
                                            }
                                        }
                                    }
                                }
                                _ => (),
                            }
                        }
                    }
                    _ => (),
                }
            }
            log::error!(target: "filler", "pyth lazer feed finished");
        }
    });

    price_rx
}
