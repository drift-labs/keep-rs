use std::time::{SystemTime, UNIX_EPOCH};

use drift_rs::dlob::MakerCrosses;
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

        return true
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
    pub fn label(&self) -> String {
        match self {
            TxIntent::None => "none".to_string(),
            TxIntent::AuctionFill { .. } => "auction_fill".to_string(),
            TxIntent::SwiftFill { .. } => "swift_fill".to_string(),
            TxIntent::LimitUncross { .. } => "limit_uncross".to_string(),
            TxIntent::VAMMTakerFill { .. } => "vamm_taker".to_string(),
        }
    }

    pub fn expected_fill_count(&self) -> usize {
        match self {
            TxIntent::None => 0,
            TxIntent::AuctionFill { maker_crosses, .. } => maker_crosses.orders.len(),
            TxIntent::SwiftFill { maker_crosses, .. } => maker_crosses.orders.len(),
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
