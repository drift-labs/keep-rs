//! Example Liquidator Bot
//!
//! Subscribes to drift accounts, market, and oracles via gRPC.
//! Identifies liquidatable accounts and forwards them to a strategy impl
//! for processing.
//!
//! The default strategy tries to liquidate perp positions against resting orders
//!
use anchor_lang::Discriminator;
use futures_util::FutureExt;
use std::{
    collections::{BTreeMap, HashMap, HashSet},
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};
use tokio::sync::mpsc::error::TryRecvError;

use drift_rs::{
    dlob::{DLOBNotifier, DLOB},
    ffi::{OraclePriceData, SimplifiedMarginCalculation},
    grpc::{grpc_subscriber::AccountFilter, TransactionUpdate},
    jupiter::SwapMode,
    market_state::MarketStateData,
    priority_fee_subscriber::PriorityFeeSubscriber,
    titan,
    types::{
        accounts::{PerpMarket, SpotMarket, User},
        MarginRequirementType, MarketId, MarketStatus, MarketType, OracleSource, Order,
        OrderStatus, PerpPosition, SpotBalanceType, SpotPosition,
    },
    DriftClient, GrpcSubscribeOpts, MarketState, Pubkey, TransactionBuilder,
};
use drift_rs::{jupiter::JupiterSwapApi, titan::TitanSwapApi};
use solana_sdk::{account::Account, clock::Slot, compute_budget::ComputeBudgetInstruction};

use crate::{
    filler::{TxSender, TxWorker},
    http::Metrics,
    util::{PythPriceUpdate, TxIntent},
    Config, UseMarkets,
};

/// min slots between successive liquidation attempts on same user
const LIQUIDATION_SLOT_RATE_LIMIT: u64 = 20; // ~8s

/// Maximum time allowed for a liquidation attempt in milliseconds
const LIQUIDATION_DEADLINE_MS: u64 = 1_000;

/// Maximum age for liquidation entries in milliseconds
const MAX_LIQUIDATION_AGE_MS: u64 = 1_000;

/// Maximum concurrent liquidation tasks to prevent system overload
const MAX_CONCURRENT_LIQUIDATIONS: usize = 20;

const TARGET: &str = "liquidator";

/// Threshold for considering a user high-risk: free margin < 20% of margin requirement
const HIGH_RISK_FREE_MARGIN_RATIO: f64 = 0.2;

/// Margin status indicating liquidation risk level
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MarginStatus {
    /// User is liquidatable (total_collateral < margin_requirement)
    Liquidatable,
    /// User is high-risk but not yet liquidatable (free margin < 20% of margin requirement)
    HighRisk,
    /// User is safe (not liquidatable and not high-risk)
    Safe,
}

/// Helper to get current time in milliseconds since epoch
fn current_time_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

/// Check margin status (liquidatable, high-risk, or safe)
fn check_margin_status(margin_info: &SimplifiedMarginCalculation) -> MarginStatus {
    const LIQUIDATION_BUFFER: f64 = 0.99;

    let buffered_margin_req = (margin_info.margin_requirement as f64 * LIQUIDATION_BUFFER) as i128;

    if margin_info.total_collateral < buffered_margin_req {
        return MarginStatus::Liquidatable;
    }

    // Calculate free margin
    let free_margin = margin_info.total_collateral - margin_info.margin_requirement as i128;

    // User is high-risk if free margin < 20% of margin requirement
    if margin_info.margin_requirement > 0 && free_margin > 0 {
        let free_margin_ratio = free_margin as f64 / margin_info.margin_requirement as f64;
        if free_margin_ratio < HIGH_RISK_FREE_MARGIN_RATIO {
            return MarginStatus::HighRisk;
        }
    }
    MarginStatus::Safe
}

/// Trait for pluggable liquidation strategies
pub trait LiquidationStrategy {
    /// Execute liquidation logic a user, including selecting makers and sending txs.
    fn liquidate_user<'a>(
        &'a self,
        liquidatee: Pubkey,
        user_account: Arc<User>,
        tx_sender: TxSender,
        priority_fee: u64,
        cu_limit: u32,
        slot: u64,
        pyth_price_update: Option<PythPriceUpdate>,
    ) -> futures_util::future::BoxFuture<'a, ()>;
}

pub enum GrpcEvent {
    OracleUpdate {
        oracle_price_data: OraclePriceData,
        market: MarketId,
        slot: Slot,
    },
    SpotMarketUpdate {
        market: SpotMarket,
        slot: Slot,
    },
    PerpMarketUpdate {
        market: PerpMarket,
        slot: Slot,
    },
    UserUpdate {
        pubkey: Pubkey,
        user: User,
        slot: Slot,
    },
}

pub struct LiquidatorBot {
    drift: DriftClient,
    dlob_notifier: DLOBNotifier,
    config: Config,
    /// stores drift perp+spot market metadata and oracle prices
    market_state: &'static MarketState,
    /// receives new updates from grpc
    events_rx: tokio::sync::mpsc::Receiver<GrpcEvent>,
    /// sends liquidatable accounts to work thread (pubkey, user, slot, timestamp_ms, pyth price)
    liq_tx: tokio::sync::mpsc::Sender<(Pubkey, User, u64, u64, Option<PythPriceUpdate>)>,
    pyth_price_feed: tokio::sync::mpsc::Receiver<PythPriceUpdate>,
}

impl LiquidatorBot {
    pub async fn new(config: Config, drift: DriftClient, metrics: Arc<Metrics>) -> Self {
        let dlob: &'static DLOB = Box::leak(Box::new(DLOB::default()));
        let tx_worker = TxWorker::new(drift.clone(), Arc::clone(&metrics), config.dry);
        let rt = tokio::runtime::Handle::current();
        let tx_sender = tx_worker.run(rt);

        let mut perp_market_ids = match config.use_markets() {
            UseMarkets::All => drift.get_all_perp_market_ids(),
            UseMarkets::Subset(m) => m,
        };

        let spot_market_ids: Vec<MarketId> = drift
            .program_data()
            .spot_market_configs()
            .iter()
            .map(|m| MarketId::spot(m.market_index))
            .collect();

        // remove bet perp markets
        perp_market_ids.retain(|x| {
            let market = drift
                .program_data()
                .perp_market_config_by_index(x.index())
                .unwrap();
            let name = core::str::from_utf8(&market.name)
                .unwrap()
                .to_ascii_lowercase();

            !name.contains("bet") && market.status != MarketStatus::Initialized
        });

        let market_pubkeys: Vec<Pubkey> = perp_market_ids
            .iter()
            .map(|x| {
                drift
                    .program_data()
                    .perp_market_config_by_index(x.index())
                    .unwrap()
                    .pubkey
            })
            .collect();

        let priority_fee_subscriber =
            PriorityFeeSubscriber::new(drift.rpc().url(), &market_pubkeys);
        let priority_fee_subscriber = priority_fee_subscriber.subscribe();

        let keeper_subaccount = drift.wallet.sub_account(config.sub_account_id);
        log::info!(target: TARGET, "liquidator 🫠 bot started: authority={:?}, subaccount={:?}", drift.wallet.authority(), keeper_subaccount);

        drift.subscribe_blockhashes().await.expect("subscribed");
        let dlob_notifier = dlob.spawn_notifier();
        let events_rx = setup_grpc(
            drift.clone(),
            dlob_notifier.clone(),
            tx_sender.clone(),
            perp_market_ids.clone(),
        )
        .await;
        log::info!(target: TARGET, "subscribed gRPC");

        // populate market data
        let mut market_state = MarketStateData::default();
        // Only use pyth price when it differs from oracle by >5 bps
        market_state.pyth_oracle_diff_threshold_bps = 5;

        for market in drift.program_data().perp_market_configs() {
            market_state.set_perp_market(*market);
            if let Some(oracle) = drift
                .backend()
                .oracle_map()
                .get_by_market(&MarketId::perp(market.market_index))
            {
                market_state.set_perp_oracle_price(market.market_index, oracle.data);
            }
        }
        for market in drift.program_data().spot_market_configs() {
            market_state.set_spot_market(*market);
            if let Some(oracle) = drift
                .backend()
                .oracle_map()
                .get_by_market(&MarketId::spot(market.market_index))
            {
                market_state.set_spot_oracle_price(market.market_index, oracle.data);
            }
        }
        let market_state: &'static MarketState =
            Box::leak(Box::new(MarketState::new(market_state)));

        let pyth_access_token = std::env::var("PYTH_LAZER_TOKEN").expect("pyth access token");
        let pyth_feed_cli = pyth_lazer_client::LazerClient::new(
            "wss://pyth-lazer.dourolabs.app/v1/stream",
            pyth_access_token.as_str(),
        )
        .expect("pyth price feed connects");
        let pyth_price_feed =
            crate::util::subscribe_price_feeds(pyth_feed_cli, &perp_market_ids, &spot_market_ids);
        log::info!(target: TARGET, "subscribed pyth price feeds");

        // start liquidation worker
        let (liq_tx, liq_rx) =
            tokio::sync::mpsc::channel::<(Pubkey, User, u64, u64, Option<PythPriceUpdate>)>(1024);
        spawn_liquidation_worker(
            tx_sender.clone(),
            // TODO: apply your own liquidation strategy here
            Arc::new(LiquidateWithMatchStrategy {
                dlob,
                drift: drift.clone(),
                market_state,
                keeper_subaccount,
                metrics: Arc::clone(&metrics),
            }),
            liq_rx,
            std::env::var("FILL_CU_LIMIT")
                .ok()
                .and_then(|s| s.parse::<u32>().ok())
                .unwrap_or(config.fill_cu_limit),
            Arc::clone(&priority_fee_subscriber),
        );

        LiquidatorBot {
            drift,
            dlob_notifier,
            events_rx,
            config,
            market_state,
            liq_tx,
            pyth_price_feed,
        }
    }

    pub async fn run(self) {
        let mut events_rx = self.events_rx;
        let drift: &'static DriftClient = Box::leak(Box::new(self.drift));
        let config = self.config.clone();
        let dlob_notifier = self.dlob_notifier;
        let mut current_slot = 0;
        let mut users = BTreeMap::<Pubkey, User>::new();
        let mut high_risk = HashSet::<Pubkey>::new();
        let liquidation_margin_buffer_ratio = drift
            .state_account()
            .map(|x| x.liquidation_margin_buffer_ratio)
            .expect("State has liquidation_margin_buffer_ratio");

        const RECHECK_CYCLE_INTERVAL: u32 = 1024;

        let mut cycle_count = 0u32;

        // initialize local User storage
        let mut exclude_count = 0;
        let mut initial_high_risk_count = 0;
        drift
            .backend()
            .account_map()
            .iter_accounts_with::<User>(|pubkey, user, _slot| {
                let margin_info = self
                    .market_state
                    .calculate_simplified_margin_requirement(
                        user,
                        MarginRequirementType::Maintenance,
                        Some(liquidation_margin_buffer_ratio),
                    ).unwrap();

                if margin_info.total_collateral < config.min_collateral as i128
                    && margin_info.margin_requirement < config.min_collateral as u128
                {
                    exclude_count+=1;
                    log::debug!(target: TARGET, "excluding user: {:?}. insignificant collateral: {}/{}", user.authority, margin_info.total_collateral, margin_info.margin_requirement);
                } else {
                    users.insert(*pubkey, *user);
                    // Check margin status and add to high-risk set if needed
                    match check_margin_status(&margin_info) {
                        MarginStatus::Liquidatable | MarginStatus::HighRisk => {
                            high_risk.insert(*pubkey);
                            initial_high_risk_count += 1;
                        }
                        MarginStatus::Safe => {}
                    }
                }
            });

        log::info!(target: TARGET, "filtered #{exclude_count} accounts with dust collateral");
        log::info!(target: TARGET, "identified #{initial_high_risk_count} high-risk accounts for monitoring");

        // main loop
        let mut event_buffer = Vec::<GrpcEvent>::with_capacity(64);
        let mut oracle_update;

        // Pyth Feed
        let mut pyth_price_feed = self.pyth_price_feed;
        let mut pyth_perp_prices = BTreeMap::<u16, PythPriceUpdate>::new();

        loop {
            oracle_update = false;

            // Drain pyth updates first (non-blocking)
            loop {
                match pyth_price_feed.try_recv() {
                    Ok(update) => {
                        let market_id = update.market_id;
                        let price = update.price;

                        match update.market_type {
                            MarketType::Perp => {
                                pyth_perp_prices.insert(market_id, update);
                                // Update market state with perp pyth price for margin calculation
                                self.market_state
                                    .set_perp_pyth_price(market_id, price as i64);
                            }
                            MarketType::Spot => {
                                // Update market state with spot pyth price for margin calculation
                                self.market_state
                                    .set_spot_pyth_price(market_id, price as i64);
                            }
                        }
                    }
                    Err(TryRecvError::Disconnected) => {
                        log::error!(target: TARGET, "pyth price feed disconnected");
                        return;
                    }
                    Err(TryRecvError::Empty) => break,
                }
            }

            let n_read = events_rx.recv_many(&mut event_buffer, 64).await;
            log::trace!(target: TARGET, "read: {n_read}, remaning: {:?}", events_rx.len());
            for event in event_buffer.drain(..) {
                match event {
                    GrpcEvent::UserUpdate {
                        pubkey,
                        user,
                        slot: update_slot,
                    } => {
                        if update_slot >= current_slot {
                            dlob_notifier.user_update(
                                pubkey,
                                users.get(&pubkey),
                                &user,
                                update_slot,
                            );
                            users.insert(pubkey, user.clone());
                            // calculate user margin after update
                            let margin_info = self
                                .market_state
                                .calculate_simplified_margin_requirement(
                                    &user,
                                    MarginRequirementType::Maintenance,
                                    Some(liquidation_margin_buffer_ratio),
                                )
                                .unwrap();

                            if margin_info.total_collateral < config.min_collateral as i128
                                && margin_info.margin_requirement < config.min_collateral as u128
                            {
                                log::trace!(target: TARGET, "filtered account with dust collateral: {pubkey:?}");
                                // Remove from high_risk if present (user is now excluded)
                                high_risk.remove(&pubkey);
                            } else {
                                match check_margin_status(&margin_info) {
                                    MarginStatus::Liquidatable => {
                                        log::debug!(target: TARGET, "found liquidatable user: {pubkey:?}, margin:{margin_info:?}");
                                        high_risk.insert(pubkey);
                                        // let pyth_price_update = user
                                        //     .perp_positions
                                        //     .iter()
                                        //     .filter(|p| p.base_asset_amount != 0)
                                        //     .max_by_key(|p| p.quote_asset_amount.abs())
                                        //     .and_then(|p| pyth_perp_prices.get(&p.market_index).cloned());
                                        //
                                        // self.liq_tx.try_send((
                                        //     pubkey,
                                        //     user.clone(),
                                        //     current_slot,
                                        //     current_time_millis(),
                                        //     pyth_price_update,
                                        // ));
                                    }
                                    MarginStatus::HighRisk => {
                                        high_risk.insert(pubkey);
                                    }
                                    MarginStatus::Safe => {
                                        high_risk.remove(&pubkey);
                                    }
                                }
                            }
                        }
                    }
                    GrpcEvent::OracleUpdate {
                        oracle_price_data,
                        market,
                        slot,
                    } => {
                        if slot >= current_slot {
                            if market.is_perp() {
                                self.market_state
                                    .set_perp_oracle_price(market.index(), oracle_price_data);
                            } else {
                                self.market_state
                                    .set_spot_oracle_price(market.index(), oracle_price_data);
                            }
                            current_slot = slot;
                            oracle_update = true;
                        }
                    }
                    GrpcEvent::PerpMarketUpdate { market, slot } => {
                        if slot >= current_slot {
                            self.market_state.set_perp_market(market);
                            current_slot = slot;
                        }
                    }
                    GrpcEvent::SpotMarketUpdate { market, slot } => {
                        if slot >= current_slot {
                            self.market_state.set_spot_market(market);
                            current_slot = slot;
                        }
                    }
                }
            }

            // Only recheck margin for high-risk users on oracle price updates
            // Process in batches to avoid blocking the main event loop
            if oracle_update {
                let high_risk_list: Vec<Pubkey> = high_risk.iter().copied().collect();
                let high_risk_count = high_risk_list.len();

                // Spawn async task to check high-risk users without blocking event loop
                let users_clone = users.clone();
                let market_state = self.market_state;
                let liq_tx_clone = self.liq_tx.clone();
                let pyth_perp_prices_clone = pyth_perp_prices.clone();

                let current_slot_clone = current_slot;

                tokio::spawn(async move {
                    let t0 = current_time_millis();
                    let mut liquidatable_users = Vec::new();

                    for pubkey in high_risk_list {
                        if let Some(user) = users_clone.get(&pubkey) {
                            let margin_info = market_state
                                .calculate_simplified_margin_requirement(
                                    user,
                                    MarginRequirementType::Maintenance,
                                    Some(liquidation_margin_buffer_ratio),
                                )
                                .unwrap();

                            match check_margin_status(&margin_info) {
                                MarginStatus::Liquidatable => {
                                    log::debug!(target: TARGET, "found liquidatable user: {pubkey:?}, margin:{margin_info:?}");

                                    // Collect spot market IDs from open spot positions
                                    let mut spot_market_ids: HashSet<u16> = user
                                        .spot_positions
                                        .iter()
                                        .filter(|p| !p.is_available())
                                        .map(|p| p.market_index)
                                        .collect();

                                    // Collect perp market IDs from open perp positions
                                    let mut perp_market_ids: HashSet<u16> = user
                                        .perp_positions
                                        .iter()
                                        .filter(|p| !p.is_available())
                                        .map(|p| p.market_index)
                                        .collect();

                                    // Add market IDs from orders to the same sets
                                    for order in
                                        user.orders.iter().filter(|o| o.base_asset_amount != 0)
                                    {
                                        match order.market_type {
                                            MarketType::Spot => {
                                                spot_market_ids.insert(order.market_index);
                                            }
                                            MarketType::Perp => {
                                                perp_market_ids.insert(order.market_index);
                                            }
                                        }
                                    }

                                    // Log market IDs
                                    log::info!(
                                        target: TARGET,
                                        "user {:?} (sub: {}) slot: {current_slot_clone}, spot positions: {:?}, perp positions: {:?}, orders: {:?}",
                                        user.authority,
                                        user.sub_account_id,
                                        user.spot_positions.iter().filter(|p| spot_market_ids.contains(&p.market_index) && !p.is_available()).collect::<Vec<&SpotPosition>>(),
                                        user.perp_positions.iter().filter(|p| perp_market_ids.contains(&p.market_index)).collect::<Vec<&PerpPosition>>(),
                                        user.orders.iter().filter(|o| o.base_asset_amount != 0 && o.status == OrderStatus::Open).collect::<Vec<&Order>>(),
                                    );

                                    // Log oracle prices for spot markets
                                    for market_index in &spot_market_ids {
                                        if let Some(oracle_price) =
                                            market_state.get_spot_oracle_price(*market_index)
                                        {
                                            log::info!(
                                                target: TARGET,
                                                "spot market {} oracle price: {} (confidence: {}, delay: {})",
                                                market_index,
                                                oracle_price.price,
                                                oracle_price.confidence,
                                                oracle_price.delay
                                            );
                                        } else {
                                            log::warn!(
                                                target: TARGET,
                                                "spot market {} oracle price not available",
                                                market_index
                                            );
                                        }
                                    }

                                    // Log oracle prices for perp markets
                                    for market_index in &perp_market_ids {
                                        if let Some(oracle_price) =
                                            market_state.get_perp_oracle_price(*market_index)
                                        {
                                            log::info!(
                                                target: TARGET,
                                                "perp market {} oracle price: {} (confidence: {}, delay: {})",
                                                market_index,
                                                oracle_price.price,
                                                oracle_price.confidence,
                                                oracle_price.delay
                                            );
                                        } else {
                                            log::warn!(
                                                target: TARGET,
                                                "perp market {} oracle price not available",
                                                market_index
                                            );
                                        }
                                    }

                                    liquidatable_users.push((pubkey, user.clone()));
                                }
                                _ => {}
                            }
                        }
                    }

                    // Send liquidations outside the loop
                    for (pubkey, user) in liquidatable_users {
                        let pyth_price_update = user
                            .perp_positions
                            .iter()
                            .filter(|p| p.base_asset_amount != 0)
                            .max_by_key(|p| p.quote_asset_amount.abs())
                            .and_then(|p| pyth_perp_prices_clone.get(&p.market_index).cloned());

                        match liq_tx_clone.try_send((
                            pubkey,
                            user,
                            current_slot_clone,
                            current_time_millis(),
                            pyth_price_update,
                        )) {
                            Ok(()) => {}
                            Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {
                                log::warn!(
                                    target: TARGET,
                                    "liquidation channel full, dropping liquidation for {:?}",
                                    pubkey
                                );
                            }
                            Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
                                log::error!(target: TARGET, "liquidation channel closed");
                            }
                        }
                    }

                    log::trace!(
                        target: TARGET,
                        "processed {} high-risk margin updates in {}ms",
                        high_risk_count,
                        current_time_millis() - t0,
                    );
                });

                // Update high_risk set synchronously but quickly (just remove safe users)
                let t0 = current_time_millis();
                high_risk.retain(|pubkey| {
                    if let Some(user) = users.get(pubkey) {
                        let margin_info = self
                            .market_state
                            .calculate_simplified_margin_requirement(
                                user,
                                MarginRequirementType::Maintenance,
                                Some(liquidation_margin_buffer_ratio),
                            )
                            .unwrap();

                        match check_margin_status(&margin_info) {
                            MarginStatus::Liquidatable | MarginStatus::HighRisk => true,
                            MarginStatus::Safe => false,
                        }
                    } else {
                        false
                    }
                });

                log::trace!(
                    target: TARGET,
                    "updated high_risk set in {}ms (now {} users)",
                    current_time_millis() - t0,
                    high_risk.len(),
                );
            }

            // Every RECHECK_CYCLE_INTERVAL cycles, recheck all users to find new high-risk users
            cycle_count += 1;
            if cycle_count % RECHECK_CYCLE_INTERVAL == 0 {
                let t0 = current_time_millis();
                let mut newly_high_risk = 0;

                for (pubkey, user) in users.iter() {
                    if high_risk.contains(pubkey) {
                        continue;
                    }

                    let margin_info = self
                        .market_state
                        .calculate_simplified_margin_requirement(
                            user,
                            MarginRequirementType::Maintenance,
                            Some(liquidation_margin_buffer_ratio),
                        )
                        .unwrap();

                    match check_margin_status(&margin_info) {
                        MarginStatus::Liquidatable => {
                            high_risk.insert(*pubkey);
                            newly_high_risk += 1;
                            log::debug!(target: TARGET, "found liquidatable user: {pubkey:?}, margin:{margin_info:?}");

                            let pyth_price_update = user
                                .perp_positions
                                .iter()
                                .filter(|p| p.base_asset_amount != 0)
                                .max_by_key(|p| p.quote_asset_amount.abs())
                                .and_then(|p| pyth_perp_prices.get(&p.market_index).cloned());

                            match self.liq_tx.try_send((
                                *pubkey,
                                user.clone(),
                                current_slot,
                                current_time_millis(),
                                pyth_price_update,
                            )) {
                                Ok(()) => {}
                                Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {
                                    log::warn!(
                                        target: TARGET,
                                        "liquidation channel full, dropping liquidation for {:?}",
                                        pubkey
                                    );
                                }
                                Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
                                    log::error!(target: TARGET, "liquidation channel closed");
                                }
                            }
                        }
                        MarginStatus::HighRisk => {
                            high_risk.insert(*pubkey);
                            newly_high_risk += 1;
                        }
                        MarginStatus::Safe => {}
                    }
                }

                log::debug!(
                    target: TARGET,
                    "margin recheck: checked {} users, {newly_high_risk} newly high-risk, took {}ms",
                    users.len(),
                    current_time_millis() - t0
                );
            }
        }
    }
}

fn on_transaction_update_fn(
    tx_sender: TxSender,
) -> impl Fn(&TransactionUpdate) + Send + Sync + 'static {
    move |tx: &TransactionUpdate| {
        if let Some(sig) = tx.transaction.signatures.first() {
            tx_sender.confirm_tx((sig.as_slice().try_into()).expect("valid signature"));
        } else {
            log::warn!(target: TARGET, "received tx without sig: {tx:?}");
        }
    }
}

fn on_slot_update_fn(
    dlob_notifier: DLOBNotifier,
    drift: DriftClient,
    market_ids: &[MarketId],
) -> impl Fn(u64) + Send + Sync + 'static {
    let market_ids: Vec<MarketId> = market_ids.to_vec();
    move |new_slot| {
        for market in market_ids.iter() {
            let oracle_price_data = drift
                .try_get_mmoracle_for_perp_market(market.index(), new_slot)
                .unwrap();
            dlob_notifier.slot_and_oracle_update(*market, new_slot, oracle_price_data.price as u64);
        }
    }
}

async fn setup_grpc(
    drift: DriftClient,
    dlob_notifier: DLOBNotifier,
    transaction_tx: TxSender,
    market_ids: Vec<MarketId>,
) -> tokio::sync::mpsc::Receiver<GrpcEvent> {
    let (tx, rx) = tokio::sync::mpsc::channel(1024);

    let _ = tokio::try_join!(
        crate::filler::sync_stats_accounts(&drift),
        crate::filler::sync_user_accounts(&drift, &dlob_notifier),
    );

    let mut oracle_to_market = HashMap::<Pubkey, Vec<(MarketId, OracleSource)>>::default();

    for (market, (oracle, source)) in drift.backend().oracle_map().oracle_by_market.iter() {
        oracle_to_market
            .entry(*oracle)
            .and_modify(|f| f.push((*market, *source)))
            .or_insert(vec![(*market, *source)]);
    }

    let _res = drift
        .grpc_subscribe(
            std::env::var("GRPC_ENDPOINT")
                .unwrap_or_else(|_| "https://api.rpcpool.com".to_string())
                .into(),
            std::env::var("GRPC_X_TOKEN").expect("GRPC_X_TOKEN set"),
            GrpcSubscribeOpts::default()
                .commitment(solana_sdk::commitment_config::CommitmentLevel::Processed)
                .transaction_include_accounts(vec![drift.wallet().default_sub_account()])
                .on_transaction(on_transaction_update_fn(transaction_tx.clone()))
                .on_slot(on_slot_update_fn(
                    dlob_notifier,
                    drift.clone(),
                    market_ids.as_ref(),
                ))
                .usermap_on()
                .statsmap_on()
                .on_account(
                    AccountFilter::partial().with_discriminator(User::DISCRIMINATOR),
                    {
                        let tx = tx.clone();
                        move |acc| {
                            let user: &User = drift_rs::utils::deser_zero_copy(&acc.data);
                            if let Err(err) = tx.try_send(GrpcEvent::UserUpdate {
                                pubkey: acc.pubkey,
                                user: user.clone(),
                                slot: acc.slot,
                            }) {
                                log::error!(target: TARGET, "failed to forward event: {err:?}");
                            }
                        }
                    },
                )
                .on_account(
                    AccountFilter::partial().with_discriminator(PerpMarket::DISCRIMINATOR),
                    {
                        let tx = tx.clone();
                        move |acc| {
                            let market: &PerpMarket = drift_rs::utils::deser_zero_copy(&acc.data);
                            if let Err(err) = tx.try_send(GrpcEvent::PerpMarketUpdate {
                                market: market.clone(),
                                slot: acc.slot,
                            }) {
                                log::error!(target: TARGET, "failed to forward event: {err:?}");
                            }
                        }
                    },
                )
                .on_account(
                    AccountFilter::partial().with_discriminator(SpotMarket::DISCRIMINATOR),
                    {
                        let tx = tx.clone();
                        move |acc| {
                            let market: &SpotMarket = drift_rs::utils::deser_zero_copy(acc.data);
                            if let Err(err) = tx.try_send(GrpcEvent::SpotMarketUpdate {
                                market: market.clone(),
                                slot: acc.slot,
                            }) {
                                log::error!(target: TARGET, "failed to forward event: {err:?}");
                            }
                        }
                    },
                )
                .on_oracle_update({
                    let tx = tx.clone();
                    move |acc| {
                        let oracle_markets = oracle_to_market
                            .get(&acc.pubkey)
                            .expect("oracle pubkey known");
                        let lamports = acc.lamports;
                        let slot = acc.slot;
                        for (market, oracle_source) in oracle_markets {
                            let oracle_price_data = drift_rs::ffi::get_oracle_price(
                                *oracle_source,
                                &mut (
                                    acc.pubkey,
                                    Account {
                                        owner: acc.owner,
                                        data: acc.data.to_vec(),
                                        lamports,
                                        executable: false,
                                        rent_epoch: u64::MAX,
                                    },
                                ),
                                slot,
                            )
                            .unwrap();
                            if let Err(err) = tx.try_send(GrpcEvent::OracleUpdate {
                                oracle_price_data,
                                market: *market,
                                slot: acc.slot,
                            }) {
                                log::error!(target: TARGET, "failed to forward event: {err:?}");
                            }
                        }
                    }
                }),
            true,
        )
        .await;

    rx
}

/// Try to fill liquidation with order match
fn try_liquidate_with_match(
    drift: &DriftClient,
    market_index: u16,
    keeper_subaccount: Pubkey,
    liquidatee_subaccount: Pubkey,
    top_makers: &[User],
    tx_sender: TxSender,
    priority_fee: u64,
    cu_limit: u32,
    slot: u64,
    pyth_price_update: Option<PythPriceUpdate>,
) {
    if top_makers.is_empty() {
        log::debug!(target: TARGET, "skip empty maker cross. market={market_index} user={liquidatee_subaccount}");
        return;
    }

    let keeper_account_data = drift.try_get_account::<User>(&keeper_subaccount);
    if keeper_account_data.is_err() {
        log::debug!(target: TARGET, "keeper acc lookup failed={keeper_subaccount:?}");
        return;
    }
    let liquidatee_subaccount_data = drift.try_get_account::<User>(&liquidatee_subaccount);
    if liquidatee_subaccount_data.is_err() {
        log::debug!(target: TARGET, "liquidatee acc lookup failed={liquidatee_subaccount:?}");
        return;
    }

    let mut tx_builder = TransactionBuilder::new(
        drift.program_data(),
        keeper_subaccount,
        std::borrow::Cow::Owned(keeper_account_data.unwrap()),
        false,
    )
    .with_priority_fee(priority_fee, Some(cu_limit));

    if let Some(ref update) = pyth_price_update {
        tx_builder = tx_builder.post_pyth_lazer_oracle_update(&[update.feed_id], &update.message);
    }

    tx_builder = tx_builder.liquidate_perp_with_fill(
        market_index,
        &liquidatee_subaccount_data.unwrap(),
        top_makers,
    );

    // large accounts list, bump CU limit to compensate
    if let Some(ix) = tx_builder.ixs().last() {
        if ix.accounts.len() >= 20 {
            tx_builder = tx_builder.set_ix(
                1,
                ComputeBudgetInstruction::set_compute_unit_limit(cu_limit * 2),
            );
        }
    }

    let tx = tx_builder.build();

    tx_sender.send_tx(
        tx,
        TxIntent::LiquidateWithFill {
            market_index,
            liquidatee: liquidatee_subaccount,
            slot,
        },
        cu_limit as u64,
    );
}

fn spawn_liquidation_worker(
    tx_sender: TxSender,
    strategy: Arc<dyn LiquidationStrategy + Send + Sync>,
    mut liq_rx: tokio::sync::mpsc::Receiver<(Pubkey, User, u64, u64, Option<PythPriceUpdate>)>,
    cu_limit: u32,
    priority_fee_subscriber: Arc<PriorityFeeSubscriber>,
) {
    let mut rate_limit = HashMap::<Pubkey, u64>::new();

    tokio::spawn(async move {
        while let Some((liquidatee, user_account, slot, ts, pyth_price_update)) =
            liq_rx.recv().await
        {
            // Drop entries older than 1 second to handle backpressure
            let now = current_time_millis();
            if now.saturating_sub(ts) > MAX_LIQUIDATION_AGE_MS {
                log::debug!(
                    target: TARGET,
                    "dropping stale liquidation for {:?} (age: {}ms)",
                    liquidatee,
                    now.saturating_sub(ts)
                );
                continue;
            }

            if rate_limit
                .get(&liquidatee)
                .is_some_and(|last| slot.abs_diff(*last) < LIQUIDATION_SLOT_RATE_LIMIT)
            {
                log::trace!(target: TARGET, "rate limited liquidation for {:?} (current: {})", liquidatee, slot);
                continue;
            } else {
                rate_limit.insert(liquidatee, slot);
            }

            let pf = priority_fee_subscriber.priority_fee_nth(0.6);

            let strategy_clone = Arc::clone(&strategy);
            let liquidatee_clone = liquidatee;
            let user_account_clone = Arc::new(user_account);
            let tx_sender_clone = tx_sender.clone();

            tokio::spawn(async move {
                let deadline = std::time::Duration::from_millis(LIQUIDATION_DEADLINE_MS);
                let start = std::time::Instant::now();
                let result = tokio::time::timeout(
                    deadline,
                    strategy_clone.liquidate_user(
                        liquidatee_clone,
                        user_account_clone,
                        tx_sender_clone,
                        pf,
                        cu_limit,
                        slot,
                        pyth_price_update,
                    ),
                )
                .await;

                let elapsed = start.elapsed();
                match result {
                    Ok(()) => {
                        if elapsed.as_millis() > LIQUIDATION_DEADLINE_MS as u128 {
                            log::warn!(
                                target: TARGET,
                                "liquidation for {:?} took {}ms (exceeded {}ms deadline but completed)",
                                liquidatee_clone,
                                elapsed.as_millis(),
                                LIQUIDATION_DEADLINE_MS
                            );
                        } else {
                            log::trace!(
                                target: TARGET,
                                "liquidation for {:?} completed in {}ms",
                                liquidatee_clone,
                                elapsed.as_millis()
                            );
                        }
                    }
                    Err(_) => {
                        // Timeout - liquidation exceeded deadline
                        log::warn!(
                            target: TARGET,
                            "liquidation for {:?} exceeded {}ms deadline, cancelled",
                            liquidatee_clone,
                            LIQUIDATION_DEADLINE_MS
                        );
                    }
                }
            });
        }
    });
}

/// Default liquidation strategy that matches against top-of-book makers and
/// submits liquidate_with_fill
pub struct LiquidateWithMatchStrategy {
    pub drift: DriftClient,
    pub dlob: &'static DLOB,
    pub market_state: &'static MarketState,
    pub keeper_subaccount: Pubkey,
    pub metrics: Arc<Metrics>,
}

impl LiquidateWithMatchStrategy {
    /// Find top makers for a perp position
    fn find_top_makers(
        drift: &DriftClient,
        dlob: &'static DLOB,
        market_state: &'static MarketState,
        market_index: u16,
        base_asset_amount: i64,
    ) -> Option<Vec<User>> {
        let l3_book = dlob.get_l3_snapshot(market_index, MarketType::Perp);

        let oracle_price = market_state
            .get_perp_oracle_price(market_index)
            .map(|x| x.price)
            .unwrap_or(0) as u64;

        let maker_pubkeys: Vec<Pubkey> = if base_asset_amount >= 0 {
            // only want maker orders so don't pass vamm or trigger price
            l3_book
                .asks(Some(oracle_price), None, None)
                .filter(|o| o.is_maker())
                .map(|m| m.user)
                .take(3)
                .collect()
        } else {
            l3_book
                .bids(Some(oracle_price), None, None)
                .filter(|o| o.is_maker())
                .map(|m| m.user)
                .take(3)
                .collect()
        };

        if maker_pubkeys.is_empty() {
            log::warn!(target: TARGET, "no makers found. market={}", market_index);
            return None;
        }

        let makers: Vec<User> = maker_pubkeys
            .iter()
            .filter_map(|p| drift.try_get_account::<User>(p).ok())
            .collect();

        if makers.is_empty() {
            log::warn!(target: TARGET, "no maker accounts. market={}", market_index);
            return None;
        }

        Some(makers)
    }

    /// Attempt perp liquidation with order matching
    fn liquidate_perp(
        drift: &DriftClient,
        dlob: &'static DLOB,
        market_state: &'static MarketState,
        metrics: Arc<Metrics>,
        keeper_subaccount: Pubkey,
        liquidatee: Pubkey,
        user_account: Arc<User>,
        tx_sender: TxSender,
        priority_fee: u64,
        cu_limit: u32,
        slot: u64,
        pyth_price_update: Option<PythPriceUpdate>,
    ) {
        let Some(pos) = user_account
            .perp_positions
            .iter()
            .filter(|p| p.base_asset_amount != 0)
            .max_by_key(|p| p.quote_asset_amount)
        else {
            return;
        };

        metrics
            .liquidation_attempts
            .with_label_values(&["perp"])
            .inc();

        log::info!(
            target: TARGET,
            "try liquidate: https://app.drift.trade/?userAccount={liquidatee:?}, market={}",
            pos.market_index
        );

        let Some(makers) = Self::find_top_makers(
            drift,
            dlob,
            market_state,
            pos.market_index,
            pos.base_asset_amount,
        ) else {
            return;
        };

        let oracle_price = market_state
            .get_perp_oracle_price(pos.market_index)
            .map(|x| x.price)
            .unwrap_or(0) as u64;

        let pyth_update = pyth_price_update.filter(|update| update.price != oracle_price);

        try_liquidate_with_match(
            &drift,
            pos.market_index,
            keeper_subaccount,
            liquidatee,
            makers.as_slice(),
            tx_sender,
            priority_fee,
            cu_limit,
            slot,
            pyth_update,
        );
    }

    /// Attempt spot liquidation with Jupiter swap
    async fn liquidate_spot(
        drift: DriftClient,
        metrics: Arc<Metrics>,
        market_state: Arc<MarketStateData>,
        keeper_subaccount: Pubkey,
        liquidatee: Pubkey,
        user_account: Arc<User>,
        tx_sender: TxSender,
        priority_fee: u64,
        cu_limit: u32,
        slot: u64,
    ) {
        let authority = drift.wallet.authority();

        for pos in user_account
            .spot_positions
            .iter()
            .filter(|p| matches!(p.balance_type, SpotBalanceType::Borrow) && !p.is_available())
        {
            let Some(spot_market) = market_state.spot_markets.get(&pos.market_index) else {
                continue;
            };

            let token_amount = match pos.get_token_amount(spot_market) {
                Ok(amount) => amount as u64,
                Err(_) => continue,
            };

            // Filter dust positions
            if token_amount < spot_market.min_order_size * 2 {
                log::trace!(
                    target: TARGET,
                    "skip dust spot position. market={}, amount={}",
                    pos.market_index,
                    token_amount
                );
                continue;
            }

            metrics
                .liquidation_attempts
                .with_label_values(&["spot"])
                .inc();

            // Find their largest deposit to use as collateral
            let Some(asset_market_index) = user_account
                .spot_positions
                .iter()
                .filter(|p| matches!(p.balance_type, SpotBalanceType::Deposit) && !p.is_available())
                .max_by_key(|p| p.scaled_balance)
                .map(|p| p.market_index)
            else {
                log::warn!(
                    target: TARGET,
                    "no asset found for user {:?}, skipping spot liquidation",
                    liquidatee
                );
                continue;
            };

            log::info!(
                target: TARGET,
                "attempting spot liquidation: user={:?}, asset_market={}, liability_market={}, amount={}",
                liquidatee,
                asset_market_index,
                pos.market_index,
                token_amount
            );

            // Fetch accounts once
            let keeper_account_data = match drift.try_get_account::<User>(&keeper_subaccount) {
                Ok(data) => data,
                Err(_) => {
                    log::info!(target: TARGET, "keeper account not found: {:?}", &keeper_subaccount);
                    continue;
                }
            };

            let liquidatee_account_data = match drift.try_get_account::<User>(&liquidatee) {
                Ok(data) => data,
                Err(_) => {
                    log::info!(target: TARGET, "liquidatee account not found: {liquidatee:?}");
                    continue;
                }
            };

            // Fetch market configs inside async block to avoid lifetime issues
            let asset_spot_market = drift
                .program_data()
                .spot_market_config_by_index(asset_market_index)
                .expect("asset spot market");

            let liability_market_index = pos.market_index;

            let liability_spot_market = drift
                .program_data()
                .spot_market_config_by_index(liability_market_index)
                .expect("liability spot market");

            let in_token_account =
                drift_rs::Wallet::derive_associated_token_address(&authority, &asset_spot_market);
            let out_token_account = drift_rs::Wallet::derive_associated_token_address(
                &authority,
                &liability_spot_market,
            );

            let t0 = std::time::Instant::now();
            let (jupiter_result, titan_result) = tokio::join!(
                drift.jupiter_swap_query(
                    &authority,
                    token_amount,
                    SwapMode::ExactIn,
                    100,
                    asset_market_index,
                    liability_market_index,
                    Some(true),
                    None,
                    None,
                ),
                drift.titan_swap_query(
                    &authority,
                    token_amount,
                    Some(50),
                    titan::SwapMode::ExactIn,
                    100,
                    asset_market_index,
                    liability_market_index,
                    Some(true),
                    None,
                    None,
                )
            );

            let quote_latency_ms = t0.elapsed().as_millis() as i64;
            metrics.swap_quote_latency_ms.set(quote_latency_ms);

            if jupiter_result.is_err() && titan_result.is_err() {
                metrics.jupiter_quote_failures.inc();
                metrics.titan_quote_failures.inc();
                log::warn!(target: TARGET, "both quotes failed after {}ms", quote_latency_ms);
                return;
            }

            let use_titan = match (&jupiter_result, &titan_result) {
                (Ok(jup), Ok(titan)) => {
                    let use_titan = titan.quote.out_amount > jup.quote.out_amount;
                    log::debug!(
                        target: TARGET,
                        "got quotes in {}ms - jup: {}, titan: {} - using {}",
                        quote_latency_ms,
                        jup.quote.out_amount,
                        titan.quote.out_amount,
                        if use_titan { "titan" } else { "jupiter" }
                    );
                    use_titan
                }
                (Ok(_), Err(e)) => {
                    metrics.titan_quote_failures.inc();
                    log::debug!(target: TARGET, "titan failed in {}ms, using jupiter: {:?}", quote_latency_ms, e);
                    false
                }
                (Err(e), Ok(_)) => {
                    metrics.jupiter_quote_failures.inc();
                    log::debug!(target: TARGET, "jupiter failed in {}ms, using titan: {:?}", quote_latency_ms, e);
                    true
                }
                _ => unreachable!(),
            };

            let tx = if use_titan {
                TransactionBuilder::new(
                    drift.program_data(),
                    keeper_subaccount,
                    std::borrow::Cow::Owned(keeper_account_data),
                    false,
                )
                .with_priority_fee(priority_fee, Some(cu_limit))
                .titan_swap_liquidate(
                    titan_result.unwrap(),
                    &asset_spot_market,
                    &liability_spot_market,
                    &in_token_account,
                    &out_token_account,
                    asset_market_index,
                    liability_market_index,
                    &liquidatee_account_data,
                )
                .build()
            } else {
                TransactionBuilder::new(
                    drift.program_data(),
                    keeper_subaccount,
                    std::borrow::Cow::Owned(keeper_account_data),
                    false,
                )
                .with_priority_fee(priority_fee, Some(cu_limit))
                .jupiter_swap_liquidate(
                    jupiter_result.unwrap(),
                    &asset_spot_market,
                    &liability_spot_market,
                    &in_token_account,
                    &out_token_account,
                    asset_market_index,
                    liability_market_index,
                    &liquidatee_account_data,
                )
                .build()
            };
            log::debug!(
                target: TARGET,
                "sending spot liq tx: {liquidatee:?}, asset={asset_market_index}, liability={}",
                liability_market_index
            );
            tx_sender.send_tx(
                tx,
                TxIntent::LiquidateSpot {
                    asset_market_index,
                    liability_market_index,
                    liquidatee,
                    slot,
                },
                cu_limit as u64,
            );
        }
    }
}

impl LiquidationStrategy for LiquidateWithMatchStrategy {
    fn liquidate_user<'a>(
        &'a self,
        liquidatee: Pubkey,
        user_account: Arc<User>,
        tx_sender: TxSender,
        priority_fee: u64,
        cu_limit: u32,
        slot: u64,
        pyth_price_update: Option<PythPriceUpdate>,
    ) -> futures_util::future::BoxFuture<'a, ()> {
        Self::liquidate_perp(
            &self.drift,
            self.dlob,
            self.market_state,
            Arc::clone(&self.metrics),
            self.keeper_subaccount,
            liquidatee,
            Arc::clone(&user_account),
            tx_sender.clone(),
            priority_fee,
            cu_limit,
            slot,
            pyth_price_update,
        );
        Self::liquidate_spot(
            self.drift.clone(),
            Arc::clone(&self.metrics),
            self.market_state.load(),
            self.keeper_subaccount,
            liquidatee,
            user_account,
            tx_sender.clone(),
            priority_fee,
            400_000,
            slot,
        )
        .boxed()
    }
}
