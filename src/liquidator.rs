//! Example Liquidator Bot
//!
//! Subscribes to drift accounts, market, and oracles via gRPC.
//! Identifies liquidatable accounts and forwards them to a strategy impl
//! for processing.
//!
//! The default strategy tries to liquidate perp positions against resting orders
//!
use std::{
    collections::{BTreeMap, HashMap, HashSet},
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use anchor_lang::prelude::*;
use drift_rs::jupiter::JupiterSwapApi;
use drift_rs::{
    dlob::{DLOBNotifier, DLOB},
    ffi::{OraclePriceData, SimplifiedMarginCalculation},
    grpc::{grpc_subscriber::AccountFilter, TransactionUpdate},
    jupiter::SwapMode,
    priority_fee_subscriber::PriorityFeeSubscriber,
    types::{
        accounts::{PerpMarket, SpotMarket, User},
        MarginRequirementType, MarketId, MarketStatus, MarketType, OracleSource, SpotBalanceType,
    },
    DriftClient, GrpcSubscribeOpts, MarketState, Pubkey, TransactionBuilder,
};
use solana_sdk::{account::Account, clock::Slot, compute_budget::ComputeBudgetInstruction};

use crate::{
    filler::{TxSender, TxWorker},
    http::Metrics,
    util::TxIntent,
    Config, UseMarkets,
};

/// min slots between successive liquidation attempts on same user
const LIQUIDATION_SLOT_RATE_LIMIT: u64 = 5;

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
    if margin_info.total_collateral < margin_info.margin_requirement as i128 {
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
    fn liquidate_user(
        &self,
        rt: tokio::runtime::Handle,
        liquidatee: &Pubkey,
        user_account: &User,
        tx_sender: &TxSender,
        priority_fee: u64,
        cu_limit: u32,
        slot: u64,
    );
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
    /// sends liquidatable accounts to work thread
    liq_tx: crossbeam::channel::Sender<(Pubkey, User, u64)>,
}

impl LiquidatorBot {
    pub async fn new(config: Config, drift: DriftClient, metrics: Arc<Metrics>) -> Self {
        let dlob: &'static DLOB = Box::leak(Box::new(DLOB::default()));
        let tx_worker = TxWorker::new(drift.clone(), Arc::clone(&metrics), config.dry);
        let rt = tokio::runtime::Handle::current();
        let tx_sender = tx_worker.run(rt);

        let mut market_ids = match config.use_markets() {
            UseMarkets::All => drift.get_all_perp_market_ids(),
            UseMarkets::Subset(m) => m,
        };
        // remove bet perp markets
        market_ids.retain(|x| {
            let market = drift
                .program_data()
                .perp_market_config_by_index(x.index())
                .unwrap();
            let name = core::str::from_utf8(&market.name)
                .unwrap()
                .to_ascii_lowercase();

            !name.contains("bet") && market.status != MarketStatus::Initialized
        });

        let market_pubkeys: Vec<Pubkey> = market_ids
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
            market_ids.clone(),
        )
        .await;
        log::info!(target: TARGET, "subscribed gRPC");

        // populate market data
        let market_state: &'static MarketState = Box::leak(Box::new(MarketState::new()));
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

        // start liquidation worker
        let (liq_tx, liq_rx) = crossbeam::channel::bounded::<(Pubkey, User, u64)>(1024);
        spawn_liquidation_worker(
            tokio::runtime::Handle::current(),
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

        const RECHECK_CYCLE_INTERVAL: u32 = 64;

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
        let mut event_buffer = Vec::<GrpcEvent>::with_capacity(32);
        let mut oracle_update;

        loop {
            oracle_update = false;
            let n_read = events_rx.recv_many(&mut event_buffer, 32).await;
            log::trace!(target: TARGET, "read: {n_read}, remaning: {:?}", events_rx.len());
            for event in event_buffer.drain(..) {
                match event {
                    GrpcEvent::UserUpdate {
                        pubkey,
                        user,
                        slot: update_slot,
                    } => {
                        current_slot = current_slot.max(update_slot);
                        dlob_notifier.user_update(pubkey, users.get(&pubkey), &user, update_slot);
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
                                    self.liq_tx
                                        .send((pubkey, user.clone(), current_slot))
                                        .expect("liq sent");
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
                    GrpcEvent::OracleUpdate {
                        oracle_price_data,
                        market,
                        slot,
                    } => {
                        current_slot = current_slot.max(slot);
                        let is_perp_update = market.is_perp();
                        if is_perp_update {
                            self.market_state
                                .set_perp_oracle_price(market.index(), oracle_price_data);
                        } else {
                            self.market_state
                                .set_spot_oracle_price(market.index(), oracle_price_data);
                        }
                        oracle_update = true;
                    }
                    GrpcEvent::PerpMarketUpdate { market, slot } => {
                        current_slot = current_slot.max(slot);
                        if let Some(existing) = self
                            .market_state
                            .load()
                            .perp_oracle_prices
                            .get(&market.market_index)
                        {
                            if existing
                                .sequence_id
                                .is_some_and(|s| s < market.amm.mm_oracle_sequence_id)
                                && market.amm.mm_oracle_price > 0
                            {
                                self.market_state.set_perp_oracle_price(
                                    market.market_index,
                                    OraclePriceData {
                                        price: market.amm.mm_oracle_price,
                                        confidence: 1,
                                        delay: 0,
                                        has_sufficient_number_of_data_points: true,
                                        sequence_id: Some(market.amm.mm_oracle_sequence_id),
                                    },
                                );
                                oracle_update = true;
                            }
                        }
                    }
                    GrpcEvent::SpotMarketUpdate { market, slot } => {
                        current_slot = current_slot.max(slot);
                        self.market_state.set_spot_market(market);
                    }
                }
            }

            // Only recheck margin for high-risk users on oracle price updates
            if oracle_update {
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
                            MarginStatus::Liquidatable => {
                                log::debug!(target: TARGET, "found liquidatable user: {pubkey:?}, margin:{margin_info:?}");
                                self.liq_tx
                                    .try_send((*pubkey, user.clone(), current_slot))
                                    .expect("liq sent");
                                true
                            }
                            MarginStatus::HighRisk => true,
                            MarginStatus::Safe => false,
                        }
                    } else {
                        false
                    }
                });

                log::trace!(
                    target: TARGET,
                    "processed {} high-risk margin updates in {}ms",
                    high_risk.len(),
                    current_time_millis() - t0,
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
                            self.liq_tx
                                .try_send((*pubkey, user.clone(), current_slot))
                                .expect("liq sent");
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
    tx_sender: &TxSender,
    priority_fee: u64,
    cu_limit: u32,
    slot: u64,
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
    .with_priority_fee(priority_fee, Some(cu_limit))
    .liquidate_perp_with_fill(
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
    rt: tokio::runtime::Handle,
    tx_sender: TxSender,
    strategy: Arc<dyn LiquidationStrategy + Send + Sync>,
    liq_rx: crossbeam::channel::Receiver<(Pubkey, User, u64)>,
    cu_limit: u32,
    priority_fee_subscriber: Arc<PriorityFeeSubscriber>,
) -> std::thread::JoinHandle<()> {
    std::thread::spawn(move || {
        let mut rate_limit = HashMap::<Pubkey, u64>::new();
        while let Ok((liquidatee, user_account, slot)) = liq_rx.recv() {
            if rate_limit
                .get(&liquidatee)
                .is_some_and(|last| slot.saturating_sub(*last) < LIQUIDATION_SLOT_RATE_LIMIT)
            {
                log::trace!(target: TARGET, "rate limited liquidation for {:?} (current: {})", liquidatee, slot);
                continue;
            } else {
                rate_limit.insert(liquidatee, slot);
            }

            let pf = priority_fee_subscriber.priority_fee_nth(0.6);
            strategy.liquidate_user(
                rt.clone(),
                &liquidatee,
                &user_account,
                &tx_sender,
                pf,
                cu_limit,
                slot,
            );
        }
    })
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
    fn find_top_makers(&self, market_index: u16, base_asset_amount: i64) -> Option<Vec<User>> {
        let l3_book = self.dlob.get_l3_snapshot(market_index, MarketType::Perp);
        let oracle_price = self
            .market_state
            .get_perp_oracle_price(market_index)
            .map(|x| x.price)
            .unwrap_or(0) as u64;

        let maker_pubkeys: Vec<Pubkey> = if base_asset_amount >= 0 {
            l3_book
                .asks(oracle_price)
                .filter(|o| o.is_maker())
                .map(|m| m.user)
                .take(3)
                .collect()
        } else {
            l3_book
                .bids(oracle_price)
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
            .filter_map(|p| self.drift.try_get_account::<User>(p).ok())
            .collect();

        if makers.is_empty() {
            log::warn!(target: TARGET, "no maker accounts. market={}", market_index);
            return None;
        }

        Some(makers)
    }

    /// Attempt perp liquidation with order matching
    fn liquidate_perp(
        &self,
        liquidatee: &Pubkey,
        user_account: &User,
        tx_sender: &TxSender,
        priority_fee: u64,
        cu_limit: u32,
        slot: u64,
    ) {
        let Some(pos) = user_account
            .perp_positions
            .iter()
            .filter(|p| p.base_asset_amount != 0)
            .max_by_key(|p| p.quote_asset_amount)
        else {
            return;
        };

        self.metrics
            .liquidation_attempts
            .with_label_values(&["perp"])
            .inc();

        log::info!(
            target: TARGET,
            "try liquidate: https://app.drift.trade/?userAccount={liquidatee:?}, market={}",
            pos.market_index
        );

        let Some(makers) = self.find_top_makers(pos.market_index, pos.base_asset_amount) else {
            return;
        };

        try_liquidate_with_match(
            &self.drift,
            pos.market_index,
            self.keeper_subaccount,
            *liquidatee,
            makers.as_slice(),
            tx_sender,
            priority_fee,
            cu_limit,
            slot,
        );
    }

    /// Attempt spot liquidation with Jupiter swap
    fn liquidate_spot(
        &self,
        rt: &tokio::runtime::Handle,
        liquidatee: &Pubkey,
        user_account: &User,
        tx_sender: &TxSender,
        priority_fee: u64,
        cu_limit: u32,
        slot: u64,
    ) {
        let authority = self.drift.wallet.authority();
        let market_state = self.market_state.load();

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

            self.metrics
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
            let keeper_account_data = match self
                .drift
                .try_get_account::<User>(&self.keeper_subaccount)
            {
                Ok(data) => data,
                Err(_) => {
                    log::info!(target: TARGET, "keeper account not found: {:?}", self.keeper_subaccount);
                    continue;
                }
            };

            let liquidatee_account_data = match self.drift.try_get_account::<User>(liquidatee) {
                Ok(data) => data,
                Err(_) => {
                    log::info!(target: TARGET, "liquidatee account not found: {liquidatee:?}");
                    continue;
                }
            };

            // Spawn async task for Jupiter swap
            let drift = self.drift.clone();
            let keeper_subaccount = self.keeper_subaccount;
            let liability_market_index = pos.market_index;
            let liquidatee = *liquidatee;
            let tx_sender = tx_sender.clone();
            let authority = *authority;
            let keeper_account_data = keeper_account_data;
            let liquidatee_account_data = liquidatee_account_data;
            let metrics = Arc::clone(&self.metrics);

            rt.spawn(async move {
                // Fetch market configs inside async block to avoid lifetime issues
                let asset_spot_market = drift
                    .program_data()
                    .spot_market_config_by_index(asset_market_index)
                    .expect("asset spot market");

                let liability_spot_market = drift
                    .program_data()
                    .spot_market_config_by_index(liability_market_index)
                    .expect("liability spot market");

                let in_token_account = drift_rs::Wallet::derive_associated_token_address(
                    &authority,
                    &asset_spot_market,
                );
                let out_token_account = drift_rs::Wallet::derive_associated_token_address(
                    &authority,
                    &liability_spot_market,
                );

                let t0 = std::time::Instant::now();
                let res = drift
                    .jupiter_swap_query(
                        &authority,
                        token_amount,
                        SwapMode::ExactIn,
                        100,
                        asset_market_index,
                        liability_market_index,
                        Some(true),
                        None,
                        None,
                    )
                    .await;

                let jupiter_swap_info = match res {
                    Ok(info) => {
                        let latency = t0.elapsed().as_millis() as f64;
                        metrics.jupiter_quote_latency.observe(latency);
                        info
                    }
                    Err(e) => {
                        metrics.jupiter_quote_failures.inc();
                        log::warn!(
                            target: TARGET,
                            "failed to get jupiter quote for user {:?}, market {}: {:?}",
                            liquidatee,
                            liability_market_index,
                            e
                        );
                        return;
                    }
                };

                let tx = TransactionBuilder::new(
                    drift.program_data(),
                    keeper_subaccount,
                    std::borrow::Cow::Owned(keeper_account_data),
                    false,
                )
                .with_priority_fee(priority_fee, Some(cu_limit))
                .jupiter_swap_liquidate(
                    jupiter_swap_info,
                    &asset_spot_market,
                    &liability_spot_market,
                    &in_token_account,
                    &out_token_account,
                    asset_market_index,
                    liability_market_index,
                    &liquidatee_account_data,
                )
                .build();

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
            });
        }
    }
}

impl LiquidationStrategy for LiquidateWithMatchStrategy {
    fn liquidate_user(
        &self,
        rt: tokio::runtime::Handle,
        liquidatee: &Pubkey,
        user_account: &User,
        tx_sender: &TxSender,
        priority_fee: u64,
        cu_limit: u32,
        slot: u64,
    ) {
        self.liquidate_perp(
            liquidatee,
            user_account,
            tx_sender,
            priority_fee,
            cu_limit,
            slot,
        );
        self.liquidate_spot(
            &rt,
            liquidatee,
            user_account,
            tx_sender,
            priority_fee,
            400_000,
            slot,
        );
    }
}
