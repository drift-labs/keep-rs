//! Liquidator Bot
use std::{
    collections::{BTreeMap, HashMap},
    sync::Arc,
};

use anchor_lang::prelude::*;
use crossbeam::channel::Receiver;
use drift_rs::{
    dlob::{DLOBNotifier, DLOB},
    ffi::{CachedMarginCalculation, OraclePriceData, SimplifiedMarginCalculation},
    grpc::{grpc_subscriber::AccountFilter, TransactionUpdate},
    priority_fee_subscriber::PriorityFeeSubscriber,
    types::{
        accounts::{PerpMarket, SpotMarket, User},
        MarginRequirementType, MarketId, MarketStatus, MarketType, OracleSource,
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

const TARGET: &str = "liquidator";

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

#[derive(Default)]
pub struct MarginRecords {
    liquidation_queue: BTreeMap<Pubkey, SimplifiedMarginCalculation>,
    user_margin: BTreeMap<Pubkey, SimplifiedMarginCalculation>,
}

impl MarginRecords {
    pub fn remove(&mut self, pubkey: &Pubkey) {
        self.liquidation_queue.remove(pubkey);
        self.user_margin.remove(pubkey);
    }
    pub fn set_margin(&mut self, pubkey: &Pubkey, margin_info: &SimplifiedMarginCalculation) {
        // Always keep in user_margin
        self.user_margin.insert(*pubkey, margin_info.clone());

        // Only manage liquidation queue based on status
        let is_liquidating = margin_info.total_collateral < margin_info.margin_requirement as i128;

        if is_liquidating {
            self.liquidation_queue.insert(*pubkey, *margin_info);
        } else {
            self.liquidation_queue.remove(pubkey);
        }
    }
}

pub struct LiquidatorBot {
    drift: DriftClient,
    dlob: &'static DLOB,
    dlob_notifier: DLOBNotifier,
    keeper_subaccount: Pubkey,
    events_rx: crossbeam::channel::Receiver<GrpcEvent>,
    market_ids: Vec<MarketId>,
    config: Config,
    tx_worker_ref: TxSender,
    priority_fee_subscriber: Arc<PriorityFeeSubscriber>,
    market_state: MarketState,
    rate_limit: HashMap<(Pubkey, u16), u64>, // (liquidatee, market_index) -> last_liquidation_slot
}

impl LiquidatorBot {
    pub async fn new(config: Config, drift: DriftClient, metrics: Arc<Metrics>) -> Self {
        let dlob: &'static DLOB = Box::leak(Box::new(DLOB::default()));

        let tx_worker = TxWorker::new(drift.clone(), metrics, config.dry);
        let rt = tokio::runtime::Handle::current();
        let tx_worker_ref = tx_worker.run(rt);

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

        tokio::try_join!(
            drift.subscribe_blockhashes(),
            drift.subscribe_account(&keeper_subaccount)
        )
        .expect("subscribed");
        let dlob_notifier = dlob.spawn_notifier();
        let events_rx = setup_grpc(
            drift.clone(),
            dlob_notifier.clone(),
            tx_worker_ref.clone(),
            &market_ids,
        )
        .await;
        log::info!(target: TARGET, "subscribed gRPC");

        // populate market data
        let market_state = MarketState::new();
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

        LiquidatorBot {
            drift,
            dlob,
            dlob_notifier,
            keeper_subaccount: keeper_subaccount,
            events_rx,
            market_ids,
            config,
            tx_worker_ref,
            priority_fee_subscriber,
            market_state,
            rate_limit: HashMap::new(),
        }
    }

    pub async fn run(self) {
        let events_rx = self.events_rx;
        let drift: &'static DriftClient = Box::leak(Box::new(self.drift));
        let dlob = self.dlob;
        let market_ids = self.market_ids;
        let keeper_subaccount = self.keeper_subaccount;
        let config = self.config.clone();
        let priority_fee_subscriber = Arc::clone(&self.priority_fee_subscriber);
        let mut current_slot = 0;
        let mut use_median_trigger_price = drift
            .state_account()
            .map(|s| s.has_median_trigger_price_feature())
            .unwrap_or(false);

        let mut margin_records = MarginRecords::default();
        let mut users = BTreeMap::<Pubkey, User>::new();
        let dlob_notifier = self.dlob_notifier;
        let mut rate_limit = self.rate_limit;
        let liquidation_margin_buffer_ratio = drift
            .state_account()
            .map(|x| x.liquidation_margin_buffer_ratio)
            .expect("State has liquidation_margin_buffer_ratio");

        // initialize local user storage
        let mut exclude_count = 0;
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
                    margin_records.set_margin(pubkey, &margin_info);
                    users.insert(*pubkey, *user);
                }
            });

        log::info!(target: TARGET, "filtered #{exclude_count} accounts with dust collateral");

        // main loop
        loop {
            match events_rx.recv() {
                Ok(GrpcEvent::UserUpdate {
                    pubkey,
                    user,
                    slot: update_slot,
                }) => {
                    current_slot = current_slot.max(update_slot);
                    // TOOD: update dlob here, builder should be easier to use
                    dlob_notifier.user_update(pubkey, users.get(&pubkey), &user, update_slot);

                    // calculate user margin after upate
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
                    } else {
                        margin_records.set_margin(&pubkey, &margin_info);
                    }
                    // TODO: check if individual user needs liquidating
                    continue;
                }
                Ok(GrpcEvent::OracleUpdate {
                    oracle_price_data,
                    market,
                    slot,
                }) => {
                    current_slot = current_slot.max(slot);
                    let is_perp_update = market.is_perp();
                    if is_perp_update {
                        self.market_state
                            .set_perp_oracle_price(market.index(), oracle_price_data);
                    } else {
                        self.market_state
                            .set_spot_oracle_price(market.index(), oracle_price_data);
                    }

                    let t0 = std::time::SystemTime::now();
                    let mut count = 0;
                    let market_index = market.index();
                    for (pubkey, user) in users.iter() {
                        let margin_info = self
                            .market_state
                            .calculate_simplified_margin_requirement(
                                &user,
                                MarginRequirementType::Maintenance,
                                Some(liquidation_margin_buffer_ratio),
                            )
                            .unwrap();
                        if is_perp_update {
                            if let Some(pos) = user
                                .perp_positions
                                .iter()
                                .find(|x| x.market_index == market_index && !x.is_available())
                            {
                                count += 1;
                                margin_records.set_margin(pubkey, &margin_info);
                            }
                        } else if !is_perp_update {
                            if let Some(pos) = user
                                .spot_positions
                                .iter()
                                .find(|x| x.market_index == market_index && !x.is_available())
                            {
                                count += 1;
                                margin_records.set_margin(pubkey, &margin_info);
                            }
                        }
                    }

                    log::debug!(
                        "processed #{count} margin updates (market={}): {:?}ms",
                        market.index(),
                        std::time::SystemTime::now()
                            .duration_since(t0)
                            .unwrap()
                            .as_millis(),
                    );
                }
                Ok(GrpcEvent::PerpMarketUpdate { market, slot }) => {
                    self.market_state.set_perp_market(market);
                    current_slot = current_slot.max(slot);
                    continue;
                }
                Ok(GrpcEvent::SpotMarketUpdate { market, slot }) => {
                    self.market_state.set_spot_market(market);
                    current_slot = current_slot.max(slot);
                    continue;
                }
                Err(err) => {
                    log::error!("grpc err: {err:?}");
                    break;
                }
            }

            // try to liquidate users
            let t0 = std::time::SystemTime::now();
            let pf = priority_fee_subscriber.priority_fee_nth(0.6);
            let mut count = 0;
            // let mut to_remove = vec![];
            for (liquidatee, margin_info) in &margin_records.liquidation_queue {
                // TODO: sort by margin shortage
                // TODO: small position skip at first?
                if (margin_info.margin_requirement as i128) < margin_info.total_collateral {
                    dbg!("can't liq", liquidatee);
                    // to_remove.push(liquidatee);
                }
                if let Some(user_account) = users.get(&liquidatee) {
                    for pos in user_account
                        .perp_positions
                        .iter()
                        .filter(|p| !p.is_available())
                    {
                        // Rate limiting: only liquidate every 3 slots per (liquidatee, market_index)
                        let rate_limit_key = (*liquidatee, pos.market_index);
                        if let Some(last_liquidation_slot) = rate_limit.get(&rate_limit_key) {
                            if current_slot - last_liquidation_slot < 10 {
                                log::debug!(target: TARGET, "rate limited liquidation for {:?} market {} (last: {}, current: {})", 
                                liquidatee, pos.market_index, last_liquidation_slot, current_slot);
                                continue;
                            }
                        }

                        log::info!(target: TARGET, "try liquidate: {liquidatee:?}, market={}, margin={:?}", pos.market_index, margin_info);

                        // TODO: cache top maker lookup
                        let l3_book = dlob.get_l3_snapshot(pos.market_index, MarketType::Perp);
                        dbg!(l3_book.bbo());
                        let top_makers = if pos.base_asset_amount > 0 {
                            l3_book.top_asks()
                        } else {
                            l3_book.top_bids()
                        };

                        let maker_accounts: Vec<User> = top_makers
                            .iter()
                            .take(3)
                            .map(|m| users.get(&m.maker).expect("maker account loaded").clone())
                            .collect();

                        // TODO: check liquidation limit price

                        count += 1;
                        try_liquidate_with_match(
                            &drift,
                            pos.market_index,
                            keeper_subaccount,
                            *liquidatee,
                            maker_accounts.as_slice(),
                            &self.tx_worker_ref,
                            pf,
                            config.fill_cu_limit,
                            current_slot,
                        );

                        // Update rate limit tracking
                        rate_limit.insert(rate_limit_key, current_slot);
                    }
                }
            }

            // for user in to_remove.drain(..) {
            //     margin_records.liquidation_queue.remove(user);
            // }

            log::debug!(
                target: TARGET,
                "processed liquidation queue: #{count}: {:?}ms",
                std::time::SystemTime::now()
                    .duration_since(t0)
                    .unwrap()
                    .as_millis(),
            );
        }
    }
}

fn on_transaction_update_fn(
    tx_worker_ref: TxSender,
) -> impl Fn(&TransactionUpdate) + Send + Sync + 'static {
    move |tx: &TransactionUpdate| {
        if let Some(sig) = tx.transaction.signatures.first() {
            tx_worker_ref.confirm_tx((sig.as_slice().try_into()).expect("valid signature"));
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
        for market in &market_ids {
            if let Some(oracle_price) = drift.try_get_oracle_price_data_and_slot(*market) {
                dlob_notifier.slot_update(*market, oracle_price.data.price as u64, new_slot);
            }
        }
    }
}

async fn setup_grpc(
    drift: DriftClient,
    dlob_notifier: DLOBNotifier,
    transaction_tx: TxSender,
    market_ids: &[MarketId],
) -> Receiver<GrpcEvent> {
    let (tx, rx) = crossbeam::channel::bounded(1024);

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
            std::env::var("GRPC_URL")
                .unwrap_or_else(|_| "https://api.rpcpool.com".to_string())
                .into(),
            std::env::var("GRPC_X_TOKEN").expect("GRPC_X_TOKEN set"),
            GrpcSubscribeOpts::default()
                .commitment(solana_sdk::commitment_config::CommitmentLevel::Processed)
                .transaction_include_accounts(vec![drift.wallet().default_sub_account()])
                .on_transaction(on_transaction_update_fn(transaction_tx.clone()))
                .on_slot(on_slot_update_fn(
                    dlob_notifier.clone(),
                    drift.clone(),
                    market_ids,
                ))
                .on_account(
                    AccountFilter::partial().with_discriminator(User::DISCRIMINATOR),
                    {
                        let tx = tx.clone();
                        move |acc| {
                            let user: &User = drift_rs::utils::deser_zero_copy(&acc.data);
                            if let Err(err) = tx.send(GrpcEvent::UserUpdate {
                                pubkey: acc.pubkey,
                                user: user.clone(),
                                slot: acc.slot,
                            }) {
                                log::error!("failed to forward event: {err:?}");
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
                            if let Err(err) = tx.send(GrpcEvent::PerpMarketUpdate {
                                market: market.clone(),
                                slot: acc.slot,
                            }) {
                                log::error!("failed to forward event: {err:?}");
                            }
                        }
                    },
                )
                .on_account(
                    AccountFilter::partial().with_discriminator(SpotMarket::DISCRIMINATOR),
                    {
                        let tx = tx.clone();
                        move |acc| {
                            let market: &SpotMarket = drift_rs::utils::deser_zero_copy(&acc.data);
                            if let Err(err) = tx.send(GrpcEvent::SpotMarketUpdate {
                                market: market.clone(),
                                slot: acc.slot,
                            }) {
                                log::error!("failed to forward event: {err:?}");
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
                            if let Err(err) = tx.send(GrpcEvent::OracleUpdate {
                                oracle_price_data,
                                market: *market,
                                slot: acc.slot,
                            }) {
                                log::error!("failed to forward event: {err:?}");
                            }
                        }
                    }
                }),
            true,
        )
        .await;

    rx
}

/// Try to fill an auction order
///
/// - `auction_crosses` list of one or more crosses to fill
fn try_liquidate_with_match(
    drift: &DriftClient,
    market_index: u16,
    keeper_subaccount: Pubkey,
    liquidatee_subaccount: Pubkey,
    top_makers: &[User],
    tx_worker_ref: &TxSender,
    priority_fee: u64,
    cu_limit: u32,
    slot: u64,
) {
    if top_makers.is_empty() {
        log::debug!(target: TARGET, "skip empty maker cross. market={market_index} user={liquidatee_subaccount}");
        return;
    }

    let keeper_account_data = drift
        .try_get_account::<User>(&keeper_subaccount)
        .expect("keeper account");

    let liquidatee_subaccount_data = drift
        .try_get_account::<User>(&liquidatee_subaccount)
        .expect("taker account");

    let mut tx_builder = TransactionBuilder::new(
        drift.program_data(),
        keeper_subaccount,
        std::borrow::Cow::Borrowed(&keeper_account_data),
        false,
    )
    .with_priority_fee(priority_fee, Some(cu_limit))
    .liquidate_perp_with_fill(market_index, &liquidatee_subaccount_data, top_makers);

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

    tx_worker_ref.send_tx(
        tx,
        TxIntent::LiquidateWithFill {
            market_index,
            liquidatee: liquidatee_subaccount,
            slot,
        },
        cu_limit as u64,
    );
}
