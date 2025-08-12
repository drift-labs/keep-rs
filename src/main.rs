//! Filler Bot
use std::{
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use anchor_lang::prelude::*;
use clap::Parser;
use drift_rs::{
    constants::PROGRAM_ID,
    dlob::{
        util::OrderDelta, CrossesAndTopMakers, CrossingRegion, DLOBEvent, DLOBNotifier,
        MakerCrosses, OrderKind, TakerOrder, DLOB,
    },
    event_subscriber::DriftEvent,
    ffi::calculate_auction_price,
    grpc::{grpc_subscriber::AccountFilter, AccountUpdate, TransactionUpdate},
    priority_fee_subscriber::PriorityFeeSubscriber,
    swift_order_subscriber::{SignedOrderInfo, SwiftOrderStream},
    types::{
        accounts::{User, UserStats},
        CommitmentConfig, MarketId, MarketStatus, MarketType, Order, OrderStatus, OrderType,
        PositionDirection, PostOnlyParam, RpcSendTransactionConfig, VersionedMessage,
    },
    DriftClient, GrpcSubscribeOpts, Pubkey, RpcClient, TransactionBuilder, Wallet,
};
use futures_util::StreamExt;
use solana_account_decoder_client_types::UiAccountEncoding;
use solana_rpc_client_api::config::{
    RpcAccountInfoConfig, RpcProgramAccountsConfig, RpcTransactionConfig,
};
use solana_sdk::{
    compute_budget::ComputeBudgetInstruction, signature::Signature, transaction::TransactionError,
};
use solana_transaction_status_client_types::UiTransactionEncoding;
use tokio::{runtime::Handle, sync::RwLock};

mod http;
mod util;
use crate::{
    http::{health_handler, metrics_handler, Metrics},
    util::{OrderSlotLimiter, PendingTxMeta, PendingTxs, PythPriceUpdate, TxIntent},
};

use mimalloc::MiMalloc;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

/// Bot configuration loaded from command line
#[derive(Debug, Clone, Parser)]
pub struct Config {
    /// fill for all markets (overrides '--market-ids')
    #[clap(long, default_value = "false")]
    pub all_markets: bool,
    /// Comma-separated list of perp market indices to fill for
    #[clap(long, env = "MARKET_IDS", default_value = "0,1,2")]
    pub market_ids: String,
    /// Use mainnet (otherwise devnet)
    #[clap(long, env = "MAINNET", default_value = "true")]
    pub mainnet: bool,
    #[clap(long, default_value = "512")]
    pub priority_fee: u64,
    #[clap(long, default_value = "364000")]
    pub swift_cu_limit: u32,
    #[clap(long, default_value = "256000")]
    pub fill_cu_limit: u32,
    #[clap(long, env = "DRY_RUN", default_value = "false")]
    pub dry: bool,
}

enum UseMarkets {
    All,
    Subset(Vec<MarketId>),
}

impl Config {
    fn use_markets(&self) -> UseMarkets {
        if self.all_markets {
            UseMarkets::All
        } else {
            UseMarkets::Subset(
                self.market_ids
                    .split(',')
                    .filter_map(|s| s.trim().parse::<u16>().ok())
                    .map(MarketId::perp)
                    .collect(),
            )
        }
    }
}

#[tokio::main]
async fn main() {
    let config = Config::parse();
    let metrics = Arc::new(Metrics::new());
    // Start Prometheus metrics server
    let metrics_port = std::env::var("METRICS_PORT")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(9898);
    let addr = format!("0.0.0.0:{metrics_port}");
    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .expect("bind metrics port");

    let metrics_ref = Arc::clone(&metrics);
    let _http_task = tokio::spawn(async move {
        axum::serve(
            listener,
            axum::Router::new()
                .route("/metrics", axum::routing::get(metrics_handler))
                .route("/health", axum::routing::get(health_handler))
                .with_state(metrics_ref),
        )
        .await
        .unwrap();
    });

    let bot = FillerBot::new(config, metrics).await;
    bot.run().await;
}

struct FillerBot {
    drift: DriftClient,
    dlob: &'static DLOB,
    filler_subaccount: Pubkey,
    slot_rx: tokio::sync::mpsc::Receiver<u64>,
    swift_order_stream: SwiftOrderStream,
    limiter: OrderSlotLimiter<40>,
    market_ids: Vec<MarketId>,
    config: Config,
    tx_worker_ref: TxSender,
    priority_fee_subscriber: Arc<PriorityFeeSubscriber>,
}

impl FillerBot {
    async fn new(config: Config, metrics: Arc<Metrics>) -> Self {
        env_logger::init();
        let _ = dotenv::dotenv();
        let wallet: Wallet = drift_rs::utils::load_keypair_multi_format(
            &std::env::var("BOT_PRIVATE_KEY").expect("base58 BOT_PRIVATE_KEY set"),
        )
        .expect("loaded BOT_PRIVATE_KEY")
        .into();

        let filler_subaccount = wallet.default_sub_account();
        log::info!(target: "filler", "bot started: authority={:?}, subaccount={:?}", wallet.authority(), filler_subaccount);
        log::info!(target: "filler", "mainnet={}, markets={}", config.mainnet, config.all_markets);

        let context = if config.mainnet {
            drift_rs::types::Context::MainNet
        } else {
            drift_rs::types::Context::DevNet
        };
        let rpc_url = std::env::var("RPC_URL")
            .unwrap_or_else(|_| "https://api.devnet.solana.com".to_string());
        let drift = DriftClient::new(context, RpcClient::new(rpc_url), wallet)
            .await
            .expect("initialized client");

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

        log::info!(target: "filler", "subsribing swift orders");
        let swift_order_stream = drift
            .subscribe_swift_orders(&market_ids, Some(true))
            .await
            .expect("subscribed swift orders");
        log::info!(target: "filler", "subscribed swift orders");

        tokio::try_join!(
            drift.subscribe_blockhashes(),
            drift.subscribe_account(&filler_subaccount)
        )
        .expect("subscribed");
        let slot_rx = setup_grpc(drift.clone(), dlob, &market_ids, tx_worker_ref.clone()).await;
        log::info!(target: "filler", "subscribed gRPC");

        // let pyth_access_token = std::env::var("PYTH_LAZER_TOKEN").expect("pyth access token");
        // let pyth_feed_cli = pyth_lazer_client::LazerClient::new(
        //     "wss://pyth-lazer.dourolabs.app/v1/stream",
        //     pyth_access_token.as_str(),
        // )
        // .expect("pyth price feed connects");
        // let pyth_price_feed = crate::util::subscribe_price_feeds(pyth_feed_cli, &market_ids);
        // log::info!(target: "filler", "subscribed pyth price feeds");

        FillerBot {
            drift,
            dlob,
            filler_subaccount,
            slot_rx,
            swift_order_stream,
            limiter: OrderSlotLimiter::new(),
            market_ids,
            config,
            tx_worker_ref,
            priority_fee_subscriber,
        }
    }

    pub async fn run(self) {
        let mut swift_order_stream = self.swift_order_stream;
        let mut slot_rx = self.slot_rx;
        let mut limiter = self.limiter;
        let drift: &'static DriftClient = Box::leak(Box::new(self.drift));
        let dlob = self.dlob;
        let market_ids = self.market_ids;
        let filler_subaccount = self.filler_subaccount;
        let config = self.config.clone();
        let tx_worker_ref = self.tx_worker_ref.clone();
        let priority_fee_subscriber = Arc::clone(&self.priority_fee_subscriber);
        let mut slot = 0;
        let min_perp_auction_duration = drift.state_account().unwrap().min_perp_auction_duration;
        let mut use_median_trigger_price = drift
            .state_account()
            .map(|s| s.feature_bit_flags & 0b0000_0010 != 0) // FeatureBitFlags::MedianTriggerPrice
            .unwrap_or(false);

        loop {
            tokio::select! {
                biased;
                _ = tokio::signal::ctrl_c() => {
                    log::warn!("filler shutting down...");
                    break;
                }
                swift_order = swift_order_stream.next() => {
                    match swift_order {
                        Some(signed_order) => {
                            let order_params = signed_order.order_params();
                            log::debug!(target: "filler", "new swift order: {signed_order:?}");
                            let tick_size = drift.program_data().perp_market_config_by_index(order_params.market_index).unwrap().amm.order_tick_size;
                            let oracle_price = drift.try_get_oracle_price_data_and_slot(MarketId::perp(order_params.market_index)).expect("got oracle price");
                            log::trace!(target: "filler", "oracle price: slot:{:?},market:{:?},price:{:?}", oracle_price.slot, order_params.market_index, oracle_price.data.price);

                            let (auction_start_price, auction_end_price, auction_duration) = match order_params.get_auction_params(&oracle_price.data, tick_size, min_perp_auction_duration) {
                                Some(params) => {
                                    log::debug!(target: "swift", "updated auction params: {params:?}");
                                    params
                                },
                                None => (order_params.auction_start_price.unwrap_or_default(), order_params.auction_end_price.unwrap_or_default(), order_params.auction_duration.unwrap_or_default())
                            };
                            let oracle_price = oracle_price.data.price;

                            // TODO: this isn't accurate, it should needs to call: OrderParams::update_perp_auction_params
                            // to get the auction params first
                            // especially if will_sanitize
                            let order = Order {
                                slot,
                                price: order_params.price,
                                base_asset_amount: order_params.base_asset_amount,
                                trigger_price: order_params.trigger_price.unwrap_or_default(),
                                auction_duration,
                                auction_start_price,
                                auction_end_price,
                                max_ts: order_params.max_ts.unwrap_or_default(),
                                oracle_price_offset: order_params.oracle_price_offset.unwrap_or_default(),
                                market_index: order_params.market_index,
                                order_type: order_params.order_type,
                                market_type: order_params.market_type,
                                user_order_id: order_params.user_order_id,
                                direction: order_params.direction,
                                reduce_only: order_params.reduce_only,
                                post_only: order_params.post_only != PostOnlyParam::None,
                                immediate_or_cancel: order_params.immediate_or_cancel(),
                                trigger_condition: order_params.trigger_condition,
                                bit_flags: order_params.bit_flags,
                                ..Default::default()
                            };

                            let lookahead = 1;
                            let pf = priority_fee_subscriber.priority_fee_nth(0.6);
                            for offset in 0..=lookahead {
                                let price = match order_params.order_type {
                                    OrderType::Market | OrderType::Oracle => {
                                        match calculate_auction_price(&order, slot + offset, tick_size, Some(oracle_price), false) {
                                            Ok(p) => p,
                                            Err(err) => {
                                                log::warn!(target: "dlob", "could not get auction price {err:?}, params: {order_params:?}, skipping...");
                                                continue;
                                            }
                                        }
                                    }
                                    OrderType::Limit => {
                                        match order.get_limit_price(Some(oracle_price), Some(oracle_price as u64), slot + offset, tick_size, false, None) {
                                            Ok(Some(p)) => p,
                                            _ => {
                                                log::warn!(target: "dlob", "could not get limit price: {order_params:?}, skipping...");
                                                continue;
                                            },
                                        }
                                    }
                                    _ => {
                                        log::warn!("invalid swift order type");
                                        unreachable!();
                                    }
                                };
                                let taker_order = TakerOrder::from_order_params(order_params, price);
                                let perp_market = drift.try_get_perp_market_account(order_params.market_index).unwrap();
                                let vamm_price = if order_params.direction == PositionDirection::Long {
                                    perp_market.calculate_ask_price()
                                } else {
                                    perp_market.calculate_bid_price()
                                };
                                let crosses = dlob.find_crosses_for_taker_order(slot + offset, oracle_price as u64, taker_order, Some(vamm_price as u64));
                                if !crosses.is_empty() {
                                    log::info!(target: "filler", "found resting cross|offset={offset}|crosses={crosses:?}");
                                    try_swift_fill(
                                        drift,
                                        pf,
                                        config.swift_cu_limit,
                                        filler_subaccount,
                                        signed_order,
                                        crosses,
                                        tx_worker_ref.clone(),
                                    ).await;
                                    break;
                                }
                            }
                        }
                        None => {
                            log::error!("swift order stream finished");
                            break;
                        }
                    }
                }
                new_slot = slot_rx.recv() => {
                    slot = new_slot.expect("got slot update");

                    let priority_fee = priority_fee_subscriber.priority_fee_nth(0.5) + slot % 2; // add entropy to produce unique tx hash on conseuctive tx resubmission
                    let t0 = std::time::SystemTime::now();
                    let unix_now = t0.duration_since(std::time::SystemTime::UNIX_EPOCH).unwrap().as_secs() as i64;

                    for market in &market_ids {
                        let market_index = market.index();

                        let perp_market = drift.try_get_perp_market_account(market_index).expect("got perp market");
                        let chain_oracle_price = drift.try_get_oracle_price_data_and_slot(*market).expect("got oracle price");
                        log::trace!(target: "filler", "oracle price: slot:{:?},market:{:?},price:{:?}", chain_oracle_price.slot, market, chain_oracle_price.data.price);
                        let oracle_price = chain_oracle_price.data.price as u64;
                        let trigger_price = perp_market.get_trigger_price(oracle_price as i64, unix_now, use_median_trigger_price).unwrap_or(oracle_price);

                        let mut crosses_and_top_makers = dlob.find_crosses_for_auctions(market_index, MarketType::Perp, slot + 1, oracle_price, trigger_price, Some(&perp_market));
                        crosses_and_top_makers.crosses.retain(|(o, _)| limiter.allow_event(slot, o.order_id));

                        if crosses_and_top_makers.crosses.len() > 0 {
                            log::info!(target: "filler", "found auction crosses. market: {},{crosses_and_top_makers:?}", market.index());
                            try_auction_fill(
                                drift,
                                priority_fee,
                                config.fill_cu_limit,
                                market_index,
                                filler_subaccount,
                                crosses_and_top_makers,
                                tx_worker_ref.clone(),
                                None,
                                perp_market.has_too_much_drawdown(),
                            ).await;
                        }

                        // ghetto rate limit
                        if slot % 2 == 0 {
                            if let Some(crosses) = dlob.find_crossing_region(slot + 1, oracle_price, market_index, MarketType::Perp) {
                                log::info!(target: "filler", "found limit crosses (market: {market_index})");
                                try_uncross(drift, slot + 1, priority_fee, config.fill_cu_limit, market_index, filler_subaccount, crosses, &tx_worker_ref);
                            }
                        }

                        // check state config ~every minute
                        if slot % 150 == 0 {
                            use_median_trigger_price = drift
                            .state_account()
                            .map(|s| s.feature_bit_flags & 0b0000_0010 != 0) // FeatureBitFlags::MedianTriggerPrice
                            .unwrap_or(false);
                        }
                    }
                    let duration = std::time::SystemTime::now().duration_since(t0).unwrap().as_millis();
                    log::debug!(target: "filler", "⏱️ checked fills at {slot}: {:?}ms", duration);
                }
            }
        }
        drift.grpc_unsubscribe();
        log::info!(target: "filler", "filler shutting down...");
    }
}

fn on_transaction_update_fn(
    tx_worker_ref: TxSender,
) -> impl Fn(&TransactionUpdate) + Send + Sync + 'static {
    move |tx: &TransactionUpdate| {
        if let Some(sig) = tx.transaction.signatures.first() {
            tx_worker_ref.confirm_tx((sig.as_slice().try_into()).expect("valid signature"));
        } else {
            log::warn!(target: "filler", "received tx without sig: {tx:?}");
        }
    }
}

fn on_slot_update_fn(
    dlob_notifier: DLOBNotifier,
    drift: DriftClient,
    slot_tx: tokio::sync::mpsc::Sender<u64>,
    market_ids: &[MarketId],
) -> impl Fn(u64) + Send + Sync + 'static {
    let market_ids: Vec<MarketId> = market_ids.to_vec();
    move |new_slot| {
        for market in &market_ids {
            if let Some(oracle_price) = drift.try_get_oracle_price_data_and_slot(*market) {
                dlob_notifier
                    .send(DLOBEvent::SlotOrPriceUpdate {
                        slot: new_slot,
                        market_index: market.index(),
                        market_type: market.kind(),
                        oracle_price: oracle_price.data.price as u64,
                    })
                    .expect("sent");
            }
        }
        slot_tx.try_send(new_slot).expect("sent");
    }
}

fn on_account_update_fn(
    dlob_notifier: DLOBNotifier,
    drift: DriftClient,
) -> impl Fn(&AccountUpdate) + Send + Sync + 'static {
    move |update| {
        let new_user = drift_rs::utils::deser_zero_copy(update.data);
        match drift
            .backend()
            .account_map()
            .account_data_and_slot::<User>(&update.pubkey)
        {
            Some(stored) => {
                if stored.slot <= update.slot {
                    let user_order_deltas = drift_rs::dlob::util::compare_user_orders(
                        update.pubkey,
                        &stored.data,
                        new_user,
                    );
                    for delta in user_order_deltas {
                        dlob_notifier
                            .send(DLOBEvent::Order {
                                delta,
                                slot: update.slot,
                            })
                            .expect("sent");
                    }
                } else {
                    log::warn!(target: "filler", "out of order update at slot: {:?} - {:?}", stored.slot, update.slot);
                }
            }
            None => {
                for order in new_user.orders {
                    if order.status == OrderStatus::Open
                        && order.base_asset_amount > order.base_asset_amount_filled
                    {
                        dlob_notifier
                            .send(DLOBEvent::Order {
                                delta: OrderDelta::Create {
                                    user: update.pubkey,
                                    order,
                                },
                                slot: update.slot,
                            })
                            .expect("sent")
                    }
                }
            }
        }
    }
}

/// Try to fill a swift order
async fn try_swift_fill(
    drift: &'static DriftClient,
    priority_fee: u64,
    cu_limit: u32,
    filler_subaccount: Pubkey,
    swift_order: SignedOrderInfo,
    crosses: MakerCrosses,
    tx_worker_ref: TxSender,
) {
    log::info!(target: "filler", "try fill swift order: {}", swift_order.order_uuid_str());
    let taker_order = swift_order.order_params();
    let taker_subaccount = swift_order.taker_subaccount();
    let taker_authority = swift_order.taker_authority;

    let filler_account_data = drift
        .try_get_account::<User>(&filler_subaccount)
        .expect("filler account");
    let taker_stats = Wallet::derive_stats_account(&taker_authority);
    let (taker_account_data, taker_stats) = tokio::try_join!(
        drift.get_account_value::<User>(&taker_subaccount),
        drift.get_account_value::<UserStats>(&taker_stats)
    )
    .unwrap();
    let tx_builder = TransactionBuilder::new(
        drift.program_data(),
        filler_subaccount,
        std::borrow::Cow::Borrowed(&filler_account_data),
        false,
    );

    let maker_accounts: Vec<User> = crosses
        .orders
        .iter()
        .filter(|m| m.0.user != taker_subaccount) // can't fill itself
        .map(|(m, _, _)| {
            drift
                .try_get_account::<User>(&m.user)
                .expect("maker account syncd")
        })
        .collect();

    if maker_accounts.is_empty() && !crosses.has_vamm_cross {
        log::warn!("invalid cross: {crosses:?}");
        return;
    }

    // let taker_order_id = taker_account_data.next_order_id;
    let tx = tx_builder
        .with_priority_fee(priority_fee, Some(cu_limit))
        .place_swift_order(&swift_order, &taker_account_data)
        .fill_perp_order(
            taker_order.market_index,
            taker_subaccount,
            &taker_account_data,
            &taker_stats,
            None, // Some(taker_order_id), // assuming we're fast enough that its the taker_order_id, should be ok for retail
            maker_accounts.as_slice(),
        )
        .build();

    tx_worker_ref.send_tx(
        tx,
        TxIntent::SwiftFill {
            maker_crosses: crosses,
        },
        cu_limit as u64,
    );
}

/// Try to fill an auction order
///
/// - `auction_crosses` list of one or more crosses to fill
async fn try_auction_fill(
    drift: &'static DriftClient,
    priority_fee: u64,
    cu_limit: u32,
    market_index: u16,
    filler_subaccount: Pubkey,
    auction_crosses: CrossesAndTopMakers,
    tx_worker_ref: TxSender,
    oracle_update: Option<PythPriceUpdate>,
    is_vamm_inactive: bool,
) {
    let filler_account_data = drift
        .try_get_account::<User>(&filler_subaccount)
        .expect("filler account");

    let top_maker_asks: Vec<User> = auction_crosses
        .top_maker_asks
        .iter()
        .map(|m| {
            drift
                .try_get_account::<User>(m)
                .expect("maker account syncd")
        })
        .collect();

    let top_maker_bids: Vec<User> = auction_crosses
        .top_maker_bids
        .iter()
        .map(|m| {
            drift
                .try_get_account::<User>(m)
                .expect("maker account syncd")
        })
        .collect();
    let mut sent_oracle_update = false;
    for (taker_order_metadata, crosses) in auction_crosses.crosses {
        log::info!("try fill auction order: {taker_order_metadata:?}");
        let taker_subaccount = taker_order_metadata.user;

        let taker_account_data = drift
            .try_get_account::<User>(&taker_subaccount)
            .expect("taker account");

        let taker_stats = drift
            .get_account_value::<UserStats>(&Wallet::derive_stats_account(
                &taker_account_data.authority,
            ))
            .await
            .expect("taker stats");

        let mut tx_builder = TransactionBuilder::new(
            drift.program_data(),
            filler_subaccount,
            std::borrow::Cow::Borrowed(&filler_account_data),
            false,
        );

        tx_builder = tx_builder.with_priority_fee(priority_fee, Some(cu_limit));

        if let Some(ref update_msg) = oracle_update {
            if !sent_oracle_update {
                tx_builder = tx_builder
                    .post_pyth_lazer_oracle_update(&[update_msg.feed_id], &update_msg.message);
                sent_oracle_update = true;
            }
        }

        let taker_is_trigger = matches!(
            taker_order_metadata.kind,
            OrderKind::TriggerMarket | OrderKind::TriggerLimit
        );
        if taker_is_trigger {
            log::info!(
                target: "filler",
                "attempting trigger and fill: {:?}/{:?}",
                taker_order_metadata.order_id,
                taker_order_metadata.user
            );
            tx_builder = tx_builder.trigger_order(
                taker_subaccount,
                &taker_account_data,
                taker_order_metadata.order_id,
                (market_index, MarketType::Perp),
            );
        }

        let mut maker_accounts: Vec<User> = crosses
            .orders
            .iter()
            .filter(|m| m.0.user != taker_subaccount) // can't fill itself
            .map(|(m, _, _)| {
                drift
                    .try_get_account::<User>(&m.user)
                    .expect("maker account syncd")
            })
            .collect();

        if crosses.has_vamm_cross && is_vamm_inactive {
            log::debug!(target: "filler", "skip inactive vamm cross: {crosses:?}");
            return;
        }
        if !crosses.has_vamm_cross && maker_accounts.is_empty() {
            log::debug!(target: "filler", "skip empty maker cross: {crosses:?}");
            return;
        }

        if crosses.taker_direction == PositionDirection::Long {
            maker_accounts.extend_from_slice(&top_maker_asks);
        } else {
            maker_accounts.extend_from_slice(&top_maker_bids);
        }

        tx_builder = tx_builder.fill_perp_order(
            market_index,
            taker_subaccount,
            &taker_account_data,
            &taker_stats,
            Some(taker_order_metadata.order_id),
            maker_accounts.as_slice(),
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

        tx_worker_ref.send_tx(
            tx,
            TxIntent::AuctionFill {
                taker_order_id: taker_order_metadata.order_id,
                maker_crosses: crosses,
                has_trigger: taker_is_trigger,
            },
            cu_limit as u64,
        );
    }
}

/// Try to uncross top of book
///
/// - `crosses` list of one or more crosses to fill
fn try_uncross(
    drift: &DriftClient,
    slot: u64,
    priority_fee: u64,
    cu_limit: u32,
    market_index: u16,
    filler_subaccount: Pubkey,
    crosses: CrossingRegion,
    tx_worker_ref: &TxSender,
) {
    let filler_account_data = drift
        .try_get_account::<User>(&filler_subaccount)
        .expect("filler account");

    let best_bid = &crosses.crossing_bids.first();
    let best_ask = &crosses.crossing_asks.first();

    if best_bid.is_none() || best_ask.is_none() {
        return;
    }

    let best_bid = best_bid.unwrap();
    let best_ask = best_ask.unwrap();

    let maker_asks: Vec<User> = crosses
        .crossing_asks
        .iter()
        .take(5)
        .filter_map(|x| {
            let maker = x.metadata.user;
            if maker != best_bid.metadata.user {
                drift.try_get_account::<User>(&maker).ok()
            } else {
                None
            }
        })
        .collect();

    let maker_bids: Vec<User> = crosses
        .crossing_bids
        .iter()
        .take(5)
        .filter_map(|x| {
            let maker = x.metadata.user;
            if maker != best_ask.metadata.user {
                drift.try_get_account::<User>(&maker).ok()
            } else {
                None
            }
        })
        .collect();

    log::info!("try uncross book={market_index},slot={slot}");
    log::info!(
        "X asks: {:?}, X bids: {:?}",
        crosses.crossing_asks,
        crosses.crossing_bids
    );

    // try valid combinations of taker/maker with all crossing asks/bids
    for (taker_order, makers) in [(best_ask, maker_bids), (best_bid, maker_asks)] {
        let taker_order_id = taker_order.metadata.order_id;
        let taker_subaccount = taker_order.metadata.user;
        let taker_account_data = drift
            .try_get_account::<User>(&taker_subaccount)
            .expect("taker account");

        let taker_stats = drift
            .try_get_account::<UserStats>(&Wallet::derive_stats_account(
                &taker_account_data.authority,
            ))
            .expect("taker stats");

        let mut tx_builder = TransactionBuilder::new(
            drift.program_data(),
            filler_subaccount,
            std::borrow::Cow::Borrowed(&filler_account_data),
            false,
        );
        tx_builder = tx_builder
            .with_priority_fee(priority_fee, Some(cu_limit))
            .fill_perp_order(
                market_index,
                taker_subaccount,
                &taker_account_data,
                &taker_stats,
                Some(taker_order_id),
                makers.as_slice(),
            );

        // large accounts list, bump CU limit to compensate
        if let Some(ix) = tx_builder.ixs().last() {
            if ix.accounts.len() >= 40 {
                tx_builder = tx_builder.set_ix(
                    1,
                    ComputeBudgetInstruction::set_compute_unit_limit((cu_limit * 25) / 10),
                );
            }
        }
        let tx = tx_builder.build();

        tx_worker_ref.send_tx(
            tx,
            TxIntent::LimitUncross {
                slot,
                market_index,
                taker_order_id,
                maker_order_id: 0,
            },
            cu_limit as u64,
        );
    }
}

/// Setup gRPC subscriptions
///
/// Syncs User orders and UserStat accounts
async fn setup_grpc(
    drift: DriftClient,
    dlob: &'static DLOB,
    market_ids: &[MarketId],
    tx_worker_ref: TxSender,
) -> tokio::sync::mpsc::Receiver<u64> {
    let dlob_notifier = dlob.spawn_notifier();

    let _ = tokio::try_join!(
        sync_stats_accounts(&drift),
        sync_user_accounts(&drift, &dlob_notifier),
    );

    let (slot_tx, slot_rx) = tokio::sync::mpsc::channel(64);

    subscribe_grpc(drift, dlob_notifier, slot_tx, tx_worker_ref, market_ids).await;

    slot_rx
}

async fn sync_stats_accounts(drift: &DriftClient) -> Result<()> {
    let stats_sync_result = drift
        .rpc()
        .get_program_accounts_with_config(
            &PROGRAM_ID,
            RpcProgramAccountsConfig {
                filters: Some(vec![drift_rs::memcmp::get_user_stats_filter()]),
                account_config: RpcAccountInfoConfig {
                    encoding: Some(UiAccountEncoding::Base64Zstd),
                    ..Default::default()
                },
                ..Default::default()
            },
        )
        .await;

    match stats_sync_result {
        Ok(accounts) => {
            for (pubkey, account) in accounts {
                drift.backend().account_map().on_account_fn()(&AccountUpdate {
                    pubkey,
                    data: &account.data,
                    lamports: account.lamports,
                    owner: PROGRAM_ID,
                    rent_epoch: u64::MAX,
                    executable: false,
                    slot: 0,
                });
            }
        }
        Err(err) => {
            log::error!(target: "dlob", "dlob sync error: {err:?}");
        }
    }
    log::info!(target: "dlob", "sync stats accounts");
    Ok(())
}

async fn sync_user_accounts(drift: &DriftClient, dlob_notifier: &DLOBNotifier) -> Result<()> {
    let sync_result = drift
        .rpc()
        .get_program_accounts_with_config(
            &PROGRAM_ID,
            RpcProgramAccountsConfig {
                filters: Some(vec![
                    drift_rs::memcmp::get_non_idle_user_filter(),
                    drift_rs::memcmp::get_user_filter(),
                ]),
                account_config: RpcAccountInfoConfig {
                    encoding: Some(UiAccountEncoding::Base64Zstd),
                    ..Default::default()
                },
                ..Default::default()
            },
        )
        .await;

    match sync_result {
        Ok(accounts) => {
            for (pubkey, account) in accounts {
                let user: &User = drift_rs::utils::deser_zero_copy(&account.data);
                for order in user.orders {
                    if order.status == OrderStatus::Open
                        && order.base_asset_amount > order.base_asset_amount_filled
                    {
                        dlob_notifier
                            .send(DLOBEvent::Order {
                                delta: OrderDelta::Create {
                                    user: pubkey,
                                    order,
                                },
                                slot: 0,
                            })
                            .expect("sent");
                    }
                }
                drift.backend().account_map().on_account_fn()(&AccountUpdate {
                    pubkey,
                    data: &account.data,
                    lamports: account.lamports,
                    owner: PROGRAM_ID,
                    rent_epoch: u64::MAX,
                    executable: false,
                    slot: 0,
                });
            }
        }
        Err(err) => {
            log::error!(target: "dlob", "dlob sync error: {err:?}");
        }
    }
    log::info!(target: "dlob", "synced initial orders");
    Ok(())
}

async fn subscribe_grpc(
    drift: DriftClient,
    dlob_notifier: DLOBNotifier,
    slot_tx: tokio::sync::mpsc::Sender<u64>,
    transaction_tx: TxSender,
    market_ids: &[MarketId],
) {
    let _res = drift
        .grpc_subscribe(
            "https://api.rpcpool.com".into(),
            std::env::var("GRPC_X_TOKEN").expect("GRPC_X_TOKEN set"),
            GrpcSubscribeOpts::default()
                .commitment(solana_sdk::commitment_config::CommitmentLevel::Processed)
                .usermap_on()
                .transaction_include_accounts(vec![drift.wallet().default_sub_account()])
                .on_transaction(on_transaction_update_fn(transaction_tx.clone()))
                .on_slot(on_slot_update_fn(
                    dlob_notifier.clone(),
                    drift.clone(),
                    slot_tx.clone(),
                    market_ids,
                ))
                .on_account(
                    AccountFilter::partial().with_discriminator(User::DISCRIMINATOR),
                    on_account_update_fn(dlob_notifier.clone(), drift.clone()),
                ),
            true,
        )
        .await;
}

pub enum TxWork {
    Send {
        tx: VersionedMessage,
        ts: u64,
        intent: TxIntent,
        cu_limit: u64,
    },
    Confirm {
        tx: Signature,
        ts: u64,
    },
}

pub struct TxWorker {
    drift: &'static DriftClient,
    pending_txs: Arc<RwLock<PendingTxs<1024>>>,
    metrics: Arc<Metrics>,
    dry_run: bool,
}

#[derive(Clone)]
pub struct TxSender(crossbeam::channel::Sender<TxWork>);

impl TxSender {
    pub fn confirm_tx(&self, tx: Signature) {
        self.0
            .send(TxWork::Confirm {
                tx,
                ts: SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_millis() as u64,
            })
            .expect("sent");
    }
    pub fn send_tx(&self, tx: VersionedMessage, intent: TxIntent, cu_limit: u64) {
        self.0
            .send(TxWork::Send {
                tx,
                ts: SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_millis() as u64,
                intent,
                cu_limit,
            })
            .expect("sent");
    }
}

impl TxWorker {
    pub fn new(drift: DriftClient, metrics: Arc<Metrics>, dry_run: bool) -> Self {
        Self {
            drift: Box::leak(Box::new(drift)),
            pending_txs: Arc::new(RwLock::new(PendingTxs::new())),
            metrics,
            dry_run,
        }
    }
    pub fn run(self, rt: tokio::runtime::Handle) -> TxSender {
        let (tx, rx) = crossbeam::channel::bounded(1024);
        std::thread::spawn(move || {
            let _ = env_logger::try_init();
            while let Ok(work) = rx.recv() {
                match work {
                    TxWork::Send {
                        tx,
                        ts,
                        intent,
                        cu_limit,
                    } => {
                        if self.dry_run {
                            log::debug!(target: "filler", "skip tx dry run: {intent:?}");
                            continue;
                        }
                        self.send_tx(&rt, tx, intent, cu_limit);
                    }
                    TxWork::Confirm { tx, ts } => {
                        self.confirm_tx(&rt, tx);
                    }
                }
            }
        });
        TxSender(tx)
    }
    fn send_tx(&self, rt: &Handle, tx: VersionedMessage, intent: TxIntent, cu_limit: u64) {
        log::debug!(target: "filler", "txworker send tx: {intent:?}");
        let drift = self.drift;
        let pending_txs = Arc::clone(&self.pending_txs);
        let metrics = self.metrics.clone();
        let intent_label = intent.label();
        metrics.tx_sent.with_label_values(&[intent_label]).inc();
        metrics
            .fill_expected
            .with_label_values(&[intent_label])
            .inc();
        if intent.expected_trigger() {
            metrics.trigger_expected.inc();
        }
        rt.spawn(async move {
            match drift
                .sign_and_send_with_config(
                    tx,
                    None,
                    RpcSendTransactionConfig {
                        skip_preflight: true,
                        max_retries: Some(0),
                        ..Default::default()
                    },
                )
                .await
            {
                Ok(sig) => {
                    log::info!(target: "filler", "sent fill ⚡️: {sig:?}");
                    let mut pending = pending_txs.write().await;
                    pending.insert(PendingTxMeta::new(sig, intent, cu_limit));
                }
                Err(err) => {
                    log::info!(target: "filler", "fill failed 🐢: {err}");
                    metrics
                        .tx_failed
                        .with_label_values(&[intent_label, "send_error"])
                        .inc();
                }
            }
        });
    }
    fn confirm_tx(&self, rt: &Handle, tx: Signature) {
        // TODO: if CU limit is too low send it again with higher amount
        log::debug!(target: "filler", "txworker confirm tx: {tx:?}");
        let drift = self.drift;
        let pending_txs = Arc::clone(&self.pending_txs);
        let metrics = self.metrics.clone();
        rt.spawn(async move {
            let pending_tx_meta = {
                let mut pending = pending_txs.write().await;
                pending.confirm(&tx)
            };
            if pending_tx_meta.is_none() {
                return;
            }
            let PendingTxMeta {
                signature: _,
                intent,
                cu_limit: sent_cu_limit,
                ts: _,
            } = pending_tx_meta.unwrap();
            let intent_label = intent.label();
            let expected_fill_count = intent.expected_fill_count();
            let _ = tokio::time::sleep(Duration::from_secs(1)).await;
            match drift
                .rpc()
                .get_transaction_with_config(
                    &tx,
                    RpcTransactionConfig {
                        encoding: Some(UiTransactionEncoding::Base64),
                        commitment: Some(CommitmentConfig::confirmed()),
                        max_supported_transaction_version: Some(0),
                    },
                )
                .await
            {
                Ok(tx_log) => {
                    if let Some(meta) = tx_log.transaction.meta {
                        match meta.err {
                            None => {
                                // tx confirmed ok
                                let sig = tx.to_string();
                                let logs = meta.log_messages.unwrap();
                                let tx_confirmed_slot = tx_log.slot;
                                let (_, sent_slot) = intent.crosses_and_slot();
                                let mut actual_fills = 0;
                                for (tx_idx, log) in logs.iter().enumerate() {
                                    if let Some(event) = drift_rs::event_subscriber::try_parse_log(
                                        log.as_str(),
                                        &sig,
                                        tx_idx,
                                    ) {
                                        if let DriftEvent::OrderFill { ..} = event
                                        {
                                            actual_fills += 1;
                                        } else if let DriftEvent::OrderTrigger { .. } = event {
                                            metrics.trigger_actual.inc();
                                        } else if log.as_str().contains("exceeded CUs meter") {
                                            metrics
                                            .tx_failed
                                            .with_label_values(&[
                                                intent_label,
                                                "insufficient_cus",
                                            ])
                                            .inc();
                                        }
                                    }
                                }
                                let confirmation_slots = tx_confirmed_slot - sent_slot;
                                log::debug!(target: "filler", "txworker: {tx:?} confirmed after {confirmation_slots} slots");
                                metrics
                                    .fill_actual
                                    .with_label_values(&[intent_label])
                                    .inc();
                                metrics
                                    .confirmation_slots
                                    .with_label_values(&[intent_label])
                                    .observe(confirmation_slots as f64);
                                let cus_spent =
                                    sent_cu_limit - meta.compute_units_consumed.unwrap();
                                metrics
                                    .cu_spent
                                    .with_label_values(&[intent_label])
                                    .observe(cus_spent as f64);

                                if actual_fills == 0 {
                                    metrics
                                        .tx_confirmed
                                        .with_label_values(&[intent_label, "no_fills"])
                                        .inc();
                                } else if actual_fills < expected_fill_count as u64 {
                                    metrics
                                        .tx_confirmed
                                        .with_label_values(&[intent_label, "partial"])
                                        .inc();
                                } else {
                                        metrics
                                        .tx_confirmed
                                        .with_label_values(&[intent_label, "ok"])
                                        .inc();
                                }
                            }
                            Some(
                                TransactionError::InsufficientFundsForFee
                                | TransactionError::InsufficientFundsForRent { .. },
                            ) => {
                                log::error!(target: "filler", "bot needs more SOL!");
                                metrics
                                    .tx_failed
                                    .with_label_values(&[
                                        intent_label,
                                        "insufficient_funds",
                                    ])
                                    .inc();
                            }
                            Some(err) => {
                            log::warn!(target: "filler", "tx failed: {err:?}");
                                // tx failed with error
                                metrics
                                    .tx_failed
                                    .with_label_values(&[
                                        intent_label,
                                        &format!("{:?}", err),
                                    ])
                                    .inc();
                            }
                        }
                    } else {
                        log::warn!(target: "filler", "tx metadata missing");
                        metrics
                            .tx_failed
                            .with_label_values(&[intent_label, "metadata_missing"])
                            .inc();
                    }
                }
                Err(err) => {
                    log::info!(target: "filler", "tx confirmation failed 🐢: {err}");
                    metrics
                        .tx_failed
                        .with_label_values(&[intent_label, "confirmation_failed"])
                        .inc();
                }
            }
        });
    }
}
