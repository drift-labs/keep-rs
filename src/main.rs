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
        util::OrderDelta, DLOBEvent, DLOBNotifier, MakerCrosses, OrderMetadata, TakerOrder, DLOB,
    },
    event_subscriber::DriftEvent,
    ffi::calculate_auction_price,
    grpc::{grpc_subscriber::AccountFilter, AccountUpdate, TransactionUpdate},
    priority_fee_subscriber::PriorityFeeSubscriber,
    swift_order_subscriber::{SignedOrderInfo, SwiftOrderStream},
    types::{
        accounts::{User, UserStats},
        CommitmentConfig, MarketId, Order, OrderStatus, OrderType, PostOnlyParam,
        RpcSendTransactionConfig, VersionedMessage,
    },
    DriftClient, GrpcSubscribeOpts, Pubkey, RpcClient, TransactionBuilder, Wallet,
};
use futures_util::StreamExt;
use solana_account_decoder_client_types::UiAccountEncoding;
use solana_rpc_client_api::config::{
    RpcAccountInfoConfig, RpcProgramAccountsConfig, RpcTransactionConfig,
};
use solana_sdk::{
    signature::{Keypair, Signature},
    transaction::TransactionError,
};
use solana_transaction_status_client_types::UiTransactionEncoding;
use tokio::{runtime::Handle, sync::RwLock};

mod http;
mod util;
use crate::{
    http::{health_handler, metrics_handler, Metrics},
    util::{OrderSlotLimiter, PendingTxMeta, PendingTxs, TxIntent},
};

/// Bot configuration loaded from command line
#[derive(Debug, Clone, Parser)]
pub struct Config {
    /// Comma-separated list of perp market indices to fill for
    #[clap(long, env = "MARKET_IDS", default_value = "0,1,2,9,59")]
    pub market_ids: String,
    /// Use mainnet (otherwise devnet)
    #[clap(long, env = "MAINNET", default_value = "true")]
    pub mainnet: bool,
    #[clap(long, default_value = "512")]
    pub priority_fee: u64,
    #[clap(long, default_value = "320000")]
    pub swift_cu_limit: u32,
    #[clap(long, default_value = "420000")]
    pub fill_cu_limit: u32,
}

impl Config {
    pub fn market_id_vec(&self) -> Vec<MarketId> {
        self.market_ids
            .split(',')
            .filter_map(|s| s.trim().parse::<u16>().ok())
            .map(MarketId::perp)
            .collect()
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
    let http_task = tokio::spawn(async move {
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
    slot: u64,
    market_ids: Vec<MarketId>,
    config: Config,
    tx_worker_ref: TxSender,
    priority_fee_subscriber: Arc<PriorityFeeSubscriber>,
}

impl FillerBot {
    async fn new(config: Config, metrics: Arc<Metrics>) -> Self {
        env_logger::init();
        let _ = dotenv::dotenv();
        let wallet: Wallet = Keypair::from_base58_string(
            &std::env::var("BOT_PRIVATE_KEY").expect("base58 BOT_PRIVATE_KEY set"),
        )
        .into();

        log::info!(target: "filler", "bot started: authority={:?}, subaccount={:?}", wallet.authority(), wallet.default_sub_account());
        log::info!(target: "filler", "mainnet={}, markets={}", config.mainnet, config.market_ids);

        let filler_subaccount = wallet.default_sub_account();
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

        let tx_worker = TxWorker::new(drift.clone(), metrics);
        let rt = tokio::runtime::Handle::current();
        let tx_worker_ref = tx_worker.run(rt);
        let slot_rx = setup_grpc(
            drift.clone(),
            dlob,
            &config.market_id_vec(),
            tx_worker_ref.clone(),
        )
        .await;

        drift
            .subscribe_blockhashes()
            .await
            .expect("subscribed blockhashes");

        drift
            .subscribe_account(&filler_subaccount)
            .await
            .expect("subscribed filler subaccount");

        let market_ids = config.market_id_vec();
        let swift_order_stream = drift
            .subscribe_swift_orders(&market_ids, Some(true))
            .await
            .expect("subscribed swift orders");

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

        FillerBot {
            drift,
            dlob,
            filler_subaccount,
            slot_rx,
            swift_order_stream,
            limiter: OrderSlotLimiter::new(),
            slot: 0,
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
        let mut slot = self.slot;
        let drift = self.drift.clone();
        let dlob = self.dlob;
        let filler_subaccount = self.filler_subaccount;
        let market_ids = self.market_ids;
        let config = self.config.clone();
        let tx_worker_ref = self.tx_worker_ref.clone();
        let priority_fee_subscriber = Arc::clone(&self.priority_fee_subscriber);

        loop {
            tokio::select! {
                biased;
                _ = tokio::signal::ctrl_c() => {
                    log::warn!("filler shutting down...");
                    break;
                }
                swift_order = swift_order_stream.next() => {
                    log::debug!(target: "filler", "new swift order: {swift_order:?}");
                    match swift_order {
                        Some(signed_order) => {
                            let order_params = signed_order.order_params();
                            let tick_size = drift.program_data().perp_market_config_by_index(order_params.market_index).unwrap().amm.order_tick_size;
                            let oracle_price = drift.try_get_oracle_price_data_and_slot(MarketId::perp(order_params.market_index)).expect("got oracle price").data.price;

                            let order = Order {
                                slot,
                                price: order_params.price,
                                base_asset_amount: order_params.base_asset_amount,
                                trigger_price: order_params.trigger_price.unwrap_or_default(),
                                auction_duration: order_params.auction_duration.unwrap_or_default(),
                                auction_start_price: order_params.auction_start_price.unwrap_or_default(),
                                auction_end_price: order_params.auction_end_price.unwrap_or_default(),
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
                            for offset in 0..=lookahead {
                                let price = match order_params.order_type {
                                    OrderType::Market | OrderType::Oracle => {
                                        match calculate_auction_price(&order, slot, tick_size, Some(oracle_price), false) {
                                            Ok(p) => p,
                                            Err(err) => {
                                                log::warn!(target: "dlob", "could not get auction price {err:?}, params: {order_params:?}, skipping...");
                                                continue;
                                            }
                                        }
                                    }
                                    OrderType::Limit => {
                                        match order.get_limit_price(Some(oracle_price), Some(oracle_price as u64), slot, tick_size, false, None) {
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
                                let crosses = dlob.find_crosses_for_taker_order(slot + offset, oracle_price as u64, taker_order);
                                if !crosses.is_empty() {
                                    log::info!(target: "filler", "found resting cross|offset={offset}|crosses={crosses:?}");
                                    try_swift_fill(
                                        drift.clone(),
                                        priority_fee_subscriber.priority_fee_nth(0.8),
                                        config.swift_cu_limit,
                                        filler_subaccount,
                                        signed_order,
                                        crosses,
                                        tx_worker_ref.clone()
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

                    for market in &market_ids {
                        let oracle_price = drift.try_get_oracle_price_data_and_slot(*market).expect("got oracle price").data.price;
                        let mut crosses = dlob.find_crosses_for_auctions(market.index(), market.kind(), slot + 1, oracle_price as u64);
                        crosses.retain(|(o, _)| limiter.allow_event(slot, o.order_id));

                        if !crosses.is_empty() {
                            log::info!(target: "filler", "found auction crosses. market: {},{crosses:?}", market.index());
                            try_auction_fill(drift.clone(), priority_fee_subscriber.priority_fee_nth(0.8), config.fill_cu_limit, market.index(), filler_subaccount, crosses, tx_worker_ref.clone()).await;
                        }
                    }
                }
            }
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
    let market_ids: &'static [MarketId] = unsafe { std::mem::transmute(market_ids) };
    move |new_slot| {
        for market in market_ids {
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
                if stored.slot < update.slot {
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
    drift: DriftClient,
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

    let filler_account_data = drift
        .try_get_account::<User>(&filler_subaccount)
        .expect("filler account");
    let taker_account_data = drift
        .get_account_value::<User>(&taker_subaccount)
        .await
        .unwrap();
    let taker_stats = drift
        .try_get_account::<UserStats>(&Wallet::derive_stats_account(&taker_account_data.authority))
        .expect("taker stats");
    let tx_builder = TransactionBuilder::new(
        drift.program_data(),
        filler_subaccount,
        std::borrow::Cow::Borrowed(&filler_account_data),
        false,
    );

    let maker_accounts: Vec<User> = crosses
        .orders
        .iter()
        .map(|(m, _, _)| {
            drift
                .try_get_account::<User>(&m.user)
                .expect("maker account syncd")
        })
        .collect();

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
    drift: DriftClient,
    priority_fee: u64,
    cu_limit: u32,
    market_index: u16,
    filler_subaccount: Pubkey,
    auction_crosses: Vec<(OrderMetadata, MakerCrosses)>,
    tx_worker_ref: TxSender,
) {
    let filler_account_data = drift
        .try_get_account::<User>(&filler_subaccount)
        .expect("filler account");

    for (taker_order_metadata, crosses) in auction_crosses {
        log::info!("try fill auction order: {taker_order_metadata:?}");
        let taker_subaccount = taker_order_metadata.user;

        let taker_account_data = drift
            .get_account_value::<User>(&taker_subaccount)
            .await
            .unwrap();
        let taker_stats = drift
            .try_get_account::<UserStats>(&Wallet::derive_stats_account(
                &taker_account_data.authority,
            ))
            .expect("taker stats");
        let tx_builder = TransactionBuilder::new(
            drift.program_data(),
            filler_subaccount,
            std::borrow::Cow::Borrowed(&filler_account_data),
            false,
        );

        let maker_accounts: Vec<User> = crosses
            .orders
            .iter()
            .map(|(m, _, _)| {
                drift
                    .try_get_account::<User>(&m.user)
                    .expect("maker account syncd")
            })
            .collect();

        // let taker_order_id = taker_account_data.next_order_id;
        let tx = tx_builder
            .with_priority_fee(priority_fee, Some(cu_limit))
            .fill_perp_order(
                market_index,
                taker_subaccount,
                &taker_account_data,
                &taker_stats,
                Some(taker_order_metadata.order_id),
                maker_accounts.as_slice(),
            )
            .build();

        tx_worker_ref.send_tx(
            tx,
            TxIntent::AuctionFill {
                taker_order_id: taker_order_metadata.order_id,
                maker_crosses: crosses,
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

    sync_stats_accounts(&drift).await;
    sync_user_accounts(&drift, &dlob_notifier).await;

    let (slot_tx, slot_rx) = tokio::sync::mpsc::channel(64);

    subscribe_grpc(drift, dlob_notifier, slot_tx, tx_worker_ref, market_ids).await;

    slot_rx
}

async fn sync_stats_accounts(drift: &DriftClient) {
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
}

async fn sync_user_accounts(drift: &DriftClient, dlob_notifier: &DLOBNotifier) {
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
                .interslot_updates_on()
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
    pub fn new(drift: DriftClient, metrics: Arc<Metrics>) -> Self {
        Self {
            drift: Box::leak(Box::new(drift)),
            pending_txs: Arc::new(RwLock::new(PendingTxs::new())),
            metrics,
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
        let expected_fill_count = intent.expected_fill_count();
        metrics
            .tx_sent
            .with_label_values(&[intent_label.as_str()])
            .inc();
        metrics
            .fill_expected
            .with_label_values(&[intent_label.as_str()])
            .inc_by(expected_fill_count as u64);
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
                        .with_label_values(&[intent_label.as_str(), "send_error"])
                        .inc();
                }
            }
        });
    }
    fn confirm_tx(&self, rt: &Handle, tx: Signature) {
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
                                let mut amm_fill = false;
                                let (mut expected_fills, sent_slot) = intent.crosses_and_slot();
                                let mut actual_fills = 0;
                                for (tx_idx, log) in logs.iter().enumerate() {
                                    if let Some(event) = drift_rs::event_subscriber::try_parse_log(
                                        log.as_str(),
                                        &sig,
                                        tx_idx,
                                    ) {
                                        if let DriftEvent::OrderFill {
                                            maker,
                                            maker_order_id,
                                            taker_order_id: _,
                                            ..
                                        } = event
                                        {
                                            if maker.is_none() {
                                                amm_fill = true;
                                            }
                                            if let Some(idx) = expected_fills.iter().position(|o| {
                                                o.0.order_id == maker_order_id
                                                    && maker.is_some_and(|m| m == o.0.user)
                                            }) {
                                                expected_fills.swap_remove(idx);
                                            }
                                            actual_fills += 1;
                                        }
                                    }
                                }
                                let confirmation_slots = tx_confirmed_slot - sent_slot;
                                metrics
                                    .tx_confirmed
                                    .with_label_values(&[intent_label.as_str(), "ok"])
                                    .inc();
                                metrics
                                    .fill_actual
                                    .with_label_values(&[
                                        intent_label.as_str(),
                                        if amm_fill { "amm" } else { "orderbook" },
                                    ])
                                    .inc_by(actual_fills);
                                metrics
                                    .confirmation_slots
                                    .with_label_values(&[intent_label.as_str()])
                                    .observe(confirmation_slots as f64);
                                let cus_spent =
                                    sent_cu_limit - meta.compute_units_consumed.unwrap();
                                metrics
                                    .cu_spent
                                    .with_label_values(&[intent_label.as_str()])
                                    .observe(cus_spent as f64);
                                if actual_fills == 0 {
                                    metrics
                                        .tx_confirmed
                                        .with_label_values(&[intent_label.as_str(), "no_fills"])
                                        .inc();
                                } else if actual_fills < expected_fill_count as u64 {
                                    metrics
                                        .tx_confirmed
                                        .with_label_values(&[intent_label.as_str(), "partial"])
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
                                        intent_label.as_str(),
                                        "insufficient_funds",
                                    ])
                                    .inc();
                            }
                            Some(err) => {
                                // tx failed with error
                                metrics
                                    .tx_failed
                                    .with_label_values(&[
                                        intent_label.as_str(),
                                        &format!("{:?}", err),
                                    ])
                                    .inc();
                            }
                        }
                    } else {
                        log::warn!(target: "filler", "tx metadata missing");
                        metrics
                            .tx_failed
                            .with_label_values(&[intent_label.as_str(), "metadata_missing"])
                            .inc();
                    }
                }
                Err(err) => {
                    log::info!(target: "filler", "tx confirmation failed 🐢: {err}");
                    metrics
                        .tx_failed
                        .with_label_values(&[intent_label.as_str(), "confirmation_failed"])
                        .inc();
                }
            }
        });
    }
}
