//! Rust Keeper Bot
use std::sync::Arc;

mod filler;
mod http;
mod liquidator;
mod util;

use crate::{
    filler::FillerBot,
    http::{
        dashboard_api_handler, dashboard_handler, health_handler, metrics_handler,
        DashboardStateRef, Metrics,
    },
    liquidator::LiquidatorBot,
};
use clap::Parser;

use drift_rs::{types::MarketId, DriftClient, RpcClient, Wallet};
use mimalloc::MiMalloc;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

/// Bot configuration loaded from command line
#[derive(Debug, Clone, Parser)]
pub struct Config {
    /// minimum collateral threshold for liquidatable accounts
    #[clap(long, default_value = "1000000")]
    pub min_collateral: u64,
    /// Run perp liquidator bot
    #[clap(long, default_value = "false")]
    pub liquidator: bool,
    /// Use spot liquidation in liquidator
    #[clap(long, default_value = "true")]
    pub use_spot_liquidation: bool,
    /// Run perp filler bot
    #[clap(long, default_value = "true")]
    pub filler: bool,
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
    #[clap(long, default_value = "0")]
    pub sub_account_id: u16,
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

#[tokio::main(flavor = "multi_thread", worker_threads = 10)]
async fn main() {
    env_logger::init();
    let _ = dotenv::dotenv();

    let config = Config::parse();
    let metrics = Arc::new(Metrics::new());
    let dashboard_state: DashboardStateRef = Arc::new(tokio::sync::RwLock::new(None));

    // Start Prometheus metrics server
    let metrics_port = std::env::var("METRICS_PORT")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(9898);
    let addr = format!("0.0.0.0:{metrics_port}");
    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .expect("bind metrics port");

    let app_state = crate::http::AppState {
        metrics: Arc::clone(&metrics),
        dashboard_state: Arc::clone(&dashboard_state),
    };
    let _http_task = tokio::spawn(async move {
        axum::serve(
            listener,
            axum::Router::new()
                .route("/metrics", axum::routing::get(metrics_handler))
                .route("/health", axum::routing::get(health_handler))
                .route("/", axum::routing::get(dashboard_handler))
                .route("/dashboard", axum::routing::get(dashboard_handler))
                .route("/api/dashboard", axum::routing::get(dashboard_api_handler))
                .with_state(app_state),
        )
        .await
        .unwrap();
    });

    let wallet: Wallet = drift_rs::utils::load_keypair_multi_format(
        &std::env::var("BOT_PRIVATE_KEY").expect("base58 BOT_PRIVATE_KEY set"),
    )
    .expect("loaded BOT_PRIVATE_KEY")
    .into();

    let keeper_subaccount = wallet.default_sub_account();
    log::info!(
        "bot started: authority={:?}, subaccount={:?}",
        wallet.authority(),
        keeper_subaccount
    );
    log::info!("mainnet={}, markets={}", config.mainnet, config.all_markets);

    let context = if config.mainnet {
        drift_rs::types::Context::MainNet
    } else {
        drift_rs::types::Context::DevNet
    };
    let rpc_url =
        std::env::var("RPC_URL").unwrap_or_else(|_| "https://api.devnet.solana.com".to_string());
    let drift = DriftClient::new(context, RpcClient::new(rpc_url), wallet)
        .await
        .expect("initialized client");

    tokio::spawn({
        let drift = drift.clone();
        async move {
            let _ = tokio::signal::ctrl_c().await;
            log::warn!("ctrl+c received, bot shutting down...");
            drift.grpc_unsubscribe();
            std::process::exit(0);
        }
    });

    if config.liquidator {
        let bot = LiquidatorBot::new(config, drift, metrics, dashboard_state).await;
        bot.run().await;
    } else if config.filler {
        let bot = FillerBot::new(config, drift, metrics).await;
        bot.run().await;
    } else {
        log::warn!("provide --filler or --liquidator mode");
    }
}
