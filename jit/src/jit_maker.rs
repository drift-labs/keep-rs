use async_trait::async_trait;
use drift::state::user::User;
use drift_sdk::constants::PRICE_PRECISION;
use drift_sdk::dlob::dlob_node::DLOBNode;
use drift_sdk::{
    dlob::dlob_builder::DLOBBuilder,
    math::leverage::{get_leverage, get_spot_asset_value},
    types::{MarketType, PositionDirection},
    utils::get_ws_url,
    AccountProvider, DriftClient,
};
use rust::jitter::{JitParams, Jitter, PriceType};
use std::sync::Arc;
use tokio::sync::Mutex;

use crate::JitResult;

pub struct JitMakerConfig<T: AccountProvider> {
    pub market_indexes: Vec<u16>,
    pub sub_account_ids: Vec<u16>,
    pub target_leverage: f64,
    pub spread: f64,
    pub market_type: MarketType,
    pub drift_client: DriftClient<T>,
    pub jitter: Arc<Jitter<T>>,
    pub dlob_builder: Arc<Mutex<DLOBBuilder>>,
    pub volatility_threshold: f64,
}

#[async_trait]
pub trait Strategy {
    async fn adjust_quotes(&mut self, market_index: u16, sub_account: u16) -> JitResult<()>;
}

#[async_trait]
impl<T: AccountProvider + Clone> Strategy for JitMaker<T> {
    async fn adjust_quotes(&mut self, market_index: u16, sub_account: u16) -> JitResult<()> {
        match self.market_type {
            MarketType::Perp => {
                self.quote_perp(market_index, sub_account).await?;
            }
            MarketType::Spot => {
                self.quote_spot(market_index, sub_account).await?;
            }
        }
        Ok(())
    }
}

pub struct JitMaker<T: AccountProvider> {
    market_indexes: Vec<u16>,
    target_leverage: f64,
    spread: f64,
    market_type: MarketType,
    drift_client: DriftClient<T>,
    jitter: Arc<Jitter<T>>,
    dlob_builder: Arc<Mutex<DLOBBuilder>>,
    sub_accounts: Vec<u16>,
    volatility_threshold: f64,
}

impl<T: AccountProvider + Clone> JitMaker<T> {
    pub async fn new(config: JitMakerConfig<T>) -> JitResult<Self> {
        let mut market_indexes = config.market_indexes.clone();
        market_indexes.sort();
        market_indexes.dedup();

        let mut sub_account_ids = config.sub_account_ids.clone();
        sub_account_ids.sort();
        sub_account_ids.dedup();

        if market_indexes.len() != sub_account_ids.len() {
            return Err(crate::JitError::Generic(
                "market_indexes and sub_account_ids must be the same length".to_string(),
            ));
        }

        if market_indexes.is_empty() {
            return Err(crate::JitError::Generic(
                "market_indexes and sub_account_ids must not be empty".to_string(),
            ));
        }

        if config.target_leverage <= 0.0 {
            return Err(crate::JitError::Generic(
                "target_leverage must be greater than 0".to_string(),
            ));
        }

        Ok(JitMaker {
            market_indexes: config.market_indexes,
            target_leverage: config.target_leverage,
            spread: config.spread,
            market_type: config.market_type,
            drift_client: config.drift_client,
            jitter: config.jitter,
            dlob_builder: config.dlob_builder,
            sub_accounts: sub_account_ids,
            volatility_threshold: config.volatility_threshold,
        })
    }

    pub async fn subscribe(&mut self) -> JitResult<()> {
        self.drift_client.subscribe().await?;

        let builder = self.dlob_builder.clone();
        DLOBBuilder::start_building(builder)
            .await
            .expect("dlob builder");

        let jitter = self.jitter.clone();
        let url = self.drift_client.inner().url();
        tokio::task::spawn(async move {
            jitter
                .clone()
                .subscribe(get_ws_url(&url).expect("valid url"))
                .await
                .expect("jitter subscription");
        });

        let market_indexes = self.market_indexes.clone();
        let sub_accounts = self.sub_accounts.clone();
        loop {
            for (market_index, sub_account) in market_indexes.iter().zip(sub_accounts.iter()) {
                self.adjust_quotes(*market_index, *sub_account).await?;
            }
        }
    }

    fn overlevered(
        &self,
        user: &User,
        market_type: MarketType,
        market_index: u16,
    ) -> JitResult<Option<PositionDirection>> {
        let current_leverage =
            get_leverage(&self.drift_client, user)? as f64 / PRICE_PRECISION as f64;
        log::info!("current leverage: {}", current_leverage);
        let overlevered = current_leverage >= self.target_leverage;
        if overlevered {
            match market_type {
                MarketType::Perp => {
                    let perp_positions = user.perp_positions.to_vec();
                    let perp_position = perp_positions
                        .iter()
                        .find(|p| p.market_index == market_index)
                        .expect("perp position");
                    let overlevered_direction = match perp_position.base_asset_amount.cmp(&0) {
                        std::cmp::Ordering::Less => Some(PositionDirection::Short),
                        std::cmp::Ordering::Greater => Some(PositionDirection::Long),
                        std::cmp::Ordering::Equal => None,
                    };
                    Ok(overlevered_direction)
                }
                MarketType::Spot => {
                    let spot_positions = user.spot_positions.to_vec();
                    let spot_position = spot_positions
                        .iter()
                        .find(|p| p.market_index == market_index)
                        .expect("spot position");
                    let overlevered_direction = match spot_position.scaled_balance.cmp(&0) {
                        std::cmp::Ordering::Less => Some(PositionDirection::Short),
                        std::cmp::Ordering::Greater => Some(PositionDirection::Long),
                        std::cmp::Ordering::Equal => None,
                    };
                    Ok(overlevered_direction)
                }
            }
        } else {
            Ok(None)
        }
    }

    async fn quote_perp(&mut self, market_index: u16, sub_account: u16) -> JitResult<()> {
        log::info!("quoting perp market {}", market_index);
        let start = std::time::Instant::now();
        let user = self.drift_client.get_user(sub_account).expect("user");
        let user_account = user.get_user_account();

        let perp_market_account = self
            .drift_client
            .get_perp_market_account(market_index)
            .expect("perp market account");
        let oracle = self
            .drift_client
            .get_oracle_price_data_and_slot_for_perp_market(market_index)
            .expect("oracle");

        let net_spot_asset_value = get_spot_asset_value(&self.drift_client, &user_account)?;
        let max_base = crate::utils::calculate_base_amount_to_mm_perp(
            perp_market_account.clone(),
            net_spot_asset_value,
            self.target_leverage,
        );

        let overlevered_direction_maybe =
            self.overlevered(&user_account, self.market_type, market_index)?;

        let dlob_builder_reader = self.dlob_builder.lock().await;
        let dlob = dlob_builder_reader.get_dlob();
        drop(dlob_builder_reader);

        let is_market_volatile = crate::utils::is_perp_market_volatile(
            perp_market_account,
            oracle.data,
            self.volatility_threshold,
        );

        self.jitter.set_exclusion_criteria(is_market_volatile);

        let best_bid_maybe = crate::utils::get_best_dlob_bid(
            dlob.clone(),
            perp_market_account.market_index,
            MarketType::Perp,
            oracle.clone(),
            user.pubkey.to_string(),
        );

        let best_ask_maybe = crate::utils::get_best_dlob_ask(
            dlob.clone(),
            perp_market_account.market_index,
            MarketType::Perp,
            oracle.clone(),
            user.pubkey.to_string(),
        );

        let reserve_price = perp_market_account
            .amm
            .reserve_price()
            .expect("reserve price");
        let (best_amm_bid, best_amm_ask) = perp_market_account
            .amm
            .bid_ask_price(reserve_price)
            .expect("amm bid ask");

        let best_bid_price = if let Some(best_dlob_bid) = best_bid_maybe {
            let best_dlob_bid_price = best_dlob_bid.get_price(oracle.data, oracle.slot);
            match best_dlob_bid_price.cmp(&best_amm_bid) {
                std::cmp::Ordering::Less => best_amm_bid,
                std::cmp::Ordering::Greater => best_dlob_bid_price,
                std::cmp::Ordering::Equal => best_amm_bid,
            }
        } else {
            best_amm_bid
        };

        let best_ask_price = if let Some(best_dlob_ask) = best_ask_maybe {
            let best_dlob_ask_price = best_dlob_ask.get_price(oracle.data, oracle.slot);
            match best_dlob_ask_price.cmp(&best_amm_ask) {
                std::cmp::Ordering::Less => best_dlob_ask_price,
                std::cmp::Ordering::Greater => best_amm_ask,
                std::cmp::Ordering::Equal => best_dlob_ask_price,
            }
        } else {
            best_amm_ask
        };

        log::info!("best bid price: {}", best_bid_price);
        log::info!("best ask price: {}", best_ask_price);
        log::info!("oracle price: {}", oracle.data.price);
        log::info!("max base: {}", max_base);

        let bid_offset = (best_bid_price as f64 - ((1.0 + self.spread) * oracle.data.price as f64))
            .floor() as i64;
        let ask_offset = (best_ask_price as f64 - ((1.0 - self.spread) * oracle.data.price as f64))
            .floor() as i64;

        let perp_min_position = -(max_base as i64);
        let perp_max_position = max_base as i64;

        let (min_position, max_position) = match overlevered_direction_maybe {
            Some(PositionDirection::Long) => (perp_min_position, 0_i64),
            Some(PositionDirection::Short) => (0_i64, perp_max_position),
            None => (perp_min_position, perp_max_position),
        };

        let new_perp_params = JitParams::new(
            bid_offset,
            ask_offset,
            min_position,
            max_position,
            PriceType::Oracle,
        );

        self.jitter
            .update_perp_params(market_index, new_perp_params);

        log::info!(
            "jitter perp params updated, market_index: {}, bid: {}, ask: {} min_position: {}, max_position: {}",
            market_index,
            bid_offset,
            ask_offset,
            min_position,
            max_position
        );
        log::info!("quote perp took: {:?}", start.elapsed());

        Ok(())
    }

    async fn quote_spot(&mut self, market_index: u16, sub_account: u16) -> JitResult<()> {
        log::info!("quoting spot market {}", market_index);
        let user = self.drift_client.get_user(sub_account).expect("user");
        let user_account = user.get_user_account();

        let spot_market_account = self
            .drift_client
            .get_spot_market_account(market_index)
            .expect("spot market account");
        let oracle = self
            .drift_client
            .get_oracle_price_data_and_slot_for_spot_market(market_index)
            .expect("oracle");

        let net_spot_asset_value = get_spot_asset_value(&self.drift_client, &user_account)?;

        let max_base = crate::utils::calculate_base_amount_to_mm_spot(
            spot_market_account.clone(),
            net_spot_asset_value,
            self.target_leverage,
        );

        let overlevered_direction_maybe =
            self.overlevered(&user_account, self.market_type, market_index)?;

        let dlob_builder_reader = self.dlob_builder.lock().await;
        let dlob = dlob_builder_reader.get_dlob();
        drop(dlob_builder_reader);

        let is_market_volatile = crate::utils::is_spot_market_volatile(
            spot_market_account,
            oracle.data,
            self.volatility_threshold,
        );

        self.jitter.set_exclusion_criteria(is_market_volatile);

        let best_bid_maybe = crate::utils::get_best_dlob_bid(
            dlob.clone(),
            spot_market_account.market_index,
            MarketType::Spot,
            oracle.clone(),
            user.pubkey.to_string(),
        );

        let best_ask_maybe = crate::utils::get_best_dlob_ask(
            dlob.clone(),
            spot_market_account.market_index,
            MarketType::Spot,
            oracle.clone(),
            user.pubkey.to_string(),
        );

        match (best_bid_maybe, best_ask_maybe) {
            (Some(best_bid), Some(best_ask)) => {
                let best_bid_price = best_bid.get_price(oracle.data, oracle.slot);
                let best_ask_price = best_ask.get_price(oracle.data, oracle.slot);

                log::info!("best bid price: {}", best_bid_price);
                log::info!("best ask price: {}", best_ask_price);
                log::info!("oracle price: {}", oracle.data.price);
                log::info!("max base: {}", max_base);

                let bid_offset = (best_bid_price as f64
                    - ((1.0 + self.spread) * oracle.data.price as f64))
                    .floor() as i64;
                let ask_offset = (best_ask_price as f64
                    - ((1.0 - self.spread) * oracle.data.price as f64))
                    .floor() as i64;

                let spot_min_position = -(max_base as i64);
                let spot_max_position = max_base as i64;

                let (min_position, max_position) = match overlevered_direction_maybe {
                    Some(PositionDirection::Long) => (spot_min_position, 0_i64),
                    Some(PositionDirection::Short) => (0_i64, spot_max_position),
                    None => (spot_min_position, spot_max_position),
                };

                let new_spot_params = JitParams::new(
                    bid_offset,
                    ask_offset,
                    min_position,
                    max_position,
                    PriceType::Oracle,
                );

                self.jitter
                    .update_spot_params(market_index, new_spot_params);

                log::info!(
                    "jitter spot params updated, market_index: {}, bid: {}, ask: {} min_position: {}, max_position: {}",
                    market_index,
                    bid_offset,
                    ask_offset,
                    min_position,
                    max_position
                );
            }
            _ => {
                log::error!("missing best bid or best ask, skipping...");
            }
        }
        Ok(())
    }
}
