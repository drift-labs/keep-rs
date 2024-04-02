use drift::{
    math::constants::{BASE_PRECISION, PERCENTAGE_PRECISION_I64, QUOTE_PRECISION},
    state::oracle::OraclePriceData,
};
use drift_sdk::{
    constants::PRICE_PRECISION,
    dlob::{
        dlob::DLOB,
        dlob_node::{DLOBNode, Node},
    },
    oraclemap::Oracle,
    types::{MarketType, PerpMarket, SpotMarket},
};

pub fn get_best_dlob_bid(
    mut dlob: DLOB,
    market_index: u16,
    market_type: MarketType,
    oracle: Oracle,
    jit_maker_pubkey: String,
) -> Option<Node> {
    let bids = dlob.get_resting_limit_bids(oracle.slot, market_type, market_index, oracle.data);

    for bid in bids.iter() {
        match bid {
            Node::OrderNode(_) => {
                if bid.get_user_account().to_string() == jit_maker_pubkey {
                    continue;
                }
                return Some(*bid);
            }
            Node::VAMMNode(_) => {
                continue;
            }
        }
    }

    None
}

pub fn get_best_dlob_ask(
    mut dlob: DLOB,
    market_index: u16,
    market_type: MarketType,
    oracle: Oracle,
    jit_maker_pubkey: String,
) -> Option<Node> {
    let asks = dlob.get_resting_limit_asks(oracle.slot, market_type, market_index, oracle.data);

    for ask in asks.iter() {
        match ask {
            Node::OrderNode(_) => {
                if ask.get_user_account().to_string() == jit_maker_pubkey {
                    continue;
                }
                return Some(*ask);
            }
            Node::VAMMNode(_) => {
                continue;
            }
        }
    }

    None
}

fn normalize(num: u128, precision: u128) -> f64 {
    (num / precision) as f64 + (num % precision) as f64 / precision as f64
}

pub fn calculate_base_amount_to_mm_spot(
    spot_market: SpotMarket,
    net_spot_market_value: i128,
    target_leverage: f64,
) -> u128 {
    let base_price_normalized = normalize(
        spot_market.historical_oracle_data.last_oracle_price_twap as u128,
        PRICE_PRECISION,
    );

    let total_collateral_normalized = normalize(net_spot_market_value as u128, QUOTE_PRECISION);

    log::info!(
        "{} -> {}",
        net_spot_market_value,
        total_collateral_normalized
    );

    let target_leverage = target_leverage * 0.95;

    let max_base = (total_collateral_normalized / base_price_normalized) * target_leverage;

    log::info!(
        "max base for spot market: {} -> {}",
        spot_market.market_index,
        max_base
    );

    (max_base * 10_u64.pow(spot_market.decimals) as f64) as u128
}

pub fn calculate_base_amount_to_mm_perp(
    perp_market: PerpMarket,
    net_spot_market_value: i128,
    target_leverage: f64,
) -> u128 {
    let base_price_normalized = normalize(
        perp_market
            .amm
            .historical_oracle_data
            .last_oracle_price_twap as u128,
        PRICE_PRECISION,
    );

    let total_collateral_normalized = normalize(net_spot_market_value as u128, QUOTE_PRECISION);

    log::info!(
        "{} -> {}",
        net_spot_market_value,
        total_collateral_normalized
    );

    let target_leverage = target_leverage * 0.95;

    let max_base = (total_collateral_normalized / base_price_normalized) * target_leverage;

    log::info!(
        "max base for perp market: {} -> {}",
        perp_market.market_index,
        max_base
    );

    (max_base * BASE_PRECISION as f64) as u128
}

pub fn is_perp_market_volatile(
    perp_market: PerpMarket,
    oracle_price_data: OraclePriceData,
    volatility_threshold: f64,
) -> bool {
    let twap_price = perp_market
        .amm
        .historical_oracle_data
        .last_oracle_price_twap_5min;
    let last_price = perp_market.amm.historical_oracle_data.last_oracle_price;
    let current_price = oracle_price_data.price;

    let min_denom = std::cmp::min(twap_price, std::cmp::min(current_price, last_price)) as u128;

    let c_vs_l = ((current_price - last_price).abs() as u128) * PRICE_PRECISION / min_denom;
    let c_vs_t = ((current_price - twap_price).abs() as u128) * PRICE_PRECISION / min_denom;

    let recent_std = (perp_market.amm.oracle_std as u128) * PRICE_PRECISION / min_denom;

    let c_vs_l_percentage = c_vs_l as f64 / PERCENTAGE_PRECISION_I64 as f64;
    let c_vs_t_percentage = c_vs_t as f64 / PERCENTAGE_PRECISION_I64 as f64;
    let recent_std_percentage = recent_std as f64 / PERCENTAGE_PRECISION_I64 as f64;

    (recent_std_percentage > volatility_threshold)
        || (c_vs_l_percentage > volatility_threshold)
        || (c_vs_t_percentage > volatility_threshold)
}

pub fn is_spot_market_volatile(
    spot_market: SpotMarket,
    oracle_price_data: OraclePriceData,
    volatility_threshold: f64,
) -> bool {
    let twap_price = spot_market
        .historical_oracle_data
        .last_oracle_price_twap_5min;
    let last_price = spot_market.historical_oracle_data.last_oracle_price;
    let current_price = oracle_price_data.price;

    let min_denom = std::cmp::min(twap_price, std::cmp::min(current_price, last_price)) as u128;

    let c_vs_l = ((current_price - last_price).abs() as u128) * PRICE_PRECISION / min_denom;
    let c_vs_t = ((current_price - twap_price).abs() as u128) * PRICE_PRECISION / min_denom;

    let c_vs_l_percentage = c_vs_l as f64 / PERCENTAGE_PRECISION_I64 as f64;
    let c_vs_t_percentage = c_vs_t as f64 / PERCENTAGE_PRECISION_I64 as f64;

    (c_vs_l_percentage > volatility_threshold) || (c_vs_t_percentage > volatility_threshold)
}
