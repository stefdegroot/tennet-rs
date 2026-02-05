use axum::{
    routing::get,
    Router,
};
use crate::AppState;

mod balance_delta;
mod merit_order;
mod settlement_prices;

pub fn tennet_router (app_state: AppState) -> Router {
    Router::new()
        .route("/balance-delta", get(balance_delta::get_balance_delta))
        .route("/merit-order", get(merit_order::get_merit_order))
        .route("/settlement-prices", get(settlement_prices::get_settlement_prices))
        .with_state(app_state)
}