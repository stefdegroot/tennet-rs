use utoipa_axum::{router::OpenApiRouter, routes};
use crate::AppState;

mod balance_delta;
mod balance_delta_high_res;
mod merit_order;
mod settlement_prices;

pub fn tennet_router (app_state: AppState) -> OpenApiRouter {
    OpenApiRouter::new()
        .routes(routes!(balance_delta::get_balance_delta))
        .routes(routes!(balance_delta_high_res::get_balance_delta_high_res))
        .routes(routes!(merit_order::get_merit_order))
        .routes(routes!(settlement_prices::get_settlement_prices))
        .with_state(app_state)
}