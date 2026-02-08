use utoipa_axum::{router::OpenApiRouter, routes};
use crate::AppState;

mod balance_delta;
mod frr_activations;
mod merit_order;
mod settlement_prices;

pub fn tennet_router (app_state: AppState) -> OpenApiRouter {
    OpenApiRouter::new()
        .routes(routes!(balance_delta::get_balance_delta))
        .routes(routes!(merit_order::get_merit_order))
        .routes(routes!(settlement_prices::get_settlement_prices))
        .routes(routes!(frr_activations::get_frr_activations))

        .with_state(app_state)
}