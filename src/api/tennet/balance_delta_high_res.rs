use axum::extract::{Query, State};
use utoipa::IntoParams;
use serde::Deserialize;
use crate::db;
use crate::{
    api::{AppState, AppJson, AppError, TENNET_TAG},
    db::balance_delta_high_res::BalanceDeltaHighResRecord,
    util::time::iso_string_to_date,
};

#[derive(Debug, Deserialize, IntoParams)]
pub struct GetBalanceDeltaHighResQuery {
    date_from: String,
    date_to: String,
}

#[utoipa::path(
    get,
    path = "/balance-delta-high-res",
    tag = TENNET_TAG,
    params(GetBalanceDeltaHighResQuery),
    responses(
        (status = 200, description = "Successful query.", body = [BalanceDeltaHighResRecord]),
        (status = 400, description = "Bad request.", body = String),
    )
)]
pub async fn get_balance_delta_high_res(
    Query(params): Query<GetBalanceDeltaHighResQuery>,
    State(state): State<AppState>,
) -> Result<AppJson<Vec<BalanceDeltaHighResRecord>>, AppError> {

    tracing::info!("/tennet/balance-delta-high-res: {:#?}", params);

    let date_from = iso_string_to_date(&params.date_from)?;
    let date_to = iso_string_to_date(&params.date_to)?;

    let data = db::balance_delta_high_res::get_range(
        &state.db_client, 
        date_from.timestamp(), 
        date_to.timestamp()
    ).await;

    if let Some(records) = data {
        Ok(AppJson(records))
    } else {
        Ok(AppJson(vec![]))
    }
}