use axum::extract::{Query, State};
use utoipa::IntoParams;
use serde::Deserialize;
use crate::db;
use crate::db::balance_delta::BalanceDeltaRecord;
use crate::api::{AppState, AppJson, AppError, TENNET_TAG};
use crate::util::time::iso_string_to_date;

#[derive(Debug, Deserialize, IntoParams)]
pub struct GetBalanceDeltaQuery {
    date_from: String,
    date_to: String,
}

#[utoipa::path(
    get,
    path = "/balance-delta",
    tag = TENNET_TAG,
    params(GetBalanceDeltaQuery),
    responses(
        (status = 200, description = "Successful query.", body = [BalanceDeltaRecord]),
        (status = 400, description = "Bad request.", body = String),
    )
)]
pub async fn get_balance_delta(
    Query(params): Query<GetBalanceDeltaQuery>,
    State(state): State<AppState>,
) -> Result<AppJson<Vec<BalanceDeltaRecord>>, AppError> {

    tracing::info!("/tennet/balance-delta: {:#?}", params);

    let date_from = iso_string_to_date(&params.date_from)?;
    let date_to = iso_string_to_date(&params.date_to)?;

    let data = db::balance_delta::get_range(
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
