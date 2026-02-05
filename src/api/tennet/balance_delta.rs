use axum::{
    extract::{Query, State},
    http::StatusCode,
};
use std::str::FromStr;
use chrono::{Utc, DateTime};
use utoipa::IntoParams;
use serde::Deserialize;
use crate::db;
use crate::db::balance_delta::BalanceDeltaRecord;
use crate::api::{AppState, AppJson, AppError, TENNET_TAG};

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
        (status = 200, description = "Successful query.", body = [BalanceDeltaRecord])
    )
)]
pub async fn get_balance_delta(
    Query(params): Query<GetBalanceDeltaQuery>,
    State(state): State<AppState>,
) -> Result<AppJson<Vec<BalanceDeltaRecord>>, AppError> {

    tracing::info!("/tennet/balance-delta: {:#?}", params);

    let date_from = match DateTime::<Utc>::from_str(&params.date_from) {
        Ok(date) => date,
        Err(err) => {
            tracing::debug!("/tennet/balance-delta:date_from {:?}", err);
            return Err(AppError::BasicError((StatusCode::BAD_REQUEST, "date_from query param is not in the correct format")))
        },
    };

    let date_to = match DateTime::<Utc>::from_str(&params.date_to) {
        Ok(date) => date,
        Err(err) => {
            tracing::debug!("/tennet/balance-delta:date_from {:?}", err);
            return Err(AppError::BasicError((StatusCode::BAD_REQUEST, "date_to query param is not in the correct format")))
        },
    };

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
