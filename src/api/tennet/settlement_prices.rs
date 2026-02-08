use axum::{
    extract::{Query, State},
    http::StatusCode,
};

use std::str::FromStr;
use utoipa::IntoParams;
use serde::Deserialize;
use chrono::{Utc, DateTime};
use crate::db;
use crate::db::settlement_prices::SettlementPriceRecord;
use crate::api::{AppState, AppJson, AppError, TENNET_TAG};

#[derive(Debug, Deserialize, IntoParams)]
pub struct GetSettlementPricesQuery {
    date_from: String,
    date_to: String,
}

#[utoipa::path(
    get,
    path = "/settlement-prices",
    tag = TENNET_TAG,
    params(GetSettlementPricesQuery),
    responses(
        (status = 200, description = "Successful query.", body = [SettlementPriceRecord]),
        (status = 400, description = "Bad request.", body = String),
    )
)]
pub async fn get_settlement_prices(
    Query(params): Query<GetSettlementPricesQuery>,
    State(state): State<AppState>,
) -> Result<AppJson<Vec<SettlementPriceRecord>>, AppError> {

    tracing::info!("/tennet/settlement-prices: {:#?}", params);

    let date_from = match DateTime::<Utc>::from_str(&params.date_from) {
        Ok(date) => date,
        Err(err) => {
            tracing::debug!("/tennet/settlement-prices:date_from {:?}", err);
            return Err(AppError::BasicError((StatusCode::BAD_REQUEST, "date_from query param is not in the correct format")))
        },
    };

    let date_to = match DateTime::<Utc>::from_str(&params.date_to) {
        Ok(date) => date,
        Err(err) => {
            tracing::debug!("/tennet/settlement-prices:date_from {:?}", err);
            return Err(AppError::BasicError((StatusCode::BAD_REQUEST, "date_to query param is not in the correct format")))
        },
    };

    let data = db::settlement_prices::get_range(
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
