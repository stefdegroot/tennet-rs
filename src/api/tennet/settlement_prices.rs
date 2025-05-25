use axum::{
    extract::{Query, State},
    http::StatusCode,
};
use std::{
    collections::HashMap,
    str::FromStr
};
use chrono::{Utc, DateTime};
use crate::db;
use crate::api::{AppState, AppJson, AppError};

pub async fn get_settlement_prices(
    Query(params): Query<HashMap<String, String>>,
    State(state): State<AppState>,
) -> Result<AppJson<Vec<db::settlement_prices::SettlementPriceRecord>>, AppError> {

    println!("get_settlement_prices: {:#?}", params);

    let date_from = match params.get("date_from") {
        Some(date) => DateTime::<Utc>::from_str(&date).unwrap(),
        None => {
            println!("date_from query param missing");
            return Err(AppError::BasicError((StatusCode::BAD_REQUEST, "date_from query param missing")))
        },
    };

    let date_to = match params.get("date_to") {
        Some(date) => DateTime::<Utc>::from_str(&date).unwrap(),
        None => {
            println!("date_to query param missing");
            return Err(AppError::BasicError((StatusCode::BAD_REQUEST, "date_to query param missing")))
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
