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

pub async fn get_frr_activations(
    Query(params): Query<HashMap<String, String>>,
    State(state): State<AppState>,
) -> Result<AppJson<Vec<db::frr_activations::FrrActivationsRecord>>, AppError> {

    tracing::info!("/tennet/frr-activations: {:#?}", params);

    let date_from = match params.get("date_from") {
        Some(date) => match DateTime::<Utc>::from_str(&date) {
            Ok(dt) => dt,
            Err(e) => {
                tracing::error!("Invalid date_from format: {} - {}", date, e);
                return Err(AppError::BasicError((StatusCode::BAD_REQUEST, "Invalid date_from format")))
            }
        },
        None => {
            tracing::error!("date_from query param missing");
            return Err(AppError::BasicError((StatusCode::BAD_REQUEST, "date_from query param missing")))
        },
    };

    let date_to = match params.get("date_to") {
        Some(date) => match DateTime::<Utc>::from_str(&date) {
            Ok(dt) => dt,
            Err(e) => {
                tracing::error!("Invalid date_to format: {} - {}", date, e);
                return Err(AppError::BasicError((StatusCode::BAD_REQUEST, "Invalid date_to format")))
            }
        },
        None => {
            tracing::error!("date_to query param missing");
            return Err(AppError::BasicError((StatusCode::BAD_REQUEST, "date_to query param missing")))
        },
    };

    let data = db::frr_activations::get_range(
        &state.db_client, 
        date_from.timestamp(), 
        date_to.timestamp()
    ).await;

    match data {
        Some(records) => {
            tracing::info!("Found {} frr_activations records", records.len());
            Ok(AppJson(records))
        },
        None => {
            tracing::warn!("No records found or error occurred");
            Ok(AppJson(vec![]))
        }
    }
}
