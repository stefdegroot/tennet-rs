use axum::{
    extract::{Query, State},
    http::StatusCode,
};
use std::str::FromStr;
use utoipa::IntoParams;
use serde::Deserialize;
use chrono::{Utc, DateTime};
use crate::db;
use crate::db::frr_activations::FrrActivationsRecord;
use crate::api::{AppState, AppJson, AppError, TENNET_TAG};

#[derive(Debug, Deserialize, IntoParams)]
pub struct GetFrrActivationsQuery {
    date_from: String,
    date_to: String,
}

#[utoipa::path(
    get,
    path = "/frr-activations",
    tag = TENNET_TAG,
    params(GetFrrActivationsQuery),
    responses(
        (status = 200, description = "Successful query.", body = [FrrActivationsRecord]),
        (status = 400, description = "Bad request.", body = String),
    )
)]
pub async fn get_frr_activations(
    Query(params): Query<GetFrrActivationsQuery>,
    State(state): State<AppState>,
) -> Result<AppJson<Vec<FrrActivationsRecord>>, AppError> {

    tracing::info!("/tennet/frr-activations: {:#?}", params);

    let date_from = match DateTime::<Utc>::from_str(&params.date_from) {
        Ok(date) => date,
        Err(err) => {
            tracing::debug!("/tennet/frr-activations:date_from {:?}", err);
            return Err(AppError::BasicError((StatusCode::BAD_REQUEST, "date_from query param is not in the correct format")))
        },
    };

    let date_to = match DateTime::<Utc>::from_str(&params.date_to) {
        Ok(date) => date,
        Err(err) => {
            tracing::debug!("/tennet/frr-activations:date_to {:?}", err);
            return Err(AppError::BasicError((StatusCode::BAD_REQUEST, "date_to query param is not in the correct format")))
        },
    };

    let data = db::frr_activations::get_range(
        &state.db_client, 
        date_from.timestamp(), 
        date_to.timestamp()
    ).await;

    match data {
        Some(records) => {
            tracing::info!("Found {} frr_activations records in requested range", records.len());
            Ok(AppJson(records))
        },
        None => {
            tracing::warn!("No records found or error occurred");
            Ok(AppJson(vec![]))
        }
    }
}
