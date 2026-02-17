use chrono::{DateTime, Utc};
use reqwest::StatusCode;
use std::str::FromStr;
use crate::api::AppError;

pub fn iso_string_to_date (iso_string: &str) -> Result<DateTime<Utc>, AppError> {
    match DateTime::<Utc>::from_str(iso_string) {
        Ok(date) => Ok(date),
        Err(_) => Err(AppError::BasicError((StatusCode::BAD_REQUEST, "Query param is not in the correct format"))),
    }
}