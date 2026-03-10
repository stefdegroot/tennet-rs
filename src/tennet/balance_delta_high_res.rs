use std::path::PathBuf;
use std::str::FromStr;
use serde::Deserialize;
use chrono::{offset::LocalResult, TimeZone, DateTime, Utc};
use std::collections::HashSet;
use chrono_tz::Europe::Amsterdam;
use lazy_static::lazy_static;

use crate::AppState;
use crate::tennet::{
    time::{
        parse_tennet_time_stamp,
    },
    TennetAPIPeriod,
};
use crate::util::{
    parse::{
        default_to_zero_option,
        default_string_to_zero,
        default_some_string_to_zero,
    },
    files::get_files_from_data_folder,
};
use crate::db::{
    balance_delta_high_res,
    balance_delta_high_res::BalanceDeltaHighResRecord,
    PG_MAX_QUERY_PARAMS,
    RECORD_COLUMNS,
};

const LATEST_RESPONSE_RANGE: i64 = 1740;

#[derive(Deserialize, Debug, Clone)]
pub struct BalanceDeltaPointHighRes {
    #[serde(rename="timeInterval_start")]
    pub time_interval_start: String,
    // #[serde(rename="timeInterval_end")]
    // pub time_interval_end: String,
    // pub sequence: String,
    pub power_afrr_in: String,
    pub power_afrr_out: String,
    pub power_igcc_in: String,
    pub power_igcc_out: String,
    pub power_mfrrda_in: String,
    pub power_mfrrda_out: String,
    pub power_picasso_in: Option<String>,
    pub power_picasso_out: Option<String>,
    pub power_mari_in: Option<String>,
    pub power_mari_out: Option<String>,
    pub max_upw_regulation_price: Option<String>,
    pub min_downw_regulation_price: Option<String>,
    pub mid_price: String,
}

#[derive(Deserialize, Debug)]
struct BalanceDeltaHighResRow {
    #[serde(rename="Timeinterval Start Loc")]
    pub time_interval_start: String,
    // #[serde(rename="Timeinterval End Loc")]
    // pub time_interval_end: String,
    // #[serde(rename="Isp")]
    // pub sequence: i32,
    #[serde(rename="Power In Activated Afrr")]
    pub power_afrr_in: Option<f32>,
    #[serde(rename="Power Out Activated Afrr")]
    pub power_afrr_out: Option<f32>,
    #[serde(rename="Power In Igcc")]
    pub power_igcc_in: Option<f32>,
    #[serde(rename="Power Out Igcc")]
    pub power_igcc_out: Option<f32>,
    #[serde(rename="Power In Mfrrda")]
    pub power_mfrrda_in: Option<f32>,
    #[serde(rename="Power Out Mfrrda")]
    pub power_mfrrda_out: Option<f32>,
    #[serde(rename="Picasso Contribution Power In")]
    pub power_picasso_in: Option<f32>,
    #[serde(rename="Picasso Contribution Power Out")]
    pub power_picasso_out: Option<f32>,
    #[serde(rename="Mari Contribution Power In")]
    pub power_mari_in: Option<f32>,
    #[serde(rename="Mari Contribution Power Out")]
    pub power_mari_out: Option<f32>,
    #[serde(rename="Highest Upward Regulation Price")]
    pub max_upw_regulation_price: Option<f32>,
    #[serde(rename="Lowest Downward Regulation Price")]
    pub min_downw_regulation_price: Option<f32>,
    #[serde(rename="Mid Price")]
    pub mid_price: Option<f32>,
}

lazy_static! {
    pub static ref FIRST_HIGH_RES_BALANCE_DATE: i64 = Amsterdam.with_ymd_and_hms(2025, 11, 19, 0, 0, 0).unwrap().timestamp();
}

pub async fn import_balance_delta_high_res (app_state: AppState) {

    let latest_record = balance_delta_high_res::get_latest(&app_state.db_client).await;
    let mut sync_from = 0;

    if let Some(latest) = latest_record {
        tracing::info!(
            "latest high res balance delta record: {:?}",
            DateTime::from_timestamp(latest.time_stamp, 0).unwrap()
        );
        sync_from = latest.time_stamp + 12;
    } else {
        tracing::info!(
            "High res balance delta db empty, syncing from start of publication {:?}",
            DateTime::from_timestamp(*FIRST_HIGH_RES_BALANCE_DATE, 0).unwrap()
        )
    }

    let files = match get_files_from_data_folder("balance_delta_high_res") {
        Ok(f) => f,
        Err(err) => {
            tracing::error!("Failed to read high res balance delta data folder {:?}", err);
            return;
        }
    };

    for (path, name, _, end_time) in files {

        if sync_from > end_time {
            continue;
        }

        tracing::info!("importing: {:?}", name);

        import_csv(&app_state, path, sync_from).await;
    }
}

async fn import_csv (app_state: &AppState, path: PathBuf, sync_from: i64) {

    let mut records: Vec<BalanceDeltaHighResRecord> = vec![];

    let mut rdr = csv::ReaderBuilder::new()
        .has_headers(true)
        .delimiter(b';')
        .trim(csv::Trim::Headers)
        .from_path(path).unwrap();

    let mut ambiguous_times = HashSet::new();

    for result in rdr.deserialize() {

        let row: BalanceDeltaHighResRow = result.unwrap();

        let time =  match parse_tennet_time_stamp(&row.time_interval_start) {
            LocalResult::Single(t) => Some(t.to_utc()),
            LocalResult::Ambiguous(first, last) => {

                let mut time_stamp = first.to_utc();

                if !ambiguous_times.contains(&time_stamp) {
                    ambiguous_times.insert(time_stamp);
                } else {
                    time_stamp = last.to_utc();
                }

                Some(time_stamp)
            },
            LocalResult::None => None
        };

        if let Some(time_stamp) = time {

            let time_stamp = time_stamp.timestamp();

            if time_stamp < sync_from {
                continue;
            }

            records.push(BalanceDeltaHighResRecord {
                time_stamp,
                power_afrr_in: row.power_afrr_in.unwrap_or(0.0),
                power_afrr_out: row.power_afrr_out.unwrap_or(0.0),
                power_igcc_in: row.power_igcc_in.unwrap_or(0.0),
                power_igcc_out: row.power_igcc_out.unwrap_or(0.0),
                power_mfrrda_in: row.power_mfrrda_in.unwrap_or(0.0),
                power_mfrrda_out: row.power_mfrrda_out.unwrap_or(0.0),
                power_picasso_in: row.power_picasso_in.unwrap_or(0.0),
                power_picasso_out: row.power_picasso_out.unwrap_or(0.0),
                power_mari_in: row.power_mari_in.unwrap_or(0.0),
                power_mari_out: row.power_mari_out.unwrap_or(0.0),
                max_upw_regulation_price: row.max_upw_regulation_price,
                min_downw_regulation_price: row.min_downw_regulation_price,
                mid_price: row.mid_price.unwrap_or(0.0),
            });
        }
    }

    for records_chunk in records.chunks(PG_MAX_QUERY_PARAMS / RECORD_COLUMNS) {
        match balance_delta_high_res::insert_many(&app_state.db_client, records_chunk).await {
            Ok(rows_affected) => {
                tracing::info!("inserted {} records into balance delta high res db", rows_affected);
            },
            Err(err) => {
                tracing::error!("{:#?}", err);
            }
        }
    }
}

pub async fn sync_balance_delta_high_res (app_state: &AppState) -> Vec<BalanceDeltaHighResRecord> {

    let latest_record = balance_delta_high_res::get_latest(&app_state.db_client).await;
    let mut sync_from = *FIRST_HIGH_RES_BALANCE_DATE;

    if let Some(latest) = latest_record {
        sync_from = latest.time_stamp;
    }

    let current_time_stamp = Utc::now().timestamp();

    let gap = current_time_stamp - sync_from;
    let start = sync_from + 12;
    let end = sync_from + i64::min(gap, 14400) + 12;

    tracing::info!(
        "syncing balance delta high res: {:?} - {:?}",
        DateTime::from_timestamp(start, 0).unwrap(),
        DateTime::from_timestamp(end, 0).unwrap(),
    );

    let mut records: Vec<BalanceDeltaHighResRecord> = vec![];

    let result = if gap > LATEST_RESPONSE_RANGE {
        match app_state.tennet_api.get_balance_delta_high_res(
            DateTime::from_timestamp(start, 0).unwrap(),
            DateTime::from_timestamp(end, 0).unwrap(),
        ).await {
            Ok(r) => r,
            Err(err) => {
                tracing::error!("{:?}", err);
                return records;
            }
        }
    } else {
        match app_state.tennet_api.get_balance_delta_high_res_latest().await {
            Ok(r) => r,
            Err(err) => {
                tracing::error!("{:?}", err);
                return records;
            }
        }
    };

    for time_series in result.response.time_series {

        let points = match time_series.period {
            TennetAPIPeriod::Object(obj) => obj.points,
            TennetAPIPeriod::Array(array) => {
                array
                    .iter()
                    .flat_map(|v| v.points.clone())
                    .collect::<Vec<BalanceDeltaPointHighRes>>()
            }
        };

        for point in points {

            let time = if point.time_interval_start.ends_with("Z") {
                Some(DateTime::<Utc>::from_str(&point.time_interval_start).unwrap().timestamp())
            } else {
                match parse_tennet_time_stamp(&point.time_interval_start) {
                    LocalResult::Single(t) => Some(t.to_utc().timestamp()),
                    LocalResult::Ambiguous(first, last) => {

                        let mut time_stamp = first.to_utc();

                        let existing_record = balance_delta_high_res::get(&app_state.db_client, time_stamp.timestamp()).await;

                        if existing_record.is_some() {
                            time_stamp = last.to_utc();
                        }

                        Some(time_stamp.timestamp())
                    },
                    LocalResult::None => None
                }
            };

            if let Some(time_stamp) = time {

                if time_stamp < start {
                    continue;
                }

                records.push(BalanceDeltaHighResRecord {
                    time_stamp,
                    power_afrr_in: default_string_to_zero(point.power_afrr_in),
                    power_afrr_out: default_string_to_zero(point.power_afrr_out),
                    power_igcc_in: default_string_to_zero(point.power_igcc_in),
                    power_igcc_out: default_string_to_zero(point.power_igcc_out),
                    power_mfrrda_in: default_string_to_zero(point.power_mfrrda_in),
                    power_mfrrda_out: default_string_to_zero(point.power_mfrrda_out),
                    power_picasso_in: default_some_string_to_zero(point.power_picasso_in),
                    power_picasso_out: default_some_string_to_zero(point.power_picasso_out),
                    power_mari_in: default_some_string_to_zero(point.power_mari_in),
                    power_mari_out: default_some_string_to_zero(point.power_mari_out),
                    max_upw_regulation_price: default_to_zero_option(point.max_upw_regulation_price),
                    min_downw_regulation_price: default_to_zero_option(point.min_downw_regulation_price),
                    mid_price: default_string_to_zero(point.mid_price),
                });
            }
        }
    }

    for records_chunk in records.chunks(PG_MAX_QUERY_PARAMS / RECORD_COLUMNS) {
        match balance_delta_high_res::insert_many(&app_state.db_client, records_chunk).await {
            Ok(rows_affected) => {
                tracing::info!("inserted {} records into balance delta high res db", rows_affected);
            },
            Err(err) => {
                tracing::error!("{:#?}", err);
            }
        }
    }

    records
}
