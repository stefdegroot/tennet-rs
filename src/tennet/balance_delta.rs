use serde::Deserialize;
use std::{io, path::PathBuf};
use std::collections::HashSet;
use chrono::{offset::LocalResult, TimeZone, DateTime, Utc};
use chrono_tz::Europe::Amsterdam;
use std::time::{Duration, Instant};
use lazy_static::lazy_static;

use crate::AppState;
use crate::tennet::time::parse_tennet_time_stamp;
use crate::db::{
    balance_delta,
    balance_delta::BalanceDeltaRecord,
    PG_MAX_QUERY_PARAMS,
    RECORD_COLUMNS,
};

#[derive(Deserialize, Debug)]
pub struct BalanceDeltaPoint {
    #[serde(rename="timeInterval_start")]
    pub time_interval_start: String,
    #[serde(rename="timeInterval_end")]
    pub time_interval_end: String,
    pub sequence: String,
    pub power_afrr_in: String,
    pub power_afrr_out: String,
    pub power_igcc_in: String,
    pub power_igcc_out: String,
    pub power_mfrrda_in: String,
    pub power_mfrrda_out: String,
    pub power_picasso_in: Option<String>,
    pub power_picasso_out: Option<String>,
    pub max_upw_regulation_price: Option<String>,
    pub min_downw_regulation_price: Option<String>,
    pub mid_price: String,
}

#[derive(Deserialize, Debug)]
struct BalanceDeltaRow {
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
    #[serde(rename="Highest Upward Regulation Price")]
    pub max_upw_regulation_price: Option<f32>,
    #[serde(rename="Lowest Downward Regulation Price")]
    pub min_downw_regulation_price: Option<f32>,
    #[serde(rename="Mid Price")]
    pub mid_price: Option<f32>,
}

lazy_static! {
    pub static ref FIRST_BALANCE_DATE: i64 = Amsterdam.with_ymd_and_hms(2018, 5, 1, 0, 0, 0).unwrap().timestamp();
}

pub async fn import_balance_delta (app_state: AppState) {

    let latest_record = balance_delta::get_latest(&app_state.db_client).await;
    let mut sync_from = 0;

    println!("{:?}", latest_record);

    if let Some(latest) = latest_record {
        sync_from = latest.time_stamp + 60;
    }

    let files = get_files().unwrap();

    for (path, name) in files {
        
        let (start_time, end_time) = get_time_from_file_name(&name);

        if sync_from > end_time {
            continue;
        }

        println!("importing: {:?}", name);

        import_csv(&app_state, path, sync_from).await;
    }
}

fn default_to_zero (option: Option<f32>) -> f32 {
    if let Some(n) = option {
        n
    } else {
        0.0
    }
}

fn get_files () -> io::Result<Vec<(PathBuf, String)>>  {

    let dir_path = format!("./data/balance_delta");
    let files = std::fs::read_dir(dir_path)?
        .map(|res| res.map(|e| (e.path(), e.file_name().into_string().unwrap())))
        .collect::<Result<Vec<_>, io::Error>>()?;

    Ok(files)
}

fn get_time_from_file_name (filename: &String) -> (i64, i64) {

    let split: Vec<&str> = filename.split("BALANCE_DELTA_MONTH_").collect();

    let year: i32 = split[1].get(0..4).unwrap().parse().unwrap();
    let month: u32 = split[1].get(5..7).unwrap().parse().unwrap();

    let start_time = Amsterdam.with_ymd_and_hms(year, month, 1, 0, 0, 0);
    let end_time = Amsterdam.with_ymd_and_hms(
        if month < 12 { year } else { year + 1 }, 
        if month < 12 { month + 1 } else { 1 }, 
        1,
        0,
        0,
        0
    );

    return (
        start_time.earliest().unwrap().timestamp(),
        end_time.earliest().unwrap().timestamp(),
    );
}

async fn import_csv (app_state: &AppState, path: PathBuf, sync_from: i64) {

    let mut records: Vec<BalanceDeltaRecord> = vec![];

    let mut rdr = csv::ReaderBuilder::new()
        .has_headers(true)
        .delimiter(b';')
        .trim(csv::Trim::Headers)
        .from_path(path).unwrap();

    let mut ambiguous_times = HashSet::new();

    for result in rdr.deserialize() {

        // println!("{:?}", result);
        let row: BalanceDeltaRow = result.unwrap();

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

            records.push(BalanceDeltaRecord { 
                time_stamp, 
                power_afrr_in: default_to_zero(row.power_afrr_in), 
                power_afrr_out: default_to_zero(row.power_afrr_out), 
                power_igcc_in: default_to_zero(row.power_igcc_in), 
                power_igcc_out: default_to_zero(row.power_igcc_out), 
                power_mfrrda_in: default_to_zero(row.power_mfrrda_in), 
                power_mfrrda_out: default_to_zero(row.power_mfrrda_out), 
                power_picasso_in: default_to_zero(row.power_picasso_in), 
                power_picasso_out: default_to_zero(row.power_picasso_out), 
                max_upw_regulation_price: row.max_upw_regulation_price, 
                min_downw_regulation_price: row.min_downw_regulation_price,
                mid_price: default_to_zero(row.mid_price),
            });
        }
    }

    for records_chunk in records.chunks(PG_MAX_QUERY_PARAMS / RECORD_COLUMNS) {
        let result = balance_delta::insert_many(&app_state.db_client, records_chunk).await;
        println!("{:?}", result);
    }
}

pub async fn sync_balance_delta (app_state: &AppState) -> Vec<BalanceDeltaRecord> {

    let latest_record = balance_delta::get_latest(&app_state.db_client).await;
    let mut sync_from = *FIRST_BALANCE_DATE;

    if let Some(latest) = latest_record {
        sync_from = latest.time_stamp;
    }

    let current_time_stamp = Utc::now().timestamp();

    let gap = current_time_stamp - sync_from;
    let start = sync_from + 60;
    let end = sync_from + i64::min(gap, 86400) + 60;

    println!("syncing: {:?} - {:?}",
        DateTime::from_timestamp(start, 0).unwrap(),
        DateTime::from_timestamp(end, 0).unwrap(),
    );

    let result = app_state.tennet_api.get_balance_delta(
        DateTime::from_timestamp(start, 0).unwrap(),
        DateTime::from_timestamp(end, 0).unwrap(),
    ).await.unwrap();

    let mut records: Vec<BalanceDeltaRecord> = vec![];

    for time_series in result.response.time_series {

        for point in time_series.period.points {

            let time =  match parse_tennet_time_stamp(&point.time_interval_start) {
                LocalResult::Single(t) => Some(t.to_utc()),
                LocalResult::Ambiguous(first, last) => {

                    let mut time_stamp = first.to_utc();

                    let existing_record = balance_delta::get(&app_state.db_client, time_stamp.timestamp()).await;

                    if let Some(_) = existing_record {
                        time_stamp = last.to_utc();
                    }

                    Some(time_stamp)
                },
                LocalResult::None => None
            };

            if let Some(time_stamp) = time {

                println!("{:?}", time_stamp);

                records.push(BalanceDeltaRecord { 
                    time_stamp: time_stamp.timestamp(), 
                    power_afrr_in: default_string_to_zero(point.power_afrr_in), 
                    power_afrr_out: default_string_to_zero(point.power_afrr_out), 
                    power_igcc_in: default_string_to_zero(point.power_igcc_in), 
                    power_igcc_out: default_string_to_zero(point.power_igcc_out), 
                    power_mfrrda_in: default_string_to_zero(point.power_mfrrda_in), 
                    power_mfrrda_out: default_string_to_zero(point.power_mfrrda_out), 
                    power_picasso_in: default_some_string_to_zero(point.power_picasso_in), 
                    power_picasso_out: default_some_string_to_zero(point.power_picasso_out), 
                    max_upw_regulation_price: default_to_zero_option(point.max_upw_regulation_price),
                    min_downw_regulation_price: default_to_zero_option(point.min_downw_regulation_price),
                    mid_price: default_string_to_zero(point.mid_price),
                });
            };
        }
    }

    for records_chunk in records.chunks(PG_MAX_QUERY_PARAMS / RECORD_COLUMNS) {
        let result = balance_delta::insert_many(&app_state.db_client, records_chunk).await;
        println!("{:?}", result);
    }

    records
}

fn default_to_zero_option (option: Option<String>) -> Option<f32> {
    if let Some(string) = option {
        match string.parse() {
            Ok(n) => Some(n),
            Err(_) => None,
        }
    } else {
        None
    }
}

fn default_string_to_zero (string: String) -> f32 {
    match string.parse() {
        Ok(n) => n,
        Err(_) => 0.0,
    }
}

fn default_some_string_to_zero (option: Option<String>) -> f32 {
    if let Some(n) = option {
        n.parse().unwrap()
    } else {
        0.0
    }
}