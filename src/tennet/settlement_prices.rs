use serde::Deserialize;
use std::path::PathBuf;
use std::collections::HashSet;
use chrono::{offset::LocalResult, TimeZone, DateTime, Utc};
use chrono_tz::Europe::Amsterdam;
use lazy_static::lazy_static;
use crate::{
    AppState,
    tennet::utils,
    db::{
        settlement_prices,
        settlement_prices::SettlementPriceRecord,
        PG_MAX_QUERY_PARAMS,
        RECORD_COLUMNS,
    }
};

#[derive(Deserialize, Debug)]
pub struct SettlementPricePoint {
    #[serde(rename="timeInterval_start")]
    pub time_interval_start: String,
    #[serde(rename="timeInterval_end")]
    // pub time_interval_end: String,
    // pub isp: String,
    pub incident_reserve_up: String,
    pub incident_reserve_down: String,
    pub dispatch_up: Option<String>,
    pub dispatch_down: Option<String>,
    pub shortage: String,
    pub surplus: String,
    pub regulation_state: i32,
    // pub regulating_condition: String
}

#[derive(Deserialize, Debug)]
pub struct SettlementPriceRow {
    #[serde(rename="Timeinterval Start Loc")]
    pub time_interval_start: String,
    // #[serde(rename="Timeinterval End Loc")]
    // pub time_interval_end: String,
    // #[serde(rename="Isp")]
    // pub isp: i32,
    #[serde(rename="Incident Reserve Up")]
    pub incident_reserve_up: String,
    #[serde(rename="Incident Reserve Down")]
    pub incident_reserve_down: String,
    #[serde(rename="Price Dispatch Up")]
    pub price_dispatch_up: Option<f32>,
    #[serde(rename="Price Dispatch Down")]
    pub price_dispatch_down: Option<f32>,
    #[serde(rename="Price Shortage")]
    pub price_shortage: f32,
    #[serde(rename="Price Surplus")]
    pub price_surplus: f32,
    #[serde(rename="Regulation State")]
    pub regulation_state: i32,
    // #[serde(rename="Regulating Condition")]
    // pub regulating_condition: String,
}

lazy_static! {
    pub static ref FIRST_SETTLEMENT_DATE: i64 = Amsterdam.with_ymd_and_hms(2018, 1, 1, 0, 0, 0).unwrap().timestamp();
}

pub async fn import_settlement_prices (app_state: AppState) {
    
    let latest_record = settlement_prices::get_latest(&app_state.db_client).await;
    let mut sync_from = 0;

    if let Some(latest) = latest_record {
         tracing::info!(
            "latest settlement price record: {:?}",
            DateTime::from_timestamp(latest.time_stamp, 0).unwrap()
        );
        sync_from = latest.time_stamp + 60;
    } else {
        tracing::info!(
            "Settlement prices db empty, syncing from start of publication {:?}",
            DateTime::from_timestamp(*FIRST_SETTLEMENT_DATE, 0).unwrap()
        )
    }

    let files = utils::get_files("settlement_prices");

    for (path, name) in files {
        
        let (_, end_time) = utils::get_time_from_file_name(&name, "SETTLEMENT_PRICES_MONTH_", Some("SETTLEMENT_PRICES_YEAR_"));

        if sync_from > end_time {
            continue;
        }

        tracing::info!("import {:?}", name);

        import_csv(&app_state, path, sync_from).await;
    }
}

fn get_files () -> io::Result<Vec<(PathBuf, String)>>  {

    let dir_path = format!("{}/settlement_prices", CONFIG.data.path);
    let files = std::fs::read_dir(dir_path)?
        .map(|res| res.map(|e| (e.path(), e.file_name().into_string().unwrap())))
        .collect::<Result<Vec<_>, io::Error>>()?;

    Ok(files)
}

fn get_time_from_file_name (filename: &str) -> (i64, i64) {

    let year: i32;
    let month: u32;

    if filename.starts_with("0") {
        let split: Vec<&str> = filename.split("0_SETTLEMENT_PRICES_YEAR_").collect();
        year = split[1].get(0..4).unwrap().parse().unwrap();
        month = 1;
    } else {
        let split: Vec<&str> = filename.split("1_SETTLEMENT_PRICES_MONTH_").collect();
        year = split[1].get(0..4).unwrap().parse().unwrap();
        month = split[1].get(5..7).unwrap().parse().unwrap();
    }

    let start_time = Amsterdam.with_ymd_and_hms(year, month, 1, 0, 0, 0);
    let end_time = Amsterdam.with_ymd_and_hms(
        if month < 12 { year } else { year + 1 }, 
        if month < 12 { month + 1 } else { 1 }, 
        1,
        0,
        0,
        0
    );

    (
        start_time.earliest().unwrap().timestamp(),
        end_time.earliest().unwrap().timestamp(),
    )
}

async fn import_csv (app_state: &AppState, path: PathBuf, sync_from: i64) {

    let mut records: Vec<SettlementPriceRecord> = vec![];

    let mut rdr = csv::ReaderBuilder::new()
        .has_headers(true)
        .delimiter(b';')
        .trim(csv::Trim::Headers)
        .from_path(path).unwrap();

    let mut ambiguous_times = HashSet::new();

    for result in rdr.deserialize() {

        let row: SettlementPriceRow = result.unwrap();

        let time =  match utils::time::parse_tennet_time_stamp(&row.time_interval_start) {
            LocalResult::Single(t) => Some(t.to_utc()),
            LocalResult::Ambiguous(first, last) => {

                let mut time_stamp: DateTime<Utc> = first.to_utc();
                let stamp = time_stamp.timestamp();

                if !ambiguous_times.contains(&stamp) {
                    ambiguous_times.insert(stamp);
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

            records.push(SettlementPriceRecord { 
                time_stamp,
                incident_reserve_up: utils::convert_string_bool(row.incident_reserve_up),
                incident_reserve_down: utils::convert_string_bool(row.incident_reserve_down),
                price_dispatch_up: row.price_dispatch_up,
                price_dispatch_down: row.price_dispatch_down,
                price_shortage: row.price_shortage,
                price_surplus: row.price_surplus,
                regulation_state: row.regulation_state,
            });
        }
    }

    for records_chunk in records.chunks(PG_MAX_QUERY_PARAMS / RECORD_COLUMNS) {
        match settlement_prices::insert_many(&app_state.db_client, records_chunk).await {
            Ok(rows_affected) => {
                tracing::info!("inserted {} records into settlement prices db", rows_affected);
            },
            Err(err) => {
                tracing::error!("{:#?}", err);
            }
        }
    }
}

pub async fn sync_settlement_prices (app_state: &AppState) -> Vec<SettlementPriceRecord> {

    let latest_record = settlement_prices::get_latest(&app_state.db_client).await;
    let mut sync_from = *FIRST_SETTLEMENT_DATE;

    if let Some(latest) = latest_record {
        sync_from = latest.time_stamp;
    }

    let current_time_stamp = Utc::now().timestamp();

    let gap = current_time_stamp - sync_from;
    let start = sync_from + 900;
    let end = sync_from + i64::min(gap, 86400) + 900;

    let mut records: Vec<SettlementPriceRecord> = vec![];

    tracing::info!(
        "syncing settlement prices: {:?} - {:?}",
        DateTime::from_timestamp(start, 0).unwrap(),
        DateTime::from_timestamp(end, 0).unwrap(),
    );

    let result = match app_state.tennet_api.get_settlement_prices(
        DateTime::from_timestamp(start, 0).unwrap(),
        DateTime::from_timestamp(end, 0).unwrap(),
    ).await {
        Ok(r) => r,
        Err(_) => {
            return records;
        }
    };

    let mut ambiguous_times = HashSet::new();

    for time_series in result.response.time_series {

        for point in time_series.period.points {

            let time =  match utils::time::parse_tennet_time_stamp(&point.time_interval_start) {
                LocalResult::Single(t) => Some(t.to_utc()),
                LocalResult::Ambiguous(first, last) => {

                    let mut time_stamp: DateTime<Utc> = first.to_utc();
                    let stamp = time_stamp.timestamp();

                    if !ambiguous_times.contains(&stamp) {

                        let existing_record = settlement_prices::get(&app_state.db_client, stamp).await;

                        if existing_record.is_some() {
                            time_stamp = last.to_utc();
                        } else {
                            ambiguous_times.insert(stamp);
                        }

                    } else {
                        time_stamp = last.to_utc();
                    }

                    Some(time_stamp)
                },
                LocalResult::None => None
            };

            if let Some(time_stamp) = time {
                records.push(SettlementPriceRecord { 
                    time_stamp: time_stamp.timestamp(),
                    incident_reserve_up: utils::convert_string_bool(point.incident_reserve_up),
                    incident_reserve_down: utils::convert_string_bool(point.incident_reserve_down),
                    price_dispatch_up: utils::default_to_zero_option(point.dispatch_up),
                    price_dispatch_down: utils::default_to_zero_option(point.dispatch_down),
                    price_shortage: utils::default_string_to_zero(point.shortage),
                    price_surplus: utils::default_string_to_zero(point.surplus),
                    regulation_state: point.regulation_state,
                });
            }
        }
    }

    for records_chunk in records.chunks(PG_MAX_QUERY_PARAMS / RECORD_COLUMNS) {
        match settlement_prices::insert_many(&app_state.db_client, records_chunk).await {
            Ok(rows_affected) => {
                tracing::info!("inserted {} records into settlement prices db", rows_affected);
            },
            Err(err) => {
                tracing::error!("{:#?}", err);
            }
        }
    }

    records
}

fn convert_string_bool (bool: String) -> bool {
    bool == "YES"
}

fn default_to_zero_option (option: Option<String>) -> Option<f32> {
    if let Some(string) = option {
        string.parse().ok()
    } else {
        None
    }
}

fn default_string_to_zero (string: String) -> f32 {
    string.parse().unwrap_or(0.0)
}
