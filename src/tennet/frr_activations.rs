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
        frr_activations,
        frr_activations::FrrActivationsRecord,
        PG_MAX_QUERY_PARAMS,
        RECORD_COLUMNS,
    }
};

#[derive(Deserialize, Debug)]
pub struct FrrActivationsPoint {
    #[serde(rename="timeInterval_start")]
    pub time_interval_start: String,
    #[serde(rename="timeInterval_end")]
    pub time_interval_end: String,
    #[serde(rename="aFRR_up")]
    pub afrr_up: String,
    #[serde(rename="aFRR_down")]
    pub afrr_down: String,
    pub total_volume: String,
    #[serde(rename="mfrrda_volume_up")]
    pub mfrrda_volume_up: String,
    #[serde(rename="mfrrda_volume_down")]
    pub mfrrda_volume_down: String,
    #[serde(rename="absolute_total_volume")]
    pub absolute_total_volume: String,
}

#[derive(Deserialize, Debug)]
struct FrrActivationsRow {
    #[serde(rename="Timeinterval Start Loc")]
    pub time_interval_start: String,
    #[serde(rename="Timeinterval End Loc")]
    pub time_interval_end: String,
    #[serde(rename="Afrr Up")]
    pub afrr_up: Option<f32>,
    #[serde(rename="Afrr Down")]
    pub afrr_down: Option<f32>,
    #[serde(rename="Total Volume")]
    pub total_volume: Option<f32>,
    #[serde(rename="Mfrrda Volume Up")]
    pub mfrrda_volume_up: Option<f32>,
    #[serde(rename="Mfrrda Volume Down")]
    pub mfrrda_volume_down: Option<f32>,
    #[serde(rename="Absolute Total Volume")]
    pub absolute_total_volume: Option<f32>,
}

lazy_static! {
    pub static ref FIRST_FRR_DATE: i64 = Amsterdam.with_ymd_and_hms(2018, 5, 1, 0, 0, 0).unwrap().timestamp();
}

pub async fn import_frr_activations (app_state: AppState) {
    
    let latest_record = frr_activations::get_latest(&app_state.db_client).await;
    let mut sync_from = 0;

    if let Some(latest) = latest_record {
        sync_from = latest.time_stamp + 900;
        tracing::info!("Latest FRR activations record: timestamp {}", latest.time_stamp);
    } else {
        tracing::info!("No existing FRR activations records found, starting from beginning");
    }

    let files = utils::get_files("frr_activations");

    for (path, name) in files {
        
        let (start_time, end_time) = utils::get_time_from_file_name(&name, "FRR_ACTIVATIONS_MONTH_", None);

        if sync_from > end_time {
            continue;
        }

        tracing::info!("importing: {:?}", name);

        import_csv(&app_state, path, sync_from).await;
    }
}


async fn import_csv (app_state: &AppState, path: PathBuf, sync_from: i64) {

    let mut records: Vec<FrrActivationsRecord> = vec![];

    let mut rdr = csv::ReaderBuilder::new()
        .has_headers(true)
        .delimiter(b';')
        .trim(csv::Trim::Headers)
        .from_path(path).unwrap();

    let mut ambiguous_times = HashSet::new();

    for result in rdr.deserialize() {

        let row: FrrActivationsRow = result.unwrap();

        let time =  match utils::time::parse_tennet_time_stamp(&row.time_interval_start) {
            LocalResult::Single(t) => Some(t.to_utc()),
            LocalResult::Ambiguous(first, last) => {

                let mut time_stamp = first.to_utc();
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

            records.push(FrrActivationsRecord { 
                time_stamp,
                afrr_up: utils::default_to_zero(row.afrr_up),
                afrr_down: utils::default_to_zero(row.afrr_down),
                total_volume: utils::default_to_zero(row.total_volume),
                mfrrda_volume_up: utils::default_to_zero(row.mfrrda_volume_up),
                mfrrda_volume_down: utils::default_to_zero(row.mfrrda_volume_down),
                absolute_total_volume: utils::default_to_zero(row.absolute_total_volume),
            });
        }
    }

    for records_chunk in records.chunks(PG_MAX_QUERY_PARAMS / RECORD_COLUMNS) {
        match frr_activations::insert_many(&app_state.db_client, records_chunk).await {
            Ok(rows_affected) => {
                tracing::info!("inserted {} records into frr activations db", rows_affected);
            },
            Err(err) => {
                tracing::error!("{:#?}", err);
            }
        }
    }
}

pub async fn sync_frr_activations (app_state: &AppState) -> Vec<FrrActivationsRecord> {

    let latest_record = frr_activations::get_latest(&app_state.db_client).await;
    let mut sync_from = *FIRST_FRR_DATE;

    if let Some(latest) = latest_record {
        sync_from = latest.time_stamp;
    }

    let current_time_stamp = Utc::now().timestamp();

    let gap = current_time_stamp - sync_from;
    let start = sync_from + 900; // 15 mins
    let end = sync_from + i64::min(gap, 86400) + 900; // max 1 day

    if start >= end {
        tracing::info!("No new data to sync. Latest: {:?}, Current: {:?}", 
            DateTime::from_timestamp(sync_from, 0),
            DateTime::from_timestamp(current_time_stamp, 0)
        );
        return vec![];
    }

    let mut records: Vec<FrrActivationsRecord> = vec![];

    tracing::info!(
        "syncing frr activations: {:?} - {:?}",
        DateTime::from_timestamp(start, 0).unwrap(),
        DateTime::from_timestamp(end, 0).unwrap(),
    );

    let result = match app_state.tennet_api.get_frr_activations(
        DateTime::from_timestamp(start, 0).unwrap(),
        DateTime::from_timestamp(end, 0).unwrap(),
    ).await {
        Ok(r) => r,
        Err(err) => {
            tracing::error!("Error fetching frr activations: {:?}", err);
            return records;
        }
    };

    let mut ambiguous_times = HashSet::new();

    for time_series in result.response.time_series {

        for point in time_series.period.points {

            let time =  match utils::time::parse_tennet_time_stamp(&point.time_interval_start) {
                LocalResult::Single(t) => Some(t.to_utc()),
                LocalResult::Ambiguous(first, last) => {

                    let mut time_stamp = first.to_utc();
                    let stamp = time_stamp.timestamp();

                    if !ambiguous_times.contains(&stamp) {

                        let existing_record = frr_activations::get(&app_state.db_client, stamp).await;

                        if let Some(_) = existing_record {
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
                records.push(FrrActivationsRecord { 
                    time_stamp: time_stamp.timestamp(),
                    afrr_up: utils::default_string_to_zero(point.afrr_up),
                    afrr_down: utils::default_string_to_zero(point.afrr_down),
                    total_volume: utils::default_string_to_zero(point.total_volume),
                    mfrrda_volume_up: utils::default_string_to_zero(point.mfrrda_volume_up),
                    mfrrda_volume_down: utils::default_string_to_zero(point.mfrrda_volume_down),
                    absolute_total_volume: utils::default_string_to_zero(point.absolute_total_volume),
                });
            }
        }
    }

    for records_chunk in records.chunks(PG_MAX_QUERY_PARAMS / RECORD_COLUMNS) {
        match frr_activations::insert_many(&app_state.db_client, records_chunk).await {
            Ok(rows_affected) => {
                tracing::info!("inserted {} records into frr activations db", rows_affected);
            },
            Err(err) => {
                tracing::error!("{:#?}", err);
            }
        }
    }

    records
}

