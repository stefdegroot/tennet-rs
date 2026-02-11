use serde::Deserialize;
use std::path::PathBuf;
use chrono::{offset::LocalResult, TimeZone, DateTime, Utc};
use chrono_tz::Europe::Amsterdam;
use std::collections::{HashMap, HashSet};
use lazy_static::lazy_static;

use crate::{
    db::{
        merit_order::{self, MeritOrderList, MeritOrderRecord},
        PG_MAX_QUERY_PARAMS,
        RECORD_COLUMNS,
    },
    tennet::utils,
    AppState
};

#[derive(Deserialize, Debug)]
pub struct MeritOrderPointThreshold {
    capacity_threshold: String,
    price_up: Option<String>,
    price_down: Option<String>,
}

#[derive(Deserialize, Debug)]
pub struct MeritOrderPoint {
    #[serde(rename="timeInterval_start")]
    pub time_interval_start: String,
    // #[serde(rename="timeInterval_end")]
    // pub time_interval_end: String,
    #[serde(rename="Thresholds")]
    pub thresholds: Vec<MeritOrderPointThreshold>,
}

#[derive(Deserialize, Debug)]
struct MeritOrderRow {
    #[serde(rename="Timeinterval Start Loc")]
    pub time_interval_start: String,
    // #[serde(rename="Timeinterval End Loc")]
    // pub time_interval_end: String,
    // #[serde(rename="Isp")]
    // pub sequence: i32,
    // #[serde(rename="Quantity Measurement Unit Name")]
    // pub quantity_unit_name: String,
    // #[serde(rename="Price Measurement Unit Name")]
    // pub price_unit_name: String,
    // #[serde(rename="Currency Unit Name")]
    // pub currency_unit_name: String,
    #[serde(rename="Capacity Threshold")]
    pub capacity_threshold: f32,
    #[serde(rename="Price Down")]
    pub price_down: Option<f32>,
    #[serde(rename="Price Up")]
    pub price_up:  Option<f32>,
}

lazy_static! {
    pub static ref FIRST_MERIT_ORDER_DATE: i64 = Amsterdam.with_ymd_and_hms(2018, 1, 1, 0, 0, 0).unwrap().timestamp();
}

pub async fn import_merit_order (app_state: AppState) {

    let latest_record = merit_order::get_latest(&app_state.db_client).await;
    let mut sync_from = 0;

    if let Some(latest) = latest_record {
        tracing::info!(
            "latest merit order record: {:?}",
            DateTime::from_timestamp(latest.time_stamp, 0).unwrap()
        );
        sync_from = latest.time_stamp + 900;
    } else {
        tracing::info!(
            "Merit order db empty, syncing from start of publication {:?}",
            DateTime::from_timestamp(*FIRST_MERIT_ORDER_DATE, 0).unwrap()
        )
    }

    let files = utils::get_files("merit_order");
    
    for (path, name) in files {
        
        let (_, end_time) = utils::get_time_from_file_name(&name, "MERIT_ORDER_LIST_MONTH_", None);

        if sync_from > end_time {
            continue;
        }

        tracing::info!("importing: {:?}", name);

        import_csv(&app_state, path, sync_from).await;
    }
}

pub async fn sync_merit_order (app_state: &AppState) -> Vec<merit_order::MeritOrderList> {

    let latest_record = merit_order::get_latest(&app_state.db_client).await;
    let mut sync_from = *FIRST_MERIT_ORDER_DATE;

    if let Some(latest) = latest_record {
        sync_from = latest.time_stamp;
    }

    let current_time_stamp = Utc::now().timestamp();
    let start ;
    let end;

    if current_time_stamp <= sync_from {
        start = current_time_stamp - current_time_stamp % 900 + 900;
        end = start + 86400;
    } else {
        start = sync_from + 900;
        end = sync_from + 86400 + 900;
    }


    println!("syncing merit order: {:?} - {:?}",
        DateTime::from_timestamp(start, 0).unwrap(),
        DateTime::from_timestamp(end, 0).unwrap(),
    );

    let mut lists = vec![];

    let result = match  app_state.tennet_api.get_merit_order_list(
        DateTime::from_timestamp(start, 0).unwrap(),
        DateTime::from_timestamp(end, 0).unwrap(),
    ).await {
        Ok(r) => r,
        Err(err) => {
            println!("{:?}", err);
            return lists;
        } 
    };

    merit_order::delete_range(&app_state.db_client, start, end).await;

    let mut ambiguous_times = HashSet::new();

    for time_series in result.response.time_series {

        for point in time_series.period.points {

             let time =  match utils::time::parse_tennet_time_stamp(&point.time_interval_start) {
                LocalResult::Single(t) => Some(t.to_utc()),
                LocalResult::Ambiguous(first, last) => {

                    let mut time_stamp = first.to_utc();
                    let stamp = time_stamp.timestamp();

                    if !ambiguous_times.contains(&stamp) {

                        let existing_list = merit_order::get(&app_state.db_client, stamp).await;

                        if existing_list.is_some() {
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

                let mut list = MeritOrderList {
                    time_stamp: time_stamp.timestamp(),
                    upward: vec![],
                    downward: vec![],
                };

                for list_item in point.thresholds {
                    
                    if let Some(price_up) = list_item.price_up {
                        list.upward.push((list_item.capacity_threshold.parse::<f32>().unwrap(), price_up.parse::<f32>().unwrap()));
                    }

                    if let Some(price_down) = list_item.price_down {
                        list.downward.push((list_item.capacity_threshold.parse::<f32>().unwrap(), price_down.parse::<f32>().unwrap()));
                    }
                }
    
                lists.push(list);
            }
        }
    }

    insert_merit_order(app_state, &lists).await;

    lists
}

async fn import_csv (app_state: &AppState, path: PathBuf, sync_from: i64) {

    let mut records: Vec<MeritOrderList> = vec![];

    let mut rdr = csv::ReaderBuilder::new()
        .has_headers(true)
        .delimiter(b';')
        .trim(csv::Trim::Headers)
        .from_path(path).unwrap();

    let mut merit_order_record: Option<MeritOrderList> = None;
    let mut ambiguous_times = HashMap::new();
    let mut last_threshold = 0.0;

    for result in rdr.deserialize() {

        let row: MeritOrderRow = result.unwrap();

        let time =  match utils::time::parse_tennet_time_stamp(&row.time_interval_start) {
            LocalResult::Single(t) => Some(t.to_utc()),
            LocalResult::Ambiguous(first, last) => {

                let mut time_stamp = first.to_utc();
                let stamp = time_stamp.timestamp();

                if let Some(ambiguous) = ambiguous_times.get(&stamp) {

                    if *ambiguous == 1 {
                        time_stamp = last.to_utc();
                    } else if *ambiguous == 0 && row.capacity_threshold < last_threshold {
                        ambiguous_times.insert(stamp, 1);
                        time_stamp = last.to_utc();
                    }

                } else {
                    ambiguous_times.insert(stamp, 0);
                }

                Some(time_stamp)
            },
            LocalResult::None => None
        };

        last_threshold = row.capacity_threshold;

        if let Some(time_stamp) = time {

            let time_stamp = time_stamp.timestamp();

            if time_stamp < sync_from {
                continue;
            }

            if let Some(list) = &mut merit_order_record {

                if list.time_stamp == time_stamp {

                    if let Some(up) = row.price_up {
                        list.upward.push((row.capacity_threshold, up))
                    }

                    if let Some(down) = row.price_down {
                        list.downward.push((row.capacity_threshold, down))
                    }

                } else {

                    records.push(list.clone());
                    merit_order_record = Some(merit_order::MeritOrderList {
                        time_stamp,
                        upward: vec![],
                        downward: vec![],
                    });

                    if let Some(current_list) = &mut merit_order_record {
                        if let Some(up) = row.price_up {
                            current_list.upward.push((row.capacity_threshold, up))
                        }

                        if let Some(down) = row.price_down {
                            current_list.downward.push((row.capacity_threshold, down))
                        }
                    }
                }

            } else {
                merit_order_record = Some(merit_order::MeritOrderList {
                    time_stamp,
                    upward: vec![],
                    downward: vec![],
                });

                if let Some(current_list) = &mut merit_order_record {
                    if let Some(up) = row.price_up {
                        current_list.upward.push((row.capacity_threshold, up))
                    }
    
                    if let Some(down) = row.price_down {
                        current_list.downward.push((row.capacity_threshold, down))
                    }
                }
            }
        }
    }

    insert_merit_order(app_state, &records).await;
}

fn merit_order_list_to_record (list: &MeritOrderList) -> Vec<MeritOrderRecord> {

    let mut records = vec![];

    for (capacity, price) in &list.downward {
        records.push(MeritOrderRecord {
            time_stamp: list.time_stamp,
            capacity_threshold: *capacity,
            price_down: Some(*price),
            price_up: None,
        });
    }

    for (capacity, price) in &list.upward {

        let existing_record = records
            .iter_mut()
            .find(|r| r.capacity_threshold == *capacity);

        if let Some(r) = existing_record {
            r.price_up = Some(*price);
        } else {
            records.push(MeritOrderRecord {
                time_stamp: list.time_stamp,
                capacity_threshold: *capacity,
                price_down: None,
                price_up: Some(*price),
            });
        }
    }

    records.sort_by(|a, b| a.capacity_threshold.partial_cmp(&b.capacity_threshold).unwrap());

    records
}

async fn insert_merit_order (app_state: &AppState, lists: &Vec<MeritOrderList>) {

    let mut parsed_records = vec![];

    for list in lists {
        parsed_records.extend(merit_order_list_to_record(list));
    }

    for records_chunk in parsed_records.chunks(PG_MAX_QUERY_PARAMS / RECORD_COLUMNS) {
        match merit_order::insert_many(&app_state.db_client, records_chunk).await {
            Ok(rows_affected) => {
                tracing::info!("inserted {} records into merit order db", rows_affected);
            },
            Err(err) => {
                tracing::error!("{:#?}", err);
            }
        }
    }
}