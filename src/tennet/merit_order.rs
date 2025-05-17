use serde::Deserialize;
use std::{io, path::PathBuf};
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
    tennet::time::parse_tennet_time_stamp,
    AppState
};

#[derive(Deserialize, Debug)]
struct MeritOrderPointThreshold {
    capacity_threshold: String,
    price_up: Option<String>,
    price_down: Option<String>,
}

#[derive(Deserialize, Debug)]
pub struct MeritOrderPoint {
    #[serde(rename="timeInterval_start")]
    pub time_interval_start: String,
    #[serde(rename="timeInterval_end")]
    pub time_interval_end: String,
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

    println!("{:?}", latest_record);

    if let Some(latest) = latest_record {
        sync_from = latest.time_stamp + 900;
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

pub async fn sync_merit_order (app_state: &AppState) -> Vec<merit_order::MeritOrderList> {

    let latest_record = merit_order::get_latest(&app_state.db_client).await;
    let mut sync_from = *FIRST_MERIT_ORDER_DATE;

    if let Some(latest) = latest_record {
        sync_from = latest.time_stamp;
    }

    let current_time_stamp = Utc::now().timestamp();

    let gap = current_time_stamp - sync_from;
    let start = sync_from + 900;
    let end = sync_from + 86400 + 900;

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

             let time =  match parse_tennet_time_stamp(&point.time_interval_start) {
                LocalResult::Single(t) => Some(t.to_utc()),
                LocalResult::Ambiguous(first, last) => {

                    let mut time_stamp = first.to_utc();
                    let stamp = time_stamp.timestamp();

                    if !ambiguous_times.contains(&stamp) {

                        let existing_list = merit_order::get(&app_state.db_client, stamp).await;

                        if let Some(_) = existing_list {
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

    let mut parsed_records = vec![];

    for list in &lists {
        parsed_records.extend(merit_order_list_to_record(list));
    }

    for records_chunk in parsed_records.chunks(PG_MAX_QUERY_PARAMS / RECORD_COLUMNS) {
        let result = merit_order::insert_many(&app_state.db_client, records_chunk).await;
        println!("{:?}", result);
    }

    lists
}

fn get_files () -> io::Result<Vec<(PathBuf, String)>>  {

    let dir_path = format!("./data/merit_order");
    let files = std::fs::read_dir(dir_path)?
        .map(|res| res.map(|e| (e.path(), e.file_name().into_string().unwrap())))
        .collect::<Result<Vec<_>, io::Error>>()?;

    Ok(files)
}

fn get_time_from_file_name (filename: &String) -> (i64, i64) {

    let split: Vec<&str> = filename.split("MERIT_ORDER_LIST_MONTH_").collect();

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

        let time =  match parse_tennet_time_stamp(&row.time_interval_start) {
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

    let mut parsed_records = vec![];

    for list in records {
        parsed_records.extend(merit_order_list_to_record(&list));
    }

    for records_chunk in parsed_records.chunks(PG_MAX_QUERY_PARAMS / RECORD_COLUMNS) {
        let result = merit_order::insert_many(&app_state.db_client, records_chunk).await;
        println!("{:?}", result);
    }
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