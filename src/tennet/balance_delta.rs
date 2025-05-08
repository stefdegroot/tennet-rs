use serde::Deserialize;
use std::{io, path::PathBuf};
use std::collections::HashSet;
use chrono::offset::LocalResult;
use std::time::{Duration, Instant};

use crate::AppState;
use crate::tennet::time::parse_tennet_time_stamp;
use crate::db::balance_delta::{insert_many, BalanceDeltaRecord};

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
    #[serde(rename="Timeinterval End Loc")]
    pub time_interval_end: String,
    #[serde(rename="Isp")]
    pub sequence: i32,
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

pub const PG_MAX_QUERY_PARAMS: usize = 65_535;
pub const RECORD_COLUMNS: usize = 12;

pub async fn import_balance_delta (app_state: AppState) {

    let files = get_files().unwrap();

    // for file in files[2..].iter() {
    for file in files {

        let mut records: Vec<BalanceDeltaRecord> = vec![];

        println!("{:?}", file);

        let mut rdr = csv::ReaderBuilder::new()
            .has_headers(true)
            .delimiter(b';')
            .trim(csv::Trim::Headers)
            .from_path(file).unwrap();

        let mut ambiguous_times = HashSet::new();

        println!("parsing csv...");

        let start = Instant::now();

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

        let duration = start.elapsed();
        println!("Time elapsed in expensive_function() is: {:?}", duration);
        
        println!("inserting records...");

        for records_chunk in records.chunks(PG_MAX_QUERY_PARAMS / RECORD_COLUMNS) {
            // let result = insert_many(&[
            //     records_chunk.get(0).unwrap().clone(),
            //     records_chunk.get(1).unwrap().clone(),
            // ]).await;
            let result = insert_many(&app_state.db_client, records_chunk).await;

            println!("{:?}", result);
        }

    }
    

}

fn default_to_zero (option: Option<f32>) -> f32 {
    if let Some(n) = option {
        n
    } else {
        0.0
    }
}

fn get_files () -> io::Result<Vec<PathBuf>>  {

    let dir_path = format!("./data/balance_delta");
    let files = std::fs::read_dir(dir_path)?
        .map(|res| res.map(|e| e.path()))
        .collect::<Result<Vec<_>, io::Error>>()?;

    Ok(files)
}