use reqwest;
use reqwest::{Url, header};
use serde::Deserialize;
use anyhow::{Error, Result};
use regex::Regex;
use chrono::{TimeZone, DateTime, Utc};
use chrono_tz::Europe::Amsterdam;

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
#[serde(rename_all = "camelCase")]
pub struct TimeSeriesPeriod <T> {
    time_interval: PeriodTimeInterval,
    #[serde(rename="Points")]
    pub points: Vec<T>
}

#[derive(Deserialize, Debug)]
pub struct TimeSeries <T> {
    #[serde(rename="mRID")]
    m_rid: i64,
    #[serde(rename="quantity_Measurement_Unit_name")]
    quantity_measurement_unit_name: String,
    #[serde(rename="currency_Unit_name")]
    currency_unit_name: String,
    #[serde(rename="Period")]
    pub period: TimeSeriesPeriod<T>,
}

#[derive(Deserialize, Debug)]
// #[serde(rename_all = "camelCase")]
pub struct PeriodTimeInterval {
    start: String,
    end: String,
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct BalanceDeltaResponse {
    information_type: String,
    #[serde(rename="period.timeInterval")]
    period_time_interval: PeriodTimeInterval,
    #[serde(rename="TimeSeries")]
    pub time_series: Vec<TimeSeries<BalanceDeltaPoint>>,
}

#[derive(Deserialize, Debug)]
// #[serde(rename_all = "camelCase")]
pub struct TennetResponse <T> {
    #[serde(rename="Response")]
    pub response: T
}

pub async fn get_balance_delta () -> Result<TennetResponse<BalanceDeltaResponse>> {

    let api_key = "";
    let base_url = "https://api.tennet.eu/publications";
    let path = "/v1/balance-delta";

    let date_from = "28-8-2024 00:00:00";
    let date_to = "28-8-2024 00:15:00";

    let query = format!("?date_from={date_from}&date_to={date_to}");


    let url = Url::parse_with_params(
        &format!("{base_url}{path}"),
        &[
            ("date_from", date_from),
            ("date_to", date_to),
        ]
    )?;

    println!("{:?}", url);

    let response = reqwest::Client::builder()
        .https_only(true)
        .use_rustls_tls()
        .build()?
        .get(url)
        .header("apikey", api_key)
        .header(header::ACCEPT, "application/json")
        .header(header::USER_AGENT, "Rust-test-agent")
        .send()
        .await?
        // .text()
        .json::<TennetResponse<BalanceDeltaResponse>>()
        .await?;


    println!("{:#?}", response);

    Ok(response)
}

// TODO: add proper Error handeling
pub fn parse_tennet_time_stamp (time_string: &str) -> DateTime<Utc> {

    let re = Regex::new(r"([0-9]+)-([0-9]{2})-([0-9]{2})T([0-9]{2}):([0-9]{2})").unwrap();

    let caps = re.captures(time_string).unwrap();

    let year = caps.get(1).unwrap().as_str().parse::<i32>().unwrap();
    let month = caps.get(2).unwrap().as_str().parse::<u32>().unwrap();
    let day = caps.get(3).unwrap().as_str().parse::<u32>().unwrap();
    let hour = caps.get(4).unwrap().as_str().parse::<u32>().unwrap();
    let min = caps.get(5).unwrap().as_str().parse::<u32>().unwrap();

    println!("{year}-{month}-{day}T{hour}:{min}");

    let amsterdam_time = Amsterdam.with_ymd_and_hms(year, month, day, hour, min, 0).unwrap();
    let utc = amsterdam_time.to_utc();

    return utc;
}