use std::env;
use reqwest::{Client, header, Url};
use serde::{Deserialize, de::DeserializeOwned};
use anyhow::Result;
use balance_delta::BalanceDeltaPoint;
use settlement_prices::SettlementPrices;
use chrono::{DateTime, Utc};
use crate::db::merit_order::MeritOrderList;

use crate::config::CONFIG;

pub mod balance_delta;
pub mod merit_order;
pub mod settlement_prices;
pub mod time;

pub struct TennetApi {
    api_key: String,
    base_url: String,
    client: Client,
}

#[derive(Deserialize, Debug)]
pub struct TimeSeriesPeriod <T> {
    #[serde(rename="timeInterval")]
    time_interval: PeriodTimeInterval,
    #[serde(rename="Points")]
    pub points: Vec<T>
}

#[derive(Deserialize, Debug)]
pub struct TimeSeries <T> {
    #[serde(rename="mRID")]
    m_rid: i64,
    #[serde(rename="quantity_Measurement_Unit_name")]
    quantity_measurement_unit_name: Option<String>,
    #[serde(rename="price_Measurement_Unit_name")]
    price_measurement_unit_name: Option<String>,
    #[serde(rename="currency_Unit_name")]
    currency_unit_name: String,
    #[serde(rename="Period")]
    pub period: TimeSeriesPeriod<T>,
}

#[derive(Deserialize, Debug)]
pub struct PeriodTimeInterval {
    start: String,
    end: String,
}

#[derive(Deserialize, Debug)]
pub struct TennetResponseInfo<T> {
    #[serde(rename="informationType")]
    information_type: String,
    #[serde(rename="period.timeInterval")]
    period_time_interval: PeriodTimeInterval,
    #[serde(rename="TimeSeries")]
    pub time_series: Vec<TimeSeries<T>>,
}

#[derive(Deserialize, Debug)]
pub struct TennetResponse <T> {
    #[serde(rename="Response")]
    pub response: TennetResponseInfo<T>
}

impl TennetApi {
    pub fn init () -> Self {

        let api_key = CONFIG.tennet.api_key.clone();
        let base_url = CONFIG.tennet.api_url.clone();

        let client = reqwest::Client::builder()
            .https_only(true)
            .use_rustls_tls()
            .build()
            .expect("Failed to create reqwest client.");

        TennetApi {
            api_key,
            base_url,
            client,
        }
    }

    async fn request <R: DeserializeOwned> (&self, route: &str, params: &[(&str, &str)]) -> Result<TennetResponse<R>> {

        let url = Url::parse_with_params(
            &format!("{}{}", self.base_url, route),
            params
        )?;

        let response = self.client
            .get(url)
            .header("apikey", &self.api_key)
            .header(header::ACCEPT, "application/json")
            .send()
            .await?
            .json::<TennetResponse<R>>()
            .await?;

        Ok(response)
    }

    pub async fn get_balance_delta (&self, from: DateTime<Utc>, to: DateTime<Utc>) -> Result<TennetResponse<BalanceDeltaPoint>> {

        let response = self.request::<BalanceDeltaPoint>(
            "/v1/balance-delta",
            &[
                ("date_from", &time::create_tennet_time_stamp(from)),
                ("date_to", &time::create_tennet_time_stamp(to)),
            ]
        ).await?;

        Ok(response)
    }

    pub async fn get_merit_order_list (&self, from: DateTime<Utc>, to: DateTime<Utc>) -> Result<TennetResponse<MeritOrderList>> {

        let response = self.request::<MeritOrderList>(
            "/v1/merit-order-list",
            &[
                ("date_from", &time::create_tennet_time_stamp(from)),
                ("date_to", &time::create_tennet_time_stamp(to)),
            ]
        ).await?;

        Ok(response)
    }

    pub async fn get_settlement_prices (&self, from: DateTime<Utc>, to: DateTime<Utc>) -> Result<TennetResponse<SettlementPrices>> {

        let response = self.request::<SettlementPrices>(
            "/v1/settlement-prices",
            &[
                ("date_from", &time::create_tennet_time_stamp(from)),
                ("date_to", &time::create_tennet_time_stamp(to)),
            ]
        ).await?;

        Ok(response)
    }
}

