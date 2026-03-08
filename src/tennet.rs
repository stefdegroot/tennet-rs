use reqwest::{Client, header, Url};
use serde::{Deserialize, de::DeserializeOwned};
use anyhow::{Result, anyhow};
use balance_delta_high_res::BalanceDeltaPointHighRes;
use balance_delta::BalanceDeltaPoint;
use settlement_prices::SettlementPricePoint;
use chrono::{DateTime, Utc};

use crate::config::CONFIG;

pub mod balance_delta;
pub mod balance_delta_high_res;
pub mod merit_order;
pub mod settlement_prices;
pub mod time;

pub struct TennetApi {
    api_key: Option<String>,
    base_url: Option<String>,
    client: Client,
}

#[derive(Deserialize, Debug, Clone)]
pub struct TimeSeriesPeriod <T> {
    // #[serde(rename="timeInterval")]
    // time_interval: PeriodTimeInterval,
    #[serde(rename="Points", alias = "points")]
    pub points: Vec<T>
}

#[derive(Deserialize, Debug)]
#[serde(untagged)]
pub enum TennetAPIPeriod <T> {
    Object(TimeSeriesPeriod<T>),
    Array(Vec<TimeSeriesPeriod<T>>),
}

#[derive(Deserialize, Debug)]
pub struct TimeSeries <T> {
    // #[serde(rename="mRID")]
    // m_rid: i64,
    // #[serde(rename="quantity_Measurement_Unit_name")]
    // quantity_measurement_unit_name: Option<String>,
    // #[serde(rename="price_Measurement_Unit_name")]
    // price_measurement_unit_name: Option<String>,
    // #[serde(rename="currency_Unit_name")]
    // currency_unit_name: String,
    #[serde(rename="Period")]
    pub period: TennetAPIPeriod<T>,
}

// #[derive(Deserialize, Debug)]
// pub struct PeriodTimeInterval {
//     start: String,
//     end: String,
// }

#[derive(Deserialize, Debug)]
pub struct TennetResponseInfo<T> {
    // #[serde(rename="informationType")]
    // information_type: String,
    // #[serde(rename="period.timeInterval")]
    // period_time_interval: PeriodTimeInterval,
    #[serde(rename="TimeSeries")]
    pub time_series: Vec<TimeSeries<T>>,
}

#[derive(Deserialize, Debug)]
pub struct TennetResponse <T> {
    #[serde(rename="Response")]
    pub response: TennetResponseInfo<T>
}

#[derive(Deserialize, Debug)]
pub struct TennetError {
    // error_date_time: String,
    // error_id: String,
    error_message: String,
}

#[derive(Deserialize, Debug)]
pub struct BasicError {
    error: String,
}

#[derive(Deserialize, Debug)]
#[serde(untagged)]
enum TennetAPIResponse <T> {
    Data(TennetResponse<T>),
    Err(TennetError),
    BasicError(BasicError),
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

        if let Some(base_url) = &self.base_url && let Some(api_key) = &self.api_key {
            let url = Url::parse_with_params(
                &format!("{}{}", base_url, route),
                params
            )?;

            let response = self.client
                .get(url)
                .header("apikey", api_key)
                .header(header::ACCEPT, "application/json")
                .send()
                .await?;

            let decoded_response = response.json::<TennetAPIResponse<R>>()
                .await?;

            match decoded_response {
                TennetAPIResponse::Data(tennet_response) => Ok(tennet_response),
                TennetAPIResponse::Err(tennet_error) => {
                    tracing::error!(tennet_error.error_message);
                    Err(anyhow!(tennet_error.error_message))
                },
                TennetAPIResponse::BasicError(error) => {
                    tracing::error!(error.error);
                    Err(anyhow!(error.error))
                }
            }
        } else {
            Err(anyhow!("TenneT API Key not configured, can not send request."))
        }
    }

    pub async fn get_balance_delta_high_res_latest (&self) -> Result<TennetResponse<BalanceDeltaPointHighRes>> {

        let response = self.request::<BalanceDeltaPointHighRes>(
            "/v1/balance-delta-high-res/latest",
            &[]
        ).await?;

        Ok(response)
    }

    pub async fn get_balance_delta_high_res (&self, from: DateTime<Utc>, to: DateTime<Utc>) -> Result<TennetResponse<BalanceDeltaPointHighRes>> {

        let response = self.request::<BalanceDeltaPointHighRes>(
            "/v1/balance-delta-high-res",
            &[
                ("date_from", &time::create_tennet_time_stamp(from)),
                ("date_to", &time::create_tennet_time_stamp(to)),
            ]
        ).await?;

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

    pub async fn get_merit_order_list (&self, from: DateTime<Utc>, to: DateTime<Utc>) -> Result<TennetResponse<merit_order::MeritOrderPoint>> {

        let response = self.request::<merit_order::MeritOrderPoint>(
            "/v1/merit-order-list",
            &[
                ("date_from", &time::create_tennet_time_stamp(from)),
                ("date_to", &time::create_tennet_time_stamp(to)),
            ]
        ).await?;

        Ok(response)
    }

    pub async fn get_settlement_prices (&self, from: DateTime<Utc>, to: DateTime<Utc>) -> Result<TennetResponse<SettlementPricePoint>> {

        let response = self.request::<SettlementPricePoint>(
            "/v1/settlement-prices",
            &[
                ("date_from", &time::create_tennet_time_stamp(from)),
                ("date_to", &time::create_tennet_time_stamp(to)),
            ]
        ).await?;

        Ok(response)
    }
}

