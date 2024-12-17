use serde::Deserialize;

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