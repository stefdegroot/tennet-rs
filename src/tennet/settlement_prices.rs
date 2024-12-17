use serde::Deserialize;

#[derive(Deserialize, Debug)]
pub struct SettlementPrices {
    #[serde(rename="timeInterval_start")]
    pub time_interval_start: String,
    #[serde(rename="timeInterval_end")]
    pub time_interval_end: String,
    pub isp: String,
    pub incident_reserve_up: String,
    pub incident_reserve_down: String,
    pub dispatch_up: Option<String>,
    pub dispatch_down: Option<String>,
    pub shortage: String,
    pub surplus: String,
    pub regulation_state: i32,
    pub regulating_condition: String,
}