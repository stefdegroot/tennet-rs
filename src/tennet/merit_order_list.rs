use serde::Deserialize;

#[derive(Deserialize, Debug)]
pub struct MeritOrderList {
    #[serde(rename="timeInterval_start")]
    pub time_interval_start: String,
    #[serde(rename="timeInterval_end")]
    pub time_interval_end: String,
    pub isp: String,
    #[serde(rename="Thresholds")]
    pub thresholds: Vec<MeritOrderThreshold>
}

#[derive(Deserialize, Debug)]
pub struct MeritOrderThreshold {
    capacity_threshold: String,
    price_down: Option<String>,
    price_up: Option<String>,
}