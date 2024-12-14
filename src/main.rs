use std::time::SystemTime;
use chrono::{Timelike, Utc};
use tokio::time::{self, sleep, Duration, Instant};

mod tennet;
mod db;

#[tokio::main]
async fn main() {

    // let utc = Utc::now();

    // let minutes = utc.minute();
    // let seconds = utc.second();
    // let nano = utc.nanosecond();

    // println!("now: {:?}", utc);
    // println!("minutes: {:?}", minutes);
    // println!("seconds: {:?}", seconds);
    // println!("nano: {:?}", nano);

    // let mark_second = 5;

    // let seconds_to_wait = u32::max(
    //     if seconds > mark_second { 5 + 60 - seconds } else { 5 - seconds },
    //     1,
    // );

    // println!("seconds_to_wait: {:?}", seconds_to_wait);
    // println!("{:?}", seconds_to_wait as u64 * u64::pow(10, 9));
    // println!("{:?}", seconds_to_wait as u64 * u64::pow(10, 9));

    // let res = sleep(Duration::from_nanos(seconds_to_wait as u64 * u64::pow(10, 9) - nano as u64)).await;

    // fetch_balance_delta().await;

    let result = tennet::get_balance_delta().await;

    match result {
        Ok(delta_reponse) => {
            let result = db::setup_db(&delta_reponse.response.time_series[0].period.points).await;

            match result {
                Ok((_)) => {},
                Err(err) => {
                    println!("{:?}", err);
                }
            }
        },
        Err(err) => {
            println!("{:?}", err);
        }
    }

    // every 5th second of the hour

}

async fn fetch_balance_delta () {
    let utc = Utc::now();

    let minutes = utc.minute();
    let seconds = utc.second();
    let nano = utc.nanosecond();

    println!("now: {:?}", utc);
    println!("minutes: {:?}", minutes);
    println!("seconds: {:?}", seconds);
    println!("nano: {:?}", nano);
}