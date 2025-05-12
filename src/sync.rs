use chrono::{DateTime, Timelike, Utc};
use tokio::{
    task,
    time::{sleep, Duration}
};
use crate::{
    // db::get_balance_delta,
    AppState,
    tennet
};
// use crate::db::{
//     get_latest_balance_delta,
//     insert_balance_delta,
// };

pub fn sync_service (app_state: AppState) {
    schedule_service(app_state.clone(), 5, 60, balance_delta_service, "balance_delta".to_string());
    schedule_service(app_state.clone(), 15, 60, merit_order_service, "merit_order".to_string());
    schedule_service(app_state.clone(), 45, 60, settlement_prices_service, "settlement_prices".to_string());
}

fn balance_delta_service (app_state: AppState) {
    println!("sync balance delta: {:#?}", Utc::now());

    task::spawn(async move {
        
        let result = tennet::balance_delta::sync_balance_delta(&app_state).await;

        if result.len() > 0 {
            app_state.mqtt_client.publish("tennet/balance-delta", serde_json::ser::to_string(&result).unwrap()).await;
        }
    });
}

fn merit_order_service (app_state: AppState) {
    println!("sync merit order: {:#?}", Utc::now());
}

fn settlement_prices_service (app_state: AppState) {
    println!("sync settlement prices: {:#?}", Utc::now());
}

const SECONDS_TO_NANO: u64 = u64::pow(10, 9);

fn schedule_service (app_state: AppState, offset: u64, interval: u64, callback: fn (app_state: AppState) -> (), name: String){
    task::spawn(async move {
        loop {
            let utc = Utc::now();
            let seconds = utc.second() as u64;
            let nano = utc.nanosecond() as u64;
            let from_start_of_minute = seconds * SECONDS_TO_NANO + nano;
            let mark = offset * SECONDS_TO_NANO;
            let minute = interval * SECONDS_TO_NANO;

            let wait = if from_start_of_minute > mark {
                mark + minute - from_start_of_minute
            } else {
                mark - from_start_of_minute
            };
            
            println!("service scheduled {}, waiting {:.3}", name, from_start_of_minute as f64 / f64::powi(10.0, 9));

            sleep(Duration::from_nanos(wait)).await;

            callback(app_state.clone());
        }
    });
}