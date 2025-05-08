use chrono::{DateTime, Timelike, Utc};
use tokio::{
    task,
    time::{sleep, Duration}
};
use crate::{
    // db::get_balance_delta,
    AppState
};
// use crate::db::{
//     get_latest_balance_delta,
//     insert_balance_delta,
// };

pub fn sync_service (app_state: AppState) {
    // schedule_service(app_state.clone(), 5, 60, balance_delta_service, "balance_delta".to_string());
    schedule_service(app_state.clone(), 15, 60, merit_order_service, "merit_order".to_string());
    schedule_service(app_state.clone(), 45, 60, settlement_prices_service, "settlement_prices".to_string());
}

// fn balance_delta_service (app_state: AppState) {
//     println!("sync balance delta: {:#?}", Utc::now());

//     task::spawn(async move {

//         // get last date from db
//         let last_time_result = get_latest_balance_delta(&app_state.db_client).await.unwrap();

//         if let Some(last_time) = last_time_result {

//             let current_time_stamp = Utc::now().timestamp();
//             let last_time_stamp = last_time.timestamp();
            
//             let gap = current_time_stamp - last_time_stamp;
//             let start = last_time_stamp + 60;
//             let end = last_time_stamp + i64::min(gap, 86400) + 60;

//             let from = DateTime::from_timestamp(start, 0).unwrap();
//             let to = DateTime::from_timestamp(end, 0).unwrap();

//             println!("from: {:#?}", from);
//             println!("to: {:#?}", to);

//             let result = app_state.tennet_api.get_balance_delta(
//                 from,
//                 to,
//             ).await.unwrap();

//             insert_balance_delta(
//                 &app_state.db_client,
//                 &result.response.time_series[0].period.points,
//             ).await.unwrap();

//             let latest = get_balance_delta(&app_state.db_client, &from, &to).await.unwrap();

//             if let Some(delta) = latest.last() {
//                 app_state.mqtt_client.publish("tennet/balance-delta", delta.into()).await;
//             };
//         }


//     });
// }

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
            
            // println!("service scheduled {}, waiting {:.3}", name, from_start_of_minute as f64 / f64::powi(10.0, 9));

            sleep(Duration::from_nanos(wait)).await;

            callback(app_state.clone());
        }
    });
}