use chrono::{Timelike, Utc};
use std::clone::Clone;
use tokio::{
    task,
    time::{sleep, Duration}
};
use crate::{
    AppState,
    tennet
};

pub fn sync_service (app_state: AppState) {

    let _ = schedule_tasks(ScheduleGranularity::Seconds, &[ 1, 13, 25, 37, 49 ],app_state.clone(), sync_balance_delta_high_res, "balance_delta_high_res");
}

fn sync_balance_delta_high_res (app_state: AppState) {

    tracing::info!("sync balance delta high res");

    task::spawn(async move {

        let result = tennet::balance_delta_high_res::sync_balance_delta_high_res(&app_state).await;

        if !result.is_empty() {
            app_state.mqtt_client.publish("tennet/balance-delta-high-res", serde_json::ser::to_string(&result).unwrap()).await;
        }
    });
}

fn balance_delta_service (app_state: AppState) {

    tracing::info!("sync balance delta");

    task::spawn(async move {
        
        let result = tennet::balance_delta::sync_balance_delta(&app_state).await;

        if !result.is_empty() {
            app_state.mqtt_client.publish("tennet/balance-delta", serde_json::ser::to_string(&result).unwrap()).await;
        }
    });
}

fn merit_order_service (app_state: AppState) {

    tracing::info!("sync merit order");

    task::spawn(async move {

        let result = tennet::merit_order::sync_merit_order(&app_state).await;

        if !result.is_empty() {
            app_state.mqtt_client.publish("tennet/merit-order", serde_json::ser::to_string(&result).unwrap()).await;
        }
    });
}

fn settlement_prices_service (app_state: AppState) {
    
    tracing::info!("sync settlement prices");

    task::spawn(async move {

        let result = tennet::settlement_prices::sync_settlement_prices(&app_state).await;

        if !result.is_empty() {
            app_state.mqtt_client.publish("tennet/settlement-prices", serde_json::ser::to_string(&result).unwrap()).await;
        }
    });
}

const SECONDS_TO_NANO: u64 = u64::pow(10, 9);


#[derive(Clone, Copy, Debug)]
enum ScheduleGranularity {
    Seconds,
    Minutes,
}

fn schedule_tasks<T: Send + Clone + 'static>(granularity: ScheduleGranularity, offsets: &[u64], ctx: T, callback: fn (ctx: T) -> (), name: &'static str) -> impl FnOnce() {

    let mut tasks = vec![];
    
    for  offset in offsets {

        let cloned_ctx = ctx.clone();
        let offset = *offset;
    
        tasks.push(task::spawn(async move {

            let task_ctx = cloned_ctx.clone();

            loop {
    
                let utc = Utc::now();
                let wait;

                match &granularity {
                    ScheduleGranularity::Seconds => {

                        let mark = offset * SECONDS_TO_NANO;
                        let seconds = utc.second() as u64;
                        let nano = utc.nanosecond() as u64;
                        let from_start_of_minute =  seconds * SECONDS_TO_NANO + nano;

                        if from_start_of_minute > mark {
                            wait = mark + 60 * SECONDS_TO_NANO - from_start_of_minute;
                        } else {
                            wait = mark - from_start_of_minute;
                        }
                    },
                    ScheduleGranularity::Minutes => {

                        let mark = offset * 60 * SECONDS_TO_NANO;
                        let nano = utc.nanosecond() as u64;
                        let seconds = utc.second() as u64;
                        let minutes = utc.minute() as u64;
                        let from_start_of_hour = minutes * 60 * SECONDS_TO_NANO + seconds * SECONDS_TO_NANO + nano;

                        if from_start_of_hour > mark {
                            wait = mark + 3600 * SECONDS_TO_NANO - from_start_of_hour;
                        } else {
                            wait = mark - from_start_of_hour;
                        }
                    },
                }

                tracing::info!("task scheduled {}, waiting {:.3} s", name, wait as f64 / SECONDS_TO_NANO as f64);

                let duration = Duration::from_nanos(wait);
                
                sleep(duration).await;
    
                callback(task_ctx.clone());
            }
        }));
    }

    || {
        for task in tasks {
            task.abort();
        }
    }
}
