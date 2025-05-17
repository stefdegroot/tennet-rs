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

    let _ = schedule_tasks(ScheduleGranularity::SECONDS, &[5],app_state.clone(), balance_delta_service, "balance_delta");
    let _ = schedule_tasks(ScheduleGranularity::MINUTES, &[0, 15, 30, 45],app_state.clone(), merit_order_service, "merit_order");
    let _ = schedule_tasks(ScheduleGranularity::MINUTES, &[5],app_state.clone(), settlement_prices_service, "settlement_prices");

}

fn balance_delta_service (app_state: AppState) {

    tracing::info!("sync balance delta");

    task::spawn(async move {
        
        let result = tennet::balance_delta::sync_balance_delta(&app_state).await;

        if result.len() > 0 {
            app_state.mqtt_client.publish("tennet/balance-delta", serde_json::ser::to_string(&result).unwrap()).await;
        }
    });
}

fn merit_order_service (app_state: AppState) {

    tracing::info!("sync merit order");

    task::spawn(async move {

        let result = tennet::merit_order::sync_merit_order(&app_state).await;

        if result.len() > 0 {
            app_state.mqtt_client.publish("tennet/merit-order", serde_json::ser::to_string(&result).unwrap()).await;
        }
    });
}

fn settlement_prices_service (app_state: AppState) {
    
    tracing::info!("sync settlement prices");

    task::spawn(async move {

        let result = tennet::settlement_prices::sync_settlement_prices(&app_state).await;

        if result.len() > 0 {
            app_state.mqtt_client.publish("tennet/settlement-prices", serde_json::ser::to_string(&result).unwrap()).await;
        }
    });
}

const SECONDS_TO_NANO: u64 = u64::pow(10, 9);


#[derive(Clone, Copy, Debug)]
enum ScheduleGranularity {
    SECONDS,
    MINUTES,
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
                let mut wait = 0;

                match &granularity {
                    ScheduleGranularity::SECONDS => {

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
                    ScheduleGranularity::MINUTES => {

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

    let stop_schedule = || {
        for task in tasks {
            task.abort();
        }
    };

    stop_schedule
}
