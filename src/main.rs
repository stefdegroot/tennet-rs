use std::str::FromStr;
use chrono::{Timelike, Utc, DateTime};
use db::BalanceDelta;
use serde::Serialize;
use tennet::TennetApi;
use notification::MQTT;
use sync::sync_service;
use tokio::signal;
use tokio_postgres::Client;
use std::collections::HashMap;
use std::sync::Arc;
use axum::{
    extract::{rejection::JsonRejection, FromRequest, Query, State},
    http::StatusCode, response::{IntoResponse, Response},
    routing::get,
    Router
};
use dotenv::dotenv;

mod tennet;
mod db;
mod sync;
mod notification;

#[derive(Clone)]
pub struct AppState {
    db_client: Arc<Client>,
    tennet_api: Arc<TennetApi>,
    mqtt_client: Arc<MQTT>,
}

#[tokio::main]
async fn main() {

    dotenv().ok();

    let db_client = db::setup_db(&vec![]).await.expect("Failed to setup database client.");

    let tennet_api = tennet::TennetApi::init();

    let mqtt_client = MQTT::init();

    
    let app_state = AppState {
        db_client: Arc::new(db_client),
        tennet_api: Arc::new(tennet_api),
        mqtt_client: Arc::new(mqtt_client),
    };

    let app = Router::new()
        .route("/balance-delta", get(get_balance_delta))
        .with_state(app_state.clone());

    sync_service(app_state.clone());

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

    // let result = tennet::get_balance_delta().await;

    // match result {
    //     Ok(delta_reponse) => {
    //         let result = db::setup_db(&delta_reponse.response.time_series[0].period.points).await;

    //         match result {
    //             Ok((_)) => {},
    //             Err(err) => {
    //                 println!("{:?}", err);
    //             }
    //         }
    //     },
    //     Err(err) => {
    //         println!("{:?}", err);
    //     }
    // }

    // every 5th second of the hour

    // match result {
    //     Ok((_)) => {},
    //     Err(err) => {
    //         println!("{:?}", err);
    //     }
    // }

    let listener = tokio::net::TcpListener::bind("0.0.0.0:3001").await.unwrap();
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await
        .unwrap();

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

async fn get_balance_delta (
    Query(params): Query<HashMap<String, String>>,
    State(state): State<AppState>,
) -> Result<AppJson<Vec<BalanceDelta>>, AppError> {

    println!("{:#?}", params);

    let date_from = match params.get("date_from") {
        Some(date) => DateTime::<Utc>::from_str(&date).unwrap(),
        None => {
            println!("date_from query param missing");
            return Err(AppError::BasicError((StatusCode::BAD_REQUEST, "date_from query param missing")))
        },
    };

    let date_to = match params.get("date_to") {
        Some(date) => DateTime::<Utc>::from_str(&date).unwrap(),
        None => {
            println!("date_to query param missing");
            return Err(AppError::BasicError((StatusCode::BAD_REQUEST, "date_to query param missing")))
        },
    };

    let data = match db::get_balance_delta(
        &state.db_client,
        &date_from,
        &date_to,
    ).await {
        Ok(data) => data,
        Err(err) => {
            println!("{:#?}", err);
            return Err(AppError::BasicError((StatusCode::INTERNAL_SERVER_ERROR, "Failed to load balance delta data")))
        }
    };

    // let _ = state.tennet_api.get_balance_delta(date_from, date_to).await;
    // match state.tennet_api.get_merit_order_list(date_from, date_to).await {
    // match state.tennet_api.get_settlement_prices(date_from, date_to).await {
    //     Ok(r) => {
    //         println!("success");
    //     },
    //     Err(err) => {
    //         println!("{:#?}", err);
    //     }
    // }

    return Ok(AppJson(data));
}

#[derive(FromRequest)]
#[from_request(via(axum::Json), rejection(AppError))]
struct AppJson<T>(T);

impl<T> IntoResponse for AppJson<T>
where
    axum::Json<T>: IntoResponse,
{
    fn into_response(self) -> Response {
        axum::Json(self.0).into_response()
    }
}

enum AppError {
    JsonRejection(JsonRejection),
    BasicError((StatusCode, &'static str)),
}

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        
        #[derive(Serialize)]
        struct ErrorResponse {
            message: String,
        }

        let (status, message) = match self {
            AppError::JsonRejection(rejection) => {
                (rejection.status(), rejection.body_text())
            },
            AppError::BasicError((status_code, message)) => {
                (status_code, message.to_string())
            },
        };

        (status, AppJson(ErrorResponse { message })).into_response()
    }
}

impl From<JsonRejection> for AppError {
    fn from(rejection: JsonRejection) -> Self {
        Self::JsonRejection(rejection)
    }
}

async fn shutdown_signal () {

    let ctrl_c = async {
        signal::ctrl_c()
            .await
            .expect("failed to install Ctrl+c handler");
    };

    #[cfg(unix)]
    let terminate = async {
        signal::unix::signal(signal::unix::SignalKind::terminate())
            .expect("failed to install signal handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }
}