use std::str::FromStr;
use chrono::{Timelike, Utc, DateTime};
// use db::BalanceDelta;
use serde::Serialize;
use sqlx::{Postgres, Pool};
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
use tracing_subscriber::prelude::*;

mod config;
mod tennet;
mod db;
mod sync;
mod notification;

#[derive(Clone)]
pub struct AppState {
    db_client: Arc<Pool<Postgres>>,
    tennet_api: Arc<TennetApi>,
    mqtt_client: Arc<MQTT>,
}

#[tokio::main]
async fn main() {

    let subscriber = tracing_subscriber::FmtSubscriber::new();
    tracing::subscriber::set_global_default(subscriber).unwrap(); 

    let tennet_api = tennet::TennetApi::init();

    let mqtt_client = MQTT::init();

    let db_client = db::setup_db().await.unwrap();

    let app_state = AppState {
        db_client: Arc::new(db_client),
        tennet_api: Arc::new(tennet_api),
        mqtt_client: Arc::new(mqtt_client),
    };

    tennet::balance_delta::import_balance_delta(app_state.clone()).await;

    let app = Router::new()
        .route("/balance-delta", get(get_balance_delta))
        .with_state(app_state.clone());

    sync_service(app_state.clone());

    let listener = tokio::net::TcpListener::bind("0.0.0.0:3001").await.unwrap();
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await
        .unwrap();

}

async fn get_balance_delta(
    Query(params): Query<HashMap<String, String>>,
    State(state): State<AppState>,
) -> Result<AppJson<Vec<db::balance_delta::BalanceDeltaRecord>>, AppError> {

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

    let data = db::balance_delta::get_range(
        &state.db_client, 
        date_from.timestamp(), 
        date_to.timestamp()
    ).await;

    if let Some(records) = data {
        Ok(AppJson(records))
    } else {
        Ok(AppJson(vec![]))
    }
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