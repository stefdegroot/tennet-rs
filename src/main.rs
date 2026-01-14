
use chrono::DateTime;
use sqlx::{Postgres, Pool};
use tennet::TennetApi;
use notification::MQTT;
use sync::sync_service;
use tokio::signal;
use std::{process, str::FromStr, sync::Arc};
use tracing_subscriber::{prelude::*};

mod config;
mod tennet;
mod db;
mod sync;
mod notification;
mod api;

#[derive(Clone)]
pub struct AppState {
    db_client: Arc<Pool<Postgres>>,
    tennet_api: Arc<TennetApi>,
    mqtt_client: Arc<MQTT>,
}

#[tokio::main]
async fn main() {

    let subscriber = tracing_subscriber::FmtSubscriber::new();
    // let subscriber = tracing_subscriber::FmtSubscriber::builder()
    //     .with_max_level(tracing::Level::DEBUG)
    //     .finish();
    tracing::subscriber::set_global_default(subscriber).unwrap(); 

    let tennet_api = tennet::TennetApi::init();

    let mqtt_client = MQTT::init();

    let db_client = db::setup_db().await.unwrap_or_else( |_| {
        tracing::error!("Failed to make a connection with the database, exiting process.");
        process::exit(1);
    });

    let app_state = AppState {
        db_client: Arc::new(db_client),
        tennet_api: Arc::new(tennet_api),
        mqtt_client: Arc::new(mqtt_client),
    };

    tennet::merit_order::import_merit_order(app_state.clone()).await;
    tennet::balance_delta::import_balance_delta(app_state.clone()).await;
    tennet::settlement_prices::import_settlement_prices(app_state.clone()).await;

    sync_service(app_state.clone());

    let app = api::setup_routes(app_state.clone());

    let listener = tokio::net::TcpListener::bind("0.0.0.0:3001").await.unwrap();
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await
        .unwrap();

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