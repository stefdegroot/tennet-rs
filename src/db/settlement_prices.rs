use std::sync::Arc;
use sqlx::{Executor, FromRow, Pool, Postgres, QueryBuilder};
use serde::{Serialize, Deserialize};
use utoipa::ToSchema;

#[derive(Clone, Debug, Serialize, Deserialize, FromRow, ToSchema)]
pub struct SettlementPriceRecord {
    pub time_stamp: i64,
    pub incident_reserve_up: bool,
    pub incident_reserve_down: bool,
    pub price_dispatch_up: Option<f32>,
    pub price_dispatch_down: Option<f32>,
    pub price_shortage: f32,
    pub price_surplus: f32,
    pub regulation_state: i32,
}

impl From<&SettlementPriceRecord> for String {
    fn from(value: &SettlementPriceRecord) -> Self {
        serde_json::ser::to_string(value).unwrap()
    }
}

impl From<SettlementPriceRecord> for String {
    fn from(value: SettlementPriceRecord) -> Self {
        serde_json::ser::to_string(&value).unwrap()
    }
}

pub async fn create_table (pool: &Pool<Postgres>) -> Result<(), sqlx::Error> {

    let _r = pool.execute(r#"
        CREATE TABLE IF NOT EXISTS settlement_prices (
            time_stamp                  BIGINT NOT NULL PRIMARY KEY,
            incident_reserve_up         BOOL NOT NULL,
            incident_reserve_down       BOOL NOT NULL,
            price_dispatch_up           REAL,
            price_dispatch_down         REAL,
            price_shortage              REAL NOT NULL,
            price_surplus               REAL NOT NULL,
            regulation_state            INT  NOT NULL
        );
        CREATE INDEX IF NOT EXISTS settlement_prices_time_stamp ON settlement_prices (time_stamp);
    "#).await?;

    Ok(())
}

pub async fn insert_many (pool: &Arc<Pool<Postgres>>, records: &[SettlementPriceRecord]) -> Result<u64, sqlx::Error> {

    let mut query_builder: QueryBuilder<Postgres> = QueryBuilder::new(r#"
        INSERT INTO settlement_prices (
            time_stamp,
            incident_reserve_up,
            incident_reserve_down,
            price_dispatch_up,
            price_dispatch_down,
            price_shortage,
            price_surplus,
            regulation_state
        ) "#);

    query_builder.push_values(records, |mut query_builder, record| {
        query_builder
            .push_bind(record.time_stamp)
            .push_bind(record.incident_reserve_up)
            .push_bind(record.incident_reserve_down)
            .push_bind(record.price_dispatch_up)
            .push_bind(record.price_dispatch_down)
            .push_bind(record.price_shortage)
            .push_bind(record.price_surplus)
            .push_bind(record.regulation_state);
    });

    query_builder.push(" ON CONFLICT (time_stamp) DO NOTHING");

    let query = query_builder.build();

    let mut tx = pool
        .begin()
        .await
        .map_err(|err| println!("{:?}", err)).unwrap();


    let result = query.execute(&mut *tx).await.unwrap();

    tx.commit().await.unwrap();

    Ok(result.rows_affected())
}

pub async fn get_latest (pool: &Pool<Postgres>) -> Option<SettlementPriceRecord> {
    sqlx::query_as(r#"
        SELECT * FROM settlement_prices ORDER BY time_stamp DESC LIMIT 1;
    "#).fetch_optional(pool).await.ok().flatten()
}

pub async fn get_range (pool: &Pool<Postgres>, start: i64, end: i64) -> Option<Vec<SettlementPriceRecord>> {

     let result: Result<Vec<SettlementPriceRecord>, sqlx::Error> = sqlx::query_as(r#"
        SELECT * FROM settlement_prices WHERE time_stamp >= $1 AND time_stamp <= $2 ORDER BY time_stamp ASC;
    "#)
        .bind(start)
        .bind(end)
        .fetch_all(pool)
        .await;

    match result {
        Ok(records) => Some(records),
        Err(err) => {
            println!("{:?}", err);
            None
        },
    }
}

pub async fn get (pool: &Pool<Postgres>, time_stamp: i64) -> Option<SettlementPriceRecord> {

    let result: Result<SettlementPriceRecord, sqlx::Error> = sqlx::query_as(r#"
        SELECT * FROM settlement_prices WHERE time_stamp = $1 LIMIT 1;
    "#)
        .bind(time_stamp)
        .fetch_one(pool)
        .await;

    match result {
        Ok(record) => Some(record),
        Err(err) => {
            println!("{:?}", err);
            None
        },
    }
}