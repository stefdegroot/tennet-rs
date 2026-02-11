use std::sync::Arc;
use sqlx::{Executor, FromRow, Pool, Postgres, QueryBuilder};
use serde::{Serialize, Deserialize};
use utoipa::ToSchema;

#[derive(Clone, Debug, Serialize, Deserialize, FromRow, ToSchema)]
pub struct FrrActivationsRecord {
    pub time_stamp: i64,
    pub afrr_up: f32,
    pub afrr_down: f32,
    pub total_volume: f32,
    pub mfrrda_volume_up: f32,
    pub mfrrda_volume_down: f32,
    pub absolute_total_volume: f32,
}

impl From<&FrrActivationsRecord> for String {
    fn from(value: &FrrActivationsRecord) -> Self {
        serde_json::ser::to_string(value).unwrap()
    }
}

impl From<FrrActivationsRecord> for String {
    fn from(value: FrrActivationsRecord) -> Self {
        serde_json::ser::to_string(&value).unwrap()
    }
}

pub async fn create_table (pool: &Pool<Postgres>) -> Result<(), sqlx::Error> {

    let _r = pool.execute(r#"
        CREATE TABLE IF NOT EXISTS frr_activations (
            time_stamp                  BIGINT NOT NULL PRIMARY KEY,
            afrr_up                     REAL NOT NULL,
            afrr_down                   REAL NOT NULL,
            total_volume                REAL NOT NULL,
            mfrrda_volume_up            REAL NOT NULL,
            mfrrda_volume_down          REAL NOT NULL,
            absolute_total_volume       REAL NOT NULL
        );
        CREATE INDEX IF NOT EXISTS frr_activations_time_stamp ON frr_activations (time_stamp);
    "#).await?;

    Ok(())
}

pub async fn insert_many (pool: &Arc<Pool<Postgres>>, records: &[FrrActivationsRecord]) -> Result<u64, sqlx::Error> {

    let mut query_builder: QueryBuilder<Postgres> = QueryBuilder::new(r#"
        INSERT INTO frr_activations (
            time_stamp,
            afrr_up,
            afrr_down,
            total_volume,
            mfrrda_volume_up,
            mfrrda_volume_down,
            absolute_total_volume
        ) "#);

    query_builder.push_values(records, |mut query_builder, record| {
        query_builder
            .push_bind(record.time_stamp)
            .push_bind(record.afrr_up)
            .push_bind(record.afrr_down)
            .push_bind(record.total_volume)
            .push_bind(record.mfrrda_volume_up)
            .push_bind(record.mfrrda_volume_down)
            .push_bind(record.absolute_total_volume);
    });

    query_builder.push(" ON CONFLICT (time_stamp) DO NOTHING");

    let query = query_builder.build();

    let mut tx = pool
        .begin()
        .await?;

    let result = query.execute(&mut *tx).await?;
    let rows_affected = result.rows_affected();

    tracing::debug!("Attempting to insert {} records, {} rows affected", records.len(), rows_affected);

    tx.commit().await?;

    Ok(rows_affected)
}

pub async fn get_latest (pool: &Pool<Postgres>) -> Option<FrrActivationsRecord> {
    sqlx::query_as(r#"
        SELECT * FROM frr_activations ORDER BY time_stamp DESC LIMIT 1;
    "#).fetch_optional(pool).await.ok().flatten()
}

pub async fn get_range (pool: &Pool<Postgres>, start: i64, end: i64) -> Option<Vec<FrrActivationsRecord>> {

    tracing::debug!("Querying frr_activations: start={} ({:?}), end={} ({:?})", 
        start, 
        chrono::DateTime::from_timestamp(start, 0),
        end,
        chrono::DateTime::from_timestamp(end, 0)
    );

     let result: Result<Vec<FrrActivationsRecord>, sqlx::Error> = sqlx::query_as(r#"
        SELECT * FROM frr_activations WHERE time_stamp >= $1 AND time_stamp <= $2 ORDER BY time_stamp ASC;
    "#)
        .bind(start)
        .bind(end)
        .fetch_all(pool)
        .await;

    match result {
        Ok(records) => {
            tracing::debug!("Found {} records in range", records.len());
            Some(records)
        },
        Err(err) => {
            tracing::error!("Error querying frr_activations: {:?}", err);
            None
        },
    }
}

pub async fn get (pool: &Pool<Postgres>, time_stamp: i64) -> Option<FrrActivationsRecord> {

    let result: Result<FrrActivationsRecord, sqlx::Error> = sqlx::query_as(r#"
        SELECT * FROM frr_activations WHERE time_stamp = $1 LIMIT 1;
    "#)
        .bind(time_stamp)
        .fetch_one(pool)
        .await;

    match result {
        Ok(record) => Some(record),
        Err(err) => {
            tracing::debug!("Record not found for timestamp {}: {:?}", time_stamp, err);
            None
        },
    }
}
