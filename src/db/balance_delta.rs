use std::sync::Arc;
use sqlx::{Executor, FromRow, Pool, Postgres, QueryBuilder};
use serde::{Serialize, Deserialize};

#[derive(Clone, Debug, Serialize, Deserialize, FromRow)]
pub struct BalanceDeltaRecord {
    pub time_stamp: i64,
    pub power_afrr_in: f32,
    pub power_afrr_out: f32,
    pub power_igcc_in: f32,
    pub power_igcc_out: f32,
    pub power_mfrrda_in: f32,
    pub power_mfrrda_out: f32,
    pub power_picasso_in: f32,
    pub power_picasso_out: f32,
    pub max_upw_regulation_price: Option<f32>,
    pub min_downw_regulation_price: Option<f32>,
    pub mid_price: f32,
}

impl From<&BalanceDeltaRecord> for String {
    fn from(value: &BalanceDeltaRecord) -> Self {
        serde_json::ser::to_string(value).unwrap()
    }
}

impl From<BalanceDeltaRecord> for String {
    fn from(value: BalanceDeltaRecord) -> Self {
        serde_json::ser::to_string(&value).unwrap()
    }
}

pub async fn create_table (pool: &Pool<Postgres>) -> Result<(), sqlx::Error> {

    let _r = pool.execute(r#"
        CREATE TABLE IF NOT EXISTS balance_delta (
            time_stamp                  BIGINT NOT NULL PRIMARY KEY,
            power_afrr_in               REAL NOT NULL,
            power_afrr_out              REAL NOT NULL,
            power_igcc_in               REAL NOT NULL,
            power_igcc_out              REAL NOT NULL,
            power_mfrrda_in             REAL NOT NULL,
            power_mfrrda_out            REAL NOT NULL,
            power_picasso_in            REAL NOT NULL,
            power_picasso_out           REAL NOT NULL,
            max_upw_regulation_price    REAL,
            min_downw_regulation_price  REAL,
            mid_price                   REAL NOT NULL
        );
        CREATE INDEX IF NOT EXISTS balance_delta_time_stamp ON balance_delta (time_stamp);
    "#).await?;

    Ok(())
}

pub async fn insert_many (pool: &Arc<Pool<Postgres>>, records: &[BalanceDeltaRecord]) -> Result<u64, sqlx::Error> {

    let mut query_builder: QueryBuilder<Postgres> = QueryBuilder::new(r#"
        INSERT INTO balance_delta (
            time_stamp,
            power_afrr_in,
            power_afrr_out,
            power_igcc_in,
            power_igcc_out,
            power_mfrrda_in,
            power_mfrrda_out,
            power_picasso_in,
            power_picasso_out,
            max_upw_regulation_price,
            min_downw_regulation_price,
            mid_price
        ) "#);

    query_builder.push_values(records, |mut query_builder, record| {
        query_builder
            .push_bind(record.time_stamp)
            .push_bind(record.power_afrr_in)
            .push_bind(record.power_afrr_out)
            .push_bind(record.power_igcc_in)
            .push_bind(record.power_igcc_out)
            .push_bind(record.power_mfrrda_in)
            .push_bind(record.power_mfrrda_out)
            .push_bind(record.power_picasso_in)
            .push_bind(record.power_picasso_out)
            .push_bind(record.max_upw_regulation_price)
            .push_bind(record.min_downw_regulation_price)
            .push_bind(record.mid_price);
    });

    let query = query_builder.build();

    let mut tx = pool
        .begin()
        .await
        .map_err(|err| println!("{:?}", err)).unwrap();


    let result = query.execute(&mut *tx).await.unwrap();

    tx.commit().await.unwrap();

    Ok(result.rows_affected())
}

pub async fn get_latest (pool: &Pool<Postgres>) -> Option<BalanceDeltaRecord> {

    let latest: Result<BalanceDeltaRecord, sqlx::Error> = sqlx::query_as(r#"
        SELECT * FROM balance_delta ORDER BY time_stamp DESC LIMIT 1;
    "#).fetch_one(pool).await;

    match latest {
        Ok(record) => Some(record),
        Err(err) => {
            println!("{:?}", err);
            None
        },
    }
}

pub async fn get_range (pool: &Pool<Postgres>, start: i64, end: i64) -> Option<Vec<BalanceDeltaRecord>> {

     let result: Result<Vec<BalanceDeltaRecord>, sqlx::Error> = sqlx::query_as(r#"
        SELECT * FROM balance_delta WHERE time_stamp >= $1 AND time_stamp <= $2 ORDER BY time_stamp ASC;
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

pub async fn get (pool: &Pool<Postgres>, time_stamp: i64) -> Option<BalanceDeltaRecord> {

    let result: Result<BalanceDeltaRecord, sqlx::Error> = sqlx::query_as(r#"
        SELECT * FROM balance_delta WHERE time_stamp = $1 LIMIT 1;
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