use std::sync::Arc;
use sqlx::{Executor, FromRow, Pool, Postgres, QueryBuilder};
use serde::{Serialize, Deserialize};
use utoipa::ToSchema;

#[derive(Clone, Debug, Serialize, Deserialize, FromRow, PartialEq, PartialOrd)]
pub struct MeritOrderRecord {
    pub time_stamp: i64,
    pub capacity_threshold: f32,
    pub price_down: Option<f32>,
    pub price_up: Option<f32>,
}

#[derive(Clone, Debug, Serialize, Deserialize, ToSchema)]
pub struct MeritOrderList {
    pub time_stamp: i64,
    pub upward: Vec<(f32, f32)>,
    pub downward: Vec<(f32, f32)>,
}

pub async fn create_table (pool: &Pool<Postgres>) -> Result<(), sqlx::Error> {

    let _r = pool.execute(r#"
        CREATE TABLE IF NOT EXISTS merit_order (
            time_stamp              BIGINT NOT NULL,
            capacity_threshold      REAL NOT NULL,
            price_down              REAL,
            price_up                REAL,
            PRIMARY KEY (time_stamp, capacity_threshold)
        );
        CREATE INDEX IF NOT EXISTS merit_order_time_stamp ON merit_order (time_stamp);
    "#).await?;

    Ok(())
}

pub async fn insert_many (pool: &Arc<Pool<Postgres>>, records: &[MeritOrderRecord]) -> Result<u64, sqlx::Error> {

    let mut query_builder: QueryBuilder<Postgres> = QueryBuilder::new(r#"
        INSERT INTO merit_order (
            time_stamp,
            capacity_threshold,
            price_down,
            price_up
        ) "#);

    query_builder.push_values(records, |mut query_builder, record| {
        query_builder
            .push_bind(record.time_stamp)
            .push_bind(record.capacity_threshold)
            .push_bind(record.price_down)
            .push_bind(record.price_up);
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

fn records_to_list (records: Vec<MeritOrderRecord>) -> Vec<MeritOrderList> {

    let mut lists  = vec![];

    if records.is_empty() {
        return lists;
    }

     let mut merit_order_list = MeritOrderList {
        time_stamp: records[0].time_stamp,
        upward: vec![],
        downward: vec![],
    }; 
    let mut last_time_stamp = None;

    for r in records {
        
        if let Some(last_time) = last_time_stamp {
            if last_time != r.time_stamp {
                lists.push(merit_order_list);
                merit_order_list = MeritOrderList {
                    time_stamp: r.time_stamp,
                    upward: vec![],
                    downward: vec![],
                };
            }
        }

        if let Some (price_down) = r.price_down  {
            merit_order_list.downward.push((r.capacity_threshold, price_down));
        }

        if let Some (price_up) = r.price_up  {
            merit_order_list.upward.push((r.capacity_threshold, price_up));
        }
        
        last_time_stamp = Some(r.time_stamp)
    }
    
    if !merit_order_list.upward.is_empty() || !merit_order_list.downward.is_empty() {
        lists.push(merit_order_list);
    }

    lists
}

pub async fn get_latest (pool: &Pool<Postgres>) -> Option<MeritOrderList> {

    let latest_records: Result<Vec<MeritOrderRecord>, sqlx::Error> = sqlx::query_as(r#"
        SELECT * FROM merit_order ORDER BY time_stamp DESC LIMIT 1;
    "#).fetch_all(pool).await;

    match latest_records {
        Ok(records) => {

            let latest_record = records_to_list(records).into_iter().next();

            if let Some(latest) = latest_record {
                get(pool, latest.time_stamp).await
            } else {
                None
            }
        },
        Err(err) => {
            println!("{:?}", err);
            None
        },
    }
}

pub async fn get_range (pool: &Pool<Postgres>, start: i64, end: i64) -> Option<Vec<MeritOrderList>> {

     let result: Result<Vec<MeritOrderRecord>, sqlx::Error> = sqlx::query_as(r#"
        SELECT * FROM merit_order WHERE time_stamp >= $1 AND time_stamp <= $2 ORDER BY time_stamp ASC, capacity_threshold ASC;
    "#)
        .bind(start)
        .bind(end)
        .fetch_all(pool)
        .await;

    match result {
        Ok(records) => Some(records_to_list(records)),
        Err(err) => {
            println!("{:?}", err);
            None
        },
    }
}

pub async fn get (pool: &Pool<Postgres>, time_stamp: i64) -> Option<MeritOrderList> {

    let latest_records: Result<Vec<MeritOrderRecord>, sqlx::Error> = sqlx::query_as(r#"
        SELECT * FROM merit_order WHERE time_stamp = $1 ORDER BY time_stamp ASC, capacity_threshold ASC;
    "#)
        .bind(time_stamp)
        .fetch_all(pool)
        .await;

    match latest_records {
        Ok(records) => records_to_list(records).into_iter().next(),
        Err(err) => {
            println!("{:?}", err);
            None
        },
    }
}

pub async fn delete_range (pool: &Pool<Postgres>, start: i64, end: i64) {

    let result = sqlx::query(r#"
        DELETE FROM merit_order WHERE time_stamp >= $1 AND time_stamp <= $2;
    "#)
        .bind(start)
        .bind(end)
        .execute(pool)
        .await;

     match result {
        Ok(_) => (),
        Err(err) => {
            println!("{:?}", err);
        },
    }
}