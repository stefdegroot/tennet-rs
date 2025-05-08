use std::sync::Arc;

use chrono::{Utc, DateTime};
use sqlx::{postgres::PgPoolOptions, Pool, Postgres, QueryBuilder};


#[derive(Clone, Debug)]
pub struct BalanceDeltaRecord {
    pub time_stamp: DateTime<Utc>,
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

pub async fn insert_many (pool: &Arc<Pool<Postgres>>, records: &[BalanceDeltaRecord]) -> Result<(), sqlx::Error> {

    // let user = "admin";
    // let password = "root";
    // let db_name = "test_db";
    // let connection_string = format!("postgres://{user}:{password}@localhost/{db_name}");
    // let pool = PgPoolOptions::new()
    //     .max_connections(5)
    //     .connect(&connection_string).await?;

    // let r = pool.execute(r#"
    //     CREATE TABLE IF NOT EXISTS balance_delta (
    //         time_stamp                  TIMESTAMP NOT NULL PRIMARY KEY,
    //         power_afrr_in               REAL NOT NULL,
    //         power_afrr_out              REAL NOT NULL,
    //         power_igcc_in               REAL NOT NULL,
    //         power_igcc_out              REAL NOT NULL,
    //         power_mfrrda_in             REAL NOT NULL,
    //         power_mfrrda_out            REAL NOT NULL,
    //         power_picasso_in            REAL NOT NULL,
    //         power_picasso_out           REAL NOT NULL,
    //         max_upw_regulation_price    REAL,
    //         min_downw_regulation_price  REAL,
    //         mid_price                   REAL NOT NULL
    //     );
    //     CREATE INDEX IF NOT EXISTS balance_delta_time_stamp ON balance_delta (time_stamp);
    // "#).await?;

    // let test = sqlx::query(r#"
    //     INSERT INTO balance_delta (
    //         time_stamp,
    //         power_afrr_in,
    //         power_afrr_out,
    //         power_igcc_in,
    //         power_igcc_out,
    //         power_mfrrda_in,
    //         power_mfrrda_out,
    //         power_picasso_in,
    //         power_picasso_out,
    //         max_upw_regulation_price,
    //         min_downw_regulation_price,
    //         mid_price
    //     ) VALUES ('2025-05-08T18:55:17Z', 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, null, null, 80.0);
    // "#).fetch_one(&pool).await?;

    // println!("{:?}", test);

    // println!("{:?}", records[0]);

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

    Ok(())
}