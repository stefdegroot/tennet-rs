use std::time::Instant;
use tokio_postgres::{Client, Error, NoTls};
use chrono::{DateTime, Utc};
use crate::tennet::{balance_delta::BalanceDeltaPoint, time::parse_tennet_time_stamp};
use rust_decimal::prelude::*;
use serde::Serialize;

pub async fn setup_db (balance_delta: &Vec<BalanceDeltaPoint>) -> Result<Client, Error> {

    let (
        client,
        connection
    ) = tokio_postgres::connect(
        "host=localhost user=admin password=root dbname=test_db", 
        NoTls
    ).await?;

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    create_balance_delta_table(&client).await?;

    Ok(client)
}

pub async fn create_balance_delta_table (client: &Client) -> Result<(), Error> {

    client.batch_execute(r#"
        CREATE TABLE IF NOT EXISTS balance_delta (
            time_stamp                  TIMESTAMP WITH TIME ZONE NOT NULL PRIMARY KEY,
            power_afrr_in               DECIMAL NOT NULL,
            power_afrr_out              DECIMAL NOT NULL,
            power_igcc_in               DECIMAL NOT NULL,
            power_igcc_out              DECIMAL NOT NULL,
            power_mfrrda_in             DECIMAL NOT NULL,
            power_mfrrda_out            DECIMAL NOT NULL,
            power_picasso_in            DECIMAL,
            power_picasso_out           DECIMAL,
            max_upw_regulation_price    DECIMAL,
            min_downw_regulation_price  DECIMAL,
            mid_price                   DECIMAL NOT NULL
        );
        CREATE INDEX IF NOT EXISTS balance_delta_time_stamp ON balance_delta (time_stamp);
    "#).await?;

    Ok(())
}

fn parse_some_f64 (str: &Option<String>) -> Option<Decimal> {
    if let Some(string) = str {
        match Decimal::from_str(&string) {
            Ok(decimal) => Some(decimal),
            Err(_) => None,
        }
    } else {
        None
    }
}

pub async fn insert_balance_delta (client: &Client, balance_delta: &Vec<BalanceDeltaPoint>) -> Result<(), Error> {

    let mut inserted_rows = 0;

    for delta in balance_delta {
        client.execute(
            r#"
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
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
            "#,
            &[
                &parse_tennet_time_stamp(&delta.time_interval_start),
                &Decimal::from_str(&delta.power_afrr_in).unwrap(),
                &Decimal::from_str(&delta.power_afrr_out).unwrap(),
                &Decimal::from_str(&delta.power_igcc_in).unwrap(),
                &Decimal::from_str(&delta.power_igcc_out).unwrap(),
                &Decimal::from_str(&delta.power_mfrrda_in).unwrap(),
                &Decimal::from_str(&delta.power_mfrrda_out).unwrap(),
                &parse_some_f64(&delta.power_picasso_in),
                &parse_some_f64(&delta.power_picasso_out),
                &parse_some_f64(&delta.max_upw_regulation_price),
                &parse_some_f64(&delta.min_downw_regulation_price),
                &Decimal::from_str(&delta.mid_price).unwrap(),
            ]
        ).await?;

        inserted_rows += 1;
    }

    println!("inserted {} rows into the balance_delta table", inserted_rows);

    Ok(())
}

pub async fn get_balance_delta (client: &Client, from: &DateTime<Utc>, to: &DateTime<Utc>) -> Result<Vec<BalanceDelta>, Error> {

    let before = Instant::now();

    let query = client.query(
        "
            SELECT * FROM balance_delta
            WHERE time_stamp >= $1
            AND time_stamp <= $2
            ORDER BY time_stamp ASC
        ",
        &[
            from,
            to,
        ]
    ).await?;

    let mut balance_delta_vec = vec![];

    for row in query {
        let balance_delta = BalanceDelta {
            time_stamp: row.get(0),
            power_afrr_in: row.get(1),
            power_afrr_out: row.get(2),
            power_igcc_in: row.get(3),
            power_igcc_out: row.get(4),
            power_mfrrda_in: row.get(5),
            power_mfrrda_out: row.get(6),
            power_picasso_in: row.get(7),
            power_picasso_out: row.get(8),
            max_upw_regulation_price: row.get(9),
            min_downw_regulation_price: row.get(10),
            mid_price: row.get(11),
        };

        balance_delta_vec.push(balance_delta)
    }

    println!("Query took: {:.2?}", before.elapsed());

    println!("{:#?}", balance_delta_vec);

    Ok(balance_delta_vec)
}

pub async fn get_latest_balance_delta (client: &Client) -> Result<Option<DateTime<Utc>>, Error> {

    let query = client.query("
       SELECT time_stamp FROM balance_delta
       ORDER BY time_stamp DESC
       LIMIT 1
    ", &[]).await?;

    let mut last_time_stamp: Option<DateTime<Utc>> = None;

    for row in query {
        last_time_stamp = Some(row.get(0))
    }

    println!("last_time_stamp: {:#?}", last_time_stamp);

    Ok(last_time_stamp)
}

#[derive(Debug, Serialize)]
pub struct BalanceDelta {
    pub time_stamp: DateTime<Utc>,
    pub power_afrr_in: Decimal,
    pub power_afrr_out: Decimal,
    pub power_igcc_in: Decimal,
    pub power_igcc_out: Decimal,
    pub power_mfrrda_in: Decimal,
    pub power_mfrrda_out: Decimal,
    pub power_picasso_in: Option<Decimal>,
    pub power_picasso_out: Option<Decimal>,
    pub max_upw_regulation_price: Option<Decimal>,
    pub min_downw_regulation_price: Option<Decimal>,
    pub mid_price: Decimal,
}
