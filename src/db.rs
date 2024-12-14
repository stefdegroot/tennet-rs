use tokio_postgres::{Client, Error, NoTls};
use chrono::{DateTime, Utc};
use crate::tennet::{parse_tennet_time_stamp, BalanceDeltaPoint};
use rust_decimal_macros::dec;
use rust_decimal::prelude::*;

pub async fn setup_db (balance_delta: &Vec<BalanceDeltaPoint>) -> Result<(), Error> {

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

    insert_balance_delta(&client, balance_delta).await?;

    Ok(())
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
            // &[
            //     &parse_tennet_time_stamp(&delta.time_interval_start),
            //     &delta.power_afrr_in.parse::<f64>().unwrap(),
            //     &delta.power_afrr_out.parse::<f64>().unwrap(),
            //     &delta.power_igcc_in.parse::<f64>().unwrap(),
            //     &delta.power_igcc_out.parse::<f64>().unwrap(),
            //     &delta.power_mfrrda_in.parse::<f64>().unwrap(),
            //     &delta.power_mfrrda_out.parse::<f64>().unwrap(),
            //     &parse_some_f64(&delta.power_picasso_in),
            //     &parse_some_f64(&delta.power_picasso_out),
            //     &parse_some_f64(&delta.max_upw_regulation_price),
            //     &parse_some_f64(&delta.min_downw_regulation_price),
            //     &delta.mid_price.parse::<f64>().unwrap(),
            // ]
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
    }

    Ok(())
}