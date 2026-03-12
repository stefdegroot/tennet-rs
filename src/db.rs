use crate::config::CONFIG;
use anyhow::{anyhow, Result};
use sqlx::{postgres::PgPoolOptions, Postgres, Pool};

pub mod balance_delta_high_res;
pub mod balance_delta;
pub mod merit_order;
pub mod settlement_prices;

pub const PG_MAX_QUERY_PARAMS: usize = 65_535;
pub const RECORD_COLUMNS: usize = 14;

pub async fn setup_db () -> Result<Pool<Postgres>> {

    let connection_string = if let Some(db) = &CONFIG.db {
        format!(
            "postgres://{}:{}@{}/{}",
            db.user,
            db.password,
            db.host,
            db.name,
        )
    } else {
        return Err(anyhow!("Database credentials not configured, could not setup database client."));
    };

    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&connection_string).await?;

    balance_delta_high_res::create_table(&pool).await?;
    balance_delta::create_table(&pool).await?;
    merit_order::create_table(&pool).await?;
    settlement_prices::create_table(&pool).await?;

    Ok(pool)
}
