use crate::config::CONFIG;
use sqlx::{postgres::PgPoolOptions, Execute, Executor, Postgres, Pool, QueryBuilder};

pub mod balance_delta;
pub mod merit_order;
pub mod settlement_prices;

pub const PG_MAX_QUERY_PARAMS: usize = 65_535;
pub const RECORD_COLUMNS: usize = 12;

pub async fn setup_db () -> Result<Pool<Postgres>, sqlx::Error> {

    let connection_string = format!(
        "postgres://{}:{}@{}/{}",
        CONFIG.db.user,
        CONFIG.db.password,
        CONFIG.db.host,
        CONFIG.db.name,
    );

    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&connection_string).await?;

    balance_delta::create_table(&pool).await?;
    merit_order::create_table(&pool).await?;
    settlement_prices::create_table(&pool).await?;

    Ok(pool)
}
