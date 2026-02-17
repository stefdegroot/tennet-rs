use axum::extract::{Query, State};
use utoipa::IntoParams;
use serde::Deserialize;
use crate::db;
use crate::db::settlement_prices::SettlementPriceRecord;
use crate::api::{AppState, AppJson, AppError, TENNET_TAG};
use crate::util::time::iso_string_to_date;

#[derive(Debug, Deserialize, IntoParams)]
pub struct GetSettlementPricesQuery {
    date_from: String,
    date_to: String,
}

#[utoipa::path(
    get,
    path = "/settlement-prices",
    tag = TENNET_TAG,
    params(GetSettlementPricesQuery),
    responses(
        (status = 200, description = "Successful query.", body = [SettlementPriceRecord]),
        (status = 400, description = "Bad request.", body = String),
    )
)]
pub async fn get_settlement_prices(
    Query(params): Query<GetSettlementPricesQuery>,
    State(state): State<AppState>,
) -> Result<AppJson<Vec<SettlementPriceRecord>>, AppError> {

    tracing::info!("/tennet/settlement-prices: {:#?}", params);

    let date_from = iso_string_to_date(&params.date_from)?;
    let date_to = iso_string_to_date(&params.date_to)?;

    let data = db::settlement_prices::get_range(
        &state.db_client, 
        date_from.timestamp(), 
        date_to.timestamp()
    ).await;


    if let Some(records) = data {
        Ok(AppJson(records))
    } else {
        Ok(AppJson(vec![]))
    }
}
