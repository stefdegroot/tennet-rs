use axum:: extract::{Query, State};
use utoipa::IntoParams;
use serde::Deserialize;
use crate::db;
use crate::db::merit_order::MeritOrderList;
use crate::api::{AppState, AppJson, AppError, TENNET_TAG};
use crate::util::time::iso_string_to_date;

#[derive(Debug, Deserialize, IntoParams)]
pub struct GetMeritOrderQuery {
    date_from: String,
    date_to: String,
}

#[utoipa::path(
    get,
    path = "/merit-order",
    tag = TENNET_TAG,
    params(GetMeritOrderQuery),
    responses(
        (status = 200, description = "Successful query.", body = [MeritOrderList]),
        (status = 400, description = "Bad request.", body = String),
    )
)]
pub async fn get_merit_order(
    Query(params): Query<GetMeritOrderQuery>,
    State(state): State<AppState>,
) -> Result<AppJson<Vec<MeritOrderList>>, AppError> {

    tracing::info!("/tennet/merit-order: {:#?}", params);

    let date_from = iso_string_to_date(&params.date_from)?;
    let date_to = iso_string_to_date(&params.date_to)?;

    let data = db::merit_order::get_range(
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
