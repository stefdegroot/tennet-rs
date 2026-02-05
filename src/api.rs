use serde::Serialize;
use axum::{
    extract::{rejection::JsonRejection, FromRequest},
    http::{StatusCode, Method},
    response::{IntoResponse, Response},
    Router,
};
use tower_http::cors::{CorsLayer, Any};
use utoipa::OpenApi;
use utoipa_axum::router::OpenApiRouter;
use crate::AppState;

mod tennet;

const TENNET_TAG: &str = "TenneT";

pub enum AppError {
    JsonRejection(JsonRejection),
    BasicError((StatusCode, &'static str)),
}

pub fn setup_routes (app_state: AppState) -> Router {

    #[derive(OpenApi)]
    #[openapi(
        tags(
            (name = TENNET_TAG, description = "TenneT balance data API")
        )
    )]
    struct ApiDoc;

    let (router, api) = OpenApiRouter::with_openapi(ApiDoc::openapi())
        .nest("/tennet", tennet::tennet_router(app_state.clone()))
        .layer(
            CorsLayer::new()
                .allow_origin(Any)
                .allow_methods([Method::GET])
        )
        .split_for_parts();

    println!("{}", api.to_pretty_json().unwrap());

    router
}

#[derive(FromRequest)]
#[from_request(via(axum::Json), rejection(AppError))]
pub struct AppJson<T>(T);

impl<T> IntoResponse for AppJson<T>
where
    axum::Json<T>: IntoResponse,
{
    fn into_response(self) -> Response {
        axum::Json(self.0).into_response()
    }
}


impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        
        #[derive(Serialize)]
        struct ErrorResponse {
            message: String,
        }

        let (status, message) = match self {
            AppError::JsonRejection(rejection) => {
                (rejection.status(), rejection.body_text())
            },
            AppError::BasicError((status_code, message)) => {
                (status_code, message.to_string())
            },
        };

        (status, AppJson(ErrorResponse { message })).into_response()
    }
}

impl From<JsonRejection> for AppError {
    fn from(rejection: JsonRejection) -> Self {
        Self::JsonRejection(rejection)
    }
}
