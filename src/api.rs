use serde::Serialize;
use axum::{
    extract::{rejection::JsonRejection, FromRequest},
    http::StatusCode, response::{IntoResponse, Response},
    Router
};
use crate::AppState;

mod tennet;

pub enum AppError {
    JsonRejection(JsonRejection),
    BasicError((StatusCode, &'static str)),
}

pub fn setup_routes (app_state: AppState) -> Router {

    let app = Router::new()
        .nest("/tennet", tennet::tennet_router(app_state.clone()));

    app
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
