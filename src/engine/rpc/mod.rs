use std::convert::TryInto;
use std::sync::Arc;

use anyhow::{Context, Result};
use axum::extract::Extension;
use axum::handler::post;
use axum::http::StatusCode;
use axum::AddExtensionLayer;
use axum::{Json, Router};

use serde::{Deserialize, Serialize};

use ton_types::UInt256;

use crate::engine::TonSubscriber;

async fn serve(engine: Arc<TonSubscriber>) {
    let app = Router::new()
        .route("/rpc", post(post_state_request))
        .layer(AddExtensionLayer::new(engine))
        .boxed();

    axum::Server::bind(&"0.0.0.0:3000".parse().unwrap())
        .serve(app.into_make_service())
        .await
        .unwrap();
}

async fn post_state_request(
    ctx: Extension<Arc<TonSubscriber>>,
    data: Json<GetState>,
) -> Result<Json<State>, (StatusCode, String)> {
    fn inner(data: Json<GetState>) -> Result<UInt256> {
        let id = hex::decode(&data.id).context("Bad data for id:")?;
        anyhow::ensure!(
            id.len() == 32,
            "expected account id length 32. Got: {}",
            id.len()
        );
        let bytes: [u8; 32] = id.try_into().unwrap();
        Ok(UInt256::with_array(bytes))
    }
    let account_id = match inner(data) {
        Ok(a) => a,
        Err(e) => return Err((StatusCode::BAD_REQUEST, e.to_string())),
    };
    let state = match ctx.get_contract_state(&account_id) {
        Ok(a) => a
            .and_then(|x| bincode::serialize(&x).ok())
            .map(base64::encode),
        Err(e) => {
            return Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                e.context("Failed getting shard account:").to_string(),
            ))
        }
    };
    Ok(Json(State { state }))
}

#[derive(Serialize, Deserialize)]
struct GetState {
    id: String,
}

#[derive(Serialize, Deserialize)]
struct State {
    state: Option<String>,
}
