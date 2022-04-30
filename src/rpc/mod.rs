use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use crate::network_scanner::{NetworkScanner, QueryError};
use anyhow::Result;
use axum::response::IntoResponse;
use axum::{routing::post, Extension, Json};
use http::StatusCode;
use nekoton::transport::models::{ExistingContract, RawContractState};
use nekoton_utils::*;
use serde::{Deserialize, Serialize};

mod requests;
use crate::network_scanner::shard_accounts_subscriber::*;
use crate::rpc::requests::{GetContractState, RawBlock, SendMessageRequest};

pub async fn serve(
    subscriber: Arc<ShardAccountsSubscriber>,
    addr: SocketAddr,
    metrics: Arc<Metrics>,
    engine: Arc<NetworkScanner>,
) {
    let state = Arc::new(State {
        subscriber,
        metrics,
        engine,
    });

    let router = axum::Router::new()
        .route("/account", post(state_receiver))
        .route("/rpc", post(jrpc_router))
        .layer(
            tower::ServiceBuilder::new()
                .layer(Extension(state))
                .layer(tower_http::compression::CompressionLayer::new().gzip(true)),
        );

    axum::Server::bind(&addr)
        .serve(router.into_make_service())
        .await
        .expect("Failed to bind to address");
}

#[derive(Default)]
pub struct Metrics {
    pub requests_processed: AtomicU64,
    pub errors: AtomicU64,
}

async fn state_receiver(
    Extension(ctx): Extension<Arc<State>>,
    Json(data): Json<StateReceiveRequest>,
) -> Result<Json<Option<ExistingContract>>, impl IntoResponse> {
    ctx.metrics
        .requests_processed
        .fetch_add(1, Ordering::Release);

    match ctx.subscriber.get_contract_state(&data.address) {
        Ok(contract) => Ok(Json(contract)),
        Err(e) => {
            ctx.metrics.errors.fetch_add(1, Ordering::Release);
            log::error!("Failed to process self state: {:?}", e);

            Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                "Internal server error".into_response(),
            ))
        }
    }
}

struct State {
    subscriber: Arc<ShardAccountsSubscriber>,
    metrics: Arc<Metrics>,
    engine: Arc<NetworkScanner>,
}

#[derive(Debug, Serialize, Deserialize, Eq, PartialEq)]
struct StateReceiveRequest {
    #[serde(with = "serde_address")]
    address: ton_block::MsgAddressInt,
}

async fn jrpc_router(
    Extension(ctx): Extension<Arc<State>>,
    req: axum_jrpc::JsonRpcExtractor,
) -> axum_jrpc::JrpcResult {
    let answer_id = req.get_answer_id();
    let method = req.method();
    let answer = match method {
        "getLatestKeyBlock" => {
            let block = ctx
                .subscriber
                .get_key_block()
                .map(|x| RawBlock { block: x });
            match block {
                None => axum_jrpc::JsonRpcRepsonse::error(answer_id, QueryError::NotReady.into()),
                Some(b) => axum_jrpc::JsonRpcRepsonse::success(answer_id, b),
            }
        }
        "getContractState" => {
            let request: GetContractState = req.parse_params()?;
            let state = ctx
                .subscriber
                .get_contract_state(&request.address)
                .ok()
                .flatten() // do we care about the error?
                .map(RawContractState::Exists)
                .unwrap_or(RawContractState::NotExists);
            axum_jrpc::JsonRpcRepsonse::success(answer_id, state)
        }
        "sendMessage" => {
            let request: SendMessageRequest = req.parse_params()?;
            match ctx.engine.send_message(request.message).await {
                Ok(_) => axum_jrpc::JsonRpcRepsonse::success(answer_id, ()),
                Err(e) => axum_jrpc::JsonRpcRepsonse::error(answer_id, e.into()),
            }
        }
        m => req.method_not_found(m),
    };

    Ok(answer)
}
