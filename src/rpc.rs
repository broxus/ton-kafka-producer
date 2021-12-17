use std::convert::Infallible;
use std::net::SocketAddr;
use std::str::FromStr;
use std::sync::Arc;

use anyhow::Result;
use nekoton_utils::TrustMe;
use serde::{Deserialize, Serialize};
use ton_block::MsgAddressInt;
use warp::http::StatusCode;
use warp::{reply, Filter, Reply};

use crate::metrics::RpcMetrics;
use crate::network_scanner::shard_accounts_subscriber::*;

pub async fn serve(
    subscriber: Arc<ShardAccountsSubscriber>,
    addr: SocketAddr,
    metrics: Arc<RpcMetrics>,
) {
    let state = Arc::new(State {
        subscriber,
        metrics,
    });
    let state = warp::any().map(move || state.clone());
    let routes = warp::path::path("account")
        .and(state.clone())
        .and(warp::post())
        .and(json_data())
        .and_then(state_receiver);
    warp::serve(routes).bind(addr).await;
}

#[derive(Serialize, Deserialize)]
struct StateReceiveRequest {
    #[serde(default = "default_wc")]
    wc: i8,
    account_id: String,
}

fn default_wc() -> i8 {
    0
}

struct State {
    subscriber: Arc<ShardAccountsSubscriber>,
    metrics: Arc<RpcMetrics>,
}

async fn state_receiver(
    ctx: Arc<State>,
    data: StateReceiveRequest,
) -> Result<Box<dyn Reply>, Infallible> {
    if data.account_id.len() != 32 {
        return Ok(Box::new(reply::with_status(
            format!("Expected len 32. Got: {}", data.account_id.len()),
            StatusCode::BAD_REQUEST,
        )));
    }
    let address = match MsgAddressInt::from_str(&format!("{}:{}", data.wc, data.account_id)) {
        Ok(a) => a,
        Err(e) => {
            log::error!("Bad request: {:?}", e);
            return Ok(Box::new(reply::with_status(
                e.to_string(),
                StatusCode::BAD_REQUEST,
            )));
        }
    };
    log::info!("Got {} request", address);
    ctx.metrics.processed();

    let state = match ctx.subscriber.get_contract_state(&address) {
        Ok(a) => a.map(|x| serde_json::to_value(x).trust_me()),
        Err(e) => {
            return Ok(Box::new(reply::with_status(
                e.context("Failed getting shard account:").to_string(),
                StatusCode::INTERNAL_SERVER_ERROR,
            )));
        }
    };

    match state {
        Some(a) => Ok(Box::new(warp::reply::json(&a))),
        None => Ok(Box::new(reply::with_status(
            "No state found".to_string(),
            StatusCode::NO_CONTENT,
        ))),
    }
}

fn json_data<T>() -> impl Filter<Extract = (T,), Error = warp::Rejection> + Clone
where
    for<'a> T: serde::Deserialize<'a> + Send,
{
    warp::body::content_length_limit(1024).and(warp::filters::body::json::<T>())
}
