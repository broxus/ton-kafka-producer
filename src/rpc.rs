use std::convert::Infallible;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use anyhow::Result;
use nekoton_utils::*;
use serde::{Deserialize, Serialize};
use warp::http::StatusCode;
use warp::{reply, Filter, Reply};

use crate::network_scanner::shard_accounts_subscriber::*;

pub async fn serve(
    subscriber: Arc<ShardAccountsSubscriber>,
    addr: SocketAddr,
    metrics: Arc<Metrics>,
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

#[derive(Default)]
pub struct Metrics {
    pub requests_processed: AtomicU64,
    pub errors: AtomicU64,
}

async fn state_receiver(
    ctx: Arc<State>,
    data: StateReceiveRequest,
) -> Result<Box<dyn Reply>, Infallible> {
    ctx.metrics
        .requests_processed
        .fetch_add(1, Ordering::Release);

    Ok(match ctx.subscriber.get_contract_state(&data.address) {
        Ok(contract) => Box::new(warp::reply::json(&contract)),
        Err(e) => {
            ctx.metrics.errors.fetch_add(1, Ordering::Release);

            Box::new(reply::with_status(
                e.context("Failed getting shard account:").to_string(),
                StatusCode::INTERNAL_SERVER_ERROR,
            ))
        }
    })
}

struct State {
    subscriber: Arc<ShardAccountsSubscriber>,
    metrics: Arc<Metrics>,
}

#[derive(Debug, Serialize, Deserialize, Eq, PartialEq)]
struct StateReceiveRequest {
    #[serde(with = "serde_address")]
    address: ton_block::MsgAddressInt,
}

fn json_data<T>() -> impl Filter<Extract = (T,), Error = warp::Rejection> + Clone
where
    for<'a> T: serde::Deserialize<'a> + Send,
{
    warp::body::content_length_limit(1024).and(warp::filters::body::json::<T>())
}
