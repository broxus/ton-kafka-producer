use std::convert::{Infallible, TryInto};
use std::net::SocketAddr;
use std::sync::Arc;

use anyhow::{Context, Result};
use nekoton_utils::TrustMe;
use serde::{Deserialize, Serialize};
use ton_types::UInt256;
use warp::http::StatusCode;
use warp::{reply, Filter, Reply};

use crate::engine::shard_accounts_subscriber::*;

pub async fn serve(subsriber: Arc<ShardAccountsSubscriber>, addr: SocketAddr) {
    let state = warp::any().map(move || subsriber.clone());
    let routes = warp::path::path("account")
        .and(state.clone())
        .and(warp::post())
        .and(json_data())
        .and_then(state_receiver);
    warp::serve(routes).bind(addr).await;
}

#[derive(Serialize, Deserialize)]
struct StateReceiveRequest {
    account_id: String,
}

async fn state_receiver(
    ctx: Arc<ShardAccountsSubscriber>,
    data: StateReceiveRequest,
) -> Result<Box<dyn Reply>, Infallible> {
    fn inner(data: StateReceiveRequest) -> Result<UInt256> {
        let id = hex::decode(&data.account_id).context("Bad data for id:")?;
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
        Err(e) => {
            return Ok(Box::new(reply::with_status(
                e.to_string(),
                StatusCode::BAD_REQUEST,
            )))
        }
    };
    let state = match ctx.get_contract_state(&account_id) {
        Ok(a) => a.map(|x| bincode::serialize(&x).trust_me()),
        Err(e) => {
            return Ok(Box::new(reply::with_status(
                e.context("Failed getting shard account:").to_string(),
                StatusCode::INTERNAL_SERVER_ERROR,
            )));
        }
    };
    match state {
        None => Ok(Box::new(reply::with_status(
            "No state found".to_string(),
            StatusCode::NO_CONTENT,
        ))),
        Some(a) => Ok(Box::new(a)),
    }
}

fn json_data<T>() -> impl Filter<Extract = (T,), Error = warp::Rejection> + Clone
where
    for<'a> T: serde::Deserialize<'a> + Send,
{
    warp::body::content_length_limit(1024).and(warp::filters::body::json::<T>())
}
