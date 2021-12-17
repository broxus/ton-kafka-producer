use std::convert::Infallible;
use std::net::SocketAddr;
use std::str::FromStr;
use std::sync::Arc;

use anyhow::Result;
use nekoton_utils::TrustMe;
use nekoton_utils::*;
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

#[derive(Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(untagged)]
enum StateReceiveRequest {
    Old {
        account_id: String,
    },
    New {
        #[serde(with = "serde_address")]
        address: ton_block::MsgAddressInt,
    },
}

impl TryFrom<StateReceiveRequest> for ton_block::MsgAddressInt {
    type Error = anyhow::Error;

    fn try_from(value: StateReceiveRequest) -> Result<Self, Self::Error> {
        match value {
            StateReceiveRequest::Old { account_id } => {
                if account_id.len() != 64 {
                    return Err(anyhow::Error::msg("Invalid account id"));
                }
                MsgAddressInt::from_str(&format!("0:{}", account_id))
            }
            StateReceiveRequest::New { address } => Ok(address),
        }
    }
}

struct State {
    subscriber: Arc<ShardAccountsSubscriber>,
    metrics: Arc<RpcMetrics>,
}

async fn state_receiver(
    ctx: Arc<State>,
    data: StateReceiveRequest,
) -> Result<Box<dyn Reply>, Infallible> {
    let address = match ton_block::MsgAddressInt::try_from(data) {
        Ok(address) => address,
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn state_receive_request_parse() {
        let target_address = ton_block::MsgAddressInt::default();

        assert_eq!(
            serde_json::from_str::<StateReceiveRequest>(
                "{\"account_id\":\"0000000000000000000000000000000000000000000000000000000000000000\"}",
            )
            .unwrap(),
            StateReceiveRequest::Old {
                account_id: hex::encode(target_address.address().get_bytestring(0))
            }
        );

        assert_eq!(
            serde_json::from_str::<StateReceiveRequest>(
                "{\"address\":\"0:0000000000000000000000000000000000000000000000000000000000000000\"}",
            )
            .unwrap(),
            StateReceiveRequest::New {
                address: target_address
            }
        );
    }
}
