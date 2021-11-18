use std::convert::{Infallible, TryInto};
use std::net::SocketAddr;
use std::str::FromStr;
use std::sync::Arc;

use anyhow::{Context, Result};
use nekoton::transport::models::ExistingContract;
use nekoton_indexer_utils::ExistingContractExt;
use nekoton_utils::TrustMe;
use serde::{Deserialize, Serialize};
use ton_block::HashmapAugType;
use ton_types::UInt256;
use warp::http::StatusCode;
use warp::{reply, Filter, Reply};

use crate::engine::shard_accounts_subscriber::*;

pub async fn serve(
    engine: Arc<ton_indexer::Engine>,
    subsriber: Arc<ShardAccountsSubscriber>,
    addr: SocketAddr,
) {
    let state = warp::any().map(move || (engine.clone(), subsriber.clone()));

    let routes = warp::any().and(
        warp::path!("account")
            .and(state.clone())
            .and(warp::post())
            .and(json_data())
            .and_then(state_receiver),
    );

    warp::serve(routes).bind(addr).await;
}

#[derive(Serialize, Deserialize)]
struct StateReceiveRequest {
    account_id: String,
    #[serde(default)]
    block_id: Option<String>,
}

async fn state_receiver(
    (engine, subscriber): (Arc<ton_indexer::Engine>, Arc<ShardAccountsSubscriber>),
    data: StateReceiveRequest,
) -> Result<Box<dyn Reply>, Infallible> {
    fn inner(data: StateReceiveRequest) -> Result<(UInt256, Option<ton_block::BlockIdExt>)> {
        let id = hex::decode(&data.account_id).context("Bad data for id:")?;
        anyhow::ensure!(
            id.len() == 32,
            "expected account id length 32. Got: {}",
            id.len()
        );
        let bytes: [u8; 32] = id.try_into().unwrap();

        Ok((
            UInt256::with_array(bytes),
            data.block_id
                .map(|id| ton_block::BlockIdExt::from_str(&id))
                .transpose()
                .context("Invalid block id")?,
        ))
    }

    log::info!("Got {} request", data.account_id);
    let (account_id, block_id) = match inner(data) {
        Ok(a) => a,
        Err(e) => {
            return Ok(Box::new(reply::with_status(
                e.to_string(),
                StatusCode::BAD_REQUEST,
            )))
        }
    };

    let state = match match block_id {
        Some(block_id) => async {
            let state = engine.load_state(&block_id).await?;
            let accounts = state
                .state()
                .read_accounts()
                .context("Failed to read accounts")?;
            let account = accounts.get(&account_id).context("Failed to get account")?;
            ExistingContract::from_shard_account_opt(&account)
        }
        .await
        .with_context(|| format!("Failed to get account state for block {:?}", block_id)),
        None => subscriber.get_contract_state(&account_id),
    } {
        Ok(a) => a.map(|x| serde_json::to_value(x).trust_me()),
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

        Some(a) => Ok(Box::new(warp::reply::json(&a))),
    }
}

fn json_data<T>() -> impl Filter<Extract = (T,), Error = warp::Rejection> + Clone
where
    for<'a> T: serde::Deserialize<'a> + Send,
{
    warp::body::content_length_limit(1024).and(warp::filters::body::json::<T>())
}
