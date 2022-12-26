use crate::blocks_handler::tx_tree_producer::TxTreeProducer;
use anyhow::{anyhow, Result};
use bytes::Bytes;
use std::path::Path;
use ton_indexer::utils::*;

use self::broxus_producer::*;
use self::gql_producer::*;
use crate::config::*;

mod broxus_producer;
mod gql_producer;
mod kafka_producer;
mod tx_tree_producer;

#[allow(clippy::large_enum_variant)]
pub enum BlocksHandler {
    Broxus(BroxusProducer),
    Gql(GqlProducer),
    TxTree(TxTreeProducer),

    None,
}

impl BlocksHandler {
    pub fn new(
        config: Option<KafkaConfig>,
        rocksdb_path: Option<&Path>,
        max_transaction_depth: Option<u32>,
    ) -> Result<Self> {
        match config {
            Some(KafkaConfig::Broxus(config)) => BroxusProducer::new(config).map(Self::Broxus),
            Some(KafkaConfig::Gql(config)) => GqlProducer::new(config).map(Self::Gql),
            Some(KafkaConfig::TxTree(config)) => match (rocksdb_path, max_transaction_depth) {
                (Some(path), Some(depth)) => {
                    TxTreeProducer::new(config.raw_transaction_producer, path, depth)
                        .map(Self::TxTree)
                }
                _ => Err(anyhow!("DB path or transaction depth is not specified")),
            },
            None => Ok(Self::None),
        }
    }

    pub async fn handle_state(&self, state: &ShardStateStuff) -> Result<()> {
        if let Self::Gql(gql) = self {
            gql.handle_state(state).await
        } else {
            Ok(())
        }
    }

    pub async fn handle_block(
        &self,
        block_stuff: &BlockStuff,
        block_data: Option<Bytes>,
        block_proof: Option<&BlockProofStuff>,
        shard_state: Option<&ShardStateStuff>,
        ignore_prepare_error: bool,
        max_transaction_depth: u32,
    ) -> Result<()> {
        match self {
            Self::Broxus(producer) => {
                producer
                    .handle_block(block_stuff.id(), block_stuff.block(), ignore_prepare_error)
                    .await
            }
            Self::Gql(producer) => {
                producer
                    .handle_block(block_stuff, block_data, block_proof, shard_state)
                    .await
            }
            Self::TxTree(producer) => {
                producer
                    .handle_block(block_stuff.id(), block_stuff.block(), max_transaction_depth)
                    .await
            }
            Self::None => Ok(()),
        }
    }
}
