pub use crate::blocks_handler::kafka_producer::{KafkaProducer, Partitions};

use anyhow::Result;
use bytes::Bytes;
use ton_indexer::utils::*;
use tx_tree_producer::TxTreeProducer;

use self::broxus_producer::*;
use self::gql_producer::*;
use crate::config::*;
use crate::output_handlers::prepare_handlers;

mod broxus_producer;
mod gql_producer;
mod kafka_producer;
mod tx_tree_producer;

#[allow(clippy::large_enum_variant)]
pub enum BlocksHandler {
    Broxus(BroxusProducer),
    Gql(GqlProducer),
    Tree(TxTreeProducer),
    None,
}

impl BlocksHandler {
    pub fn new(config: Option<ProducerConfig>) -> Result<Self> {
        match config {
            Some(ProducerConfig::DefaultKafka(config)) => {
                BroxusProducer::new(config).map(Self::Broxus)
            }
            Some(ProducerConfig::GqlKafka(config)) => GqlProducer::new(config).map(Self::Gql),
            Some(ProducerConfig::Tree(config)) => {
                let handlers = prepare_handlers(config.handlers)?;
                let producer = TxTreeProducer::new(config.storage_path, handlers)?;
                Ok(Self::Tree(producer))
            }
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
            Self::Tree(producer) => producer.handle_block(block_stuff.block()).await,
            Self::None => Ok(()),
        }
    }
}
