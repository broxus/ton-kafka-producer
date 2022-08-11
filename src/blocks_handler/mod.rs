use anyhow::Result;
use ton_indexer::utils::*;

use self::broxus_producer::*;
use self::gql_producer::*;
use crate::config::*;

mod broxus_producer;
mod gql_producer;
mod kafka_producer;

#[allow(clippy::large_enum_variant)]
pub enum BlocksHandler {
    Broxus(BroxusProducer),
    Gql(GqlProducer),
    None,
}

impl BlocksHandler {
    pub fn new(config: Option<KafkaConfig>) -> Result<Self> {
        match config {
            Some(KafkaConfig::Broxus(config)) => BroxusProducer::new(config).map(Self::Broxus),
            Some(KafkaConfig::Gql(config)) => GqlProducer::new(config).map(Self::Gql),
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
        block_data: Option<Vec<u8>>,
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
            Self::None => Ok(()),
        }
    }
}
