use anyhow::Result;

use self::broxus_producer::*;
use crate::config::*;

mod broxus_producer;
mod gql_producer;

pub enum BlocksHandler {
    Broxus(BroxusProducer),
}

impl BlocksHandler {
    pub fn new(config: KafkaConfig) -> Result<Self> {
        match config {
            KafkaConfig::Broxus {
                raw_transaction_producer,
            } => BroxusProducer::new(raw_transaction_producer).map(Self::Broxus),
            KafkaConfig::Gql(_) => todo!(),
        }
    }

    pub async fn handle_block(
        &self,
        block_id: &ton_block::BlockIdExt,
        block: &ton_block::Block,
        ignore_prepare_error: bool,
    ) -> Result<()> {
        match self {
            Self::Broxus(producer) => {
                producer
                    .handle_block(block_id, block, ignore_prepare_error)
                    .await
            }
        }
    }
}
