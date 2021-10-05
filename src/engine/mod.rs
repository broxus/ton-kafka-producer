use std::sync::Arc;

use anyhow::{Context, Result};
use ton_indexer::utils::*;

use self::kafka_producer::*;
use crate::config::*;

mod kafka_producer;

pub struct Engine {
    indexer: Arc<ton_indexer::Engine>,
}

impl Engine {
    pub async fn new(
        config: AppConfig,
        global_config: ton_indexer::GlobalConfig,
    ) -> Result<Arc<Self>> {
        let subscriber: Arc<dyn ton_indexer::Subscriber> =
            TonSubscriber::new(config.kafka_settings)?;

        let indexer = ton_indexer::Engine::new(
            config
                .node_settings
                .build_indexer_config()
                .await
                .context("Failed to build node config")?,
            global_config,
            vec![subscriber],
        )
        .await
        .context("Failed to start TON node")?;

        Ok(Arc::new(Self { indexer }))
    }

    pub async fn start(self: &Arc<Self>) -> Result<()> {
        self.indexer.start().await?;
        Ok(())
    }
}

struct TonSubscriber {
    block_producer: Option<KafkaProducer>,
    raw_block_producer: Option<KafkaProducer>,
    message_producer: Option<KafkaProducer>,
    transaction_producer: Option<KafkaProducer>,
    account_producer: Option<KafkaProducer>,
    block_proof_producer: Option<KafkaProducer>,
}

impl TonSubscriber {
    fn new(config: KafkaConfig) -> Result<Arc<Self>> {
        fn make_producer(config: Option<KafkaProducerConfig>) -> Result<Option<KafkaProducer>> {
            config.map(KafkaProducer::new).transpose()
        }

        Ok(Arc::new(Self {
            block_producer: make_producer(config.block_producer)?,
            raw_block_producer: make_producer(config.raw_block_producer)?,
            message_producer: make_producer(config.message_producer)?,
            transaction_producer: make_producer(config.transaction_producer)?,
            account_producer: make_producer(config.account_producer)?,
            block_proof_producer: make_producer(config.block_proof_producer)?,
        }))
    }
}

impl TonSubscriber {
    async fn handle_block(
        &self,
        _block: &BlockStuff,
        _block_proof: Option<&BlockProofStuff>,
        _shard_state: Option<&ShardStateStuff>,
    ) -> Result<()> {
        // TODO: handle block
        Ok(())
    }
}

#[async_trait::async_trait]
impl ton_indexer::Subscriber for TonSubscriber {
    async fn process_block(
        &self,
        block: &BlockStuff,
        block_proof: Option<&BlockProofStuff>,
        shard_state: &ShardStateStuff,
    ) -> Result<()> {
        self.handle_block(block, block_proof, Some(shard_state))
            .await
    }

    async fn process_archive_block(
        &self,
        block: &BlockStuff,
        block_proof: Option<&BlockProofStuff>,
    ) -> Result<()> {
        self.handle_block(block, block_proof, None).await
    }
}
