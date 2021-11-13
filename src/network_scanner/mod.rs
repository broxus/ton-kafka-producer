use std::sync::Arc;

use anyhow::{anyhow, Context, Result};
use ton_indexer::utils::*;
use ton_indexer::BriefBlockMeta;

use crate::config::*;

use self::shard_accounts_subscriber::*;
use crate::blocks_handler::*;

pub mod shard_accounts_subscriber;

pub struct NetworkScanner {
    indexer: Arc<ton_indexer::Engine>,
}

impl NetworkScanner {
    pub async fn new(
        kafka_settings: KafkaConfig,
        node_settings: NodeConfig,
        global_config: ton_indexer::GlobalConfig,
        shard_accounts_subscriber: Arc<ShardAccountsSubscriber>,
    ) -> Result<Arc<Self>> {
        let subscriber: Arc<dyn ton_indexer::Subscriber> =
            TonSubscriber::new(kafka_settings, shard_accounts_subscriber)?;

        let indexer = ton_indexer::Engine::new(
            node_settings
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
    handler: BlocksHandler,
    shard_accounts_subscriber: Arc<ShardAccountsSubscriber>,
}

impl TonSubscriber {
    fn new(
        config: KafkaConfig,
        shard_accounts_subscriber: Arc<ShardAccountsSubscriber>,
    ) -> Result<Arc<Self>> {
        Ok(Arc::new(Self {
            handler: BlocksHandler::new(config)?,
            shard_accounts_subscriber,
        }))
    }
}

impl TonSubscriber {
    async fn handle_block(
        &self,
        block_stuff: &BlockStuff,
        shard_state: Option<&ShardStateStuff>,
    ) -> Result<()> {
        self.shard_accounts_subscriber
            .handle_block(block_stuff, shard_state)
            .await
            .context("Failed to update shard accounts subscriber")?;

        futures::future::join_all(
            self.handler
                .handle_block(block_stuff.id(), block_stuff.block(), true)
                .await
                .context("Failed to handle block")?,
        )
        .await
        .into_iter()
        .map(|item| item.map(|_| ()))
        .find(|r| r.is_err())
        .unwrap_or(Ok(()))
        .map_err(|e| anyhow!("Failed to send message to kafka: {}", e))
    }
}

#[async_trait::async_trait]
impl ton_indexer::Subscriber for TonSubscriber {
    async fn process_block(
        &self,
        _: BriefBlockMeta,
        block: &BlockStuff,
        _: Option<&BlockProofStuff>,
        shard_state: &ShardStateStuff,
    ) -> Result<()> {
        self.handle_block(block, Some(shard_state)).await
    }

    async fn process_archive_block(
        &self,
        _: BriefBlockMeta,
        block: &BlockStuff,
        _: Option<&BlockProofStuff>,
    ) -> Result<()> {
        self.handle_block(block, None).await
    }
}
