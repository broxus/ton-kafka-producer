use std::sync::Arc;

use anyhow::{Context, Result};
use axum_jrpc::error::{JsonRpcError, JsonRpcErrorReason};
use ton_block::Serializable;
use ton_indexer::utils::*;
use ton_indexer::ProcessBlockContext;

use self::message_consumer::*;
use self::shard_accounts_subscriber::*;
use crate::blocks_handler::*;
use crate::config::*;

mod message_consumer;
pub mod shard_accounts_subscriber;

pub struct NetworkScanner {
    indexer: Arc<ton_indexer::Engine>,
    message_consumer: Option<MessageConsumer>,
}

impl NetworkScanner {
    pub async fn new(
        kafka_settings: Option<KafkaConfig>,
        node_settings: NodeConfig,
        global_config: ton_indexer::GlobalConfig,
        shard_accounts_subscriber: Arc<ShardAccountsSubscriber>,
    ) -> Result<Arc<Self>> {
        let requests_consumer_config = match &kafka_settings {
            Some(KafkaConfig::Gql(gql)) => gql.requests_consumer.clone(),
            _ => None,
        };

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
        .context("Failed to start node")?;

        let message_consumer = if let Some(config) = requests_consumer_config {
            Some(
                MessageConsumer::new(indexer.clone(), config)
                    .context("Failed to create message consumer")?,
            )
        } else {
            None
        };

        Ok(Arc::new(Self {
            indexer,
            message_consumer,
        }))
    }

    pub async fn start(self: &Arc<Self>) -> Result<()> {
        self.indexer.start().await?;
        if let Some(consumer) = &self.message_consumer {
            consumer.start();
        }
        Ok(())
    }

    pub fn indexer(&self) -> &ton_indexer::Engine {
        self.indexer.as_ref()
    }

    pub async fn send_message(&self, message: ton_block::Message) -> Result<(), QueryError> {
        let to = match message.header() {
            ton_block::CommonMsgInfo::ExtInMsgInfo(header) => header.dst.workchain_id(),
            _ => return Err(QueryError::ExternalTonMessageExpected),
        };

        let cells = message
            .write_to_new_cell()
            .map_err(|_| QueryError::FailedToSerialize)?
            .into();

        let serialized =
            ton_types::serialize_toc(&cells).map_err(|_| QueryError::FailedToSerialize)?;

        self.indexer
            .broadcast_external_message(to, &serialized)
            .map_err(|_| QueryError::ConnectionError)
    }
}

struct TonSubscriber {
    handler: BlocksHandler,
    shard_accounts_subscriber: Arc<ShardAccountsSubscriber>,
    extract_all: bool,
}

impl TonSubscriber {
    fn new(
        config: Option<KafkaConfig>,
        shard_accounts_subscriber: Arc<ShardAccountsSubscriber>,
    ) -> Result<Arc<Self>> {
        let extract_all = matches!(&config, Some(KafkaConfig::Gql(_)));

        Ok(Arc::new(Self {
            handler: BlocksHandler::new(config)?,
            shard_accounts_subscriber,
            extract_all,
        }))
    }
}

impl TonSubscriber {
    async fn handle_block(
        &self,
        block_stuff: &BlockStuff,
        block_data: Option<Vec<u8>>,
        block_proof: Option<&BlockProofStuff>,
        shard_state: Option<&ShardStateStuff>,
        is_key_block: bool,
    ) -> Result<()> {
        self.shard_accounts_subscriber
            .handle_block(block_stuff, shard_state, is_key_block)
            .await
            .context("Failed to update shard accounts subscriber")?;

        self.handler
            .handle_block(block_stuff, block_data, block_proof, shard_state, true)
            .await
            .context("Failed to handle block")
    }
}

#[async_trait::async_trait]
impl ton_indexer::Subscriber for TonSubscriber {
    async fn process_block(&self, ctx: ProcessBlockContext<'_>) -> Result<()> {
        let (block_data, block_proof) = if self.extract_all {
            let block_data = Some(ctx.load_block_data().await?);
            let block_proof = Some(ctx.load_block_proof().await?);
            (block_data, block_proof)
        } else {
            (None, None)
        };

        let is_key_block = ctx.meta().is_key_block();
        self.handle_block(
            ctx.block_stuff(),
            block_data,
            block_proof.as_ref(),
            ctx.shard_state_stuff(),
            is_key_block,
        )
        .await
    }

    async fn process_full_state(&self, state: &ShardStateStuff) -> Result<()> {
        self.handler.handle_state(state).await
    }
}

#[derive(thiserror::Error, Clone, Debug)]
pub enum QueryError {
    #[error("Connection error")]
    ConnectionError,
    #[error("Failed to serialize message")]
    FailedToSerialize,
    #[error("Invalid account state")]
    InvalidAccountState,
    #[error("Invalid block")]
    InvalidBlock,
    #[error("Unknown")]
    Unknown,
    #[error("Not ready")]
    NotReady,
    #[error("External message expected")]
    ExternalTonMessageExpected,
}

impl QueryError {
    pub fn code(&self) -> i64 {
        match self {
            QueryError::ConnectionError => -32001,
            QueryError::FailedToSerialize => -32002,
            QueryError::InvalidAccountState => -32004,
            QueryError::InvalidBlock => -32006,
            QueryError::NotReady => -32007,
            QueryError::Unknown => -32603,
            QueryError::ExternalTonMessageExpected => -32005,
        }
    }
}

impl From<QueryError> for JsonRpcError {
    fn from(error: QueryError) -> JsonRpcError {
        let code = error.code();
        let message = error.to_string();
        let reason = JsonRpcErrorReason::ServerError(code as i32);
        JsonRpcError::new(reason, message, serde_json::Value::Null)
    }
}
