use std::sync::Arc;

use anyhow::{Context, Result};
use futures::StreamExt;
use rdkafka::consumer::stream_consumer::StreamConsumer;
use rdkafka::consumer::{CommitMode, Consumer};
use rdkafka::Message;
use ton_block::Deserializable;

use crate::config::*;

pub struct MessageConsumer {
    engine: Arc<ton_indexer::Engine>,
    consumer: Arc<StreamConsumer>,
}

impl MessageConsumer {
    pub fn new(engine: Arc<ton_indexer::Engine>, config: KafkaConsumerConfig) -> Result<Self> {
        let mut client_config = rdkafka::config::ClientConfig::new();
        client_config
            .set("group.id", &config.group_id)
            .set("bootstrap.servers", &config.brokers)
            .set("enable.partition.eof", "false")
            .set("session.timeout.ms", &config.session_timeout_ms.to_string())
            .set("enable.auto.commit", "false");

        if let Some(SecurityConfig::Sasl(sasl)) = &config.security_config {
            client_config
                .set("security.protocol", &sasl.security_protocol)
                .set("ssl.ca.location", &sasl.ssl_ca_location)
                .set("sasl.mechanism", &sasl.sasl_mechanism)
                .set("sasl.username", &sasl.sasl_username)
                .set("sasl.password", &sasl.sasl_password);
        }

        let consumer: StreamConsumer = client_config
            .create()
            .context("Failed to create consumer")?;

        log::info!("Subscribing to topic: {}", config.topic);
        consumer
            .subscribe(&[&config.topic])
            .context("Failed to subscribe to the topic")?;

        Ok(Self {
            engine,
            consumer: Arc::new(consumer),
        })
    }

    pub fn start(&self) {
        let engine = self.engine.clone();
        let consumer = self.consumer.clone();

        tokio::spawn(async move {
            let mut stream = consumer.stream();

            while let Some(message) = stream.next().await {
                let message = match message {
                    Ok(message) => message,
                    Err(e) => {
                        log::error!("Kafka error: {:?}", e);
                        continue;
                    }
                };

                if let Some(payload) = message.payload() {
                    if let Err(e) = send_external_message(&engine, payload).await {
                        log::error!("Failed to broadcast external message: {:?}", e);
                    }
                } else {
                    log::warn!(
                        "Record with empty payload (topic: {}, partition: {}, offset: {})",
                        message.topic(),
                        message.partition(),
                        message.offset()
                    );
                }

                if let Err(e) = consumer.commit_message(&message, CommitMode::Async) {
                    log::error!(
                        "Failed to commit message (topic: {}, partition: {}, offset: {}): {:?}",
                        message.topic(),
                        message.partition(),
                        message.offset(),
                        e
                    );
                }
            }
        });
    }
}

async fn send_external_message(
    engine: &ton_indexer::Engine,
    data: &[u8],
) -> Result<(), MessageBroadcastError> {
    if data.len() > MAX_EXTERNAL_MESSAGE_SIZE {
        return Err(MessageBroadcastError::TooLarge(data.len()));
    }

    let root = ton_types::deserialize_tree_of_cells(&mut std::io::Cursor::new(data))
        .map_err(MessageBroadcastError::InvalidBoc)?;

    if root.level() != 0 {
        return Err(MessageBroadcastError::InvalidLevel(root.level()));
    }
    if root.repr_depth() >= MAX_EXTERNAL_MESSAGE_DEPTH {
        return Err(MessageBroadcastError::TooDeep(root.repr_depth()));
    }

    let message = ton_block::Message::construct_from(&mut root.clone().into())
        .map_err(MessageBroadcastError::InvalidMessage)?;

    let to = match message.header() {
        ton_block::CommonMsgInfo::ExtInMsgInfo(header) if header.dst.rewrite_pfx().is_none() => {
            ton_block::AccountIdPrefixFull::prefix(&header.dst)
                .map_err(MessageBroadcastError::InvalidAccountPrefix)?
        }
        _ => return Err(MessageBroadcastError::InvalidHeader),
    };

    engine
        .broadcast_external_message(&to, data)
        .await
        .map_err(MessageBroadcastError::OverlayBroadcastFailed)
}

#[derive(Debug, thiserror::Error)]
enum MessageBroadcastError {
    #[error("External message is too large ({0} bytes)")]
    TooLarge(usize),
    #[error("Invalid BOC")]
    InvalidBoc(#[source] anyhow::Error),
    #[error("External message must have zero level (message level: {0})")]
    InvalidLevel(u8),
    #[error("External message {:x} is too deep (depth: {0})")]
    TooDeep(u16),
    #[error("Invalid message")]
    InvalidMessage(#[source] anyhow::Error),
    #[error("Invalid header")]
    InvalidHeader,
    #[error("Invalid account prefix")]
    InvalidAccountPrefix(#[source] anyhow::Error),
    #[error("Overlay broadcast failed")]
    OverlayBroadcastFailed(#[source] anyhow::Error),
}

const MAX_EXTERNAL_MESSAGE_SIZE: usize = 65535;
const MAX_EXTERNAL_MESSAGE_DEPTH: u16 = 512;
