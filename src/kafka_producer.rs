use std::time::Duration;

use anyhow::Result;
use rdkafka::error::{KafkaError, RDKafkaErrorCode};
use ton_types::UInt256;

use crate::config::*;

pub struct KafkaProducer {
    config: KafkaProducerConfig,
    producer: rdkafka::producer::FutureProducer,
}

impl KafkaProducer {
    pub fn new(config: KafkaProducerConfig) -> Result<Self> {
        let mut client_config = rdkafka::config::ClientConfig::new();
        client_config.set("bootstrap.servers", &config.brokers);
        client_config.set("batch.size", (128 * 1024 * 1024).to_string());
        client_config.set("linger.ms", 2000.to_string());
        client_config.set("acks", 1.to_string());

        if let Some(message_timeout_ms) = config.message_timeout_ms {
            client_config.set("message.timeout.ms", message_timeout_ms.to_string());
        }
        if let Some(message_max_size) = config.message_max_size {
            client_config.set("message.max.bytes", message_max_size.to_string());
        }

        if let Some(SecurityConfig::Sasl(sasl)) = &config.security_config {
            client_config
                .set("security.protocol", &sasl.security_protocol)
                .set("ssl.ca.location", &sasl.ssl_ca_location)
                .set("sasl.mechanism", &sasl.sasl_mechanism)
                .set("sasl.username", &sasl.sasl_username)
                .set("sasl.password", &sasl.sasl_password);
        }

        let producer = client_config.create()?;

        Ok(Self { config, producer })
    }

    pub async fn write(
        &self,
        partition: i32,
        key: UInt256,
        value: Vec<u8>,
        timestamp: Option<i64>,
    ) -> rdkafka::producer::DeliveryFuture {
        const HEADER_NAME: &str = "raw_block_timestamp";

        let header_value = timestamp.unwrap_or_default().to_be_bytes();
        let headers = rdkafka::message::OwnedHeaders::new().add(HEADER_NAME, &header_value);

        let interval = Duration::from_millis(self.config.attempt_interval_ms);

        let mut record = rdkafka::producer::FutureRecord::to(&self.config.topic)
            .partition(partition)
            .key(key.as_slice())
            .payload(&value)
            .headers(headers.clone());

        loop {
            match self.producer.send_result(record) {
                Ok(fut) => break fut,
                Err((e, sent_record))
                    if e == KafkaError::MessageProduction(RDKafkaErrorCode::QueueFull) =>
                {
                    record = sent_record;
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
                Err((e, sent_record)) => {
                    record = sent_record;
                    log::warn!(
                        "Failed to send message to kafka topic {}: {:?}",
                        self.config.topic,
                        e
                    );
                    tokio::time::sleep(interval).await;
                }
            };
        }
    }
}
