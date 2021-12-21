use std::collections::VecDeque;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use futures::future::Either;
use rdkafka::error::{KafkaError, RDKafkaErrorCode};
use rdkafka::producer::{DeliveryFuture, FutureProducer, FutureRecord};
use tiny_adnl::utils::*;
use tokio::sync::Mutex;
use ton_types::UInt256;

use crate::config::*;

pub struct KafkaProducer {
    config: KafkaProducerConfig,
    batch_flush_threshold: Duration,
    producer: FutureProducer,
    batches: FxHashMap<i32, Arc<Batch>>,
}

impl KafkaProducer {
    pub fn new(config: KafkaProducerConfig) -> Result<Self> {
        let mut client_config = rdkafka::config::ClientConfig::new();
        client_config.set("bootstrap.servers", &config.brokers);

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

        let batch_flush_threshold = Duration::from_millis(config.batch_flush_threshold_ms);

        Ok(Self {
            config,
            batch_flush_threshold,
            producer,
            batches: (0..=8)
                .map(|partition| (partition, Default::default()))
                .collect(),
        })
    }

    pub async fn write(
        &self,
        partition: i32,
        key: UInt256,
        value: Vec<u8>,
        timestamp: Option<i64>,
    ) -> Result<()> {
        let batch = self
            .batches
            .get(&partition)
            .context("Partition not found")?
            .clone();

        let mut records = batch.records.lock().await;

        // Check if batch is big enough to check
        if records.len() > self.config.batch_flush_threshold_size {
            let now = Instant::now();

            let mut batch_to_retry: Option<Vec<([u8; 32], Vec<u8>)>> = None;

            // Check pending records
            while let Some(item) = records.front() {
                // Break if successfully reached recent records
                if now.saturating_duration_since(item.created_at) < self.batch_flush_threshold {
                    break;
                }

                // Pop the oldest item
                let item = match records.pop_front() {
                    Some(item) => item,
                    None => break,
                };
                let key = item.key;

                // Check if it was delivered
                if let Err((e, _)) = item.delivery_future.await.with_context(|| {
                    format!("Delivery future cancelled for tx {}", hex::encode(key))
                })? {
                    log::error!(
                        "Batch item delivery error tx {}: {:?}. Retrying full batch",
                        hex::encode(item.key),
                        e
                    );
                } else {
                    // Continue to next pending record on successful delivery
                    continue;
                }

                // Create batch to retry
                batch_to_retry = Some(
                    futures::future::join_all(
                        // Include first failed item
                        std::iter::once(Either::Left(futures::future::ready((
                            item.key, item.value,
                        ))))
                        .chain(
                            // Wait all subsequent records and add them despite result
                            std::mem::take(&mut *records).into_iter().map(|item| {
                                Either::Right(async move {
                                    item.delivery_future.await.ok();
                                    (item.key, item.value)
                                })
                            }),
                        ),
                    )
                    .await,
                );
            }

            // Write batch
            if let Some(batch_to_retry) = batch_to_retry {
                log::error!(
                    "FOUND BATCH TO RETRY: {} items in partition {}",
                    batch_to_retry.len(),
                    partition
                );

                let batch_len = batch_to_retry.len();

                // Send all items sequentially
                for (key, mut value) in batch_to_retry {
                    // Repeat as many times
                    loop {
                        let now = chrono::Utc::now().timestamp();

                        // Send single record
                        let record = self.send_record(partition, key, value, Some(now)).await;

                        // Wait until it is delivered
                        match record.delivery_future.await.with_context(|| {
                            format!("Delivery future cancelled for tx {}", hex::encode(key))
                        })? {
                            // Move to the next item on successful delivery
                            Ok(_) => break,
                            // Log error and retry on failure
                            Err((e, _)) => log::error!(
                                "Batch item delivery error tx {}: {:?}. Retrying full batch",
                                hex::encode(key),
                                e
                            ),
                        }

                        // Update value
                        value = record.value;
                    }
                }

                // Done
                log::info!("Retried batch of {} elements", batch_len);
            }
        }

        // Append record to the batch
        records.push_back(
            self.send_record(partition, *key.as_slice(), value, timestamp)
                .await,
        );

        Ok(())
    }

    async fn send_record(
        &self,
        partition: i32,
        key: [u8; 32],
        value: Vec<u8>,
        timestamp: Option<i64>,
    ) -> PendingRecord {
        const HEADER_NAME: &str = "raw_block_timestamp";

        let header_value = timestamp.unwrap_or_default().to_be_bytes();
        let headers = rdkafka::message::OwnedHeaders::new().add(HEADER_NAME, &header_value);

        let interval = Duration::from_millis(self.config.attempt_interval_ms);

        let mut record = FutureRecord::to(&self.config.topic)
            .partition(partition)
            .key(&key)
            .payload(&value)
            .headers(headers.clone());

        loop {
            match self.producer.send_result(record) {
                Ok(delivery_future) => {
                    break PendingRecord {
                        key,
                        value,
                        created_at: Instant::now(),
                        delivery_future,
                    }
                }
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

#[derive(Default)]
struct Batch {
    records: Mutex<VecDeque<PendingRecord>>,
}

struct PendingRecord {
    key: [u8; 32],
    value: Vec<u8>,
    created_at: Instant,
    delivery_future: DeliveryFuture,
}
