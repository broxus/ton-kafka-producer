use std::collections::VecDeque;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use bytes::Bytes;
use futures_util::future::Either;
use rdkafka::error::{KafkaError, RDKafkaErrorCode};
use rdkafka::message::Header;
use rdkafka::producer::{DeliveryFuture, FutureProducer, FutureRecord, Producer};
use tokio::sync::Mutex;
use ton_types::FxDashMap;

use crate::config::*;

pub struct KafkaProducer {
    config: KafkaProducerConfig,
    batch_flush_threshold: Duration,
    producer: FutureProducer,
    batches: FxDashMap<i32, Arc<Batch>>,
    fixed_partitions: bool,
}

pub enum Partitions<T> {
    Fixed(T),
    Any,
}

impl Partitions<std::iter::Empty<i32>> {
    pub fn any() -> Self {
        Self::Any
    }
}

impl KafkaProducer {
    pub fn new(
        config: KafkaProducerConfig,
        partitions: Partitions<impl Iterator<Item = i32>>,
    ) -> Result<Self> {
        let mut client_config = rdkafka::config::ClientConfig::new();
        client_config.set("bootstrap.servers", &config.brokers);

        if let Some(message_timeout_ms) = config.message_timeout_ms {
            client_config.set("message.timeout.ms", message_timeout_ms.to_string());
        }
        if let Some(message_max_size) = config.message_max_size {
            client_config.set("message.max.bytes", message_max_size.to_string());
        }

        #[cfg(any(feature = "sasl", feature = "ssl"))]
        match &config.security_config {
            #[cfg(feature = "sasl")]
            Some(SecurityConfig::Sasl(config)) => {
                client_config
                    .set("security.protocol", &config.security_protocol)
                    .set("ssl.ca.location", &config.ssl_ca_location)
                    .set("sasl.mechanism", &config.sasl_mechanism)
                    .set("sasl.username", &config.sasl_username)
                    .set("sasl.password", &config.sasl_password);

                if let Some(ssl_keystore_location) = config.ssl_keystore_location.clone() {
                    client_config.set("ssl.keystore.location", ssl_keystore_location);
                }
                if let Some(ssl_keystore_password) = config.ssl_keystore_password.clone() {
                    client_config.set("ssl.keystore.password", ssl_keystore_password);
                }
            }
            #[cfg(feature = "ssl")]
            Some(SecurityConfig::Ssl(config)) => {
                client_config
                    .set("security.protocol", &config.security_protocol)
                    .set("ssl.ca.location", &config.ssl_ca_location)
                    .set("ssl.key.location", &config.ssl_key_location)
                    .set("ssl.certificate.location", &config.ssl_certificate_location);

                if let Some(enable_verification) = config.enable_ssl_certificate_verification {
                    client_config.set(
                        "enable.ssl.certificate.verification",
                        if enable_verification { "true" } else { "false" },
                    );
                }
            }
            None => {}
        }

        let producer = client_config.create()?;

        let batch_flush_threshold = Duration::from_millis(config.batch_flush_threshold_ms);

        let (batches, fixed_partitions) = match partitions {
            Partitions::Fixed(partitions) => (
                partitions
                    .map(|partition| (partition, Default::default()))
                    .collect(),
                true,
            ),
            Partitions::Any => (Default::default(), false),
        };

        Ok(Self {
            config,
            batch_flush_threshold,
            producer,
            batches,
            fixed_partitions,
        })
    }

    pub async fn write(
        &self,
        partition: i32,
        key: Bytes,
        value: Bytes,
        timestamp: Option<i64>,
    ) -> Result<()> {
        let batch = if self.fixed_partitions {
            self.batches
                .get(&partition)
                .context("Partition not found")?
                .clone()
        } else {
            self.batches.entry(partition).or_default().clone()
        };

        let mut records = batch.records.lock().await;

        // Check if batch is big enough to check
        if records.len() > self.config.batch_flush_threshold_size {
            let now = Instant::now();

            let mut batch_to_retry: Option<Vec<(Bytes, Bytes)>> = None;

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

                // Check if it was delivered
                if let Err((e, _)) = item.delivery_future.await.with_context(|| {
                    format!(
                        "Delivery future cancelled for tx {}",
                        hex::encode(&item.key)
                    )
                })? {
                    tracing::error!(
                        key = hex::encode(&item.key),
                        "batch item delivery error: {e:?}, retrying full batch",
                    );
                } else {
                    // Continue to next pending record on successful delivery
                    continue;
                }

                // Create batch to retry
                batch_to_retry = Some(
                    futures_util::future::join_all(
                        // Include first failed item
                        std::iter::once(Either::Left(futures_util::future::ready((
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
                tracing::error!(
                    items = batch_to_retry.len(),
                    partition,
                    "found batch to retry"
                );

                let batch_len = batch_to_retry.len();

                // Send all items sequentially
                for (mut key, mut value) in batch_to_retry {
                    // Repeat as many times
                    loop {
                        let now = chrono::Utc::now().timestamp();

                        // Send single record
                        let record = self.send_record(partition, key, value, Some(now)).await;

                        // Wait until it is delivered
                        match record.delivery_future.await.with_context(|| {
                            format!(
                                "Delivery future cancelled for tx {}",
                                hex::encode(&record.key)
                            )
                        })? {
                            // Move to the next item on successful delivery
                            Ok(_) => break,
                            // Log error and retry on failure
                            Err((e, _)) => tracing::error!(
                                key = hex::encode(&record.key),
                                "batch item delivery error {e:?}, retrying full batch",
                            ),
                        }

                        // Update key and value
                        key = record.key;
                        value = record.value;
                    }
                }

                // Done
                tracing::info!(len = batch_len, "retried batch");
            }
        }

        // Append record to the batch
        records.push_back(self.send_record(partition, key, value, timestamp).await);

        Ok(())
    }

    async fn send_record(
        &self,
        partition: i32,
        key: Bytes,
        value: Bytes,
        timestamp: Option<i64>,
    ) -> PendingRecord {
        const HEADER_NAME: &str = "raw_block_timestamp";

        let header_value = timestamp.unwrap_or_default().to_be_bytes();
        let headers = rdkafka::message::OwnedHeaders::new().insert(Header {
            key: HEADER_NAME,
            value: Some(&header_value),
        });

        let interval = Duration::from_millis(self.config.attempt_interval_ms);

        let mut record = FutureRecord::to(&self.config.topic)
            .partition(partition)
            .key(key.as_ref())
            .payload(value.as_ref())
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
                Err((KafkaError::MessageProduction(RDKafkaErrorCode::QueueFull), sent_record)) => {
                    record = sent_record;
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
                Err((e, sent_record)) => {
                    record = sent_record;
                    tracing::warn!(topic = &self.config.topic, "failed to send message: {e:?}",);
                    tokio::time::sleep(interval).await;
                }
            };
        }
    }
}

impl Drop for KafkaProducer {
    fn drop(&mut self) {
        tracing::warn!("flushing kafka producer");
        self.producer.flush(None).unwrap();
    }
}

#[derive(Default)]
struct Batch {
    records: Mutex<VecDeque<PendingRecord>>,
}

struct PendingRecord {
    key: Bytes,
    value: Bytes,
    created_at: Instant,
    delivery_future: DeliveryFuture,
}
