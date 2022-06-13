use anyhow::{Context, Result};
use ton_block::{Deserializable, HashmapAugType};
use ton_block_compressor::ZstdWrapper;
use ton_types::{HashmapType, UInt256};

use super::kafka_producer::*;
use crate::config::*;

pub struct BroxusProducer {
    compressor: ton_block_compressor::ZstdWrapper,
    raw_transaction_producer: KafkaProducer,
}

impl BroxusProducer {
    pub fn new(config: KafkaProducerConfig) -> Result<Self> {
        Ok(Self {
            compressor: Default::default(),
            raw_transaction_producer: KafkaProducer::new(config, Partitions::Fixed(0..=8))?,
        })
    }

    pub async fn handle_block(
        &self,
        block_id: &ton_block::BlockIdExt,
        block: &ton_block::Block,
        ignore_prepare_error: bool,
    ) -> Result<()> {
        let records = match self.prepare_records(block_id, block) {
            Ok(records) if records.is_empty() => return Ok(()),
            Ok(records) => records,
            Err(e) if ignore_prepare_error => {
                log::error!("Failed to process block {block_id}: {e:?}");
                return Ok(());
            }
            Err(e) => return Err(e).context("Failed to prepare records"),
        };

        let mut futures = Vec::with_capacity(records.len());

        let now = chrono::Utc::now().timestamp();
        for record in records {
            futures.push(self.raw_transaction_producer.write(
                record.partition,
                record.key.into_vec(),
                record.value,
                Some(now),
            ));
        }

        futures_util::future::join_all(futures)
            .await
            .into_iter()
            .collect::<Result<Vec<_>>>()?;

        Ok(())
    }

    fn prepare_records(
        &self,
        block_id: &ton_block::BlockIdExt,
        block: &ton_block::Block,
    ) -> Result<Vec<TransactionRecord>> {
        let block_extra = block.read_extra()?;

        let mut records = Vec::new();

        block_extra
            .read_account_blocks()?
            .iterate_objects(|account_block| {
                let partition = if block_id.shard_id.is_masterchain() {
                    0
                } else {
                    1 + account_block.account_id().get_bits(0, 3)? as i32
                };

                account_block
                    .transactions()
                    .iterate_slices(|_, raw_transaction| {
                        records.extend(prepare_raw_transaction_record(
                            partition,
                            &self.compressor,
                            raw_transaction,
                        )?);
                        Ok(true)
                    })?;
                Ok(true)
            })?;

        Ok(records)
    }
}

fn prepare_raw_transaction_record(
    partition: i32,
    compressor: &ZstdWrapper,
    raw_transaction: ton_types::SliceData,
) -> Result<Option<TransactionRecord>> {
    let cell = raw_transaction.reference(0)?;
    let boc = ton_types::serialize_toc(&cell)?;
    let transaction = ton_block::Transaction::construct_from(&mut cell.into())?;
    match transaction.description.read_struct()? {
        ton_block::TransactionDescr::Ordinary(_) => {}
        _ => return Ok(None),
    };

    let key = raw_transaction.hash(ton_types::MAX_LEVEL);
    let value = compressor.compress_owned(&boc)?;

    Ok(Some(TransactionRecord {
        partition,
        key,
        value,
    }))
}

struct TransactionRecord {
    partition: i32,
    key: UInt256,
    value: Vec<u8>,
}
