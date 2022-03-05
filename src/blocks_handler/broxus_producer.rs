use anyhow::{Context, Result};
use ton_block::{Deserializable, HashmapAugType};
use ton_block_compressor::ZstdWrapper;
use ton_indexer::utils::BlockIdExtExtension;
use ton_types::{HashmapType, UInt256};

use crate::config::*;
use crate::kafka_producer::*;

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
        let records = match self.prepare_records(block) {
            Ok(records) if records.is_empty() => return Ok(()),
            Ok(records) => records,
            Err(e) if ignore_prepare_error => {
                log::error!("Failed to process block {}: {:?}", block_id, e);
                return Ok(());
            }
            Err(e) => return Err(e).context("Failed to prepare records"),
        };

        let partition = compute_partition(block_id);

        let mut futures = Vec::with_capacity(records.len());

        let now = chrono::Utc::now().timestamp();
        for TransactionRecord { key, value } in records {
            futures.push(self.raw_transaction_producer.write(
                partition,
                key.into_vec(),
                value,
                Some(now),
            ));
        }

        futures::future::join_all(futures)
            .await
            .into_iter()
            .collect::<Result<Vec<_>>>()?;

        Ok(())
    }

    fn prepare_records(&self, block: &ton_block::Block) -> Result<Vec<TransactionRecord>> {
        let block_extra = block.read_extra()?;

        let mut records = Vec::new();

        block_extra
            .read_account_blocks()?
            .iterate_objects(|account_block| {
                account_block
                    .transactions()
                    .iterate_slices(|_, raw_transaction| {
                        let tx = prepare_raw_transaction_record(&self.compressor, raw_transaction)?;
                        if let Some(rec) = tx {
                            records.push(rec);
                        }
                        Ok(true)
                    })?;
                Ok(true)
            })?;

        Ok(records)
    }
}

fn prepare_raw_transaction_record(
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

    let key = raw_transaction.hash(ton_types::DEPTH_SIZE);
    let value = compressor.compress_owned(&boc)?;

    Ok(Some(TransactionRecord { key, value }))
}

struct TransactionRecord {
    key: UInt256,
    value: Vec<u8>,
}

pub fn compute_partition(block_id: &ton_block::BlockIdExt) -> i32 {
    if block_id.is_masterchain() {
        0
    } else {
        let first_bits = (block_id.shard_id.shard_prefix_with_tag() & 0xe000000000000000u64) >> 61;
        1 + first_bits as i32
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn is_in_right_partition() {
        let shard_targets = [
            (0x0800000000000000u64, 1),
            (0x1800000000000000u64, 1),
            (0x2800000000000000u64, 2),
            (0x3800000000000000u64, 2),
            (0x4800000000000000u64, 3),
            (0x5800000000000000u64, 3),
            (0x6800000000000000u64, 4),
            (0x7800000000000000u64, 4),
            (0x8800000000000000u64, 5),
            (0x9800000000000000u64, 5),
            (0xa800000000000000u64, 6),
            (0xb800000000000000u64, 6),
            (0xc800000000000000u64, 7),
            (0xd800000000000000u64, 7),
            (0xe800000000000000u64, 8),
            (0xf800000000000000u64, 8),
        ];

        assert_eq!(
            compute_partition(&ton_block::BlockIdExt {
                shard_id: ton_block::ShardIdent::masterchain(),
                ..Default::default()
            }),
            0
        );

        for (shard, partition) in shard_targets {
            assert_eq!(
                compute_partition(&ton_block::BlockIdExt {
                    shard_id: ton_block::ShardIdent::with_tagged_prefix(0, shard).unwrap(),
                    ..Default::default()
                }),
                partition
            );
        }
    }
}
