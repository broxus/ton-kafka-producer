use anyhow::Result;
use std::path::Path;
use std::sync::Arc;
use ton_block::{HashmapAugType, Serializable};

use crate::blocks_handler::kafka_producer::{KafkaProducer, Partitions};
use crate::config::KafkaProducerConfig;
use crate::transaction_storage::storage::TransactionStorage;

pub struct TxTreeProducer {
    producer: Option<KafkaProducer>,
    transaction_storage: Arc<TransactionStorage>,
}

impl TxTreeProducer {
    pub fn new(
        config: KafkaProducerConfig,
        storage_path: &Path,
        max_store_depth: u32,
    ) -> Result<Self> {
        //let kafka_producer = KafkaProducer::new(config, Partitions::any())?;
        let transaction_storage = TransactionStorage::new(storage_path, max_store_depth)?;
        Ok(Self {
            producer: None,
            transaction_storage,
        })
    }

    pub async fn handle_block(
        &self,
        _: &ton_block::BlockIdExt,
        block: &ton_block::Block,
        max_horizontal_transactions: u8,
    ) -> Result<()> {
        let block_extra = block.read_extra()?;

        block_extra
            .read_account_blocks()?
            .iterate_objects(|account_block| {
                account_block.transactions().iterate_objects(|tx| {
                    let tx = &tx.inner();
                    let tx_cell = tx.serialize()?;
                    let tx_hash = tx_cell.hash(ton_types::MAX_LEVEL as usize);
                    let hex_hash = hex::encode(tx_hash.as_slice());
                    tracing::info!("tx hash: {hex_hash}");
                    //let base64_boc = tx_cell.write_to_bytes()?;

                    let tx_out_message_size = tx.out_msgs.len()?;

                    if max_horizontal_transactions as usize > tx_out_message_size {
                        return Ok(true);
                    }

                    match &tx.in_msg {
                        Some(in_msg) => {
                            let message = in_msg.read_struct()?;
                            let mut out_msgs = Vec::with_capacity(tx.out_msgs.len()?);

                            tx.out_msgs.iterate_slices(|m| {
                                out_msgs.push(m.hash(ton_types::MAX_LEVEL as usize));
                                Ok(true)
                            })?;

                            let message_hash = in_msg.hash();
                            let (external_in, internal_in) = if message.src().is_some() {
                                (None, Some(&message_hash))
                            } else {
                                (Some(&message_hash), None)
                            };

                            // self.transaction_storage.add_transaction(
                            //     tx_hash.inner(),
                            //     base64_boc.as_slice(),
                            //     external_in,
                            //     internal_in,
                            //     out_msgs.as_slice(),
                            // )?;
                        }
                        _ => (),
                    }

                    //if &tx.out_msgs.is_empty() {}

                    Ok(true)
                })?;
                Ok(true)
            })?;

        Ok(())
    }
}
