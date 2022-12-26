use anyhow::Result;
use std::path::Path;
use std::str::FromStr;
use std::sync::Arc;
use ton_block::{
    Deserializable, GetRepresentationHash, HashmapAugType, MsgAddressInt, Serializable,
    Transaction, TransactionDescr,
};
use ton_types::UInt256;

use crate::blocks_handler::kafka_producer::KafkaProducer;
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
        tracing::info!("Transaction tree producer initialized");
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
        max_horizontal_transactions: u32,
    ) -> Result<()> {
        let block_extra = block.read_extra()?;
        let ignored_1 = MsgAddressInt::from_str(
            "-1:5555555555555555555555555555555555555555555555555555555555555555",
        )?;
        let ignored_2 = MsgAddressInt::from_str(
            "-1:3333333333333333333333333333333333333333333333333333333333333333",
        )?;

        let ignored_3 = MsgAddressInt::from_str(
            "-1:0000000000000000000000000000000000000000000000000000000000000000",
        )?;

        let ignored_recipients = vec![ignored_1.clone(), ignored_2.clone(), ignored_3.clone()];
        let ignored_senders = vec![ignored_1, ignored_2, ignored_3];

        block_extra
            .read_account_blocks()?
            .iterate_objects(|account_block| {
                account_block.transactions().iterate_objects(|tx| {
                    let tx = &tx.inner();
                    let description = tx.description.read_struct()?;
                    if matches!(description, TransactionDescr::Ordinary(_)) {
                        match self.handle_transaction(
                            tx,
                            max_horizontal_transactions,
                            &ignored_senders,
                            &ignored_recipients,
                        ) {
                            Ok(_) => (),
                            Err(e) => tracing::info!("Failed: {e:?}"),
                        };
                    }

                    Ok(true)
                })?;
                Ok(true)
            })?;

        Ok(())
    }

    fn handle_transaction(
        &self,
        transaction: &Transaction,
        max_horizontal_transactions: u32,
        ignored_senders: &[MsgAddressInt],
        ignored_recipients: &[MsgAddressInt],
    ) -> Result<()> {
        let tx_cell = transaction.serialize()?;
        let tx_hash = tx_cell.hash(ton_types::MAX_LEVEL as usize);
        let hex_hash = hex::encode(tx_hash.as_slice());
        tracing::info!("Handling ordinary transaction: {hex_hash}");
        let boc = tx_cell.write_to_bytes()?;

        let tx_out_message_size = transaction.out_msgs.len()?;

        if tx_out_message_size > max_horizontal_transactions as usize {
            tracing::info!("Max size reached: MAX: {max_horizontal_transactions}. FOUND: {tx_out_message_size}");
            return Ok(());
        }

        let mut all_messages_external = true;

        match transaction.in_msg.as_ref() {
            Some(in_msg) => {
                let message = in_msg.read_struct()?;
                tracing::info!("Handling message: {}", hex::encode(message.hash()?));
                if let Some(src) = message.src_ref() {
                    if ignored_senders.contains(&src) {
                        tracing::info!("Ignoring sender: {}", src.to_string());
                        return Ok(());
                    }
                }

                if let Some(dst) = message.dst_ref() {
                    if ignored_recipients.contains(&dst) {
                        tracing::info!("Ignoring recipient: {}", dst.to_string());
                        return Ok(());
                    }
                }

                let mut out_msgs = Vec::with_capacity(transaction.out_msgs.len()?);

                transaction.out_msgs.iterate(|message| {
                    if message.as_ref().is_internal() {
                        all_messages_external = false;
                    }
                    out_msgs.push(message.hash()?);
                    Ok(true)
                })?;

                let message_hash = in_msg.hash();
                let (external_in, internal_in) = if message.src().is_some() {
                    (None, Some(&message_hash))
                } else {
                    (Some(&message_hash), None)
                };
                match self.transaction_storage.add_transaction(
                    &tx_hash,
                    external_in,
                    internal_in,
                    boc.as_slice(),
                    out_msgs.as_slice(),
                ) {
                    Ok(_) => (),
                    Err(e) => tracing::error!("Failed to add transaction: {e:?}"),
                }
            }
            _ => tracing::info!("No internal message: {hex_hash}"),
        }

        if transaction.out_msgs.is_empty() || all_messages_external {
            tracing::info!("Assembling tree for transaction : {hex_hash}");
            let tree = self.transaction_storage.try_assemble_tree(&tx_hash)?;
            if let Some(tree) = tree {
                tracing::info!(
                    "Init transaction {:?}. Children len : {}",
                    hex::encode(tree.init_transaction_hash().as_slice()),
                    tree.root_children().len()
                );
            }
        }

        Ok(())
    }
}
