use anyhow::Result;
use std::path::Path;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use ton_block::{
    GetRepresentationHash, HashmapAugType, MsgAddressInt, Serializable, Transaction,
    TransactionDescr,
};
use ton_types::UInt256;

use crate::blocks_handler::kafka_producer::KafkaProducer;
use crate::config::KafkaProducerConfig;
use crate::models::{TransactionNode, Tree};
use crate::transaction_storage::storage::TransactionStorage;

const IGNORED_ADDRESSES: [&str; 3] = [
    "-1:5555555555555555555555555555555555555555555555555555555555555555",
    "-1:3333333333333333333333333333333333333333333333333333333333333333",
    "-1:0000000000000000000000000000000000000000000000000000000000000000",
];

pub struct TxTreeProducer {
    producer: Option<KafkaProducer>,
    transaction_storage: Arc<TransactionStorage>,
    ignored_recipients: Vec<MsgAddressInt>,
    ignored_senders: Vec<MsgAddressInt>,
}

impl TxTreeProducer {
    pub fn new(
        config: KafkaProducerConfig,
        storage_path: &Path,
        max_store_depth: u32,
    ) -> Result<Self> {
        //let kafka_producer = KafkaProducer::new(config, Partitions::any())?;
        tracing::info!("Transaction tree producer initialized");
        let transaction_storage = TransactionStorage::new(storage_path, max_store_depth, false)?;
        let ts_cloned = transaction_storage.clone();

        let ignored_recipients = IGNORED_ADDRESSES
            .iter()
            .filter_map(|x| MsgAddressInt::from_str(x).ok())
            .collect::<Vec<_>>();

        let ignored_senders = IGNORED_ADDRESSES
            .iter()
            .filter_map(|x| MsgAddressInt::from_str(x).ok())
            .collect::<Vec<_>>();

        tokio::spawn(async move {
            match reassemble_skipped_transactions(ts_cloned).await {
                Ok(_) => (),
                Err(e) => tracing::error!("Failed to complete reassembling. Err: {e}"),
            }
        });

        Ok(Self {
            producer: None,
            transaction_storage,
            ignored_recipients,
            ignored_senders,
        })
    }

    pub async fn handle_block(
        &self,
        _: &ton_block::BlockIdExt,
        block: &ton_block::Block,
        max_horizontal_transactions: u32,
    ) -> Result<()> {
        let block_extra = block.read_extra()?;
        block_extra
            .read_account_blocks()?
            .iterate_objects(|account_block| {
                account_block.transactions().iterate_objects(|tx| {
                    let tx = &tx.inner();
                    let description = tx.description.read_struct()?;

                    match description {
                        TransactionDescr::Ordinary(_) | TransactionDescr::TickTock(_) => {
                            match self.handle_transaction(
                                tx,
                                max_horizontal_transactions,
                                &self.ignored_senders,
                                &self.ignored_recipients,
                            ) {
                                Ok(_) => (),
                                Err(e) => tracing::info!("Failed: {e:?}"),
                            };
                        }
                        _ => (),
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
        let start = broxus_util::now_ms_u64();

        let tx_hash = transaction.hash()?;

        let hex_hash = hex::encode(tx_hash.as_slice());
        tracing::debug!("Handling ordinary transaction: {hex_hash}");
        let boc = transaction.serialize()?.write_to_bytes()?;
        let wtb = broxus_util::now_ms_u64();
        tracing::debug!("Write tx to boc: {} ms", wtb - start);

        let tx_out_message_size = transaction.out_msgs.len()?;

        if tx_out_message_size > max_horizontal_transactions as usize {
            tracing::info!("Max size reached: MAX: {max_horizontal_transactions}. FOUND: {tx_out_message_size}");
            return Ok(());
        }

        let mut all_messages_external = true;

        match transaction.in_msg.as_ref() {
            Some(in_msg) => {
                let message = in_msg.read_struct()?;
                tracing::debug!("Handling message: {}", hex::encode(message.hash()?));
                if let Some(src) = message.src_ref() {
                    if ignored_senders.contains(&src) {
                        return Ok(());
                    }
                }

                if let Some(dst) = message.dst_ref() {
                    if ignored_recipients.contains(&dst) {
                        return Ok(());
                    }
                }

                let mut out_msgs = Vec::with_capacity(transaction.out_msgs.len()?);

                transaction.out_msgs.iterate(|message| {
                    if message.as_ref().is_internal() {
                        all_messages_external = false;
                    }
                    let hash = message.0.hash()?;

                    out_msgs.push(hash);
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
                    transaction.lt,
                    external_in,
                    internal_in,
                    boc.as_slice(),
                    out_msgs,
                ) {
                    Ok(_) => (),
                    Err(e) => tracing::error!("Failed to add transaction: {e:?}"),
                }
            }
            _ => tracing::debug!("No internal message: {hex_hash}"),
        }

        Ok(())
    }
}

pub async fn reassemble_skipped_transactions(storage: Arc<TransactionStorage>) -> Result<()> {
    loop {
        let trees = storage.try_reassemble_pending_trees().await?;
        for tree in trees.iter() {
            match tree {
                Tree::Full(transaction) => {
                    tracing::debug!(
                        "Assembled full tree {:?}. Children len : {}",
                        hex::encode(transaction.hash().as_slice()),
                        transaction.children().len()
                    );

                    if transaction.children().len() > 0 {
                        visualize_tree(transaction);
                    }

                    let st = storage.clone();
                    let tx = transaction.clone();
                    tokio::spawn(async move {
                        if let Err(e) = st.clean_transaction_tree(&tx) {
                            tracing::error!("Failed to clean transaction tree. Err: {e}")
                        }
                    });
                }
                Tree::Partial(transaction) => {
                    tracing::warn!(
                        "Loop triggered. Assembled partial tree {:?}. Children len : {}",
                        hex::encode(transaction.hash().as_slice()),
                        transaction.children().len()
                    );
                }
                Tree::Empty => {
                    tracing::warn!("Loop triggered. Tree is empty",);
                }
            }
        }
        tokio::time::sleep(Duration::from_secs(10)).await;
    }
}

pub fn visualize_tree(tree: &TransactionNode) {
    tracing::info!("------------------------------------------------------");
    tracing::info!("ROOT IS: {}", hex::encode(tree.hash().as_slice()));
    for n in tree.children() {
        visualize_ancestor(tree.hash(), n);
    }
}

fn visualize_ancestor(parent_hash: &UInt256, node: &TransactionNode) {
    tracing::info!(
        "Child: {}. Parent: {}",
        hex::encode(node.hash().as_slice()),
        hex::encode(parent_hash.as_slice())
    );
    for c in node.children() {
        visualize_ancestor(node.hash(), c);
    }
}
