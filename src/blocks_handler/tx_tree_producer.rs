use anyhow::{anyhow, Result};
use async_trait::async_trait;
use bytes::Bytes;
use std::path::Path;
use std::str::FromStr;
use std::sync::Arc;
use ton_block::{
    GetRepresentationHash, HashmapAugType, MsgAddressInt, Serializable, Transaction,
    TransactionDescr,
};
use ton_types::{serialize_toc, UInt256};

use crate::blocks_handler::kafka_producer::{KafkaProducer, Partitions};
use crate::config::{KafkaProducerConfig, TxTreeSettings};
use crate::models::{TransactionNode, Tree};
use crate::transaction_storage::storage::TransactionStorage;
use crate::tree_packer::TreePacker;

pub struct TxTreeProducer {
    transaction_storage: Arc<TransactionStorage>,
    ignored_recipients: Vec<MsgAddressInt>,
    ignored_senders: Vec<MsgAddressInt>,
    max_transaction_width: u32,
    handlers: Vec<Box<dyn OutputHandler>>,
}

#[async_trait]
pub trait OutputHandler: Send + Sync {
    async fn handle_output(&self, trees: &[TransactionNode]);
}

pub struct KafkaOutputHandler {
    kafka_producer: KafkaProducer,
}

impl KafkaOutputHandler {
    pub fn new(config: KafkaProducerConfig) -> Result<Self> {
        let kafka_producer = KafkaProducer::new(config, Partitions::any())?;
        Ok(Self { kafka_producer })
    }
}

#[async_trait]
impl OutputHandler for KafkaOutputHandler {
    async fn handle_output(&self, trees: &[TransactionNode]) {
        let tree_packer = Arc::new(TreePacker::default());
        for tree in trees {
            let now = broxus_util::now_sec_u64() as i64;
            let write_result = match prepare_record(tree_packer.clone(), tree) {
                Ok((key, value)) => self.kafka_producer.write(0, key, value, Some(now)).await,
                Err(e) => {
                    tracing::error!("Failed to prepare message. Err: {e}");
                    Ok(())
                }
            };
            if let Err(e) = write_result {
                tracing::error!("Failed to send message to kafka. Err: {e}");
            }
        }
    }
}

#[derive(Default)]
pub struct SimpleHandler {}

#[async_trait]
impl OutputHandler for SimpleHandler {
    async fn handle_output(&self, trees: &[TransactionNode]) {
        let packer = TreePacker::default();
        for tree in trees {
            visualize_tree(tree);
            match packer.pack(tree) {
                Ok(cell) => {
                    if let Ok(boc) = serialize_toc(&cell) {
                        tracing::info!("Tree size {} bytes", boc.len())
                    }
                }
                Err(e) => {
                    tracing::debug!("Failed to pack: {:x}. Err: {:?}", tree.hash(), e)
                }
            }
        }
    }
}

impl TxTreeProducer {
    pub fn new(settings: TxTreeSettings, config: Option<KafkaProducerConfig>) -> Result<Self> {
        tracing::info!("Transaction tree producer initialized");
        let max_tx_depth = settings.max_transaction_depth.unwrap_or(100);
        let transaction_storage =
            TransactionStorage::new(Path::new(&settings.db_path), max_tx_depth, false)?;

        let strorage_cloned = transaction_storage.clone();
        tokio::spawn(async move {
            if let Err(e) = strorage_cloned.clean_transaction_trees().await {
                tracing::error!("{:?}", e)
            }
        });

        let ignored_recipients = settings
            .ignored_receivers
            .iter()
            .filter_map(|x| MsgAddressInt::from_str(x).ok())
            .collect::<Vec<_>>();

        let ignored_senders = settings
            .ignored_senders
            .iter()
            .filter_map(|x| MsgAddressInt::from_str(x).ok())
            .collect::<Vec<_>>();

        let handlers = TxTreeProducer::prepare_handlers(config)?;

        Ok(Self {
            transaction_storage,
            ignored_recipients,
            ignored_senders,
            max_transaction_width: settings.max_transaction_width.unwrap_or(100),
            handlers,
        })
    }

    fn prepare_handlers(
        kafka_config: Option<KafkaProducerConfig>,
    ) -> Result<Vec<Box<dyn OutputHandler>>> {
        let mut handlers: Vec<Box<dyn OutputHandler>> = Vec::new();

        let simple_handler = Box::new(SimpleHandler::default());
        handlers.push(simple_handler);

        if let Some(config) = kafka_config {
            match KafkaOutputHandler::new(config) {
                Ok(_) => {
                    // let kafka_handler = Box::new(kafka_handler);
                    // handlers.push(kafka_handler);
                }
                Err(e) => {
                    tracing::error!("Failed to create kafka handler. Err: {e}");
                }
            }
        }

        if handlers.is_empty() {
            return Err(anyhow!("Not output handlers specified"));
        }

        Ok(handlers)
    }

    pub async fn handle_block(
        &self,
        _: &ton_block::BlockIdExt,
        block: &ton_block::Block,
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
                            self.handle_transaction(tx)?;
                        }
                        _ => (),
                    }

                    Ok(true)
                })?;
                Ok(true)
            })?;

        let transactions =
            reassemble_skipped_transactions(self.transaction_storage.clone()).await?;

        for handler in self.handlers.as_slice() {
            let x = transactions.as_slice();
            handler.handle_output(x).await;
        }

        Ok(())
    }

    pub fn handle_transaction(&self, transaction: &Transaction) -> Result<()> {
        let start = broxus_util::now_ms_u64();

        let tx_hash = transaction.hash()?;

        let hex_hash = hex::encode(tx_hash.as_slice());
        tracing::debug!("Handling ordinary transaction: {hex_hash}");
        let boc = transaction.serialize()?.write_to_bytes()?;
        let wtb = broxus_util::now_ms_u64();
        tracing::debug!("Write tx to boc: {} ms", wtb - start);

        let tx_out_message_size = transaction.out_msgs.len()?;

        if tx_out_message_size > self.max_transaction_width as usize {
            tracing::info!(
                "Max size reached: MAX: {}. FOUND: {tx_out_message_size}",
                self.max_transaction_width
            );
            return Ok(());
        }

        let mut all_messages_external = true;

        match transaction.in_msg.as_ref() {
            Some(in_msg) => {
                let message = in_msg.read_struct()?;
                tracing::debug!("Handling message: {}", hex::encode(message.hash()?));
                if let Some(src) = message.src_ref() {
                    if self.ignored_senders.contains(src) {
                        return Ok(());
                    }
                }

                if let Some(dst) = message.dst_ref() {
                    if self.ignored_recipients.contains(dst) {
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
                self.transaction_storage.add_transaction(
                    &tx_hash,
                    transaction.lt,
                    external_in,
                    internal_in,
                    boc.as_slice(),
                    out_msgs,
                )?;
            }
            _ => tracing::debug!("No internal message: {hex_hash}"),
        }

        Ok(())
    }
}

pub fn prepare_record(packer: Arc<TreePacker>, tree: &TransactionNode) -> Result<(Bytes, Bytes)> {
    let cell = packer.pack(tree)?;
    let boc = serialize_toc(&cell)?;
    Ok((tree.hash().into_vec().into(), boc.into()))
}

pub async fn reassemble_skipped_transactions(
    storage: Arc<TransactionStorage>,
) -> Result<Vec<TransactionNode>> {
    //let start = broxus_util::now_ms_u64();
    let trees = storage.try_reassemble_pending_trees().await?;
    //let end = broxus_util::now_ms_u64();
    //tracing::warn!("Reassembling took {} ms", end - start);
    let mut full_trees: Vec<TransactionNode> = Vec::new();
    for tree in trees {
        match tree {
            Tree::Full(transaction) => {
                // tracing::info!(
                //     "Assembled full tree {:?}. Children len : {}",
                //     hex::encode(transaction.hash().as_slice()),
                //     transaction.children().len()
                // );

                full_trees.push(transaction.clone());
            }
            Tree::Partial(transaction) => {
                tracing::debug!(
                    "Assembled partial tree {:?}. Children len : {}",
                    hex::encode(transaction.hash().as_slice()),
                    transaction.children().len()
                );
            }
            Tree::Empty => {
                tracing::debug!("Tree is empty",);
            }
        }
    }

    Ok(full_trees)
}

pub fn visualize_tree(tree: &TransactionNode) {
    println!("------------------------------------------------------");
    println!("ROOT IS: {}", hex::encode(tree.hash().as_slice()));
    for n in tree.children() {
        visualize_ancestor(tree.hash(), n);
    }
}

fn visualize_ancestor(parent_hash: &UInt256, node: &TransactionNode) {
    println!(
        "Child: {}. Parent: {}",
        hex::encode(node.hash().as_slice()),
        hex::encode(parent_hash.as_slice())
    );
    for c in node.children() {
        visualize_ancestor(node.hash(), c);
    }
}
