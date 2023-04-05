use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;
use bytes::Bytes;
use ton_types::serialize_toc;

use crate::blocks_handler::{KafkaProducer, Partitions};
use crate::config::TreesKafkaConfig;
use crate::models::TransactionNode;
use crate::output_handlers::OutputHandler;
use crate::tree_packer::TreePacker;

pub struct KafkaOutputHandler {
    max_tree_depth: u32,
    max_tree_size_bytes: u32,
    kafka_producer: KafkaProducer,
}

impl KafkaOutputHandler {
    pub fn new(config: TreesKafkaConfig) -> Result<Self> {
        let kafka_producer = KafkaProducer::new(config.kafka_settings, Partitions::any())?;
        Ok(Self {
            kafka_producer,
            max_tree_depth: config.max_tree_depth,
            max_tree_size_bytes: config.max_tree_size_bytes,
        })
    }

    pub fn check_restrictions(&self, tree: &TransactionNode, size: usize) -> bool {
        if tree.depth() >= self.max_tree_depth {
            return false;
        }
        if size as u32 >= self.max_tree_size_bytes {
            return false;
        }

        true
    }
}

#[async_trait]
impl OutputHandler for KafkaOutputHandler {
    async fn handle_output(&self, trees: &[TransactionNode]) -> Result<()> {
        let tree_packer = Arc::new(TreePacker::default());
        for tree in trees {
            let now = broxus_util::now_sec_u64() as i64;
            let (key, value) = prepare_record(tree_packer.clone(), tree)?;
            if !self.check_restrictions(tree, value.len()) {
                tracing::warn!(
                    "Kafka output restrictions are not met. Tree root hash: {:x}",
                    tree.hash()
                );
                continue;
            } else {
                self.kafka_producer.write(0, key, value, Some(now)).await?
            }
        }

        Ok(())
    }
}

pub fn prepare_record(packer: Arc<TreePacker>, tree: &TransactionNode) -> Result<(Bytes, Bytes)> {
    let cell = packer.pack(tree)?;
    let boc = serialize_toc(&cell)?;
    Ok((tree.hash().into_vec().into(), boc.into()))
}
