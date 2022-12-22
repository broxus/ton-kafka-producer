use anyhow::{anyhow, Result};
use everscale_network::tl::hash;
use itertools::Itertools;
use rocksdb::{BoundColumnFamily, ColumnFamilyDescriptor, IteratorMode, Options, DB};
use std::path::{Path, PathBuf};
use std::string::ToString;
use std::sync::Arc;
use ton_types::UInt256;

const TX_EXT_IN_MSG: String = "tx_external_in_msgs".to_string();
const TX_INT_IN_MSG: String = "tx_internal_in_msgs".to_string();
const TX_INT_OUT_MSGS: String = "tx_internal_out_msgs".to_string();
const TX_BOC: String = "tx_boc".to_string();
const TX_DEPTH: String = "tx_depth".to_string();

pub struct TransactionStorage {
    file_db_path: PathBuf,
    db: Arc<DB>,
    max_depth: u32,
}

impl TransactionStorage {
    fn get_external_in_message_cf(&self) -> Arc<BoundColumnFamily> {
        self.db.cf_handle(&TX_EXT_IN_MSG).trust_me()
    }

    fn get_internal_in_message_cf(&self) -> Arc<BoundColumnFamily> {
        self.db.cf_handle(&TX_INT_IN_MSG).trust_me()
    }

    fn get_internal_out_messages_cf(&self) -> Arc<BoundColumnFamily> {
        self.db.cf_handle(&TX_INT_OUT_MSGS).trust_me()
    }

    fn get_tx_boс_cf(&self) -> Arc<BoundColumnFamily> {
        self.db.cf_handle(&TX_BOC).trust_me()
    }

    fn get_tx_depth_cf(&self) -> Arc<BoundColumnFamily> {
        self.db.cf_handle(&TX_DEPTH).trust_me()
    }

    pub fn new(file_db_path: &Path, max_depth: u32) -> Result<Arc<TransactionStorage>> {
        let mut opts = Options::default();

        opts.set_log_level(rocksdb::LogLevel::Error);
        opts.set_keep_log_file_num(2);
        opts.set_recycle_log_file_num(2);

        opts.create_missing_column_families(true);
        opts.create_if_missing(true);

        let ext_in_msg_cf = ColumnFamilyDescriptor::new(&TX_EXT_IN_MSG, Options::default());
        let int_in_msg_cf = ColumnFamilyDescriptor::new(&TX_INT_IN_MSG, Options::default());
        let out_msgs_cf = ColumnFamilyDescriptor::new(&TX_INT_OUT_MSGS, Options::default());
        let boc_cf = ColumnFamilyDescriptor::new(&TX_BOC, Options::default());
        let depth_cf = ColumnFamilyDescriptor::new(&TX_DEPTH, Options::default());

        let db = DB::open_cf_descriptors(
            &opts,
            file_db_path,
            vec![ext_in_msg_cf, int_in_msg_cf, out_msgs_cf, boc_cf, depth_cf],
        )?;
        Ok(Arc::new(Self {
            file_db_path: file_db_path.to_path_buf(),
            db: Arc::new(db),
            max_depth,
        }))
    }

    pub async fn add_transaction(
        &self,
        tx_hash: UInt256,
        ext_in_msg_hash: Option<UInt256>,
        int_in_msg_hash: Option<UInt256>,
        boc: &[u8],
        int_out_msgs: &[UInt256],
    ) -> Result<()> {
        let int_in_msg = self.get_internal_in_message_cf();
        let ext_in_msg = self.get_external_in_message_cf();
        let boc_cf = self.get_tx_boс_cf();
        let out_msgs_cf = self.get_internal_out_messages_cf();
        let depth = self.get_tx_depth_cf();

        let int_out_msgs = bincode::serialize(int_out_msgs)?;

        match (ext_in_msg_hash, int_in_msg_hash) {
            (Some(ext), None) => {
                self.db.put_cf(&ext_in_msg, tx_hash.as_slice(), ext)?;
                self.db
                    .put_cf(&depth, tx_hash.as_slice(), 0.to_be_bytes())?;
            }
            (None, Some(int)) => {
                match self.get_parent_transaction_by_msg_hash(&tx_hash)? {
                    Some(parent) => {
                        match self.get_depth(&parent)? {
                            Some(depth) => {
                                let new_depth = depth + 1;
                                if new_depth > self.max_depth {
                                    anyhow::bail!("Transaction tree is too deep.");
                                    // remove tree here in separate thread.
                                } else {
                                    self.db.put_cf(&ext_in_msg, tx_hash, int)?;
                                    self.db.put_cf(&depth, tx_hash, new_depth.to_be_bytes())?;
                                }
                            }
                            None => anyhow::bail!("Failed to check transaction depth"),
                        }
                    }
                    None => anyhow::bail!("Corrupted data. Failed to find parent transaction"),
                }
            }
            _ => anyhow::bail!("Corrupted transaction. No internal or external in message"),
        }

        self.db.put_cf(&ext_in_msg, tx_hash, ext_in_msg_hash)?;
        self.db.put_cf(&int_in_msg, tx_hash, int_in_msg_hash)?;
        self.db.put_cf(&boc_cf, tx_hash, boc)?;
        self.db.put_cf(&out_msgs_cf, tx_hash, int_out_msgs)?;

        Ok(())
    }

    pub async fn try_assemble_tree(&self, tx_hash: &UInt256) -> Result<Option<TreeNode>> {
        let external_in_cf = self.get_internal_in_message_cf();
        let internal_message_opt = self.db.get_cf(&external_in_cf, tx_hash.as_slice())?;

        let mut lowest_branch_node = self.assemble_node(tx_hash, Vec::new())?;

        // match lowest_branch_node {
        //     Some(mut node) => {
        //
        //     }
        //     None => None
        // }
        //
        // let parent_tx = match internal_message_opt {
        //     Some(int_in) => {
        //         self.get_parent_transaction_by_msg_hash(tx_hash)?;
        //     }
        //     None => return Ok(None),
        // }

        Ok(None)
    }

    fn get_depth(&self, tx_hash: &UInt256) -> Result<Option<u32>> {
        let depth_cf = self.get_tx_depth_cf();
        let result = self
            .db
            .get_cf(&depth_cf, tx_hash.as_slice())?
            .and_then(|x| bincode::deserialize::<u32>(x.as_slice()).ok());

        Ok(result)
    }

    fn get_parent_transaction_by_msg_hash(&self, msg_hash: &UInt256) -> Result<Option<UInt256>> {
        let out_msgs = self.get_internal_out_messages_cf();
        for i in self.db.iterator_cf(&out_msgs, IteratorMode::Start) {
            match i {
                Ok((key, value)) => {
                    let out_msgs = bincode::deserialize::<[[u8; 32]]>(&value)?;
                    if out_msgs.contains(msg_hash.as_slice()) {
                        let hash = UInt256::from_slice(key.as_ref());
                        return Ok(Some(hash));
                    }
                }
                Err(e) => {
                    tracing::error!("Failed to get next element for column family {e:?}");
                    return Ok(None);
                }
            }
        }

        Ok(None)
    }

    fn append_children_transaction_tree(
        &self,
        msg_hash: &UInt256,
        parent_node: &mut TreeNode,
    ) -> Result<TreeNode> {
        let out_msg_cf = self.get_internal_out_messages_cf();

        let child_hash = self.get_children_transaction_by_msg_hash(msg_hash)?;
        let node_opt = match child_hash {
            Some(hash) => self.assemble_node(&hash, Vec::new())?,
            None => return Ok(parent_node.clone()),
        };

        if let Some(node) = node_opt {
            let children_out_messages_raw = self
                .db
                .get_cf(&out_msg_cf, node.tx_hash.as_slice())?
                .unwrap_or_default();

            let children_out_messages =
                bincode::deserialize::<Vec<UInt256>>(children_out_messages_raw.as_slice())?;

            if children_out_messages.is_empty() {
                return Ok(parent_node.clone());
            } else {
                for i in children_out_messages {
                    let tree_node_opt = match self.get_children_transaction_by_msg_hash(&i)? {
                        Some(tx) => self.assemble_node(&tx, Vec::new())?,
                        None => None,
                    };

                    if let Some(tree_node_opt) = tree_node_opt {
                        parent_node.children.push(tree_node_opt);
                    }
                }
            }

            for mut c in parent_node.children {
                self.append_children_transaction_tree(&c.tx_hash, &mut c)
            }
        }

        Ok(parent_node.clone())
    }

    fn get_children_transaction_by_msg_hash(&self, msg_hash: &UInt256) -> Result<Option<UInt256>> {
        let int_in_msg = self.get_internal_in_message_cf();

        for i in self.db.iterator_cf(&int_in_msg, IteratorMode::Start) {
            match i {
                Ok((key, value)) => {
                    if msg_hash.as_slice() == &value.as_ref() {
                        let hash = UInt256::from_slice(key.as_ref());
                        return Ok(Some(hash));
                    }
                }
                Err(e) => {
                    tracing::error!("Failed to get next element for column family {e:?}");
                    return Ok(None);
                }
            }
        }

        Ok(None)
    }

    fn assemble_node(
        &self,
        tx_hash: &UInt256,
        children: Vec<TreeNode>,
    ) -> Result<Option<TreeNode>> {
        let boc = self.get_tx_boс_cf();
        Ok(self.db.get_cf(&boc, tx_hash.as_slice())?.map(|b| TreeNode {
            tx_hash: tx_hash.clone(),
            boc: b,
            children,
        }))
    }
}

#[derive(Debug, Clone)]
pub struct TreeNode {
    pub tx_hash: UInt256,
    pub boc: Vec<u8>,
    pub children: Vec<TreeNode>,
}
