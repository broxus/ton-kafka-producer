use anyhow::{anyhow, Result};
use everscale_network::tl::hash;
use itertools::Itertools;
use rocksdb::{BoundColumnFamily, ColumnFamilyDescriptor, IteratorMode, Options, DB};
use std::path::{Path, PathBuf};
use std::string::ToString;
use std::sync::Arc;
use ton_types::UInt256;

const TX_EXT_IN_MSG: &str = "tx_external_in_msgs";
const TX_INT_IN_MSG: &str = "tx_internal_in_msgs";
const TX_INT_OUT_MSGS: &str = "tx_internal_out_msgs";
const TX_BOC: &str = "tx_boc";
const TX_DEPTH: &str = "tx_depth";

pub struct TransactionStorage {
    file_db_path: PathBuf,
    db: Arc<DB>,
    max_depth: u32,
}

impl TransactionStorage {
    fn get_external_in_message_cf(&self) -> Arc<BoundColumnFamily> {
        self.db.cf_handle(&TX_EXT_IN_MSG).expect("Trust me")
    }

    fn get_internal_in_message_cf(&self) -> Arc<BoundColumnFamily> {
        self.db.cf_handle(&TX_INT_IN_MSG).expect("Trust me")
    }

    fn get_internal_out_messages_cf(&self) -> Arc<BoundColumnFamily> {
        self.db.cf_handle(&TX_INT_OUT_MSGS).expect("Trust me")
    }

    fn get_tx_boс_cf(&self) -> Arc<BoundColumnFamily> {
        self.db.cf_handle(&TX_BOC).expect("Trust me")
    }

    fn get_tx_depth_cf(&self) -> Arc<BoundColumnFamily> {
        self.db.cf_handle(&TX_DEPTH).expect("Trust me")
    }

    pub fn new(file_db_path: &Path, max_depth: u32) -> Result<Arc<TransactionStorage>> {
        let mut opts = Options::default();

        opts.set_log_level(rocksdb::LogLevel::Error);
        opts.set_keep_log_file_num(2);
        opts.set_recycle_log_file_num(2);

        opts.create_missing_column_families(true);
        opts.create_if_missing(true);

        let ext_in_msg_cf = ColumnFamilyDescriptor::new(TX_EXT_IN_MSG, Options::default());
        let int_in_msg_cf = ColumnFamilyDescriptor::new(TX_INT_IN_MSG, Options::default());
        let out_msgs_cf = ColumnFamilyDescriptor::new(TX_INT_OUT_MSGS, Options::default());
        let boc_cf = ColumnFamilyDescriptor::new(TX_BOC, Options::default());
        let depth_cf = ColumnFamilyDescriptor::new(TX_DEPTH, Options::default());
        println!("hello");

        let db = DB::open_cf_descriptors(
            &opts,
            file_db_path,
            vec![ext_in_msg_cf, int_in_msg_cf, out_msgs_cf, boc_cf, depth_cf],
        )?;
        println!("hello1");
        Ok(Arc::new(Self {
            file_db_path: file_db_path.to_path_buf(),
            db: Arc::new(db),
            max_depth,
        }))
    }

    pub fn add_transaction(
        &self,
        tx_hash: UInt256,
        ext_in_msg_hash: Option<UInt256>,
        int_in_msg_hash: Option<UInt256>,
        boc: &[u8],
        int_out_msgs: &[UInt256],
    ) -> Result<()> {
        let int_in_msg_cf = self.get_internal_in_message_cf();
        let ext_in_msg_cf = self.get_external_in_message_cf();
        let boc_cf = self.get_tx_boс_cf();
        let out_msgs_cf = self.get_internal_out_messages_cf();
        let depth_cf = self.get_tx_depth_cf();

        let int_out_msgs = int_out_msgs.iter().map(|x| x.inner()).collect::<Vec<_>>();
        let int_out_msgs = bincode::serialize(int_out_msgs.as_slice())?;

        match (ext_in_msg_hash, int_in_msg_hash) {
            (Some(ext), None) => {
                self.db.put_cf(&int_in_msg_cf, tx_hash.as_slice(), ext)?;
                self.db
                    .put_cf(&depth_cf, tx_hash.as_slice(), 0u32.to_be_bytes())?;
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
                                    self.db.put_cf(&ext_in_msg_cf, tx_hash, int)?;
                                    self.db
                                        .put_cf(&depth_cf, tx_hash, new_depth.to_be_bytes())?;
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

        self.db.put_cf(&boc_cf, tx_hash, boc)?;
        self.db.put_cf(&out_msgs_cf, tx_hash, int_out_msgs)?;

        Ok(())
    }

    pub fn try_assemble_tree(&self, tx_hash: &UInt256) -> Result<Option<Transaction>> {
        let external_in_cf = self.get_internal_in_message_cf();

        let mut node = self.get_plain_node(tx_hash)?;

        loop {
            if let Some(mut n) = node.clone() {
                let internal_message_opt = self
                    .db
                    .get_cf(&external_in_cf.clone(), n.tx_hash.as_slice())?;
                let parent_message = match internal_message_opt {
                    Some(in_msg) => UInt256::from_slice(in_msg.as_slice()),
                    _ => return Ok(node),
                };

                let new_parent = self.find_parent_transaction(&parent_message)?;

                match new_parent {
                    Some(mut new_parent) => {
                        let out_msgs = self.get_tx_out_messages(&new_parent.tx_hash)?;

                        for i in &out_msgs {
                            if i == &parent_message {
                                continue;
                            }
                            self.append_children_transaction_tree(&i, &mut new_parent)?;
                        }
                    }
                    _ => return Ok(node),
                }
            } else {
                return Ok(None);
            }
        }
    }

    fn get_depth(&self, tx_hash: &UInt256) -> Result<Option<u32>> {
        let depth_cf = self.get_tx_depth_cf();
        let result = self
            .db
            .get_cf(&depth_cf, tx_hash.as_slice())?
            .and_then(|x| bincode::deserialize::<u32>(x.as_slice()).ok());

        Ok(result)
    }

    fn find_parent_transaction(&self, msg_hash: &UInt256) -> Result<Option<Transaction>> {
        if let Some(hash) = self.get_parent_transaction_by_msg_hash(&msg_hash)? {
            self.get_plain_node(&hash)
        } else {
            Ok(None)
        }
    }

    fn append_children_transaction_tree(
        &self,
        msg_hash: &UInt256,
        parent_node: &mut Transaction,
    ) -> Result<Transaction> {
        let child_hash = self.get_children_transaction_by_msg_hash(msg_hash)?;
        let node_opt = match child_hash {
            Some(hash) => self.get_plain_node(&hash)?,
            None => return Ok(parent_node.clone()),
        };

        if let Some(node) = node_opt {
            let children_out_messages = self.get_tx_out_messages(&node.tx_hash)?;

            if children_out_messages.is_empty() {
                return Ok(parent_node.clone());
            } else {
                for i in children_out_messages.iter() {
                    let tree_node_opt = match self.get_children_transaction_by_msg_hash(i)? {
                        Some(tx) => self.get_plain_node(&tx)?,
                        None => None,
                    };

                    if let Some(tree_node_opt) = tree_node_opt {
                        parent_node.children.push(tree_node_opt);
                    }
                }
            }

            for (node, message) in parent_node
                .children
                .iter_mut()
                .zip(children_out_messages.iter())
            {
                self.append_children_transaction_tree(message, node)?;
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

    fn get_parent_transaction_by_msg_hash(&self, msg_hash: &UInt256) -> Result<Option<UInt256>> {
        let out_msgs = self.get_internal_out_messages_cf();
        for i in self.db.iterator_cf(&out_msgs, IteratorMode::Start) {
            match i {
                Ok((key, value)) => {
                    let out_msgs = bincode::deserialize::<Vec<[u8; 32]>>(&value)?;
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

    fn get_plain_node(&self, tx_hash: &UInt256) -> Result<Option<Transaction>> {
        let boc = self.get_tx_boс_cf();
        Ok(self
            .db
            .get_cf(&boc, tx_hash.as_slice())?
            .map(|b| Transaction {
                tx_hash: tx_hash.clone(),
                boc: b,
                children: Vec::new(),
            }))
    }

    fn get_tx_out_messages(&self, tx_hash: &UInt256) -> Result<Vec<UInt256>> {
        let out_msg_cf = self.get_internal_out_messages_cf();

        let children_out_messages_raw = self
            .db
            .get_cf(&out_msg_cf, tx_hash.as_slice())?
            .unwrap_or_default();

        let messages = bincode::deserialize::<Vec<[u8; 32]>>(children_out_messages_raw.as_slice())?
            .iter()
            .map(|x| UInt256::from_slice(x))
            .collect();
        Ok(messages)
    }
}

#[derive(Debug, Clone)]
pub struct Transaction {
    pub tx_hash: UInt256,
    pub boc: Vec<u8>,
    pub children: Vec<Transaction>,
}

mod tests {
    use crate::transaction_storage;
    use crate::transaction_storage::storage::TransactionStorage;
    use std::path::Path;
    use ton_types::UInt256;

    #[tokio::test]
    pub async fn test() {
        let path = Path::new("./test_db");
        let transaction_storage =
            TransactionStorage::new(path, 10).expect("Failed transaction storage");
        let transaction_hash_1 = UInt256::from_slice(
            hex::decode("54f47f19522023404e66999ebfdd029212efb917114579f4687afc40b3adb119")
                .expect("0")
                .as_slice(),
        );
        let incoming_message_1 = UInt256::from_slice(
            hex::decode("3d60c92fba3fa3440abd7e2f2ebcf4cd8d9c98c2567cdd0c03b2cadc857e11e4")
                .expect("1")
                .as_slice(),
        );

        let y = transaction_storage
            .add_transaction(
                transaction_hash_1.clone(),
                Some(incoming_message_1),
                None,
                &[],
                &[],
            )
            .expect("3");
        let x = transaction_storage
            .try_assemble_tree(&transaction_hash_1)
            .expect("2");
        if let Some(x) = x {
            println!("{:?}", x.tx_hash.as_slice());
        }
    }
}
