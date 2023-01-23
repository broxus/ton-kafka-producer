use crate::models::TransactionNode;
use rocksdb::{BoundColumnFamily, ColumnFamilyDescriptor, IteratorMode, Options, WriteBatch, DB};
use std::collections::HashMap;
use std::hash::Hash;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use ton_block::Serializable;
use ton_types::UInt256;

use crate::models::Tree;
use crate::utils::error::Result;
use crate::utils::error::StorageError;
use crate::utils::storage_utils::{get_key_bytes, get_key_data_from_bytes};

const TX_EXT_IN_MSG: &str = "tx_external_in_msgs";
const TX_INT_IN_MSG: &str = "tx_internal_in_msgs";
const TX_INT_OUT_MSGS: &str = "tx_internal_out_msgs";
const TX_BOC: &str = "tx_boc";
const TX_DEPTH: &str = "tx_depth";
const TX_PROCESSED: &str = "tx_processed";
//const TX_PREV: &str = "tx_previous_tx";

const MSG_PARENT_TX: &str = "msg_parent_transaction";
const MSG_CHILD_TX: &str = "msg_child_transaction";

pub struct TransactionStorage {
    file_db_path: PathBuf,
    db: Arc<DB>,
    applied_rules: StorageRules,
}

impl TransactionStorage {
    fn get_external_in_message_cf(&self) -> Arc<BoundColumnFamily> {
        self.db.cf_handle(TX_EXT_IN_MSG).expect("Trust me")
    }

    fn get_internal_in_message_cf(&self) -> Arc<BoundColumnFamily> {
        self.db.cf_handle(TX_INT_IN_MSG).expect("Trust me")
    }

    fn get_internal_out_messages_cf(&self) -> Arc<BoundColumnFamily> {
        self.db.cf_handle(TX_INT_OUT_MSGS).expect("Trust me")
    }

    fn get_tx_boc_cf(&self) -> Arc<BoundColumnFamily> {
        self.db.cf_handle(TX_BOC).expect("Trust me")
    }

    fn get_tx_depth_cf(&self) -> Arc<BoundColumnFamily> {
        self.db.cf_handle(TX_DEPTH).expect("Trust me")
    }

    fn get_tx_processed_cf(&self) -> Arc<BoundColumnFamily> {
        self.db.cf_handle(TX_PROCESSED).expect("trust me")
    }

    fn get_message_parent_transaction_cf(&self) -> Arc<BoundColumnFamily> {
        self.db.cf_handle(MSG_PARENT_TX).expect("trust me")
    }

    fn get_message_child_transaction_cf(&self) -> Arc<BoundColumnFamily> {
        self.db.cf_handle(MSG_CHILD_TX).expect("trust me")
    }

    pub fn new(
        file_db_path: &Path,
        max_depth: u32,
        search_for_parent: bool,
    ) -> Result<Arc<TransactionStorage>> {
        let mut db_opts = Options::default();

        db_opts.set_log_level(rocksdb::LogLevel::Error);
        db_opts.set_keep_log_file_num(2);
        db_opts.set_recycle_log_file_num(2);

        db_opts.create_missing_column_families(true);
        db_opts.create_if_missing(true);

        let columns = Self::init_columns();

        let db = DB::open_cf_descriptors(&db_opts, file_db_path, columns)?;
        let applied_rules = StorageRules {
            max_tx_depth: max_depth,
            search_for_parent,
        };

        Ok(Arc::new(Self {
            file_db_path: file_db_path.to_path_buf(),
            db: Arc::new(db),
            applied_rules,
        }))
    }

    fn init_columns() -> Vec<ColumnFamilyDescriptor> {
        let ext_in_msg_cf = ColumnFamilyDescriptor::new(TX_EXT_IN_MSG, Options::default());
        let int_in_msg_cf = ColumnFamilyDescriptor::new(TX_INT_IN_MSG, Options::default());
        let out_msgs_cf = ColumnFamilyDescriptor::new(TX_INT_OUT_MSGS, Options::default());
        let boc_cf = ColumnFamilyDescriptor::new(TX_BOC, Options::default());
        let depth_cf = ColumnFamilyDescriptor::new(TX_DEPTH, Options::default());
        let processed_cf = ColumnFamilyDescriptor::new(TX_PROCESSED, Options::default());
        let msg_parent_tx = ColumnFamilyDescriptor::new(MSG_PARENT_TX, Options::default());
        let msg_child_tx = ColumnFamilyDescriptor::new(MSG_CHILD_TX, Options::default());

        vec![
            ext_in_msg_cf,
            int_in_msg_cf,
            out_msgs_cf,
            boc_cf,
            depth_cf,
            processed_cf,
            msg_parent_tx,
            msg_child_tx,
        ]
    }

    pub fn add_transaction(
        &self,
        tx_hash: &UInt256,
        tx_lt: u64,
        ext_in_msg_hash: Option<&UInt256>,
        int_in_msg_hash: Option<&UInt256>,
        boc: &[u8],
        out_msgs: Vec<UInt256>,
    ) -> Result<()> {
        let start = broxus_util::now_ms_u64();

        let mut batch = rocksdb::WriteBatch::default();

        let int_in_msg_cf = self.get_internal_in_message_cf();
        let ext_in_msg_cf = self.get_external_in_message_cf();
        let boc_cf = self.get_tx_boc_cf();
        let out_msgs_cf = self.get_internal_out_messages_cf();
        let depth_cf = self.get_tx_depth_cf();
        let processed_cf = self.get_tx_processed_cf();

        let tx_key = get_key_bytes(tx_lt, tx_hash);
        // if previous_tx_lt == 0 {
        //     tracing::error!(
        //         "Saving transaction with empty parent. Hash: {}",
        //         hex::encode(tx_hash.as_slice())
        //     );
        //     //tracing::warn!("Saving transaction. Hash: {}", hex::encode(tx_hash.as_slice()))
        // }

        let tx_key = tx_key.as_slice();

        let depth = match (ext_in_msg_hash, int_in_msg_hash) {
            (Some(ext), None) => {
                batch.put_cf(&ext_in_msg_cf, tx_key, ext.as_slice());
                self.save_message_connection_external(&tx_key, out_msgs.as_slice(), &mut batch);
                1
            }
            (None, Some(int)) => match self.get_parent_transaction_by(int)? {
                Some(parent) => {
                    let parent_depth = self.get_depth(parent.db_key().as_slice())?;
                    match parent_depth {
                        Some(depth) => {
                            batch.put_cf(&int_in_msg_cf, tx_key, int.as_slice());
                            self.save_message_connection(
                                &tx_key,
                                &int,
                                out_msgs.as_slice(),
                                &mut batch,
                            );
                            depth + 1
                        }
                        None => {
                            return Err(StorageError::TransactionDepthMissing(hex::encode(
                                parent.hash().as_slice(),
                            )));
                        }
                    }
                }
                None if !self.applied_rules.search_for_parent => {
                    self.db.put_cf(&int_in_msg_cf, tx_key, int.as_slice())?;
                    1
                }
                _ => {
                    return Err(StorageError::ParentTransactionMissing(hex::encode(
                        int.as_slice(),
                    )));
                }
            },
            _ => return Err(StorageError::BadTransaction),
        };

        let mut msgs: Vec<u8> = Vec::with_capacity(out_msgs.len() * 32usize);
        for i in out_msgs {
            msgs.extend_from_slice(i.as_slice());
        }

        batch.put_cf(&depth_cf, tx_key, depth.to_be_bytes());
        batch.put_cf(&boc_cf, tx_key, boc);
        batch.put_cf(&out_msgs_cf, tx_key, msgs.as_slice());
        batch.put_cf(&processed_cf, tx_key, &[0]); //false

        self.db.write(batch)?;

        Ok(())
    }

    fn mark_transaction_processed(&self, key: &[u8], wb: &mut WriteBatch) -> () {
        let (_, hash) = get_key_data_from_bytes(key);
        //tracing::info!("Marking transaction as processed: {:x}", hash);
        let processed_cf = self.get_tx_processed_cf();
        wb.put_cf(&processed_cf, key, &[1]);
    }

    fn mark_transaction_not_processed(&self, key: &[u8]) -> Result<()> {
        let processed_cf = self.get_tx_processed_cf();
        self.db.put_cf(&processed_cf, key, &[0])?;
        Ok(())
    }

    // fn mark_tree_as_processed(&self, transaction: &TransactionNode) -> Result<()> {
    //     self.mark_transaction_processed(transaction.db_key().as_slice())?;
    //     for t in transaction.children().iter() {
    //         self.mark_tree_as_processed(t)?
    //     }
    //     Ok(())
    // }

    fn rollback_tree(&self, transaction: &TransactionNode) -> Result<()> {
        self.mark_transaction_not_processed(transaction.db_key().as_slice())?;
        for t in transaction.children().iter() {
            self.rollback_tree(t)?
        }
        Ok(())
    }

    fn mark_transaction_tree_map_as_processed(
        &self,
        transaction: &TransactionNode,
        tree_map: &mut HashMap<UInt256, bool>,
    ) -> () {
        let mut processed_opt = tree_map.get_mut(transaction.hash());
        if let Some(processed) = processed_opt {
            *processed = true;
        }

        for i in transaction.children() {
            self.mark_transaction_tree_map_as_processed(i, tree_map);
        }
    }

    pub fn clean_transaction_tree(&self, transaction: &TransactionNode) -> Result<()> {
        // tracing::info!(
        //     "Cleaning transaction tree: {}",
        //     hex::encode(transaction.tx_hash.inner())
        // );

        let int_in_msg_cf = self.get_internal_in_message_cf();
        let ext_in_msg_cf = self.get_external_in_message_cf();
        let boc_cf = self.get_tx_boc_cf();
        let out_msgs_cf = self.get_internal_out_messages_cf();
        let depth_cf = self.get_tx_depth_cf();
        let processed_cf = self.get_tx_processed_cf();

        let msg_parent_tx = self.get_message_parent_transaction_cf();
        let msg_child_tx = self.get_message_child_transaction_cf();

        let columns = [
            int_in_msg_cf,
            ext_in_msg_cf,
            boc_cf,
            out_msgs_cf,
            depth_cf,
            processed_cf,
            msg_parent_tx,
            msg_child_tx,
        ];

        let key = get_key_bytes(transaction.lt(), transaction.hash());

        for c in columns {
            self.db.delete_cf(&c, key.as_slice())?;
        }

        if !transaction.children().is_empty() {
            for i in transaction.children().iter() {
                self.clean_transaction_tree(i)?;
            }
        }

        Ok(())
    }

    pub async fn try_reassemble_pending_trees(&self) -> Result<Vec<Tree>> {
        let mut trees = Vec::new();

        //let sem = Arc::new(tokio::sync::Semaphore::new(10));
        //let mut futures_ordered = FuturesOrdered::new();
        let column_cf = self.get_tx_processed_cf();

        let mut processed_map: HashMap<UInt256, bool> = HashMap::new();
        let mut pending_transaction: Vec<Vec<u8>> = Vec::new();

        let mut processed_cf_iterator = self.db.iterator_cf(&column_cf, IteratorMode::Start);

        for i in processed_cf_iterator {
            match i {
                Ok((key, value)) => {
                    let (_, hash) = get_key_data_from_bytes(key.as_ref());
                    processed_map.insert(hash.clone(), false);
                    pending_transaction.push(Vec::from(key.as_ref()));
                }
                Err(e) => {
                    tracing::error!("Failed to get pending external in messages. Err: {e}");
                    processed_map.clear();
                    break;
                }
            }
        }

        for pending_transaction in pending_transaction.iter() {
            let (_, hash) = get_key_data_from_bytes(pending_transaction.as_slice());
            tracing::info!("Transaction {:x} picked for processing", &hash);
            let processed_opt = processed_map.get(&hash);
            match processed_opt {
                Some(processed) => {
                    tracing::info!("Transaction {:x} processed = {}", &hash, processed);
                    if *processed {
                        continue;
                    }
                }
                None => {
                    tracing::error!(
                        "Transaction {:x} does not exist in transaction processed map",
                        hash
                    );
                    continue; //maybe return Err here?
                }
            }

            let out_messages = self.get_tx_out_messages(pending_transaction.as_slice())?;

            if !out_messages.is_empty() {
                continue;
            }

            tracing::error!("Processing transaction: {:x}", hash);

            let maybe_tree = self.try_assemble_tree(pending_transaction.as_slice());
            match maybe_tree {
                Ok(Tree::Full(node)) => {
                    self.mark_transaction_tree_map_as_processed(&node, &mut processed_map);
                    trees.push(Tree::Full(node));
                }
                Err(e) => {
                    tracing::error!("Failed to reassemble tree. Err: {e}");
                }
                _ => continue,
            }
        }

        let mut batch = WriteBatch::default();

        processed_cf_iterator = self.db.iterator_cf(&column_cf, IteratorMode::Start);

        for processed_res in processed_cf_iterator {
            if let Ok((key, _)) = processed_res {
                let (_, hash) = get_key_data_from_bytes(key.as_ref());
                let processed = processed_map.get(&hash).map(|x| *x);
                if let Some(true) = processed {
                    self.mark_transaction_processed(key.as_ref(), &mut batch);
                }
            }
        }

        self.db.write(batch)?;

        Ok(trees)
    }

    pub fn try_assemble_tree(
        &self,
        key: &[u8],
        //processed_transactions: &mut HashMap<UInt256, bool>,
    ) -> Result<Tree> {
        let mut root: Option<TransactionNode> = None;
        //let mut inititial_tx: Option<TransactionNode> = None;

        loop {
            //let mut temp = root.clone();
            let (lt, hash) = get_key_data_from_bytes(key);

            if root.is_none() {
                tracing::info!("Assembling first root for hash {:x}", &hash);
                let node = self.get_plain_node(lt, &hash)?;
                if node.is_none() {
                    tracing::error!(
                        "Node is empty. Cant find transaction for tx_hash: {}",
                        hex::encode(hash.inner())
                    );
                    return Ok(Tree::Empty);
                }
                root = node.clone();

                continue;
            }

            let mut current_root = root.clone().unwrap(); // always Some(_) because we check it on previous step

            //append children tx to current node
            tracing::info!("Finding ancestors for root: {:x}", current_root.hash());
            let key = get_key_bytes(current_root.lt(), &current_root.hash());
            let out_msgs = self.get_tx_out_messages(key.as_slice())?;
            tracing::info!(
                "Found {} ancestors for root {:x}",
                out_msgs.len(),
                current_root.hash()
            );

            'out_mes: for i in &out_msgs {
                if let Err(e) = self.append_children_transaction_tree(&i, &mut current_root) {
                    tracing::error!("Failed to append child to parent. Err: {e:?}");
                }
            }

            //find parent
            let internal_message_opt =
                self.get_internal_in_message(current_root.db_key().as_slice())?;
            let external_message_opt =
                self.get_external_in_message(current_root.db_key().as_slice())?;

            tracing::info!("Finding parent for root: {:x}", current_root.hash());

            match (internal_message_opt, external_message_opt) {
                (Some(message), None) => {
                    ////let key = get_key_bytes(current_root.lt(), &current_root.hash());
                    let parent_opt = self.get_parent_transaction_by(&message)?;
                    match parent_opt {
                        Some(mut parent) => {
                            tracing::info!(
                                "Parent found for root: {:x}. It is: {:x}",
                                current_root.hash(),
                                parent.hash()
                            );
                            let out_msgs = self.get_tx_out_messages(parent.db_key().as_slice())?;
                            'out_mes: for i in &out_msgs {
                                if i == &message {
                                    parent.append_child(current_root.clone());
                                    continue 'out_mes;
                                }
                                tracing::info!(
                                    "Appending child to parent. Out message hash: {}",
                                    hex::encode(i.inner())
                                );
                                if let Err(e) =
                                    self.append_children_transaction_tree(&i, &mut parent)
                                {
                                    tracing::error!("Failed to append child to parent. Err: {e:?}");
                                }
                            }
                            root = Some(parent.clone());
                        }
                        None => {
                            tracing::info!("Parent not found for root: {:x}", current_root.hash());
                            return Ok(Tree::Partial(current_root));
                        }
                    }
                }
                (None, Some(_)) => {
                    tracing::info!(
                        "Node {:x} has no parents. Top level node",
                        current_root.hash()
                    );
                    return Ok(Tree::Full(current_root));
                }
                _ => return Err(StorageError::BadTransaction),
            };
        }
    }

    //fn append_curren

    fn get_depth(&self, key: &[u8]) -> Result<Option<u32>> {
        let depth_cf = self.get_tx_depth_cf();
        let result = self
            .db
            .get_cf(&depth_cf, key)?
            .and_then(|x| bincode::deserialize::<u32>(x.as_slice()).ok());

        Ok(result)
    }

    fn save_message_connection(
        &self,
        tx: &[u8],
        in_msg: &UInt256,
        out_msgs: &[UInt256],
        wb: &mut WriteBatch,
    ) -> () {
        let parent_cf = self.get_message_parent_transaction_cf();
        let child_cf = self.get_message_child_transaction_cf();

        wb.put_cf(&child_cf, in_msg.as_slice(), tx);

        for m in out_msgs {
            wb.put_cf(&parent_cf, m.as_slice(), tx);
        }
    }

    fn save_message_connection_external(
        &self,
        tx: &[u8],
        out_msgs: &[UInt256],
        wb: &mut WriteBatch,
    ) -> () {
        let parent_cf = self.get_message_parent_transaction_cf();

        for m in out_msgs {
            wb.put_cf(&parent_cf, m.as_slice(), tx);
        }
    }

    fn get_parent_transaction_by(&self, out_msg_hash: &UInt256) -> Result<Option<TransactionNode>> {
        let parent_tx_cf = self.get_message_parent_transaction_cf();
        let node = match self.db.get_cf(&parent_tx_cf, out_msg_hash)? {
            Some(parent_tx) => {
                let (lt, hash) = get_key_data_from_bytes(parent_tx.as_slice());
                self.get_plain_node(lt, &hash)?
            }
            None => None,
        };
        Ok(node)
    }

    fn append_children_transaction_tree(
        &self,
        msg_hash: &UInt256,
        parent_node: &mut TransactionNode,
    ) -> Result<()> {
        let node_opt = self.get_child_transaction_by(parent_node.lt(), msg_hash)?;

        if let Some(mut node) = node_opt {
            let key = get_key_bytes(node.lt(), &node.hash());
            let children_out_messages = self.get_tx_out_messages(key.as_slice())?;

            if !children_out_messages.is_empty() {
                for i in children_out_messages.iter() {
                    let tree_node_opt = self.get_child_transaction_by(node.lt(), i)?;

                    if let Some(tree_node_opt) = tree_node_opt {
                        tracing::info!(
                            "Appending child {} to node {}",
                            hex::encode(tree_node_opt.hash().inner()),
                            hex::encode(parent_node.hash().inner())
                        );
                        node.append_child(tree_node_opt);
                    }
                }
            }

            for (node, message) in node
                .children_mut()
                .iter_mut()
                .zip(children_out_messages.iter())
            {
                self.append_children_transaction_tree(message, node)?;
            }

            parent_node.append_child(node);
        }

        Ok(())
    }

    fn get_child_transaction_by(
        &self,
        lt: u64,
        msg_hash: &UInt256,
    ) -> Result<Option<TransactionNode>> {
        let child_tx_cf = self.get_message_child_transaction_cf();
        let node = match self.db.get_cf(&child_tx_cf, msg_hash)? {
            Some(child_tx) => {
                let (lt, hash) = get_key_data_from_bytes(child_tx.as_slice());
                self.get_plain_node(lt, &hash)?
            }
            None => None,
        };
        Ok(node)
    }

    fn get_plain_node(&self, lt: u64, tx_hash: &UInt256) -> Result<Option<TransactionNode>> {
        let boc = self.get_tx_boc_cf();
        let key = get_key_bytes(lt, &tx_hash);
        Ok(self
            .db
            .get_cf(&boc, key.as_slice())?
            .map(|b| TransactionNode::new(tx_hash.clone(), lt, b, Vec::new())))
    }

    fn get_internal_in_message(&self, key: &[u8]) -> Result<Option<UInt256>> {
        let int_in_msg = self.get_internal_in_message_cf();
        let message = self
            .db
            .get_cf(&int_in_msg, key)?
            .map(|x| UInt256::from_slice(x.as_slice()));
        Ok(message)
    }

    fn get_external_in_message(&self, key: &[u8]) -> Result<Option<UInt256>> {
        let ext_in_msg = self.get_external_in_message_cf();
        let message = self
            .db
            .get_cf(&ext_in_msg, key)?
            .map(|x| UInt256::from_slice(x.as_slice()));
        Ok(message)
    }

    fn get_tx_out_messages(&self, key: &[u8]) -> Result<Vec<UInt256>> {
        let out_msg_cf = self.get_internal_out_messages_cf();

        let children_out_messages_raw = self.db.get_cf(&out_msg_cf, key)?.unwrap_or_default();

        let mut messages: Vec<UInt256> = Vec::new();

        for ms in children_out_messages_raw.chunks(32) {
            messages.push(UInt256::from_slice(ms));
        }
        Ok(messages)
    }
}

struct StorageRules {
    max_tx_depth: u32,
    search_for_parent: bool,
}

pub mod tests {
    use crate::transaction_storage::storage::{
        TransactionStorage, MSG_PARENT_TX, TX_EXT_IN_MSG, TX_INT_IN_MSG, TX_INT_OUT_MSGS,
        TX_PROCESSED,
    };
    use crate::utils::storage_utils::{get_key_bytes, get_key_data_from_bytes};
    use rocksdb::{Options, DB};
    use std::path::Path;
    use std::str::FromStr;
    use ton_types::UInt256;

    #[test]
    pub fn key_bytes() {
        let (lt, hash) = (
            13259412000001,
            UInt256::from_str("fb8882413dd7d549231c0f94600ba5fa827bc56b8bd570e1d3137c43bcd876f9")
                .expect("Failed tx"),
        );
        let key = get_key_bytes(lt, &hash);
        let (new_lt, new_hash) = get_key_data_from_bytes(key.as_slice());

        assert_eq!(new_lt, lt);
        assert_eq!(new_hash, hash);
    }
    #[tokio::test]
    pub async fn test() {
        let path = Path::new("./test_db");
        let transaction_storage =
            TransactionStorage::new(path, 10, false).expect("Failed transaction storage");
        let transaction_hash_1 = UInt256::from_slice(
            hex::decode("54f47f19522023404e66999ebfdd029212efb917114579f4687afc40b3adb119")
                .expect("00")
                .as_slice(),
        );
        let incoming_message_1 = UInt256::from_slice(
            hex::decode("3d60c92fba3fa3440abd7e2f2ebcf4cd8d9c98c2567cdd0c03b2cadc857e11e4")
                .expect("01")
                .as_slice(),
        );

        let outcoming_message_1 = UInt256::from_slice(
            hex::decode("3d60c92fba3fa3440abd7e2f2ebcf4cd8d9c98c2567cdd0c03b2cadc857e11e3")
                .expect("001")
                .as_slice(),
        );

        let outcoming_message_2 = UInt256::from_slice(
            hex::decode("3d60c92fba3fa3440abd7e2f2ebcf4cd8d9c98c2567cdd0c03b2cadc857e11ea")
                .expect("002")
                .as_slice(),
        );

        // transaction_storage
        //     .add_transaction(
        //         &transaction_hash_1,
        //         Some(&incoming_message_1),
        //         None,
        //         &[0, 1, 1, 1],
        //         &[outcoming_message_1, outcoming_message_2],
        //     )
        //     .expect("3");

        let transaction_hash_2 = UInt256::from_slice(
            hex::decode("54f47f19522023404e66999ebfdd029212efb917114579f4687afc40b3adb110")
                .expect("0")
                .as_slice(),
        );
        let incoming_message_2 = UInt256::from_slice(
            hex::decode("3d60c92fba3fa3440abd7e2f2ebcf4cd8d9c98c2567cdd0c03b2cadc857e11e3")
                .expect("1")
                .as_slice(),
        );

        // transaction_storage
        //     .add_transaction(
        //         &transaction_hash_2,
        //         None,
        //         Some(&incoming_message_2),
        //         &[0, 1, 1, 1],
        //         &[],
        //     )
        //     .expect("4");

        let transaction_hash_3 = UInt256::from_slice(
            hex::decode("54f47f19522023404e66999ebfdd029212efb917114579f4687afc40b3adb1bc")
                .expect("0")
                .as_slice(),
        );
        let incoming_message_3 = UInt256::from_slice(
            hex::decode("3d60c92fba3fa3440abd7e2f2ebcf4cd8d9c98c2567cdd0c03b2cadc857e11ea")
                .expect("1")
                .as_slice(),
        );

        // transaction_storage
        //     .add_transaction(
        //         &transaction_hash_3,
        //         None,
        //         Some(&incoming_message_3),
        //         &[0, 1, 1, 1],
        //         &[],
        //     )
        //     .expect("4");

        // let x = transaction_storage
        //     .try_assemble_tree(&transaction_hash_2)
        //     .expect("2");
        // if let Some(x) = x {
        //     println!("{:?}", x);
        // }
    }

    #[tokio::test]
    pub async fn get_transaction_data() {
        //let tx_hash = "2a236fc4708994e6317cc986b0ef3775d93c1cd5f21e1c514271c4081cf252be";
        //let lt = 13315145000001u64;

        let tx_hash = "b618d7c26b41e83cab6246ef03e28efb27018adbd41591712c11fec0ca606839";
        let lt = 13411941000006u64;

        let transaction = hex::decode(tx_hash).expect("decode");
        let key = get_key_bytes(lt, &UInt256::from_slice(transaction.as_slice()));
        let path = Path::new("./db");

        let mut db_opts = Options::default();

        db_opts.set_log_level(rocksdb::LogLevel::Error);
        db_opts.set_keep_log_file_num(2);
        db_opts.set_recycle_log_file_num(2);

        db_opts.create_missing_column_families(true);
        db_opts.create_if_missing(true);

        let columns = TransactionStorage::init_columns();

        let db = DB::open_cf_descriptors(&db_opts, path, columns).expect("descr");

        let tx_ext_in_msg = db.cf_handle(TX_EXT_IN_MSG).expect("TX_EXT_IN_MSG");
        let ext_in_msg_opt = db.get_cf(&tx_ext_in_msg, key.as_slice()).expect("column");
        match ext_in_msg_opt {
            Some(msg) => {
                println!("External in message: {}", hex::encode(msg.as_slice()));
            }
            None => {
                println!("External in message is empty");
            }
        }

        let tx_int_in_msg = db.cf_handle(TX_INT_IN_MSG).expect("TX_INT_IN_MSG");
        let int_in_msg_opt = db.get_cf(&tx_int_in_msg, key.as_slice()).expect("column");
        let in_msg = match int_in_msg_opt {
            Some(msg) => {
                println!("Internal in message: {}", hex::encode(msg.as_slice()));
                msg
            }
            None => {
                println!("Internal in message is empty");
                Vec::new()
            }
        };

        let tx_int_out_msg = db.cf_handle(TX_INT_OUT_MSGS).expect("TX_INT_OUT_MSGS");
        let int_out_msgs_opt = db.get_cf(&tx_int_out_msg, key.as_slice()).expect("column");
        match int_out_msgs_opt {
            Some(msgs) => {
                let mut messages: Vec<UInt256> = Vec::new();

                for ms in msgs.chunks(32) {
                    println!("Int out msg: {}", hex::encode(ms));
                    //messages.push(UInt256::from_slice(ms));
                }
            }
            None => {
                println!("Internal out messages are empty");
            }
        }

        //db.cf_handle(TX_BOC).expect("TX_BOC"),
        //db.cf_handle(TX_DEPTH).expect("TX_DEPTH"),

        let tx_parent = db.cf_handle(MSG_PARENT_TX).expect("PARENT");
        let tx_prev_opt = db.get_cf(&tx_parent, in_msg.as_slice()).expect("column");
        match tx_prev_opt {
            Some(prev) => {
                let (lt, hash) = get_key_data_from_bytes(prev.as_slice());
                println!("Prev tx lt: {}, hash: {}", lt, hex::encode(hash.as_slice()));
            }
            None => {
                println!("Prev tx is empty");
            }
        }

        let processed = db.cf_handle(TX_PROCESSED).expect("TX_PROCESSED");
        let processed = db.get_cf(&processed, key.as_slice()).expect("column");

        match processed {
            Some(processed) => {
                println!("Processed: {:?}", processed.as_slice());
            }
            None => {
                println!("Processed is empty");
            }
        }
    }
}
