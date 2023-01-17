use rocksdb::{BoundColumnFamily, ColumnFamilyDescriptor, IteratorMode, Options, DB};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use ton_block::Serializable;
use ton_types::UInt256;

use crate::utils::error::Result;
use crate::utils::error::StorageError;
use crate::utils::storage_utils::{get_key_bytes, get_key_data_from_bytes};

const TX_EXT_IN_MSG: &str = "tx_external_in_msgs";
const TX_INT_IN_MSG: &str = "tx_internal_in_msgs";
const TX_INT_OUT_MSGS: &str = "tx_internal_out_msgs";
const TX_BOC: &str = "tx_boc";
const TX_DEPTH: &str = "tx_depth";
const TX_PROCESSED: &str = "tx_processed";
const TX_PREV: &str = "tx_previous_tx";

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

    fn get_prev_transaction_cf(&self) -> Arc<BoundColumnFamily> {
        self.db.cf_handle(TX_PREV).expect("Trust me")
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
        let tx_previous = ColumnFamilyDescriptor::new(TX_PREV, Options::default());

        vec![
            ext_in_msg_cf,
            int_in_msg_cf,
            out_msgs_cf,
            boc_cf,
            depth_cf,
            processed_cf,
            tx_previous,
        ]
    }

    pub fn add_transaction(
        &self,
        tx_hash: &UInt256,
        tx_lt: u64,
        previous_tx_hash: &UInt256,
        previous_tx_lt: u64,
        ext_in_msg_hash: Option<&UInt256>,
        int_in_msg_hash: Option<&UInt256>,
        boc: &[u8],
        int_out_msgs: &[UInt256],
    ) -> Result<()> {
        // tracing::error!(
        //     "Saving transaction lt: {}, hash: {}",
        //     tx_lt,
        //     hex::encode(tx_hash.as_slice())
        // );

        let start = broxus_util::now_ms_u64();
        let int_in_msg_cf = self.get_internal_in_message_cf();
        let ext_in_msg_cf = self.get_external_in_message_cf();
        let boc_cf = self.get_tx_boc_cf();
        let out_msgs_cf = self.get_internal_out_messages_cf();
        let depth_cf = self.get_tx_depth_cf();
        let processed_cf = self.get_tx_processed_cf();
        let previous_tx_cf = self.get_prev_transaction_cf();

        let tx_key = get_key_bytes(tx_lt, tx_hash);
        let parent_tx_key = get_key_bytes(previous_tx_lt, previous_tx_hash);

        let tx_key = tx_key.as_slice();
        let parent_tx_key = parent_tx_key.as_slice();

        let columns_t = broxus_util::now_ms_u64();
        tracing::debug!("Columns handles time: {} ms ", columns_t - start);

        let int_out_msgs = int_out_msgs.iter().map(|x| x.inner()).collect::<Vec<_>>();
        let int_out_msgs = bincode::serialize(int_out_msgs.as_slice())?;
        let ser = broxus_util::now_ms_u64();
        tracing::debug!("Internal out messages serialization {} ms", ser - columns_t);

        tracing::debug!(
            "External in msg: {:?}, Internal in msg: {:?}",
            ext_in_msg_hash,
            int_in_msg_hash
        );
        let depth = match (ext_in_msg_hash, int_in_msg_hash) {
            (Some(ext), None) => {
                self.db.put_cf(&ext_in_msg_cf, tx_key, ext.as_slice())?;
                1
            }
            (None, Some(int)) => match self.check_if_key_exists(parent_tx_key)? {
                Some(parent) => {
                    let end = broxus_util::now_ms_u64();
                    //tracing::warn!("Searching for parent transaction time {} ms", end - ser);
                    let parent_depth = self.get_depth(&parent)?;
                    match parent_depth {
                        Some(depth) => {
                            self.db.put_cf(&int_in_msg_cf, tx_key, int.as_slice())?;
                            depth + 1
                        }
                        None => {
                            let end = broxus_util::now_ms_u64();
                            tracing::warn!(
                                "Searching for parent transaction time {} ms",
                                end - ser
                            );
                            return Err(StorageError::TransactionDepthMissing(hex::encode(
                                parent.as_slice(),
                            )));
                        }
                    }
                }
                None if !self.applied_rules.search_for_parent => {
                    let end = broxus_util::now_ms_u64();
                    //tracing::warn!("Searching for parent transaction time {} ms", end - ser);
                    self.db.put_cf(&int_in_msg_cf, tx_key, int.as_slice())?;
                    1
                }
                _ => {
                    let end = broxus_util::now_ms_u64();
                    //tracing::warn!("Searching for parent transaction time {} ms", end - ser);
                    return Err(StorageError::ParentTransactionMissing(hex::encode(
                        int.as_slice(),
                    )));
                }
            },
            _ => return Err(StorageError::BadTransaction),
        };

        let start_saving = broxus_util::now_ms_u64();

        self.db.put_cf(&depth_cf, tx_key, depth.to_be_bytes())?;
        self.db.put_cf(&boc_cf, tx_key, boc)?;
        self.db.put_cf(&out_msgs_cf, tx_key, int_out_msgs)?;
        self.db.put_cf(&processed_cf, tx_key, &[0])?; //false
        self.db.put_cf(&previous_tx_cf, tx_key, parent_tx_key)?;

        let end_saving = broxus_util::now_ms_u64();
        //tracing::warn!("Saving tx data time {} ms", end_saving - start_saving);

        Ok(())
    }

    // pub fn recalc_tree_depth(&self, ) -> Result<()> {
    //
    // }

    fn mark_transaction_processed(&self, key: &[u8]) -> Result<()> {
        let processed_cf = self.get_tx_processed_cf();
        self.db.put_cf(&processed_cf, key, &[1])?;
        Ok(())
    }

    fn mark_transaction_not_processed(&self, key: &[u8]) -> Result<()> {
        let processed_cf = self.get_tx_processed_cf();
        self.db.put_cf(&processed_cf, key, &[0])?;
        Ok(())
    }

    fn mark_tree_as_processed(&self, transaction: &Transaction) -> Result<()> {
        let key = get_key_bytes(transaction.tx_lt, &transaction.tx_hash);
        self.mark_transaction_processed(key.as_slice())?;
        for t in transaction.children.iter() {
            self.mark_tree_as_processed(t)?
        }
        Ok(())
    }

    fn rollback_tree(&self, transaction: &Transaction) -> Result<()> {
        let key = get_key_bytes(transaction.tx_lt, &transaction.tx_hash);
        self.mark_transaction_not_processed(key.as_slice())?;
        for t in transaction.children.iter() {
            self.rollback_tree(t)?
        }
        Ok(())
    }

    pub fn clean_transaction_tree(&self, transaction: &Transaction) -> Result<()> {
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

        let columns = [
            int_in_msg_cf,
            ext_in_msg_cf,
            boc_cf,
            out_msgs_cf,
            depth_cf,
            processed_cf,
        ];

        let key = get_key_bytes(transaction.tx_lt, &transaction.tx_hash);

        for c in columns {
            self.db.delete_cf(&c, key.as_slice())?;
        }

        if !transaction.children.is_empty() {
            for i in transaction.children.iter() {
                self.clean_transaction_tree(i)?;
            }
        }

        Ok(())
    }

    pub fn try_reassemble_pending_trees(&self) -> Result<Vec<Tree>> {
        let mut trees = Vec::new();

        //let sem = Arc::new(tokio::sync::Semaphore::new(10));
        //let mut futures_ordered = FuturesOrdered::new();
        let column_cf = self.get_tx_processed_cf();

        for item in self.db.iterator_cf(&column_cf, IteratorMode::Start) {
            //let sem = sem.clone();
            match item {
                Ok((key, _)) => {
                    let (lt, hash) = get_key_data_from_bytes(key.as_ref());
                    // tracing::error!(
                    //     "Found transaction: lt: {}, hash: {}",
                    //     lt,
                    //     hex::encode(hash.as_slice())
                    // );
                    let out_messages = self.get_tx_out_messages(key.as_ref())?;

                    if !out_messages.is_empty() {
                        continue;
                    }

                    let maybe_tree = self.try_assemble_tree(key.as_ref());
                    match maybe_tree {
                        Ok(tree) => {
                            if matches!(tree, Tree::Full(_)) {
                                trees.push(tree);
                            }
                        }
                        Err(e) => {
                            tracing::error!("Failed to reassemble tree. Err: {e}");
                        }
                    }
                }
                Err(e) => {
                    tracing::error!("Failed to get pending external in messages. Err: {e}");
                    break;
                }
            }
        }

        Ok(trees)
    }

    pub fn try_assemble_tree(&self, key: &[u8]) -> Result<Tree> {
        let mut root: Option<Transaction> = None;
        let mut inititial_tx: Option<Transaction> = None;

        loop {
            // tracing::info!(
            //     "Started assemble loop. Root is {}",
            //     root.as_ref()
            //         .map(|x| hex::encode(x.tx_hash.inner()))
            //         .unwrap_or(String::from("Empty"))
            // );

            let (lt, hash) = get_key_data_from_bytes(key);

            if root.is_none() {
                let node = self.get_plain_node(lt, &hash)?;
                if node.is_none() {
                    tracing::error!(
                        "Node is empty. Cant find transaction for tx_hash: {}",
                        hex::encode(hash.inner())
                    );
                    return Ok(Tree::Empty);
                }
                root = node.clone();
                inititial_tx = node;
                continue;
            }

            let mut current_root = root.unwrap(); // always Some(_) because we check it on previous step

            let internal_message_opt = self.get_internal_in_message(key)?;
            let external_message_opt = self.get_external_in_message(key)?;
            // let out_messages = self.get_tx_out_messages(key)?;
            //
            // for i in out_messages {
            //     self.append_children_transaction_tree(&i, &mut current_root)?;
            // }

            // tracing::debug!(
            //     "Found internal and external message. {:?} {:?}",
            //     internal_message_opt.as_ref(),
            //     external_message_opt.as_ref()
            // );

            let (parent_transaction, int_message) =
                match (internal_message_opt, external_message_opt) {
                    (Some(message), None) => {
                        let key = get_key_bytes(current_root.tx_lt, &current_root.tx_hash);
                        (
                            self.find_parent_transaction(
                                key.as_slice(),
                                &inititial_tx.clone().map(|x| x.tx_hash).unwrap_or_default(),
                            )?,
                            message,
                        )
                    }
                    (None, Some(_)) => {
                        self.mark_tree_as_processed(&current_root)?;
                        return Ok(Tree::Full(current_root));
                    }
                    _ => return Err(StorageError::BadTransaction),
                };

            if let Some(mut parent) = parent_transaction {
                tracing::info!(
                    "Found parent transaction. {}",
                    hex::encode(parent.tx_hash.inner())
                );
                let key = get_key_bytes(parent.tx_lt, &parent.tx_hash);
                let out_msgs = self.get_tx_out_messages(key.as_slice())?;

                'out_mes: for i in &out_msgs {
                    if i == &int_message {
                        parent.children.push(current_root.clone());
                        continue 'out_mes;
                    }
                    tracing::info!(
                        "Appending child to parent. Out message hash: {}",
                        hex::encode(i.inner())
                    );
                    self.append_children_transaction_tree(&i, &mut parent)?;
                }
                root = Some(parent);
            } else {
                tracing::info!("No parent transaction stored. Partial tree");
                return Ok(Tree::Partial(current_root));
            }
        }
    }

    fn get_depth(&self, key: &[u8]) -> Result<Option<u32>> {
        let depth_cf = self.get_tx_depth_cf();
        let result = self
            .db
            .get_cf(&depth_cf, key)?
            .and_then(|x| bincode::deserialize::<u32>(x.as_slice()).ok());

        Ok(result)
    }

    fn find_parent_transaction(
        &self,
        parent_key: &[u8],
        initial: &UInt256,
    ) -> Result<Option<Transaction>> {
        tracing::info!(
            "Trying to find parent transaction. Current transaction: {}. Initial: {}",
            hex::encode(&parent_key[8..40]),
            hex::encode(initial.as_slice())
        );
        if let Some(hash) = self.check_if_key_exists(&parent_key)? {
            let (lt, hash) = get_key_data_from_bytes(hash.as_slice());
            self.get_plain_node(lt, &hash)
        } else {
            Ok(None)
        }
    }

    fn append_children_transaction_tree(
        &self,
        msg_hash: &UInt256,
        parent_node: &mut Transaction,
    ) -> Result<()> {
        let child_hash = self.get_children_transaction_by_msg_hash(msg_hash)?;
        let node_opt = match child_hash {
            Some(hash) => {
                let (lt, hash) = get_key_data_from_bytes(hash.as_slice());
                self.get_plain_node(lt, &hash)?
            }
            None => return Ok(()),
        };

        if let Some(node) = node_opt {
            let key = get_key_bytes(node.tx_lt, &node.tx_hash);
            let children_out_messages = self.get_tx_out_messages(key.as_slice())?;

            if children_out_messages.is_empty() {
                return Ok(());
            } else {
                for i in children_out_messages.iter() {
                    let tree_node_opt = match self.get_children_transaction_by_msg_hash(i)? {
                        Some(key) => {
                            let (lt, hash) = get_key_data_from_bytes(key.as_slice());
                            self.get_plain_node(lt, &hash)?
                        }
                        None => None,
                    };

                    if let Some(tree_node_opt) = tree_node_opt {
                        tracing::info!(
                            "Appending child {}",
                            hex::encode(tree_node_opt.tx_hash.inner())
                        );
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

            parent_node.children.push(node);
        }

        Ok(())
    }

    fn get_children_transaction_by_msg_hash(&self, msg_hash: &UInt256) -> Result<Option<Vec<u8>>> {
        let int_in_msg = self.get_internal_in_message_cf();

        for i in self.db.iterator_cf(&int_in_msg, IteratorMode::Start) {
            match i {
                Ok((key, value)) => {
                    if msg_hash.as_slice() == &value.as_ref() {
                        return Ok(Some(Vec::from(key.as_ref())));
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

    fn check_if_key_exists(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let parent_key = self.db.get(key)?;
        Ok(parent_key)
    }

    fn get_plain_node(&self, lt: u64, tx_hash: &UInt256) -> Result<Option<Transaction>> {
        let boc = self.get_tx_boc_cf();
        let key = get_key_bytes(lt, &tx_hash);
        Ok(self.db.get_cf(&boc, key.as_slice())?.map(|b| Transaction {
            tx_hash: tx_hash.clone(),
            tx_lt: lt,
            boc: b,
            children: Vec::new(),
        }))
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

        let messages = bincode::deserialize::<Vec<[u8; 32]>>(children_out_messages_raw.as_slice())?
            .iter()
            .map(|x| UInt256::from_slice(x))
            .collect();
        Ok(messages)
    }
}

pub enum Tree {
    Full(Transaction),
    Partial(Transaction),
    Empty,
}

#[derive(Debug, Clone)]
pub struct Transaction {
    tx_hash: UInt256,
    tx_lt: u64,
    boc: Vec<u8>,
    children: Vec<Transaction>,
}

impl Transaction {
    pub fn init_transaction_hash(&self) -> &UInt256 {
        &self.tx_hash
    }

    pub fn transaction_lt(&self) -> u64 {
        self.tx_lt
    }

    pub fn boc(&self) -> &[u8] {
        self.boc.as_slice()
    }

    pub fn base_64_boc(&self) -> String {
        let slice = self.boc.as_slice();
        base64::encode(slice)
    }

    pub fn root_children(&self) -> &[Transaction] {
        self.children.as_slice()
    }
}

struct StorageRules {
    max_tx_depth: u32,
    search_for_parent: bool,
}

pub mod tests {
    use crate::transaction_storage::storage::TransactionStorage;
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
        let tx_hash = "ad836b087e5b644aaa5078aa8040e8a9ed1699739eb28f0f83ce91a606cbfe3a";
        let transaction = hex::decode(tx_hash).expect("decode");
        let path = Path::new("./db");

        let mut db_opts = Options::default();

        db_opts.set_log_level(rocksdb::LogLevel::Error);
        db_opts.set_keep_log_file_num(2);
        db_opts.set_recycle_log_file_num(2);

        db_opts.create_missing_column_families(true);
        db_opts.create_if_missing(true);

        let columns = TransactionStorage::init_columns();

        let db = DB::open_cf_descriptors(&db_opts, path, columns).expect("descr");

        const TX_EXT_IN_MSG: &str = "tx_external_in_msgs";
        const TX_INT_IN_MSG: &str = "tx_internal_in_msgs";
        const TX_INT_OUT_MSGS: &str = "tx_internal_out_msgs";
        const TX_BOC: &str = "tx_boc";
        const TX_DEPTH: &str = "tx_depth";
        const TX_PROCESSED: &str = "tx_processed";

        let vec = vec![
            db.cf_handle(TX_EXT_IN_MSG).expect("TX_EXT_IN_MSG"),
            db.cf_handle(TX_INT_IN_MSG).expect("TX_INT_IN_MSG"),
            db.cf_handle(TX_INT_OUT_MSGS).expect("TX_INT_OUT_MSGS"),
            db.cf_handle(TX_BOC).expect("TX_BOC"),
            db.cf_handle(TX_DEPTH).expect("TX_DEPTH"),
            db.cf_handle(TX_PROCESSED).expect("TX_PROCESSED"),
        ];

        for i in vec.iter() {
            let x = db.get_cf(i, transaction.as_slice()).expect("column");
            println!("{:?}", x)
        }
    }
}
