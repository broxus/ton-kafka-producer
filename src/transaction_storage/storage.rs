use crate::models::TransactionNode;
use futures_util::stream::FuturesUnordered;
use futures_util::stream::StreamExt;
use parking_lot::Mutex;
use rocksdb::{BoundColumnFamily, ColumnFamilyDescriptor, IteratorMode, Options, WriteBatch, DB};
use schnellru::ByMemoryUsage;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;
use ton_types::{FxDashMap, UInt256};

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

const MSG_PARENT_TX: &str = "msg_parent_transaction";
const MSG_CHILD_TX: &str = "msg_child_transaction";

pub struct TransactionStorage {
    db: Arc<DB>,
    applied_rules: StorageRules,
    child_transaction_cache: Mutex<schnellru::LruMap<UInt256, TransactionNode, ByMemoryUsage>>,
    parent_transaction_cache: Mutex<schnellru::LruMap<UInt256, TransactionNode, ByMemoryUsage>>,
    out_messages_cache: Mutex<schnellru::LruMap<Vec<u8>, Vec<UInt256>, ByMemoryUsage>>,
    boc_cache: Mutex<schnellru::LruMap<Vec<u8>, Vec<u8>, ByMemoryUsage>>,
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
            db: Arc::new(db),
            applied_rules,
            child_transaction_cache: Mutex::new(schnellru::LruMap::with_memory_budget(
                1024 * 1024 * 1024,
            )),
            parent_transaction_cache: Mutex::new(schnellru::LruMap::with_memory_budget(
                1024 * 1024 * 1024,
            )),
            out_messages_cache: Mutex::new(schnellru::LruMap::with_memory_budget(
                1024 * 1024 * 1024,
            )),
            boc_cache: Mutex::new(schnellru::LruMap::with_memory_budget(1024 * 1024 * 1024)),
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
        let mut batch = rocksdb::WriteBatch::default();

        let int_in_msg_cf = self.get_internal_in_message_cf();
        let ext_in_msg_cf = self.get_external_in_message_cf();
        let boc_cf = self.get_tx_boc_cf();
        let out_msgs_cf = self.get_internal_out_messages_cf();
        let depth_cf = self.get_tx_depth_cf();
        let processed_cf = self.get_tx_processed_cf();

        let tx_key = get_key_bytes(tx_lt, tx_hash);

        let tx_key = tx_key.as_slice();

        let depth = match (ext_in_msg_hash, int_in_msg_hash) {
            (Some(ext), None) => {
                batch.put_cf(&ext_in_msg_cf, tx_key, ext.as_slice());
                self.save_message_connection_external(tx_key, out_msgs.as_slice(), &mut batch);
                1
            }
            (None, Some(int)) => {
                self.save_message_connection(tx_key, int, out_msgs.as_slice(), &mut batch);
                match self.get_parent_transaction_by(int)? {
                    Some(parent) => {
                        let parent_depth = self.get_depth(parent.db_key().as_slice())?;
                        match parent_depth {
                            Some(depth) => {
                                batch.put_cf(&int_in_msg_cf, tx_key, int.as_slice());
                                depth + 1
                            }
                            None => {
                                return Err(StorageError::TransactionDepthMissing(hex::encode(
                                    parent.hash().as_slice(),
                                )));
                            }
                        }
                    }
                    None => {
                        if !self.applied_rules.search_for_parent {
                            self.db.put_cf(&int_in_msg_cf, tx_key, int.as_slice())?;
                            1
                        } else {
                            return Err(StorageError::ParentTransactionMissing(hex::encode(
                                int.as_slice(),
                            )));
                        }
                    }
                }
            }
            _ => return Err(StorageError::BadTransaction),
        };

        let mut msgs: Vec<u8> = Vec::with_capacity(out_msgs.len() * 32usize);
        for i in out_msgs {
            msgs.extend_from_slice(i.as_slice());
        }

        batch.put_cf(&depth_cf, tx_key, depth.to_be_bytes());
        batch.put_cf(&boc_cf, tx_key, boc);
        batch.put_cf(&out_msgs_cf, tx_key, msgs.as_slice());
        batch.put_cf(&processed_cf, tx_key, [0]); //false

        if let Err(e) = self.db.write(batch) {
            tracing::error!("Failed to add transaction: {:?}", e);
        }

        Ok(())
    }

    fn mark_transaction_processed(&self, key: &[u8], wb: &mut WriteBatch) {
        let processed_cf = self.get_tx_processed_cf();
        wb.put_cf(&processed_cf, key, [1]);
    }

    fn mark_transaction_tree_map_as_processed(
        &self,
        transaction: &TransactionNode,
        tree_map: &mut Arc<FxDashMap<Vec<u8>, bool>>,
    ) {
        let key = get_key_bytes(transaction.lt(), transaction.hash());
        {
            let processed_opt = tree_map.get_mut(key.as_slice());
            if let Some(mut processed) = processed_opt {
                *processed = true;
            }
        }

        for i in transaction.children() {
            self.mark_transaction_tree_map_as_processed(i, tree_map);
        }
    }

    pub async fn clean_transaction_trees(&self) -> Result<()> {
        loop {
            {
                let columns = {
                    let int_in_msg_cf = self.get_internal_in_message_cf();
                    let ext_in_msg_cf = self.get_external_in_message_cf();
                    let boc_cf = self.get_tx_boc_cf();
                    let out_msgs_cf = self.get_internal_out_messages_cf();
                    let depth_cf = self.get_tx_depth_cf();
                    let processed_cf = self.get_tx_processed_cf();

                    let msg_parent_tx = self.get_message_parent_transaction_cf();
                    let msg_child_tx = self.get_message_child_transaction_cf();

                    [
                        int_in_msg_cf,
                        ext_in_msg_cf,
                        boc_cf,
                        out_msgs_cf,
                        depth_cf,
                        processed_cf,
                        msg_parent_tx,
                        msg_child_tx,
                    ]
                };

                let processed_cf = self.get_tx_processed_cf();
                let iter = self.db.iterator_cf(&processed_cf, IteratorMode::Start);
                let mut total = 0;
                let mut wb = WriteBatch::default();
                for i in iter {
                    match i {
                        Ok((key, value)) => {
                            if value.as_ref() == &[1] {
                                for c in columns.as_slice() {
                                    wb.delete_cf(c, key.as_ref())
                                }
                                total += 1;
                            }
                        }
                        Err(e) => {
                            tracing::error!("Failed to clean processed transactions. Err: {:?}", e);
                        }
                    }
                }
                self.db.write(wb)?;
                tracing::info!("Cleaned total of : {total} processed transactions");
            }
            tokio::time::sleep(Duration::from_secs(30)).await;
        }
    }

    pub async fn try_reassemble_pending_trees(&self) -> Result<Vec<Tree>> {
        let processed_map: Arc<FxDashMap<Vec<u8>, bool>> = Arc::new(FxDashMap::default());
        let mut pending_transactions: Vec<Vec<u8>> = Vec::new();

        {
            let column_cf = self.get_tx_processed_cf();
            let processed_cf_iterator = self.db.iterator_cf(&column_cf, IteratorMode::Start);

            //move current pending transactions to memory
            for i in processed_cf_iterator {
                match i {
                    Ok((key, value)) => {
                        let key = Vec::from(key.as_ref());
                        if value.as_ref() == &[0] {
                            processed_map.insert(key.to_vec(), false);
                            pending_transactions.push(key);
                        }
                    }
                    Err(e) => {
                        tracing::error!("Failed to get pending external in messages. Err: {e}");
                        processed_map.clear();
                        break;
                    }
                }
            }
        }

        let sem = Arc::new(tokio::sync::Semaphore::new(1000));
        let futures_ordered = FuturesUnordered::new();

        for pending_transaction in pending_transactions.iter() {
            let sem = sem.clone();
            let mut map = processed_map.clone();
            let future = async move {
                let _g = sem.acquire();
                let tree = self.try_process_pending_transaction(pending_transaction, &map);

                if let Ok(Some(Tree::Full(node))) = tree.as_ref() {
                    self.mark_transaction_tree_map_as_processed(node, &mut map);
                }

                tree
            };
            futures_ordered.push(future);
        }

        let mut trees = Vec::new();
        let completed = futures_ordered.collect::<Vec<_>>().await;

        for i in completed {
            match i {
                Ok(Some(tree)) => {
                    trees.push(tree);
                }
                _ => continue,
            }
        }

        let mut batch = WriteBatch::default();

        for i in processed_map.iter() {
            if *i.value() {
                self.mark_transaction_processed(i.key().as_slice(), &mut batch);
            }
        }

        self.db.write(batch)?;

        Ok(trees)
    }

    fn try_process_pending_transaction(
        &self,
        transaction_key: &Vec<u8>,
        processed_map: &FxDashMap<Vec<u8>, bool>,
    ) -> Result<Option<Tree>> {
        let (_, hash) = get_key_data_from_bytes(transaction_key.as_slice());
        tracing::debug!("Transaction {:x} picked for processing", &hash);
        let processed_opt = processed_map.get(transaction_key.as_slice());
        match processed_opt {
            Some(processed) => {
                tracing::debug!("Transaction {:x} processed = {}", &hash, processed.value());
                if *processed {
                    return Ok(None);
                }
            }
            None => {
                tracing::error!(
                    "Transaction {:x} does not exist in transaction processed map",
                    hash
                );
                return Err(StorageError::DataInconsistency);
            }
        }

        let out_messages = self.get_tx_out_messages(transaction_key.as_slice())?;

        if !out_messages.is_empty() {
            return Ok(None);
        }

        let tree = self.try_assemble_tree(transaction_key.as_slice())?;

        Ok(Some(tree))
    }

    pub fn try_assemble_tree(&self, key: &[u8]) -> Result<Tree> {
        let mut root: Option<TransactionNode> = None;

        loop {
            let (lt, hash) = get_key_data_from_bytes(key);

            if root.is_none() {
                tracing::debug!("Assembling first root for hash {:x}", &hash);
                let node = self.get_plain_node(lt, &hash)?;
                if node.is_none() {
                    tracing::debug!(
                        "Node is empty. Cant find transaction for tx_hash: {:x}",
                        hash
                    );
                    return Ok(Tree::Empty);
                }
                root = node.clone();

                continue;
            }

            let current_root = root.clone().unwrap(); // always Some(_) because we check it on previous step

            //find parent
            let internal_message_opt =
                self.get_internal_in_message(current_root.db_key().as_slice())?;
            let external_message_opt =
                self.get_external_in_message(current_root.db_key().as_slice())?;

            tracing::debug!("Finding parent for root: {:x}", current_root.hash());

            match (internal_message_opt, external_message_opt) {
                (Some(message), None) => {
                    let parent_opt = self.get_parent_transaction_by(&message)?;
                    match parent_opt {
                        Some(mut parent) => {
                            tracing::debug!(
                                "Parent found for root: {:x}. It is: {:x}",
                                current_root.hash(),
                                parent.hash()
                            );
                            let out_msgs = self.get_tx_out_messages(parent.db_key().as_slice())?;
                            'out_mes: for i in &out_msgs {
                                if i == message {
                                    parent.append_child(current_root.clone());
                                    continue 'out_mes;
                                }
                                tracing::debug!(
                                    "Appending child to parent. Out message hash: {:x}",
                                    i
                                );
                                if let Err(e) =
                                    self.append_children_transaction_tree(i, &mut parent)
                                {
                                    println!("Failed to append child to parent. Err: {e:?}");
                                }
                            }
                            root = Some(parent.clone());
                        }
                        None => {
                            tracing::debug!("Parent not found for root: {:x}", current_root.hash());
                            return Ok(Tree::Partial(current_root));
                        }
                    }
                }
                (None, Some(_)) => {
                    tracing::debug!(
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
    ) {
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
    ) {
        let parent_cf = self.get_message_parent_transaction_cf();

        for m in out_msgs {
            wb.put_cf(&parent_cf, m.as_slice(), tx);
        }
    }

    fn get_from_cache_parent(&self, msg_hash: &UInt256) -> Option<TransactionNode> {
        let mut guard = self.parent_transaction_cache.lock();
        guard.get(msg_hash).cloned()
    }

    fn get_parent_transaction_by(&self, out_msg_hash: &UInt256) -> Result<Option<TransactionNode>> {
        let parent = self.get_from_cache_parent(out_msg_hash);
        if let Some(parent) = parent {
            return Ok(Some(parent));
        }

        let parent_tx_cf = self.get_message_parent_transaction_cf();
        let node = match self.db.get_cf(&parent_tx_cf, out_msg_hash)? {
            Some(parent_tx) => {
                let (lt, hash) = get_key_data_from_bytes(parent_tx.as_slice());
                let node = self.get_plain_node(lt, &hash)?;
                if let Some(ref node) = node {
                    self.parent_transaction_cache
                        .lock()
                        .insert(out_msg_hash.clone(), node.clone());
                }
                node
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
        let node_opt = self.get_child_transaction_by(msg_hash)?;

        if let Some(mut node) = node_opt {
            let key = get_key_bytes(node.lt(), node.hash());
            let children_out_messages = self.get_tx_out_messages(key.as_slice())?;

            for i in children_out_messages.iter() {
                let tree_node_opt = self.get_child_transaction_by(i)?;

                if let Some(tree_node) = tree_node_opt {
                    tracing::trace!(
                        "Appending child {:x} to node {:x}",
                        tree_node.hash(),
                        parent_node.hash()
                    );
                    node.append_child(tree_node);
                }
            }

            let temp_node = node.clone();

            for n in node.children_mut() {
                tracing::trace!(
                    "Trying to process child {:x} for parent {:x}",
                    n.hash(),
                    temp_node.hash(),
                );
                let children_out_messages = self.get_tx_out_messages(n.db_key().as_slice())?;
                tracing::trace!(
                    "Node {:x} has {} children",
                    n.hash(),
                    children_out_messages.len(),
                );

                for mes in children_out_messages {
                    self.append_children_transaction_tree(&mes, n)?;
                }
            }

            tracing::trace!("Appended {:x} to {:x}", node.hash(), parent_node.hash());
            parent_node.append_child(node);
        } else {
            tracing::trace!("Failed to find ancestor info for {:x}", parent_node.hash())
        }

        Ok(())
    }

    fn get_from_cache_child(&self, msg_hash: &UInt256) -> Option<TransactionNode> {
        let mut guard = self.child_transaction_cache.lock();
        guard.get(msg_hash).cloned()
    }

    fn get_child_transaction_by(&self, msg_hash: &UInt256) -> Result<Option<TransactionNode>> {
        if let Some(node) = self.get_from_cache_child(msg_hash) {
            return Ok(Some(node));
        }

        let child_tx_cf = self.get_message_child_transaction_cf();
        let node = match self.db.get_cf(&child_tx_cf, msg_hash)? {
            Some(child_tx) => {
                let (lt, hash) = get_key_data_from_bytes(child_tx.as_slice());
                let node = self.get_plain_node(lt, &hash)?;
                let mut guard = self.child_transaction_cache.lock();
                if let Some(ref node) = node {
                    guard.insert(*msg_hash, node.clone());
                }
                node
            }
            None => None,
        };
        Ok(node)
    }

    fn get_from_cache_boc(&self, key: &[u8]) -> Option<Vec<u8>> {
        let mut guard = self.boc_cache.lock();
        guard.get(key).cloned()
    }

    fn get_plain_node(&self, lt: u64, tx_hash: &UInt256) -> Result<Option<TransactionNode>> {
        let boc = self.get_tx_boc_cf();
        let key = get_key_bytes(lt, tx_hash);
        let boc = match self.get_from_cache_boc(key.as_slice()) {
            Some(boc) => Some(boc),
            None => self.db.get_cf(&boc, key.as_slice())?,
        };

        if let Some(boc) = boc {
            let mut guard = self.boc_cache.lock();
            guard.insert(key.to_vec(), boc.clone());
            Ok(Some(TransactionNode::new(*tx_hash, lt, boc, Vec::new())))
        } else {
            Ok(None)
        }
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

    fn get_from_cache_out_msgs(&self, key: &[u8]) -> Option<Vec<UInt256>> {
        let mut guard = self.out_messages_cache.lock();
        guard.get(key).cloned()
    }

    fn get_tx_out_messages(&self, key: &[u8]) -> Result<Vec<UInt256>> {
        if let Some(messages) = self.get_from_cache_out_msgs(key) {
            return Ok(messages);
        }
        let out_msg_cf = self.get_internal_out_messages_cf();

        let children_out_messages_raw = self.db.get_cf(&out_msg_cf, key)?.unwrap_or_default();

        let mut messages: Vec<UInt256> = Vec::new();

        for ms in children_out_messages_raw.chunks(32) {
            messages.push(UInt256::from_slice(ms));
        }
        let mut guard = self.out_messages_cache.lock();
        guard.insert(key.to_vec(), messages.clone());

        Ok(messages)
    }
}

struct StorageRules {
    max_tx_depth: u32,
    search_for_parent: bool,
}

#[cfg(test)]
mod tests {
    use crate::transaction_storage::storage::{
        TransactionStorage, MSG_CHILD_TX, MSG_PARENT_TX, TX_EXT_IN_MSG, TX_INT_IN_MSG,
        TX_INT_OUT_MSGS, TX_PROCESSED,
    };
    use crate::utils::storage_utils::{get_key_bytes, get_key_data_from_bytes};
    use rocksdb::{DBWithThreadMode, IteratorMode, MultiThreaded, Options, DB};
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

    fn init_db() -> DBWithThreadMode<MultiThreaded> {
        let path = Path::new("../../db");

        let mut db_opts = Options::default();

        db_opts.set_log_level(rocksdb::LogLevel::Error);
        db_opts.set_keep_log_file_num(2);
        db_opts.set_recycle_log_file_num(2);

        db_opts.create_missing_column_families(true);
        db_opts.create_if_missing(true);

        let columns = TransactionStorage::init_columns();

        DB::open_cf_descriptors(&db_opts, path, columns).expect("descr")
    }
    #[test]
    pub fn get_all_data() {
        let db = init_db();
        let tx_processed_cf = db.cf_handle(TX_PROCESSED).expect("CHILD");
        let tx_processed_iter = db.iterator_cf(&tx_processed_cf, IteratorMode::Start);
        let mut i = 0;
        for _ in tx_processed_iter {
            i += 1;
        }
        println!("TOTAL UNPROCESSED: {}", i);
    }

    #[test]
    pub fn get_all_child_data() {
        let db = init_db();

        let tx_child = db.cf_handle(MSG_CHILD_TX).expect("CHILD");
        let tx_child_cf = db.iterator_cf(&tx_child, IteratorMode::Start);

        for i in tx_child_cf {
            match i {
                Ok((key, value)) => {
                    let (_lt, hash) = get_key_data_from_bytes(value.as_ref());
                    println!(
                        "msg: {} -> transation {:x}",
                        hex::encode(key.as_ref()),
                        hash
                    )
                }
                Err(e) => {
                    println!("ERROR: {:?}", e);
                }
            }
        }
    }

    #[test]
    pub fn get_all_external_out_msgs() {
        let db = init_db();
        let column = db.cf_handle(TX_INT_OUT_MSGS).expect("trust me");
        let out_msg_cf = db.iterator_cf(&column, IteratorMode::Start);
        for i in out_msg_cf {
            match i {
                Ok((key, value)) => {
                    let (lt, hash) = get_key_data_from_bytes(key.as_ref());
                    println!(
                        "Transaction with no out messages. {:x}. value {}",
                        hash,
                        hex::encode(value.as_ref())
                    );
                }
                Err(e) => {
                    println!("Failed. Err: {:?}", e);
                }
            }
        }
    }

    #[test]
    pub fn get_all_parent_data() {
        let db = init_db();

        let tx_parent = db.cf_handle(MSG_PARENT_TX).expect("PARENT");
        let tx_parent_cf = db.iterator_cf(&tx_parent, IteratorMode::Start);

        for i in tx_parent_cf {
            match i {
                Ok((key, value)) => {
                    let (_lt, hash) = get_key_data_from_bytes(value.as_ref());
                    println!(
                        "msg: {} -> transation {:x}",
                        hex::encode(key.as_ref()),
                        hash
                    )
                }
                Err(e) => {
                    println!("ERROR: {:?}", e);
                }
            }
        }
    }

    #[test]
    #[test]
    pub fn get_transaction_data() {
        //let tx_hash = "2a236fc4708994e6317cc986b0ef3775d93c1cd5f21e1c514271c4081cf252be";
        //let lt = 13315145000001u64;

        let tx_hash = "5f9dd27c531d7c94b63e52bc821059c899d1e868bdab6a80fb14cf2c00fd166f";
        let lt = 34771592000003u64;

        let transaction = hex::decode(tx_hash).expect("decode");
        let key = get_key_bytes(lt, &UInt256::from_slice(transaction.as_slice()));
        let db = init_db();

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
        let tx_child = db.cf_handle(MSG_CHILD_TX).expect("CHILD");

        match int_out_msgs_opt {
            Some(msgs) => {
                let _messages: Vec<UInt256> = Vec::new();

                for ms in msgs.chunks(32) {
                    println!("Int out msg: {}", hex::encode(ms));

                    let tx_next_opt = db.get_cf(&tx_child, ms).expect("column");
                    match tx_next_opt {
                        Some(next) => {
                            let (lt, hash) = get_key_data_from_bytes(next.as_slice());
                            println!("Next tx lt: {}, hash: {}", lt, hex::encode(hash.as_slice()));
                        }
                        None => {
                            println!("Next tx is empty");
                        }
                    }
                }
            }
            None => {
                println!("Internal out messages are empty");
            }
        }

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
