use std::path::Path;
use std::sync::Arc;
use ton_block::{
    GetRepresentationHash, HashmapAugType, Serializable, Transaction, TransactionDescr,
};
use ton_types::{serialize_toc, UInt256};

use crate::models::{TransactionNode, Tree};
use crate::output_handlers::OutputHandler;
use crate::transaction_storage::storage::TransactionStorage;
use crate::utils::error::Result;

pub struct TxTreeProducer {
    transaction_storage: Arc<TransactionStorage>,
    handlers: Vec<Box<dyn OutputHandler>>,
    lock: tokio::sync::RwLock<()>,
}

impl TxTreeProducer {
    pub fn new(db_path: String, handlers: Vec<Box<dyn OutputHandler>>) -> Result<Self> {
        tracing::info!("Transaction tree producer initialized");
        let transaction_storage = TransactionStorage::new(Path::new(&db_path))?;

        let storage_cloned = transaction_storage.clone();
        tokio::spawn(async move {
            if let Err(e) = storage_cloned.clean_transaction_trees().await {
                tracing::error!("{:?}", e)
            }
        });

        let lock = tokio::sync::RwLock::new(());

        Ok(Self {
            transaction_storage,
            handlers,
            lock,
        })
    }

    pub async fn handle_block(&self, block: &ton_block::Block) -> anyhow::Result<()> {
        let block_extra = block.read_extra()?;
        let custom = block_extra.read_custom()?;

        let sys_hash = match custom {
            Some(custom) => {
                let recover_message = custom.read_recover_create_msg()?;
                match recover_message {
                    Some(msg) => msg.transaction_cell().map(|x| x.repr_hash()),
                    None => None,
                }
            }
            None => None,
        };

        block_extra
            .read_account_blocks()?
            .iterate_objects(|account_block| {
                account_block.transactions().iterate_objects(|tx| {
                    let tx = &tx.inner();
                    let cur_hash = tx.hash()?;

                    let description = tx.description.read_struct()?;

                    match sys_hash {
                        Some(sys_hash) if cur_hash == sys_hash => Ok(true),
                        _ => match description {
                            TransactionDescr::Ordinary(_) => {
                                self.handle_transaction(tx, cur_hash)?;
                                Ok(true)
                            }
                            _ => Ok(true),
                        },
                    }
                })?;
                Ok(true)
            })?;

        {
            let _ = self.lock.write().await;
            let transactions = get_transaction_trees(self.transaction_storage.clone()).await?;
            for handler in self.handlers.as_slice() {
                let x = transactions.as_slice();
                handler.handle_output(x).await?;
            }

            self.transaction_storage
                .mark_transactions_as_processed(transactions.as_slice())?;
        }

        Ok(())
    }

    pub fn handle_transaction(&self, transaction: &Transaction, tx_hash: UInt256) -> Result<()> {
        let hex_hash = hex::encode(tx_hash.as_slice());
        tracing::debug!("Handling ordinary transaction: {hex_hash}");
        let boc = serialize_toc(&transaction.serialize()?)?;

        match transaction.in_msg.as_ref() {
            Some(in_msg) => {
                let message = in_msg.read_struct()?;
                tracing::debug!("Handling message: {}", hex::encode(message.hash()?));

                let mut out_msgs = Vec::with_capacity(transaction.out_msgs.len()?);

                transaction.out_msgs.iterate(|message| {
                    if message.as_ref().is_internal() {
                        //all_messages_external = false;
                        let hash = message.0.hash()?;

                        out_msgs.push(hash);
                    }

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

pub async fn get_transaction_trees(
    storage: Arc<TransactionStorage>,
) -> Result<Vec<TransactionNode>> {
    let trees = storage
        .try_assemble_trees_from_unprocessed_transaction()
        .await?;
    let mut full_trees: Vec<TransactionNode> = Vec::new();
    for tree in trees {
        match tree {
            Tree::Full(transaction) => {
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
