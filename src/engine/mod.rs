use std::sync::Arc;

use anyhow::{Context, Result};
use futures::TryStreamExt;
use once_cell::sync::OnceCell;
use tiny_adnl::utils::*;
use ton_block::{Deserializable, HashmapAugType, Serializable};
use ton_block_compressor::ZstdWrapper;
use ton_indexer::utils::*;
use ton_types::{HashmapType, UInt256};

use crate::config::*;

use self::kafka_producer::*;

mod kafka_producer;

pub struct Engine {
    indexer: Arc<ton_indexer::Engine>,
}

impl Engine {
    pub async fn new(
        config: AppConfig,
        global_config: ton_indexer::GlobalConfig,
    ) -> Result<Arc<Self>> {
        let subscriber: Arc<dyn ton_indexer::Subscriber> =
            TonSubscriber::new(config.kafka_settings)?;

        let indexer = ton_indexer::Engine::new(
            config
                .node_settings
                .build_indexer_config()
                .await
                .context("Failed to build node config")?,
            global_config,
            vec![subscriber],
        )
        .await
        .context("Failed to start TON node")?;

        Ok(Arc::new(Self { indexer }))
    }

    pub async fn start(self: &Arc<Self>) -> Result<()> {
        self.indexer.start().await?;
        Ok(())
    }
}

struct TonSubscriber {
    compressor: ton_block_compressor::ZstdWrapper,
    block_producer: Option<KafkaProducer>,
    raw_block_producer: Option<KafkaProducer>,
    raw_transaction_producer: Option<KafkaProducer>,
    message_producer: Option<KafkaProducer>,
    transaction_producer: Option<KafkaProducer>,
    account_producer: Option<KafkaProducer>,
    block_proof_producer: Option<KafkaProducer>,
}

impl TonSubscriber {
    fn new(config: KafkaConfig) -> Result<Arc<Self>> {
        fn make_producer(config: Option<KafkaProducerConfig>) -> Result<Option<KafkaProducer>> {
            config.map(KafkaProducer::new).transpose()
        }

        Ok(Arc::new(Self {
            compressor: Default::default(),
            block_producer: make_producer(config.block_producer)?,
            raw_block_producer: make_producer(config.raw_block_producer)?,
            raw_transaction_producer: make_producer(config.raw_transaction_producer)?,
            message_producer: make_producer(config.message_producer)?,
            transaction_producer: make_producer(config.transaction_producer)?,
            account_producer: make_producer(config.account_producer)?,
            block_proof_producer: make_producer(config.block_proof_producer)?,
        }))
    }
}

impl TonSubscriber {
    async fn handle_block(
        &self,
        block_stuff: &BlockStuff,
        block_proof: Option<&BlockProofStuff>,
        shard_state: Option<&ShardStateStuff>,
    ) -> Result<()> {
        let records = match self.prepare_records(block_stuff, block_proof, shard_state) {
            Ok(records) => records,
            Err(e) => {
                log::error!("Failed to process block {}: {:?}", block_stuff.id(), e);
                return Ok(());
            }
        };

        let futures = futures::stream::FuturesUnordered::new();

        for record in records {
            match record {
                DbRecord::Message(key, value) => {
                    if let Some(producer) = &self.message_producer {
                        futures.push(producer.write(key.into_bytes(), value.into_bytes(), None));
                    }
                }
                DbRecord::Transaction(key, value) => {
                    if let Some(producer) = &self.transaction_producer {
                        futures.push(producer.write(key.into_bytes(), value.into_bytes(), None));
                    }
                }
                DbRecord::Account(key, value) => {
                    if let Some(producer) = &self.account_producer {
                        futures.push(producer.write(key.into_bytes(), value.into_bytes(), None));
                    }
                }
                DbRecord::Block(key, value) => {
                    if let Some(producer) = &self.block_producer {
                        futures.push(producer.write(key.into_bytes(), value.into_bytes(), None));
                    }
                }
                DbRecord::BlockProof(key, value) => {
                    if let Some(producer) = &self.block_proof_producer {
                        futures.push(producer.write(key.into_bytes(), value.into_bytes(), None));
                    }
                }
                DbRecord::RawBlock(key, value) => {
                    if let Some(producer) = &self.raw_block_producer {
                        let now = chrono::Utc::now().timestamp();
                        futures.push(producer.write(key, value, Some(now)));
                    }
                }
                DbRecord::RawTransaction(key, value) => {
                    if let Some(producer) = &self.raw_transaction_producer {
                        let now = chrono::Utc::now().timestamp();
                        futures.push(producer.write(key, value, Some(now)));
                    }
                }
            }
        }

        futures.try_collect::<Vec<_>>().await?;

        Ok(())
    }

    fn prepare_records(
        &self,
        block_stuff: &BlockStuff,
        block_proof: Option<&BlockProofStuff>,
        shard_state: Option<&ShardStateStuff>,
    ) -> Result<Vec<DbRecord>> {
        let block_id = block_stuff.id();
        let block = block_stuff.block();

        let block_extra = block.read_extra()?;

        let mut records = Vec::new();

        // Process messages
        if self.message_producer.is_some() {
            block_extra.read_in_msg_descr()?.iterate_objects(|in_msg| {
                records.push(prepare_in_msg_record(in_msg, &block_id.root_hash)?);
                Ok(true)
            })?;

            block_extra
                .read_out_msg_descr()?
                .iterate_objects(|out_msg| {
                    let record = prepare_out_msg_record(out_msg, &block_id.root_hash)?;
                    if let Some(record) = record {
                        records.push(record);
                    }
                    Ok(true)
                })?;
        }

        // Process transactions
        let mut changed_accounts = FxHashSet::default();
        let mut deleted_accounts = FxHashSet::default();

        let process_transactions = self.transaction_producer.is_some();
        let process_raw_transactions = self.raw_transaction_producer.is_some();
        let process_accounts = self.account_producer.is_some();
        if process_transactions || process_accounts {
            let workchain_id = block_id.shard_id.workchain_id();

            block_extra
                .read_account_blocks()?
                .iterate_objects(|account_block| {
                    let state_update = account_block.read_state_update()?;

                    if process_accounts && state_update.old_hash != state_update.new_hash {
                        if state_update.new_hash == default_account_hash() {
                            deleted_accounts.insert(account_block.account_id().clone());
                        } else {
                            changed_accounts.insert(account_block.account_id().clone());
                        }
                    }

                    if process_transactions {
                        account_block
                            .transactions()
                            .iterate_slices(|_, raw_transaction| {
                                records.push(prepare_transaction_record(
                                    raw_transaction,
                                    &block_id.root_hash,
                                    workchain_id,
                                )?);
                                Ok(true)
                            })?;
                    }
                    if process_raw_transactions {
                        account_block
                            .transactions()
                            .iterate_slices(|_, raw_transaction| {
                                records.push(prepare_raw_transaction_record(
                                    &self.compressor,
                                    raw_transaction,
                                )?);
                                Ok(true)
                            })?;
                    }

                    Ok(true)
                })?;

            if process_accounts {
                if let Some(shard_accounts) =
                    shard_state.map(|s| s.state().read_accounts()).transpose()?
                {
                    for account_id in changed_accounts {
                        let account = shard_accounts
                            .account(&account_id)?
                            .ok_or(TonSubscriberError::BlockShardStateMismatch)?;
                        records.push(prepare_account_record(account)?);
                    }
                }

                for account_id in deleted_accounts {
                    records.push(prepare_deleted_account_record(account_id, workchain_id)?);
                }
            }
        }

        // Process block
        if self.block_producer.is_some() {
            records.push(prepare_block_record(
                block,
                &block_id.root_hash,
                block_stuff.data().to_vec(),
            )?);
        }

        // Process block proof
        if self.block_proof_producer.is_some() {
            if let Some(proof_stuff) = block_proof {
                records.push(prepare_block_proof_record(proof_stuff.proof())?);
            }
        }

        // Process raw block
        if self.raw_block_producer.is_some() {
            records.push(prepare_raw_block_record(
                &block_id.root_hash,
                block_stuff.data().to_vec(),
            )?);
        }

        // Done
        Ok(records)
    }
}

#[async_trait::async_trait]
impl ton_indexer::Subscriber for TonSubscriber {
    async fn process_block(
        &self,
        block: &BlockStuff,
        block_proof: Option<&BlockProofStuff>,
        shard_state: &ShardStateStuff,
    ) -> Result<()> {
        self.handle_block(block, block_proof, Some(shard_state))
            .await
    }

    async fn process_archive_block(
        &self,
        block: &BlockStuff,
        block_proof: Option<&BlockProofStuff>,
    ) -> Result<()> {
        self.handle_block(block, block_proof, None).await
    }
}

fn prepare_in_msg_record(in_msg: ton_block::InMsg, block_id: &UInt256) -> Result<DbRecord> {
    let transaction_id = in_msg.transaction_cell().map(|cell| cell.repr_hash());
    let transaction_now = in_msg
        .read_transaction()?
        .map(|transaction| transaction.now);
    let message = in_msg.read_message()?;
    let cell = in_msg.message_cell()?;
    let boc = ton_types::serialize_toc(&cell)?;

    let set = ton_block_json::MessageSerializationSet {
        message,
        id: cell.repr_hash(),
        block_id: Some(*block_id),
        transaction_id,
        transaction_now,
        status: ton_block::MessageProcessingStatus::Finalized,
        boc,
        proof: None,
    };

    let value = ton_block_json::db_serialize_message("id", &set)?;
    Ok(DbRecord::Message(
        value["id"].to_string(),
        serde_json::to_string(&value)?,
    ))
}

fn prepare_out_msg_record(
    out_msg: ton_block::OutMsg,
    block_id: &UInt256,
) -> Result<Option<DbRecord>> {
    let transaction_id = out_msg.transaction_cell().map(|cell| cell.repr_hash());
    let (message, cell) = match (out_msg.read_message()?, out_msg.message_cell()?) {
        (Some(message), Some(cell)) => (message, cell),
        _ => return Ok(None),
    };

    let boc = ton_types::serialize_toc(&cell)?;

    let set = ton_block_json::MessageSerializationSet {
        message,
        id: cell.repr_hash(),
        block_id: Some(*block_id),
        transaction_id,
        transaction_now: None,
        status: ton_block::MessageProcessingStatus::Finalized,
        boc,
        proof: None,
    };

    let value = ton_block_json::db_serialize_message("id", &set)?;
    Ok(Some(DbRecord::Message(
        value["id"].to_string(),
        serde_json::to_string(&value)?,
    )))
}

fn prepare_transaction_record(
    raw_transaction: ton_types::SliceData,
    block_id: &UInt256,
    workchain_id: i32,
) -> Result<DbRecord> {
    let cell = raw_transaction.reference(0)?;
    let boc = ton_types::serialize_toc(&cell)?;
    let transaction = ton_block::Transaction::construct_from(&mut cell.clone().into())?;

    let set = ton_block_json::TransactionSerializationSet {
        transaction,
        id: cell.repr_hash(),
        status: ton_block::TransactionProcessingStatus::Finalized,
        block_id: Some(*block_id),
        workchain_id,
        boc,
        proof: None,
    };

    let value = ton_block_json::db_serialize_transaction("id", &set)?;
    Ok(DbRecord::Transaction(
        value["id"].to_string(),
        serde_json::to_string(&value)?,
    ))
}

fn prepare_raw_transaction_record(
    compressor: &ZstdWrapper,
    raw_transaction: ton_types::SliceData,
) -> Result<DbRecord> {
    let cell = raw_transaction.reference(0)?;
    let boc = ton_types::serialize_toc(&cell)?;
    let boc = compressor.compress_owned(&boc)?;
    let hash = raw_transaction
        .hash(ton_types::DEPTH_SIZE)
        .as_slice()
        .to_vec();

    Ok(DbRecord::RawTransaction(hash, boc))
}

fn prepare_account_record(account: ton_block::ShardAccount) -> Result<DbRecord> {
    let boc = ton_types::serialize_toc(&account.account_cell())?;
    let account = account.read_account()?;

    let set = ton_block_json::AccountSerializationSet {
        account,
        boc,
        proof: None,
    };

    let value = ton_block_json::db_serialize_account("id", &set)?;
    Ok(DbRecord::Account(
        value["id"].to_string(),
        serde_json::to_string(&value)?,
    ))
}

fn prepare_deleted_account_record(
    account_id: ton_types::AccountId,
    workchain_id: i32,
) -> Result<DbRecord> {
    let set = ton_block_json::DeletedAccountSerializationSet {
        account_id,
        workchain_id,
    };

    let value = ton_block_json::db_serialize_deleted_account("id", &set)?;
    Ok(DbRecord::Account(
        value["id"].to_string(),
        serde_json::to_string(&value)?,
    ))
}

fn prepare_block_record(
    block: &ton_block::Block,
    block_id: &UInt256,
    boc: Vec<u8>,
) -> Result<DbRecord> {
    let set = ton_block_json::BlockSerializationSet {
        block: block.clone(),
        id: *block_id,
        status: ton_block::BlockProcessingStatus::Finalized,
        boc,
    };

    let value = ton_block_json::db_serialize_block("id", &set)?;
    Ok(DbRecord::Block(
        value["id"].to_string(),
        serde_json::to_string(&value)?,
    ))
}

fn prepare_block_proof_record(proof: &ton_block::BlockProof) -> Result<DbRecord> {
    let value = ton_block_json::db_serialize_block_proof("id", proof)?;
    Ok(DbRecord::BlockProof(
        value["id"].to_string(),
        serde_json::to_string(&value)?,
    ))
}

fn prepare_raw_block_record(block_id: &UInt256, boc: Vec<u8>) -> Result<DbRecord> {
    Ok(DbRecord::RawBlock(block_id.as_slice().to_vec(), boc))
}

fn default_account_hash() -> &'static UInt256 {
    static HASH: OnceCell<UInt256> = OnceCell::new();
    HASH.get_or_init(|| {
        ton_block::Account::default()
            .serialize()
            .unwrap()
            .repr_hash()
    })
}

enum DbRecord {
    Message(String, String),
    Transaction(String, String),
    RawTransaction(Vec<u8>, Vec<u8>),
    Account(String, String),
    BlockProof(String, String),
    Block(String, String),
    RawBlock(Vec<u8>, Vec<u8>),
}

#[derive(thiserror::Error, Debug)]
enum TonSubscriberError {
    #[error("Block and shard state mismatch")]
    BlockShardStateMismatch,
}
