use anyhow::Result;
use futures_util::TryStreamExt;
use once_cell::race::OnceBox;
use rustc_hash::FxHashSet;
use ton_block::{Deserializable, HashmapAugType, Serializable};
use ton_indexer::utils::{BlockProofStuff, BlockStuff, ShardStateStuff};
use ton_types::HashmapType;

use super::kafka_producer::*;
use crate::config::*;

pub struct GqlProducer {
    block_producer: Option<KafkaProducer>,
    raw_block_producer: Option<KafkaProducer>,
    message_producer: Option<KafkaProducer>,
    transaction_producer: Option<KafkaProducer>,
    account_producer: Option<KafkaProducer>,
    block_proof_producer: Option<KafkaProducer>,
}

impl GqlProducer {
    pub fn new(config: GqlKafkaConfig) -> Result<Self> {
        fn make_producer(config: Option<KafkaProducerConfig>) -> Result<Option<KafkaProducer>> {
            config
                .map(|config| KafkaProducer::new(config, Partitions::any()))
                .transpose()
        }

        Ok(Self {
            block_producer: make_producer(config.block_producer)?,
            raw_block_producer: make_producer(config.raw_block_producer)?,
            message_producer: make_producer(config.message_producer)?,
            transaction_producer: make_producer(config.transaction_producer)?,
            account_producer: make_producer(config.account_producer)?,
            block_proof_producer: make_producer(config.block_proof_producer)?,
        })
    }

    pub async fn handle_state(&self, state: &ShardStateStuff) -> Result<()> {
        let producer = match &self.account_producer {
            Some(producer) => producer,
            None => return Ok(()),
        };

        let tasks = futures_util::stream::FuturesUnordered::new();

        state.state().read_accounts()?.iterate_objects(|account| {
            let (id, data) = match DbRecord::account(account, None)? {
                DbRecord::Account { id, data } => (id, data),
                _ => return Ok(true),
            };

            tasks.push(producer.write(-1, id.into_bytes(), data.into_bytes(), None));
            Ok(true)
        })?;

        tasks.try_collect().await?;

        Ok(())
    }

    pub async fn handle_block(
        &self,
        block_stuff: &BlockStuff,
        block_data: Option<Vec<u8>>,
        block_proof: Option<&BlockProofStuff>,
        shard_state: Option<&ShardStateStuff>,
    ) -> Result<()> {
        let records = match self.prepare_records(block_stuff, block_data, block_proof, shard_state)
        {
            Ok(records) => records,
            Err(e) => {
                log::error!("Failed to process block {}: {:?}", block_stuff.id(), e);
                return Ok(());
            }
        };

        let mut futures = Vec::with_capacity(records.len());

        for record in records {
            match record {
                DbRecord::Message { id, data } => {
                    if let Some(producer) = &self.message_producer {
                        futures.push(producer.write(-1, id.into_bytes(), data.into_bytes(), None));
                    }
                }
                DbRecord::Transaction { id, data } => {
                    if let Some(producer) = &self.transaction_producer {
                        futures.push(producer.write(-1, id.into_bytes(), data.into_bytes(), None));
                    }
                }
                DbRecord::Account { id, data } => {
                    if let Some(producer) = &self.account_producer {
                        futures.push(producer.write(-1, id.into_bytes(), data.into_bytes(), None));
                    }
                }
                DbRecord::Block { id, data } => {
                    if let Some(producer) = &self.block_producer {
                        futures.push(producer.write(-1, id.into_bytes(), data.into_bytes(), None));
                    }
                }
                DbRecord::BlockProof { id, data } => {
                    if let Some(producer) = &self.block_proof_producer {
                        futures.push(producer.write(-1, id.into_bytes(), data.into_bytes(), None));
                    }
                }
                DbRecord::RawBlock { root_hash, boc, .. } => {
                    if let Some(producer) = &self.raw_block_producer {
                        let now = chrono::Utc::now().timestamp();
                        futures.push(producer.write(-1, root_hash.to_vec(), boc, Some(now)));
                    }
                }
            }
        }

        futures_util::future::join_all(futures)
            .await
            .into_iter()
            .collect::<Result<Vec<_>>>()?;

        Ok(())
    }

    fn prepare_records(
        &self,
        block_stuff: &BlockStuff,
        block_data: Option<Vec<u8>>,
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
                records.push(DbRecord::in_msg(in_msg, &block_id.root_hash)?);
                Ok(true)
            })?;

            block_extra
                .read_out_msg_descr()?
                .iterate_objects(|out_msg| {
                    let record = DbRecord::out_msg(out_msg, &block_id.root_hash)?;
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
                                records.push(DbRecord::transaction(
                                    raw_transaction,
                                    &block_id.root_hash,
                                    workchain_id,
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

                        // TODO: add prev account state
                        records.push(DbRecord::account(account, None)?);
                    }
                }

                for account_id in deleted_accounts {
                    // TODO: add prev account state
                    records.push(DbRecord::deleted_account(account_id, workchain_id, None)?);
                }
            }
        }

        if self.block_producer.is_some() {
            records.push(DbRecord::block(
                block,
                block_id,
                block_data.as_deref().unwrap_or(&[]),
            )?);
        }

        // Process block
        if let Some(raw_data) = block_data {
            // Process raw block
            if self.raw_block_producer.is_some() {
                records.push(DbRecord::raw_block(block_id, raw_data)?);
            }
        }

        // Process block proof
        if self.block_proof_producer.is_some() {
            if let Some(proof_stuff) = block_proof {
                records.push(DbRecord::block_proof(proof_stuff.proof())?);
            }
        }

        // Done
        Ok(records)
    }
}

enum DbRecord {
    Message { id: String, data: String },
    Transaction { id: String, data: String },
    Account { id: String, data: String },
    BlockProof { id: String, data: String },
    Block { id: String, data: String },
    RawBlock { root_hash: [u8; 32], boc: Vec<u8> },
}

impl DbRecord {
    fn in_msg(in_msg: ton_block::InMsg, block_id: &ton_types::UInt256) -> Result<Self> {
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
            ..Default::default()
        };

        let value = ton_block_json::db_serialize_message("id", &set)?;
        Ok(Self::Message {
            id: cell.repr_hash().as_hex_string(),
            data: serde_json::to_string(&value)?,
        })
    }

    fn out_msg(out_msg: ton_block::OutMsg, block_id: &ton_types::UInt256) -> Result<Option<Self>> {
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
            status: ton_block::MessageProcessingStatus::Finalized,
            boc,
            ..Default::default()
        };

        let value = ton_block_json::db_serialize_message("id", &set)?;
        Ok(Some(Self::Message {
            id: cell.repr_hash().as_hex_string(),
            data: serde_json::to_string(&value)?,
        }))
    }

    fn transaction(
        raw_transaction: ton_types::SliceData,
        block_id: &ton_types::UInt256,
        workchain_id: i32,
    ) -> Result<Self> {
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
            ..Default::default()
        };

        let value = ton_block_json::db_serialize_transaction("id", &set)?;
        Ok(DbRecord::Transaction {
            id: cell.repr_hash().as_hex_string(),
            data: serde_json::to_string(&value)?,
        })
    }

    fn account(
        account: ton_block::ShardAccount,
        prev_account_state: Option<ton_block::Account>,
    ) -> Result<Self> {
        let boc = ton_types::serialize_toc(&account.account_cell())?;
        let account = account.read_account()?;

        let id = account
            .get_addr()
            .map(ToString::to_string)
            .unwrap_or_default();

        let set = ton_block_json::AccountSerializationSet {
            account,
            prev_account_state,
            proof: None,
            boc,
        };

        let value = ton_block_json::db_serialize_account("id", &set)?;
        Ok(DbRecord::Account {
            id,
            data: serde_json::to_string(&value)?,
        })
    }

    fn deleted_account(
        account_id: ton_types::AccountId,
        workchain_id: i32,
        prev_account_state: Option<ton_block::Account>,
    ) -> Result<Self> {
        let id = construct_address(workchain_id, account_id.clone())?;
        let set = ton_block_json::DeletedAccountSerializationSet {
            account_id,
            workchain_id,
            prev_account_state,
        };

        let value = ton_block_json::db_serialize_deleted_account("id", &set)?;
        Ok(DbRecord::Account {
            id: id.to_string(),
            data: serde_json::to_string(&value)?,
        })
    }

    fn block_proof(proof: &ton_block::BlockProof) -> Result<Self> {
        let value = ton_block_json::db_serialize_block_proof("id", proof)?;
        Ok(Self::BlockProof {
            id: proof.proof_for.root_hash.as_hex_string(),
            data: serde_json::to_string(&value)?,
        })
    }

    fn block(
        block: &ton_block::Block,
        block_id: &ton_block::BlockIdExt,
        boc: &[u8],
    ) -> Result<Self> {
        let set = ton_block_json::BlockSerializationSetFH {
            block,
            id: &block_id.root_hash,
            status: ton_block::BlockProcessingStatus::Finalized,
            boc,
            file_hash: Some(&block_id.file_hash),
        };

        let value = ton_block_json::db_serialize_block("id", set)?;
        Ok(Self::Block {
            id: block_id.root_hash.as_hex_string(),
            data: serde_json::to_string(&value)?,
        })
    }

    fn raw_block(block_id: &ton_block::BlockIdExt, block_boc: Vec<u8>) -> Result<Self> {
        Ok(Self::RawBlock {
            root_hash: block_id.root_hash.inner(),
            boc: block_boc,
        })
    }
}

fn construct_address(
    workchain_id: i32,
    account_id: ton_types::AccountId,
) -> Result<ton_block::MsgAddressInt> {
    if workchain_id <= 127 && workchain_id >= -128 && account_id.remaining_bits() == 256 {
        ton_block::MsgAddressInt::with_standart(None, workchain_id as i8, account_id)
    } else {
        ton_block::MsgAddressInt::with_variant(None, workchain_id, account_id)
    }
}

fn default_account_hash() -> &'static ton_types::UInt256 {
    static HASH: OnceBox<ton_types::UInt256> = OnceBox::new();
    HASH.get_or_init(|| {
        Box::new(
            ton_block::Account::default()
                .serialize()
                .unwrap()
                .repr_hash(),
        )
    })
}

#[derive(thiserror::Error, Debug)]
enum TonSubscriberError {
    #[error("Block and shard state mismatch")]
    BlockShardStateMismatch,
}
