use std::collections::BTreeMap;

use anyhow::Result;
use bytes::Bytes;
use futures_util::TryStreamExt;
use once_cell::race::OnceBox;
use rustc_hash::{FxHashMap, FxHashSet};
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
            let (id, data) = match DbRecord::account(account, None, None)? {
                DbRecord::Account { id, data } => (id, data),
                _ => return Ok(true),
            };

            tasks.push(producer.write(-1, id.into(), data.into(), None));
            Ok(true)
        })?;

        tasks.try_collect().await?;

        Ok(())
    }

    pub async fn handle_block(
        &self,
        meta: ton_indexer::BriefBlockMeta,
        block_stuff: &BlockStuff,
        block_data: Option<Bytes>,
        block_proof: Option<&BlockProofStuff>,
        shard_state: Option<&ShardStateStuff>,
    ) -> Result<()> {
        let records =
            match self.prepare_records(meta, block_stuff, block_data, block_proof, shard_state) {
                Ok(records) => records,
                Err(e) => {
                    tracing::error!(block_id = ?block_stuff.id(), "failed to process block {e:?}");
                    return Ok(());
                }
            };

        let mut futures = Vec::with_capacity(records.len());

        for record in records {
            match record {
                DbRecord::Message { id, data } => {
                    if let Some(producer) = &self.message_producer {
                        futures.push(producer.write(-1, id.into(), data.into(), None));
                    }
                }
                DbRecord::Transaction { id, data } => {
                    if let Some(producer) = &self.transaction_producer {
                        futures.push(producer.write(-1, id.into(), data.into(), None));
                    }
                }
                DbRecord::Account { id, data } => {
                    if let Some(producer) = &self.account_producer {
                        futures.push(producer.write(-1, id.into(), data.into(), None));
                    }
                }
                DbRecord::Block { id, data } => {
                    if let Some(producer) = &self.block_producer {
                        futures.push(producer.write(-1, id.into(), data.into(), None));
                    }
                }
                DbRecord::BlockProof { id, data } => {
                    if let Some(producer) = &self.block_proof_producer {
                        futures.push(producer.write(-1, id.into(), data.into(), None));
                    }
                }
                DbRecord::RawBlock { root_hash, boc, .. } => {
                    if let Some(producer) = &self.raw_block_producer {
                        let now = chrono::Utc::now().timestamp();
                        futures.push(producer.write(-1, root_hash.to_vec().into(), boc, Some(now)));
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
        meta: ton_indexer::BriefBlockMeta,
        block_stuff: &BlockStuff,
        block_data: Option<Bytes>,
        block_proof: Option<&BlockProofStuff>,
        shard_state: Option<&ShardStateStuff>,
    ) -> Result<Vec<DbRecord>> {
        let block_id = block_stuff.id();
        let block = block_stuff.block();

        let block_extra = block.read_extra()?;
        let block_order = ton_block_json::block_order(block, meta.masterchain_ref_seqno())?;

        let mut records = Vec::new();

        // Process transactions
        let mut changed_accounts = FxHashSet::default();
        let mut deleted_accounts = FxHashSet::default();

        let process_messages = self.message_producer.is_some();
        let process_transactions = self.transaction_producer.is_some();
        let process_accounts = self.account_producer.is_some();
        if process_messages || process_transactions || process_accounts {
            let workchain_id = block_id.shard_id.workchain_id();

            let mut acc_last_trans_chain_order = FxHashMap::default();
            let mut acc_last_trans_lt = FxHashMap::default();
            let mut transactions = BTreeMap::new();
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

                    if process_transactions || process_messages {
                        let account_id = ton_types::UInt256::from_le_bytes(
                            &account_block.account_id().get_bytestring_on_stack(0),
                        );

                        account_block
                            .transactions()
                            .iterate_slices(|_, raw_transaction| {
                                let cell = raw_transaction.reference(0)?;
                                let transaction =
                                    ton_block::Transaction::construct_from_cell(cell.clone())?;
                                let ordering_key = (transaction.logical_time(), account_id);
                                transactions.insert(ordering_key, (cell, transaction));

                                Ok(true)
                            })?;
                    }

                    Ok(true)
                })?;

            let mut messages = Default::default();
            for (index, (cell, transaction)) in transactions.into_values().enumerate() {
                let tr_chain_order = format!(
                    "{}{}",
                    block_order,
                    ton_block_json::u64_to_string(index as u64)
                );

                if process_messages {
                    Self::process_transaction_messages(
                        &transaction,
                        &cell.repr_hash(),
                        &block_id.root_hash,
                        &tr_chain_order,
                        &mut messages,
                    )?;
                }

                if process_accounts {
                    let account_id = transaction.account_id().clone();
                    acc_last_trans_chain_order.insert(account_id.clone(), tr_chain_order.clone());
                    let last_trans_lt = transaction.logical_time()
                        + transaction.out_msgs.len().unwrap_or(0) as u64
                        + 1;
                    acc_last_trans_lt.insert(account_id, last_trans_lt);
                }

                if process_transactions {
                    records.push(DbRecord::transaction(
                        &cell,
                        transaction,
                        block_id.root_hash(),
                        workchain_id,
                        &tr_chain_order,
                    )?);
                }
            }

            for (id, value) in messages {
                records.push(DbRecord::Message {
                    id: id.as_hex_string(),
                    data: serde_json::to_string(&value)?,
                });
            }

            if process_accounts {
                if let Some(shard_accounts) =
                    shard_state.map(|s| s.state().read_accounts()).transpose()?
                {
                    for account_id in changed_accounts {
                        let account = shard_accounts
                            .account(&account_id)?
                            .ok_or(TonSubscriberError::BlockShardStateMismatch)?;

                        // TODO: add prev account state
                        records.push(DbRecord::account(
                            account,
                            None,
                            acc_last_trans_chain_order.remove(&account_id),
                        )?);
                    }
                }

                for account_id in deleted_accounts {
                    let chain_order = acc_last_trans_chain_order.remove(&account_id);
                    let last_trans_lt = acc_last_trans_lt.remove(&account_id);

                    // TODO: add prev account state
                    records.push(DbRecord::deleted_account(
                        account_id,
                        workchain_id,
                        None,
                        chain_order,
                        last_trans_lt,
                    )?);
                }
            }
        }

        if self.block_producer.is_some() {
            records.push(DbRecord::block(
                block,
                block_id,
                block_data.as_deref().unwrap_or(&[]),
                block_order,
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

    fn process_transaction_messages(
        transaction: &ton_block::Transaction,
        _transaction_id: &ton_types::UInt256,
        block_id: &ton_types::UInt256,
        tr_chain_order: &str,
        messages: &mut FxHashMap<ton_types::UInt256, serde_json::Map<String, serde_json::Value>>,
    ) -> Result<()> {
        if let Some(in_msg) = transaction.in_msg_cell() {
            let message_id = in_msg.repr_hash();
            let message = ton_block::Message::construct_from_cell(in_msg.clone())?;
            let mut value =
                if message.is_inbound_external() || message.src_ref() == Some(minter_address()) {
                    Self::make_message_json(&in_msg, message, block_id, Some(transaction.now))?
                } else {
                    messages.remove(&message_id).unwrap_or_else(|| {
                        let mut value = serde_json::Map::with_capacity(2);
                        value.insert("id".into(), message_id.as_hex_string().into());
                        value
                    })
                };

            value.insert(
                "dst_chain_order".to_owned(),
                format!("{}{}", tr_chain_order, ton_block_json::u64_to_string(0)).into(),
            );

            messages.insert(message_id, value);
        }

        let mut index: u64 = 1;
        transaction.out_msgs.iterate_slices(|slice| {
            let message_cell = slice.reference(0)?;
            let message_id = message_cell.repr_hash();
            let message = ton_block::Message::construct_from_cell(message_cell.clone())?;
            let mut doc = Self::make_message_json(
                &message_cell,
                message,
                block_id,
                None, // transaction_now affects ExtIn messages only
            )?;

            // messages are ordered by created_lt
            doc.insert(
                "src_chain_order".into(),
                format!("{}{}", tr_chain_order, ton_block_json::u64_to_string(index)).into(),
            );

            index += 1;
            messages.insert(message_id, doc);
            Ok(true)
        })?;

        Ok(())
    }

    fn make_message_json(
        message_raw: &ton_types::Cell,
        message: ton_block::Message,
        block_id: &ton_types::UInt256,
        transaction_now: Option<u32>,
    ) -> Result<serde_json::Map<String, serde_json::Value>> {
        let boc = ton_types::serialize_toc(message_raw)?;
        let set = ton_block_json::MessageSerializationSet {
            message,
            id: message_raw.repr_hash(),
            block_id: Some(*block_id),
            transaction_id: None, // it would be ambiguous for internal or replayed messages
            status: ton_block::MessageProcessingStatus::Finalized,
            boc,
            proof: None,
            transaction_now, // affects ExtIn messages only
        };
        let value = ton_block_json::db_serialize_message("id", &set)?;
        Ok(value)
    }
}

enum DbRecord {
    Message { id: String, data: String },
    Transaction { id: String, data: String },
    Account { id: String, data: String },
    BlockProof { id: String, data: String },
    Block { id: String, data: String },
    RawBlock { root_hash: [u8; 32], boc: Bytes },
}

impl DbRecord {
    fn transaction(
        raw_transaction: &ton_types::Cell,
        transaction: ton_block::Transaction,
        block_id: &ton_types::UInt256,
        workchain_id: i32,
        tr_chain_order: &str,
    ) -> Result<Self> {
        let boc = ton_types::serialize_toc(raw_transaction)?;
        let id = raw_transaction.repr_hash();

        let set = ton_block_json::TransactionSerializationSet {
            transaction,
            id,
            status: ton_block::TransactionProcessingStatus::Finalized,
            block_id: Some(*block_id),
            workchain_id,
            boc,
            ..Default::default()
        };

        let mut value = ton_block_json::db_serialize_transaction("id", &set)?;
        value.insert("chain_order".to_owned(), tr_chain_order.into());
        Ok(DbRecord::Transaction {
            id: id.as_hex_string(),
            data: serde_json::to_string(&value)?,
        })
    }

    fn account(
        account: ton_block::ShardAccount,
        prev_code_hash: Option<ton_types::UInt256>,
        last_trans_chain_order: Option<String>,
    ) -> Result<Self> {
        let boc = ton_types::serialize_toc(&account.account_cell())?;
        let account = account.read_account()?;

        let id = account
            .get_addr()
            .map(ToString::to_string)
            .unwrap_or_default();

        let set = ton_block_json::AccountSerializationSet {
            account,
            prev_code_hash,
            proof: None,
            boc,
            boc1: None,
        };

        let mut value = ton_block_json::db_serialize_account("id", &set)?;
        if let Some(last_trans_chain_order) = last_trans_chain_order {
            value.insert(
                "last_trans_chain_order".to_owned(),
                last_trans_chain_order.into(),
            );
        }
        Ok(DbRecord::Account {
            id,
            data: serde_json::to_string(&value)?,
        })
    }

    fn deleted_account(
        account_id: ton_types::AccountId,
        workchain_id: i32,
        prev_code_hash: Option<ton_types::UInt256>,
        last_trans_chain_order: Option<String>,
        last_trans_lt: Option<u64>,
    ) -> Result<Self> {
        let id = construct_address(workchain_id, account_id.clone())?;
        let set = ton_block_json::DeletedAccountSerializationSet {
            account_id,
            workchain_id,
            prev_code_hash,
        };

        let mut value = ton_block_json::db_serialize_deleted_account("id", &set)?;
        if let Some(last_trans_chain_order) = last_trans_chain_order {
            value.insert(
                "last_trans_chain_order".to_owned(),
                last_trans_chain_order.into(),
            );
        }
        if let Some(lt) = last_trans_lt {
            value.insert(
                "last_trans_lt".to_owned(),
                ton_block_json::u64_to_string(lt).into(),
            );
        }
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
        block_order: String,
    ) -> Result<Self> {
        let set = ton_block_json::BlockSerializationSetFH {
            block,
            id: &block_id.root_hash,
            status: ton_block::BlockProcessingStatus::Finalized,
            boc,
            file_hash: Some(&block_id.file_hash),
        };

        let mut value = ton_block_json::db_serialize_block("id", set)?;
        value.insert("chain_order".to_owned(), block_order.into());
        Ok(Self::Block {
            id: block_id.root_hash.as_hex_string(),
            data: serde_json::to_string(&value)?,
        })
    }

    fn raw_block(block_id: &ton_block::BlockIdExt, block_boc: Bytes) -> Result<Self> {
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
    if (-128..=127).contains(&workchain_id) && account_id.remaining_bits() == 256 {
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

fn minter_address() -> &'static ton_block::MsgAddressInt {
    static ADDRESS: OnceBox<ton_block::MsgAddressInt> = OnceBox::new();
    ADDRESS.get_or_init(|| {
        Box::new(ton_block::MsgAddressInt::AddrStd(
            ton_block::MsgAddrStd::with_address(None, -1, [0; 32].into()),
        ))
    })
}

#[derive(thiserror::Error, Debug)]
enum TonSubscriberError {
    #[error("Block and shard state mismatch")]
    BlockShardStateMismatch,
}
