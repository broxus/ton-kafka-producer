use std::collections::HashMap;
use std::hash::BuildHasherDefault;
use std::sync::Arc;

use anyhow::{Context, Result};
use nekoton::transport::models::ExistingContract;
use nekoton_abi::{GenTimings, LastTransactionId, TransactionId};
use nekoton_indexer_utils::contains_account;
use parking_lot::{Mutex, RwLock};
use rustc_hash::FxHasher;
use serde::Serialize;
use ton_block::{Deserializable, HashmapAugType};
use ton_indexer::utils::{BlockIdExtExtension, BlockStuff, RefMcStateHandle, ShardStateStuff};

pub type FxHashMap<K, V> = HashMap<K, V, BuildHasherDefault<FxHasher>>;

#[derive(Default)]
pub struct ShardAccountsSubscriber {
    masterchain_accounts_cache: RwLock<Option<ShardAccounts>>,
    shard_accounts_cache: RwLock<FxHashMap<ton_block::ShardIdent, ShardAccounts>>,
    current_keyblock: Arc<Mutex<Option<ton_block::Block>>>,
}

impl ShardAccountsSubscriber {
    pub fn new() -> (Arc<Self>, Arc<Mutex<Option<ton_block::Block>>>) {
        let this = Self::default();
        let current_keyblock = this.current_keyblock.clone();
        (Arc::new(this), current_keyblock)
    }

    pub(crate) async fn handle_block(
        &self,
        block_stuff: &BlockStuff,
        shard_state: Option<&ShardStateStuff>,
        is_key_block: bool,
    ) -> Result<()> {
        let shard_state = match shard_state {
            Some(state) => state,
            None => return Ok(()),
        };

        let block_info = &block_stuff.block().read_info()?;
        let shard_accounts = ShardAccounts {
            accounts: shard_state.state().read_accounts()?,
            state_handle: shard_state.ref_mc_state_handle().clone(),
        };

        if block_stuff.id().is_masterchain() {
            *self.masterchain_accounts_cache.write() = Some(shard_accounts);
            if is_key_block {
                *self.current_keyblock.lock() = Some(block_stuff.block().clone());
            }
        } else {
            let mut cache = self.shard_accounts_cache.write();

            cache.insert(*block_info.shard(), shard_accounts);
            if block_info.after_merge() || block_info.after_split() {
                log::warn!("Clearing shard states cache after shards merge/split");

                let block_ids = block_info.read_prev_ids()?;

                match block_ids.len() {
                    // Block after split
                    //       |
                    //       *  - block A
                    //      / \
                    //     *   *  - blocks B', B"
                    1 => {
                        // Find all split shards for the block A
                        let (left, right) = block_ids[0].shard_id.split()?;

                        // Remove parent shard of the block A
                        if cache.contains_key(&left) && cache.contains_key(&right) {
                            cache.remove(&block_ids[0].shard_id);
                        }
                    }

                    // Block after merge
                    //     *   *  - blocks A', A"
                    //      \ /
                    //       *  - block B
                    //       |
                    2 => {
                        // Find and remove all parent shards
                        for block_id in block_info.read_prev_ids()? {
                            cache.remove(&block_id.shard_id);
                        }
                    }
                    _ => {}
                }
            }
        };

        Ok(())
    }

    pub fn get_contract_state(
        &self,
        account: &ton_block::MsgAddressInt,
    ) -> Result<Option<ShardAccount>> {
        let is_masterchain = account.is_masterchain();
        let account = account.address().get_bytestring_on_stack(0);
        let account = ton_types::UInt256::from_slice(account.as_slice());

        if is_masterchain {
            let state = self.masterchain_accounts_cache.read();
            state.as_ref().context("Not initialized yet")?.get(&account)
        } else {
            let cache = self.shard_accounts_cache.read();
            for (shard_ident, shard_accounts) in cache.iter() {
                if !contains_account(shard_ident, &account) {
                    continue;
                }
                return shard_accounts.get(&account);
            }
            Ok(None)
        }
    }

    pub fn get_key_block(&self) -> Option<ton_block::Block> {
        self.current_keyblock.lock().clone()
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ShardAccount {
    #[serde(with = "nekoton_utils::serde_cell")]
    data: ton_types::Cell,
    last_transaction_id: LastTransactionId,
    #[serde(skip)]
    _state_handle: Arc<RefMcStateHandle>,
}

pub fn make_existing_contract(state: Option<ShardAccount>) -> Result<Option<ExistingContract>> {
    let state = match state {
        Some(this) => this,
        None => return Ok(None),
    };

    match ton_block::Account::construct_from_cell(state.data)? {
        ton_block::Account::AccountNone => Ok(None),
        ton_block::Account::Account(account) => Ok(Some(ExistingContract {
            account,
            timings: GenTimings::Unknown,
            last_transaction_id: state.last_transaction_id,
        })),
    }
}

struct ShardAccounts {
    accounts: ton_block::ShardAccounts,
    state_handle: Arc<RefMcStateHandle>,
}

impl ShardAccounts {
    fn get(&self, account: &ton_types::UInt256) -> Result<Option<ShardAccount>> {
        match self.accounts.get(account)? {
            Some(account) => Ok(Some(ShardAccount {
                data: account.account_cell(),
                last_transaction_id: LastTransactionId::Exact(TransactionId {
                    lt: account.last_trans_lt(),
                    hash: *account.last_trans_hash(),
                }),
                _state_handle: self.state_handle.clone(),
            })),
            None => Ok(None),
        }
    }
}
