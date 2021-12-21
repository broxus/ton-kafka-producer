use std::collections::HashMap;
use std::hash::BuildHasherDefault;

use anyhow::Result;
use nekoton::transport::models::ExistingContract;
use nekoton_indexer_utils::{contains_account, ExistingContractExt};
use parking_lot::RwLock;
use rustc_hash::FxHasher;
use ton_block::HashmapAugType;
use ton_indexer::utils::{BlockIdExtExtension, BlockStuff, ShardStateStuff};

pub type FxHashMap<K, V> = HashMap<K, V, BuildHasherDefault<FxHasher>>;

#[derive(Default)]
pub struct ShardAccountsSubscriber {
    masterchain_accounts_cache: RwLock<ton_block::ShardAccounts>,
    shard_accounts_cache: RwLock<FxHashMap<ton_block::ShardIdent, ton_block::ShardAccounts>>,
}

impl ShardAccountsSubscriber {
    pub(crate) async fn handle_block(
        &self,
        block_stuff: &BlockStuff,
        shard_state: Option<&ShardStateStuff>,
    ) -> Result<()> {
        let shard_state = match shard_state {
            Some(state) => state,
            None => return Ok(()),
        };

        let block_info = &block_stuff.block().read_info()?;
        let shard_accounts = shard_state.state().read_accounts()?;

        if block_stuff.id().is_masterchain() {
            *self.masterchain_accounts_cache.write() = shard_accounts;
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
    ) -> Result<Option<ExistingContract>> {
        let is_masterchain = account.is_masterchain();
        let account = account.address().get_bytestring_on_stack(0);
        let account = ton_types::UInt256::from_slice(account.as_slice());

        if is_masterchain {
            let cache = self.masterchain_accounts_cache.read();
            ExistingContract::from_shard_account_opt(&cache.get(&account)?)
        } else {
            let cache = self.shard_accounts_cache.read();
            for (shard_ident, shard_accounts) in cache.iter() {
                if !contains_account(shard_ident, &account) {
                    continue;
                }
                return ExistingContract::from_shard_account_opt(&shard_accounts.get(&account)?);
            }
            Ok(None)
        }
    }
}
