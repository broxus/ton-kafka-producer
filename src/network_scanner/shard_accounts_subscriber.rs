use std::collections::HashMap;
use std::hash::BuildHasherDefault;

use anyhow::Result;
use nekoton::transport::models::ExistingContract;
use nekoton_indexer_utils::{contains_account, ExistingContractExt};
use parking_lot::RwLock;
use rustc_hash::FxHasher;
use ton_block::HashmapAugType;
use ton_indexer::utils::{BlockIdExtExtension, BlockStuff, ShardStateStuff};
use ton_types::UInt256;

pub type FxHashMap<K, V> = HashMap<K, V, BuildHasherDefault<FxHasher>>;

#[derive(Default)]
pub struct ShardAccountsSubscriber {
    shard_accounts_cache: RwLock<FxHashMap<ton_block::ShardIdent, ton_block::ShardAccounts>>,
}

impl ShardAccountsSubscriber {
    pub(crate) async fn handle_block(
        &self,
        block_stuff: &BlockStuff,
        shard_state: Option<&ShardStateStuff>,
    ) -> Result<()> {
        if !block_stuff.id().is_masterchain() {
            let mut shard_accounts_cache = self.shard_accounts_cache.write();
            let block_info = &block_stuff.block().read_info()?;
            let shard_state = if let Some(a) = shard_state {
                a
            } else {
                return Ok(());
            };
            let shard_accounts = shard_state.state().read_accounts()?;
            shard_accounts_cache.insert(*block_info.shard(), shard_accounts);
            if block_info.after_merge() || block_info.after_split() {
                let block_ids = block_info.read_prev_ids()?;
                match block_ids.len() {
                    1 => {
                        let (left, right) = block_ids[0].shard_id.split()?;
                        if shard_accounts_cache.contains_key(&left)
                            && shard_accounts_cache.contains_key(&right)
                        {
                            shard_accounts_cache.remove(&block_ids[0].shard_id);
                        }
                    }
                    len if len > 1 => {
                        for block_id in block_info.read_prev_ids()? {
                            shard_accounts_cache.remove(&block_id.shard_id);
                        }
                    }
                    _ => {}
                }
            }
        }

        Ok(())
    }

    pub fn get_contract_state(&self, account: &UInt256) -> Result<Option<ExistingContract>> {
        let shard_accounts_cache = self.shard_accounts_cache.read();
        for (shard_ident, shard_accounts) in shard_accounts_cache.iter() {
            if contains_account(shard_ident, account) {
                match shard_accounts.get(account) {
                    Ok(account) => return ExistingContract::from_shard_account_opt(&account),
                    Err(e) => {
                        log::error!("Failed to get account {}: {:?}", account.to_hex_string(), e);
                    }
                };
            }
        }

        Ok(None)
    }
}
