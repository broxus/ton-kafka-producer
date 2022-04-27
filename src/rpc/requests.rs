use nekoton_utils::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SendMessageRequest {
    #[serde(with = "serde_ton_block")]
    pub message: ton_block::Message,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RawBlock {
    #[serde(with = "serde_ton_block")]
    pub block: ton_block::Block,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GetContractState {
    #[serde(with = "serde_address")]
    pub address: ton_block::MsgAddressInt,
}
