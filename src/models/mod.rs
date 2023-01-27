use crate::utils::storage_utils::get_key_bytes;
use ton_types::UInt256;

pub enum Tree {
    Full(TransactionNode),
    Partial(TransactionNode),
    Empty,
}

#[derive(Debug, Clone)]
pub struct TransactionNode {
    tx_hash: UInt256,
    tx_lt: u64,
    boc: Vec<u8>,
    children: Vec<TransactionNode>,
}

impl TransactionNode {
    pub fn new(hash: UInt256, lt: u64, boc: Vec<u8>, children: Vec<TransactionNode>) -> Self {
        TransactionNode {
            tx_hash: hash,
            tx_lt: lt,
            boc,
            children,
        }
    }

    pub fn db_key(&self) -> Vec<u8> {
        get_key_bytes(self.tx_lt, &self.tx_hash)
    }

    pub fn hash(&self) -> &UInt256 {
        &self.tx_hash
    }

    pub fn lt(&self) -> u64 {
        self.tx_lt
    }

    pub fn boc(&self) -> &[u8] {
        self.boc.as_slice()
    }

    pub fn base_64_boc(&self) -> String {
        let slice = self.boc.as_slice();
        base64::encode(slice)
    }

    pub fn children(&self) -> &Vec<TransactionNode> {
        &self.children
    }

    pub fn children_mut(&mut self) -> &mut Vec<TransactionNode> {
        &mut self.children
    }

    pub fn append_child(&mut self, tx: TransactionNode) -> () {
        if let Some(_) = self.children.iter().find(|x| x.tx_hash == tx.hash()) {
            tracing::error!("Trying to add existing child: {:x}", tx.hash());
            return ();
        }
        self.children.push(tx);
    }
}
