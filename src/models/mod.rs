use crate::utils::storage_utils::get_key_bytes;
use base64::{engine::general_purpose, Engine as _};
use std::cmp::Ordering;
use ton_types::UInt256;

pub enum Tree {
    Full(TransactionNode),
    Partial(TransactionNode),
    AssembleFailed(TransactionNode),
    Empty,
}

#[derive(Debug, Clone, PartialEq)]
pub struct TransactionNode {
    tx_hash: UInt256,
    tx_lt: u64,
    boc: Vec<u8>,
    children: Vec<TransactionNode>,
    depth: u32,
}

// #[derive(Debug, Clone, PartialEq)]
// pub struct TransactionNodeUnpacked {
//     tx_hash: UInt256,
//     tx_lt: u64,
//     tx: ton_block::Transaction,
//     children: Vec<TransactionNodeUnpacked>,
//     depth: u32,
// }
//
// impl TransactionNodeUnpacked {
//     pub fn new(
//         hash: UInt256,
//         lt: u64,
//         tx: ton_block::Transaction,
//         children: Vec<TransactionNodeUnpacked>,
//     ) -> Self {
//         TransactionNodeUnpacked {
//             tx_hash: hash,
//             tx_lt: lt,
//             tx,
//             children,
//             depth: 1,
//         }
//     }
//
//     pub fn append_child(&mut self, tx: TransactionNodeUnpacked) {
//         if self.children.iter().any(|x| x.tx_hash == tx.tx_hash) {
//             tracing::error!("Trying to add existing child: {:x}", tx.tx_hash);
//         } else {
//             if self.depth == tx.depth {
//                 self.depth += tx.depth;
//             } else if self.depth < tx.depth {
//                 self.depth = tx.depth + 1;
//             }
//
//             self.children.push(tx);
//         }
//     }
// }

impl TransactionNode {
    pub fn new(hash: UInt256, lt: u64, boc: Vec<u8>, children: Vec<TransactionNode>) -> Self {
        TransactionNode {
            tx_hash: hash,
            tx_lt: lt,
            boc,
            children,
            depth: 1,
        }
    }

    pub fn depth(&self) -> u32 {
        self.depth
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
        general_purpose::STANDARD.encode(slice)
    }

    pub fn children(&self) -> &Vec<TransactionNode> {
        &self.children
    }

    pub fn children_mut(&mut self) -> &mut Vec<TransactionNode> {
        &mut self.children
    }

    pub fn append_child(&mut self, tx: TransactionNode) {
        if self.children.iter().any(|x| x.tx_hash == tx.hash()) {
            tracing::error!("Trying to add existing child: {:x}", tx.hash());
        } else {
            match self.depth.cmp(&tx.depth) {
                Ordering::Equal => self.depth += tx.depth,
                Ordering::Less => self.depth = tx.depth + 1,
                Ordering::Greater => (),
            }

            self.children.push(tx);
        }
    }
}
