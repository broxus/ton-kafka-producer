use crate::models::TransactionNode;
use ton_types::UInt256;

pub fn get_key_bytes(lt: u64, tx_hash: &UInt256) -> Vec<u8> {
    let mut key_vec = Vec::with_capacity(40);
    key_vec.extend_from_slice(&lt.to_be_bytes());
    key_vec.extend_from_slice(tx_hash.as_slice());
    key_vec
}

pub fn get_key_data_from_bytes(key: &[u8]) -> (u64, UInt256) {
    let mut u64_bytes = [0u8; 8];
    u64_bytes.copy_from_slice(&key[..8]);
    let lt = u64::from_be_bytes(u64_bytes);

    let hash = UInt256::from_slice(&key[8..40]);

    (lt, hash)
}

pub fn visualize_tree(tree: &TransactionNode) {
    println!("------------------------------------------------------");
    println!("ROOT IS: {}", hex::encode(tree.hash().as_slice()));
    println!("DEPTH IS: {}", tree.depth());
    for n in tree.children() {
        visualize_ancestor(tree.hash(), n);
    }
}

fn visualize_ancestor(parent_hash: &UInt256, node: &TransactionNode) {
    println!(
        "Child: {}. Parent: {}",
        hex::encode(node.hash().as_slice()),
        hex::encode(parent_hash.as_slice())
    );
    for c in node.children() {
        visualize_ancestor(node.hash(), c);
    }
}
