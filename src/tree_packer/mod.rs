use crate::models::TransactionNode;
use anyhow::Result;

use ton_block::{Deserializable, GetRepresentationHash, Transaction};
use ton_types::{serialize_toc, BuilderData, Cell, IBitstring, SliceData};

#[derive(Default)]
pub struct TreePacker {}

impl TreePacker {
    pub fn pack(&self, transaction_tree: &TransactionNode) -> Result<Cell> {
        self.pack_cell(transaction_tree)
    }

    pub fn unpack(&self, cell: &Cell) -> Result<TransactionNode> {
        let mut slice_data = SliceData::from(cell);
        self.unpack_cell(&mut slice_data)
    }

    fn unpack_cell(&self, sd: &mut SliceData) -> Result<TransactionNode> {
        let mut i = 0;
        let mut root: Option<TransactionNode> = None;

        let is_self_reference = sd.get_next_bit()?;
        if !is_self_reference {
            return Err(anyhow::Error::msg("Bad tree. Starts from reference node"));
        }

        loop {
            let bit_res = sd.get_next_bit();
            let bit = match bit_res {
                Ok(bit) => bit,
                Err(_) => break,
            };

            if i == 0 && is_self_reference {
                let cell = sd.reference(i)?;
                let rt = self.unpack_self_cell(cell)?;
                root = Some(rt);
                i += 1;
                continue;
            }

            let root = root.as_mut().unwrap();

            let cell = sd.reference(i)?;
            let mut sd = SliceData::from(&cell);

            if bit {
                let node = self.unpack_cell(&mut sd)?;
                root.append_child(node);
            } else {
                let children = self.unpack_multiple_cell_reference(&mut sd)?;
                for c in children {
                    root.append_child(c);
                }
            }
            i += 1;
        }

        if let Some(root) = root {
            Ok(root)
        } else {
            Err(anyhow::Error::msg("Failed to build tree from cell"))
        }
    }

    fn unpack_self_cell(&self, cell: Cell) -> Result<TransactionNode> {
        let boc = serialize_toc(&cell)?;
        let transaction = Transaction::construct_from_cell(cell)?;
        let node = TransactionNode::new(transaction.hash()?, transaction.lt, boc, Vec::new());
        Ok(node)
    }

    fn unpack_multiple_cell_reference(&self, sd: &mut SliceData) -> Result<Vec<TransactionNode>> {
        let mut children = Vec::new();
        let mut i = 0;
        loop {
            let bit_res = sd.get_next_bit();
            let bit = match bit_res {
                Ok(bit) => bit,
                Err(_) => break,
            };

            let cell = sd.reference(i)?;
            let mut sd = SliceData::from(&cell);

            if bit {
                let node = self.unpack_cell(&mut sd)?;
                children.push(node);
            } else {
                let child_refs = self.unpack_multiple_cell_reference(&mut sd)?;
                for c in child_refs {
                    children.push(c);
                }
            }

            i += 1;
        }

        Ok(children)
    }

    fn pack_cell(&self, root: &TransactionNode) -> Result<Cell> {
        let mut bd = BuilderData::new();
        bd.append_bits(1, 1)?; // First bit of 1 means this node includes transaction itself
        bd.append_bits(1, 1)?; // Second bit of 1 means first reference references transaction cell.

        let mut data = root.boc();
        let transaction_cell = ton_types::deserialize_tree_of_cells(&mut data)?;
        bd.append_reference_cell(transaction_cell);

        let children = root.children();
        if children.len() <= 3 {
            for child in children.iter() {
                let cell = self.pack_cell(child)?;
                bd.append_bits(1, 1)?;
                bd.append_reference_cell(cell);
            }
        } else {
            let children = children.as_slice();
            let len = children.len();

            for child in children[0..2].iter() {
                let cell = self.pack_cell(child)?;
                bd.append_bits(1, 1)?;
                bd.append_reference_cell(cell);
            }
            let cell = self.construct_node_reference(&children[2..len])?;
            bd.append_reference_cell(cell);
        }

        let cell = bd.into_cell()?;

        Ok(cell)
    }

    fn construct_node_reference(&self, children: &[TransactionNode]) -> Result<Cell> {
        let mut bd = BuilderData::new();
        bd.append_bits(0, 1)?; //First bit of 0 means this node is reference to multiple cells.
        match children.len() {
            len if len <= 4 => {
                for i in children {
                    let cell = self.pack_cell(i)?;
                    bd.append_bits(1, 1)?;
                    bd.append_reference_cell(cell)
                }
            }
            len if len > 4 => {
                let cc = &children[0..3];
                for i in cc {
                    let cell = self.pack_cell(i)?;
                    bd.append_bits(1, 1)?;
                    bd.append_reference_cell(cell)
                }
                self.construct_node_reference(&children[3..len])?;
            }
            _ => (),
        }

        let cell = bd.into_cell()?;
        Ok(cell)
    }
}

pub mod tests {
    use crate::models::TransactionNode;
    use crate::tree_packer::TreePacker;
    use std::str::FromStr;
    use ton_types::UInt256;

    #[test]
    fn pack_unpack() {
        let child_bytes = base64::decode("te6ccgECBwEAAZIAA7VwizdAIAIZNvOU1ntM3pIcU0s7o6p5s3/r36JRvy6cNVAAAfuSwVPG7Kb6LwzuYN5vS60PzQ6lYSEuZYN6LcnJkIniJGTU1lCAAAH7ksFTxsY9fMyQABRiCXwIBQQBAhUMCS9ZqT0YYgl8EQMCAFvAAAAAAAAAAAAAAAABLUUtpEnlC4z33SeGHxRhIq/htUa7i3D8ghbwxhQTn44EAJ5AhYw9CQAAAAAAAAAAABcAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAIJyJP80WCeHeruoPwMkUa1AtzPHy2XaQp+ZEn2pCS1+nWJaydNijrvG2pbz0g6bDSwmyJgT56sCiQ3qeG0r4PipCwEBoAYAsUgAAP3r4grgtCknz5m6KJDluZ4EtaFLFrrESocSpnr7GtEAAizdAIAIZNvOU1ntM3pIcU0s7o6p5s3/r36JRvy6cNVS9ZqT0AYUWGAAAD9yWCp42sevmZJA").expect("child1_bytes");
        let child = TransactionNode::new(
            UInt256::from_str("836c8b49fc4d25a2b0722eafcbb503aecdce09cb459771852ef9e7f84268b5ca")
                .expect("hash1"),
            34880169000046,
            child_bytes,
            Vec::new(),
        );

        let parent_bytes = base64::decode("te6ccgECBwEAAZIAA7VwizdAIAIZNvOU1ntM3pIcU0s7o6p5s3/r36JRvy6cNVAAAfuSwVPGwijCFCj3nPbKFs8ubWyVTvAlATKi5bKnSgZNOwHI9ZmQAAH7ksFTxkY9fMyQABRiCXwIBQQBAhUMCQJ4cEwYYgl8EQMCAFvAAAAAAAAAAAAAAAABLUUtpEnlC4z33SeGHxRhIq/htUa7i3D8ghbwxhQTn44EAJ5AhYwKHngAAAAAAAAAABcAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAIJy/VPEu/3VAapldiPQVvyOEVCbwD0Z3wDBIKLiuc9CxbMk/zRYJ4d6u6g/AyRRrUC3M8fLZdpCn5kSfakJLX6dYgEBoAYAsUgA4pVG7jBlKHXhsq5IktZHkkisKIeg8wQ4+6IGlFY+9PsAAizdAIAIZNvOU1ntM3pIcU0s7o6p5s3/r36JRvy6cNVQJ4cEwAYUWGAAAD9yWCp41sevmZJA").expect("parent_bytes");
        let parent = TransactionNode::new(
            UInt256::from_str("ca6fa2f0cee60de6f4bad0fcd0ea561212e65837a2dc9c99089e22464d4d6508")
                .expect("parent_hash"),
            34880169000044,
            parent_bytes,
            vec![child],
        );

        let packer = TreePacker::default();
        let cell = packer.pack(&parent).expect("Failed to pack");
        let bytes = ton_types::serialize_toc(&cell).expect("Failed to write bytes");
        let node = packer.unpack(&cell).expect("Failed to unpack");
        assert_eq!(node, parent)
    }
}
