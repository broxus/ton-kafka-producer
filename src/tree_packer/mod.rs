use crate::models::TransactionNode;
use anyhow::Result;

use ton_block::{Deserializable, GetRepresentationHash, Serializable, Transaction};
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
            if i == 0 && is_self_reference {
                let cell = sd.reference(i)?;
                let rt = self.unpack_self_cell(cell)?;
                root = Some(rt);
                i += 1;
                continue;
            }

            let bit_res = sd.get_next_bit();
            let bit = match bit_res {
                Ok(bit) => bit,
                Err(_) => break,
            };

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
        let transaction = Transaction::construct_from_cell(cell)?;
        let boc = serialize_toc(&transaction.serialize()?)?;
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
                               //bd.append_bits(1, 1)?; // Second bit of 1 means first reference references transaction cell.

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
            let len = children.len();
            let children = children.as_slice();

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

#[cfg(test)]
mod tests {
    use crate::models::TransactionNode;
    use crate::tree_packer::TreePacker;
    use base64::Engine;
    use std::str::FromStr;
    use ton_types::UInt256;

    #[test]
    fn pack_unpack() {
        let base64_string = "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";

        let packer = TreePacker::default();
        let bytes = base64::engine::general_purpose::STANDARD
            .decode(base64_string)
            .expect("base64");
        let cell = ton_types::deserialize_tree_of_cells(&mut bytes.as_slice()).expect("cell");
        let tree = packer.unpack(&cell).expect("tree");
        println!("{tree:?}");
        let cell = packer.pack(&tree).expect("Failed to pack");

        let _bytes = ton_types::serialize_toc(&cell).expect("Failed to write bytes");
        let string = base64::engine::general_purpose::STANDARD.encode(_bytes);
        let node = packer.unpack(&cell).expect("Failed to unpack");
        println!("{string}")
        //assert_eq!(node, parent)
    }
}
