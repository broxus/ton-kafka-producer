use crate::models::TransactionNode;
use anyhow::Result;

use ton_block::{Deserializable, GetRepresentationHash, Serializable};
use ton_types::{BuilderData, Cell, IBitstring, SliceData};

pub struct TreePacker {}

impl TreePacker {
    pub fn pack(&self, transaction_tree: &TransactionNode) -> Result<Cell> {
        self.pack_cell(transaction_tree)
    }

    pub fn unpack(&self, cell: Cell) -> Result<TransactionNode> {
        let mut slice_data = SliceData::from(cell);
        self.unpack_cell(&mut slice_data)
    }

    fn unpack_cell(&self, sd: &mut SliceData) -> Result<TransactionNode> {
        let mut i = 0;
        let mut root: Option<TransactionNode> = None;

        loop {
            let bit_res = sd.get_next_bit();
            let bit = match bit_res {
                Ok(bit) => bit,
                Err(_) => break,
            };

            if !bit && i == 0 {
                return Err(anyhow::Error::msg("Bad tree. Starts from reference node"));
            }

            if i == 0 {
                let cell = sd.reference(i)?;
                let rt = self.unpack_self_cell(&cell)?;
                root = Some(rt);
                i = i + 1;
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

            i = i + 1;
        }

        if let Some(root) = root {
            Ok(root)
        } else {
            Err(anyhow::Error::msg("Failed to build tree from cell"))
        }
    }

    fn unpack_self_cell(&self, cell: &Cell) -> Result<TransactionNode> {
        let cell_bytes = cell.write_to_bytes()?;
        let transaction = ton_block::Transaction::construct_from_bytes(cell_bytes.as_slice())?;
        let node =
            TransactionNode::new(transaction.hash()?, transaction.lt, cell_bytes, Vec::new());
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

            i = i + 1;
        }

        Ok(children)
    }

    fn pack_cell(&self, root: &TransactionNode) -> Result<Cell> {
        let mut bd = BuilderData::new();
        bd.append_bits(1, 1)?; // First bit of 1 means this node includes transaction itself
        bd.append_bits(0, 1)?; // Second bit of 0 means first reference references transaction cell.

        let transaction_cell = Cell::construct_from_bytes(root.boc())?;
        bd.append_reference_cell(transaction_cell);

        let children = root.children();
        if children.len() <= 3 {
            for child in children.iter() {
                let cell = self.pack_cell(child)?;
                bd.append_bits(0, 1)?;
                bd.append_reference_cell(cell);
            }
        } else {
            let children = children.as_slice();
            let len = children.len();

            for child in children[0..2].iter() {
                let cell = self.pack_cell(child)?;
                bd.append_bits(0, 1)?;
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
                    bd.append_bits(0, 1)?;
                    bd.append_reference_cell(cell)
                }
            }
            len if len > 4 => {
                let cc = &children[0..3];
                for i in cc {
                    let cell = self.pack_cell(i)?;
                    bd.append_bits(0, 1)?;
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

impl Default for TreePacker {
    fn default() -> Self {
        Self {}
    }
}

pub mod tests {
    #[test]
    fn test() {
        let mut slice = [0, 1, 2, 3, 4, 5, 6, 7];
        let first = &slice[0..3];
        let second = &slice[3..8];
        println!("{:?}", first);
        println!("{:?}", second);
    }
}
