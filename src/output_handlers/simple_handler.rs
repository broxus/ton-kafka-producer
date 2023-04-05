use async_trait::async_trait;
use base64::Engine;
use ton_types::{deserialize_tree_of_cells, serialize_toc};

use crate::models::TransactionNode;
use crate::output_handlers::OutputHandler;
use crate::tree_packer::TreePacker;

#[derive(Default)]
pub struct ExampleHandler {}

#[async_trait]
impl OutputHandler for ExampleHandler {
    async fn handle_output(&self, trees: &[TransactionNode]) -> anyhow::Result<()> {
        let packer = TreePacker::default();

        for tree in trees {
            match packer.pack(tree) {
                Ok(cell) => {
                    let vec = serialize_toc(&cell)?;
                    let base64 = base64::engine::general_purpose::STANDARD.encode(vec.as_slice());
                    let decoded = base64::engine::general_purpose::STANDARD.decode(&base64)?;

                    let deserialized_cell = deserialize_tree_of_cells(&mut decoded.as_slice())?;

                    if cell.repr_hash() == deserialized_cell.repr_hash() {
                        tracing::debug!("Cells are equal");
                    }

                    let unpacked_tree = packer.unpack(&deserialized_cell)?;
                    tracing::debug!("{unpacked_tree:?}");
                }
                Err(e) => {
                    tracing::error!("Failed to pack: {:x}. Err: {:?}", tree.hash(), e)
                }
            }
        }
        Ok(())
    }
}
