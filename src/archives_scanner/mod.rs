use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use indicatif::{ProgressBar, ProgressStyle};

use crate::archive::*;
use crate::blocks_handler::*;
use crate::config::*;

pub struct ArchivesScanner {
    handler: Arc<BlocksHandler>,
    list: String,
}

impl ArchivesScanner {
    pub fn new(kafka_settings: KafkaConfig, list_path: PathBuf) -> Result<Self> {
        let list = std::fs::read_to_string(list_path)?;

        Ok(Self {
            handler: Arc::new(BlocksHandler::new(Some(kafka_settings))?),
            list,
        })
    }

    pub async fn run(self) -> Result<()> {
        let len = self.list.lines().count();

        let pb = ProgressBar::new(len as u64);

        let total_style = ProgressStyle::default_bar()
            .template(
                "ETA: {eta_precise} {wide_bar} Archives processed: {percent}%|{pos}/{len} Speed: {per_sec}. {msg}",
            )?
            .progress_chars("##-");
        pb.set_style(total_style);

        let task_counter = Arc::new(AtomicUsize::new(0));

        let (tx, rx) = tokio::sync::mpsc::channel(1000);

        tokio::spawn(start_writing_blocks(
            pb.clone(),
            task_counter.clone(),
            self.handler.clone(),
            rx,
        ));

        for task in self
            .list
            .lines()
            .filter_map(|path| match std::fs::read(path) {
                Ok(a) => Some((path.to_owned(), a)),
                Err(e) => {
                    pb.println(format!("Failed reading archive {path}: {e:?}"));
                    None
                }
            })
            .filter_map(|(path, x)| match parse_archive(x) {
                Ok(blocks) => {
                    pb.println(format!("Parsed: {path}"));
                    pb.inc(1);
                    Some(blocks)
                }
                Err(e) => {
                    pb.println(format!("Failed parsing archive {path}: {e:?}"));
                    None
                }
            })
            .flatten()
        {
            task_counter.fetch_add(1, Ordering::Release);
            tx.send(task)
                .await
                .map_err(|_| anyhow::anyhow!("Failed to send task"))?;
        }

        // Drop tx so tasks writer will stop
        drop(tx);

        while task_counter.load(Ordering::Acquire) > 0 {
            pb.println("Waiting tasks to complete...");
            tokio::time::sleep(Duration::from_secs(1)).await;
        }

        Ok(())
    }
}

async fn start_writing_blocks(
    pb: ProgressBar,
    counter: Arc<AtomicUsize>,
    handler: Arc<BlocksHandler>,
    mut rx: BlockTaskRx,
) {
    while let Some((block_id, parsed)) = rx.recv().await {
        let (stuff, data) = parsed.block_stuff;

        if let Err(e) = handler
            .handle_block(
                ton_indexer::BriefBlockMeta::default(), // TODO: Replace this stub
                &stuff,
                Some(data),
                parsed.block_proof_stuff.as_ref(),
                None,
                false,
            )
            .await
            .context("Failed to handle block")
        {
            pb.println(format!("Failed processing block {block_id}: {e:?}"));
        }
        counter.fetch_sub(1, Ordering::Release);
    }

    pb.println("Complete tasks");
}

type BlockTaskRx = tokio::sync::mpsc::Receiver<BlockTask>;
type BlockTask = (ton_block::BlockIdExt, ParsedEntry);
