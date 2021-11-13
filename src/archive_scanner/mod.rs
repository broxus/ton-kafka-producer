use std::collections::{hash_map, HashMap};
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use indicatif::{ProgressBar, ProgressStyle};

use self::archive::*;
use crate::blocks_handler::*;
use crate::config::*;

mod archive;

pub struct ArchivesScanner {
    handler: Arc<BlocksHandler>,
    list: String,
}

impl ArchivesScanner {
    pub fn new(config: KafkaConfig, list_path: PathBuf) -> Result<Self> {
        let list = std::fs::read_to_string(list_path)?;

        Ok(Self {
            handler: Arc::new(BlocksHandler::new(config)?),
            list,
        })
    }

    pub async fn run(self) -> Result<()> {
        let len = self.list.lines().count();

        let pb = ProgressBar::new(len as u64);
        pb.set_draw_rate(10);

        let total_style = ProgressStyle::default_bar()
            .template(
                "ETA: {eta_precise} {wide_bar} Archives processed: {percent}%|{pos}/{len} Speed: {per_sec}. {msg}",
            )
            .progress_chars("##-");
        pb.set_style(total_style);

        let task_counter = Arc::new(AtomicUsize::new(0));

        let mut partitions = HashMap::<i32, BlockTaskTx>::with_capacity(9);

        for task in self
            .list
            .lines()
            .filter_map(|pat| match std::fs::read(pat) {
                Ok(a) => Some((pat.to_string(), a)),
                Err(e) => {
                    pb.println(format!(
                        "Failed reading archive. Filename: {}, : {:?}",
                        pat, e
                    ));
                    None
                }
            })
            .filter_map(|(pat, x)| match parse_archive(x) {
                Ok(blocks) => {
                    pb.println(format!("Parsed: {}", pat));
                    pb.inc(1);
                    Some(blocks)
                }
                Err(e) => {
                    pb.println(format!(
                        "Failed parsing archive. Filename: {}, : {:?}",
                        pat, e
                    ));
                    None
                }
            })
            .flatten()
        {
            task_counter.fetch_add(1, Ordering::Release);

            let partition = compute_partition(&task.0);
            match partitions.entry(partition) {
                hash_map::Entry::Occupied(entry) => {
                    let tx = entry.get();
                    tx.send(task).await.context("Failed to send task")?;
                }
                hash_map::Entry::Vacant(entry) => {
                    let (tx, rx) = tokio::sync::mpsc::channel(1000);

                    let tx = entry.insert(tx);
                    tx.send(task).await.context("Failed to send task")?;

                    tokio::spawn(start_writing_blocks(
                        partition,
                        pb.clone(),
                        task_counter.clone(),
                        self.handler.clone(),
                        rx,
                    ));
                }
            }
        }

        drop(partitions);

        while task_counter.load(Ordering::Acquire) > 0 {
            pb.println("Waiting tasks to complete...");
            tokio::time::sleep(Duration::from_secs(1)).await;
        }

        Ok(())
    }
}

async fn start_writing_blocks(
    partition: i32,
    pb: ProgressBar,
    counter: Arc<AtomicUsize>,
    handler: Arc<BlocksHandler>,
    mut rx: BlockTaskRx,
) {
    let mut delivery_futures = Vec::new();
    let wait_futures = |futures: Vec<rdkafka::producer::DeliveryFuture>| async {
        futures::future::join_all(futures)
            .await
            .into_iter()
            .for_each(|item| {
                if item.is_err() {
                    pb.println("Failed to write transaction to kafka");
                }
            });
    };

    while let Some((block_id, block)) = rx.recv().await {
        match handler
            .handle_block(&block_id, &block, false)
            .await
            .context("Failed to handle block")
        {
            Ok(futures) => {
                delivery_futures.extend(futures);
            }
            Err(e) => {
                pb.println(format!("Failed processing block {} : {:?}", block_id, e));
            }
        }
        counter.fetch_sub(1, Ordering::Release);

        if delivery_futures.len() > 1024 {
            wait_futures(std::mem::take(&mut delivery_futures)).await;
        }
    }

    wait_futures(std::mem::take(&mut delivery_futures)).await;

    pb.println(format!("Complete tasks for partition {}", partition));
}

type BlockTaskTx = tokio::sync::mpsc::Sender<BlockTask>;
type BlockTaskRx = tokio::sync::mpsc::Receiver<BlockTask>;
type BlockTask = (ton_block::BlockIdExt, ton_block::Block);
