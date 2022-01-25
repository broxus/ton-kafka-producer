use std::sync::atomic::Ordering;
use std::sync::Arc;

use anyhow::{Context, Result};
use argh::FromArgs;
use pomfrit::formatter::*;
use serde::Deserialize;

use ton_kafka_producer::archive_scanner::*;
use ton_kafka_producer::config::*;
use ton_kafka_producer::metrics::RpcMetrics;
use ton_kafka_producer::network_scanner::shard_accounts_subscriber::ShardAccountsSubscriber;
use ton_kafka_producer::network_scanner::*;
use ton_kafka_producer::rpc;

#[tokio::main]
async fn main() -> Result<()> {
    run(argh::from_env()).await
}

async fn run(app: App) -> Result<()> {
    let config: AppConfig = read_config(app.config)?;

    match config.scan_type {
        ScanType::FromNetwork { node_config } => {
            let global_config = ton_indexer::GlobalConfig::from_file(
                &app.global_config.context("Global config not found")?,
            )
            .context("Failed to open global config")?;

            init_logger(&config.logger_settings).context("Failed to init logger")?;

            log::info!("Initializing producer...");

            let shard_accounts_subscriber = Arc::new(ShardAccountsSubscriber::default());

            let rpc_metrics = RpcMetrics::new();

            if let Some(config) = config.rpc_config {
                rpc::serve(
                    shard_accounts_subscriber.clone(),
                    config.address,
                    rpc_metrics.clone(),
                )
                .await;
            }

            let engine = NetworkScanner::new(
                config.kafka_settings,
                node_config,
                global_config,
                shard_accounts_subscriber,
            )
            .await
            .context("Failed to create engine")?;

            let engine_metrics = engine.metrics().clone();
            let (_exporter, metrics_writer) = pomfrit::create_exporter(Some(pomfrit::Config {
                listen_address: config.metrics_path,
                ..Default::default()
            }))
            .await?;

            metrics_writer.spawn({
                let rpc_metrics = rpc_metrics;
                let metrics_engine = engine.clone();
                move |buf| {
                    buf.write(Metrics {
                        engine_metrics: &engine_metrics,
                        rpc_metrics: &rpc_metrics,
                        engine: metrics_engine.clone(),
                    });
                }
            });

            engine.start().await.context("Failed to start engine")?;

            log::info!("Initialized producer");
            futures::future::pending().await
        }
        ScanType::FromArchives { list_path } => {
            let scanner = ArchivesScanner::new(config.kafka_settings, list_path)
                .context("Failed to create scanner")?;
            scanner.run().await.context("Failed to scan archives")
        }
    }?;

    log::info!("Initialized producer");
    futures::future::pending().await
}

#[derive(Debug, PartialEq, FromArgs)]
#[argh(description = "A simple service to stream TON data into Kafka")]
struct App {
    /// path to config file ('config.yaml' by default)
    #[argh(option, short = 'c', default = "String::from(\"config.yaml\")")]
    config: String,

    /// path to global config file
    #[argh(option, short = 'g')]
    global_config: Option<String>,
}

fn read_config<P, T>(path: P) -> Result<T>
where
    P: AsRef<std::path::Path>,
    for<'de> T: Deserialize<'de>,
{
    let data = std::fs::read_to_string(path).context("Failed to read config")?;
    let re = regex::Regex::new(r"\$\{([a-zA-Z_][0-9a-zA-Z_]*)\}").unwrap();
    let result = re.replace_all(&data, |caps: &regex::Captures| {
        match std::env::var(&caps[1]) {
            Ok(value) => value,
            Err(_) => {
                log::warn!("Environment variable {} was not set", &caps[1]);
                String::default()
            }
        }
    });

    let mut config = config::Config::new();
    config.merge(config::File::from_str(
        result.as_ref(),
        config::FileFormat::Yaml,
    ))?;

    config.try_into().context("Failed to parse config")
}

fn init_logger(config: &serde_yaml::Value) -> Result<()> {
    let config = serde_yaml::from_value(config.clone())?;
    log4rs::config::init_raw_config(config)?;
    Ok(())
}

struct Metrics<'a> {
    engine_metrics: &'a ton_indexer::EngineMetrics,
    rpc_metrics: &'a RpcMetrics,
    engine: Arc<NetworkScanner>,
}

impl std::fmt::Display for Metrics<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.begin_metric("mc_time_diff")
            .value(self.engine_metrics.mc_time_diff.load(Ordering::Acquire))?;
        f.begin_metric("shard_client_time_diff").value(
            self.engine_metrics
                .shard_client_time_diff
                .load(Ordering::Acquire),
        )?;
        f.begin_metric("last_mc_block_seqno").value(
            self.engine_metrics
                .last_mc_block_seqno
                .load(Ordering::Acquire),
        )?;
        f.begin_metric("last_shard_client_mc_block_seqno").value(
            self.engine_metrics
                .last_shard_client_mc_block_seqno
                .load(Ordering::Acquire),
        )?;

        let rpc_metrics = self.rpc_metrics.take_metrics();
        f.begin_metric("requests_processed")
            .value(rpc_metrics.requests_processed)?;
        f.begin_metric("rps").value(rpc_metrics.rps)?;

        let ton_indexer::alloc::JemallocStats {
            allocated,
            active,
            metadata,
            resident,
            mapped,
            retained,
            dirty,
            fragmentation,
        } = ton_indexer::alloc::fetch_stats().map_err(|e| {
            log::error!("Failed to fetch allocator stats: {}", e);
            std::fmt::Error
        })?;

        f.begin_metric("allocated_bytes").value(allocated)?;
        f.begin_metric("active_bytes").value(active)?;
        f.begin_metric("metadata_bytes").value(metadata)?;
        f.begin_metric("resident_bytes").value(resident)?;
        f.begin_metric("mapped_bytes").value(mapped)?;
        f.begin_metric("retained_bytes").value(retained)?;
        f.begin_metric("dirty_bytes").value(dirty)?;
        f.begin_metric("fragmentation_bytes").value(fragmentation)?;

        let ton_indexer::RocksdbStats {
            whole_db_stats,
            uncompressed_block_cache_usage,
            uncompressed_block_cache_pined_usage,
            compressed_block_cache_usage,
            compressed_block_cache_pined_usage,
        } = self.engine.db_metrics().map_err(|e| {
            log::error!("Failed to fetch rocksdb stats: {}", e);
            std::fmt::Error
        })?;

        f.begin_metric("uncompressed_block_cache_usage_bytes")
            .value(uncompressed_block_cache_usage)?;
        f.begin_metric("uncompressed_block_cache_pined_usage_bytes")
            .value(uncompressed_block_cache_pined_usage)?;
        f.begin_metric("compressed_block_cache_usage_bytes")
            .value(compressed_block_cache_usage)?;
        f.begin_metric("compressed_block_cache_pined_usage_bytes")
            .value(compressed_block_cache_pined_usage)?;
        f.begin_metric("memtable_total_size_bytes")
            .value(whole_db_stats.mem_table_total)?;
        f.begin_metric("memtable_unflushed_size_bytes")
            .value(whole_db_stats.mem_table_unflushed)?;
        f.begin_metric("memtable_cache_bytes")
            .value(whole_db_stats.cache_total)?;
        Ok(())
    }
}
