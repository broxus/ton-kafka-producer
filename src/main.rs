use std::sync::atomic::Ordering;
use std::sync::Arc;

use anyhow::{Context, Result};
use argh::FromArgs;
use pomfrit::formatter::*;
use serde::Deserialize;

use ton_kafka_producer::archive_scanner::*;
use ton_kafka_producer::config::*;
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

            let rpc_metrics = Arc::new(rpc::Metrics::default());

            if let Some(config) = config.rpc_config {
                tokio::spawn(rpc::serve(
                    shard_accounts_subscriber.clone(),
                    config.address,
                    rpc_metrics.clone(),
                ));
            }

            let engine = NetworkScanner::new(
                config.kafka_settings,
                node_config,
                global_config,
                shard_accounts_subscriber,
            )
            .await
            .context("Failed to create engine")?;

            let (_exporter, metrics_writer) = pomfrit::create_exporter(Some(pomfrit::Config {
                listen_address: config.metrics_path,
                ..Default::default()
            }))
            .await?;

            metrics_writer.spawn({
                let rpc_metrics = rpc_metrics;
                let engine = engine.clone();
                move |buf| {
                    buf.write(Metrics {
                        rpc_metrics: &rpc_metrics,
                        engine: &engine,
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
    rpc_metrics: &'a rpc::Metrics,
    engine: &'a NetworkScanner,
}

impl std::fmt::Display for Metrics<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // TON indexer
        let indexer_metrics = self.engine.indexer_metrics();

        let last_mc_utime = indexer_metrics.last_mc_utime.load(Ordering::Acquire);
        if last_mc_utime > 0 {
            f.begin_metric("ton_indexer_mc_time_diff")
                .value(indexer_metrics.mc_time_diff.load(Ordering::Acquire))?;
            f.begin_metric("ton_indexer_sc_time_diff").value(
                indexer_metrics
                    .shard_client_time_diff
                    .load(Ordering::Acquire),
            )?;

            f.begin_metric("ton_indexer_last_mc_utime")
                .value(last_mc_utime)?;
        }

        let last_mc_block_seqno = indexer_metrics.last_mc_block_seqno.load(Ordering::Acquire);
        if last_mc_block_seqno > 0 {
            f.begin_metric("ton_indexer_last_mc_block_seqno")
                .value(last_mc_block_seqno)?;
        }

        let last_shard_client_mc_block_seqno = indexer_metrics
            .last_shard_client_mc_block_seqno
            .load(Ordering::Acquire);
        if last_shard_client_mc_block_seqno > 0 {
            f.begin_metric("ton_indexer_last_sc_block_seqno")
                .value(last_shard_client_mc_block_seqno)?;
        }

        // Internal metrics
        let internal_metrics = self.engine.internal_metrics();

        f.begin_metric("ton_indexer_shard_states_cache_len")
            .value(internal_metrics.shard_states_cache_len)?;
        f.begin_metric("ton_indexer_shard_states_operations_len")
            .value(internal_metrics.shard_states_operations_len)?;
        f.begin_metric("ton_indexer_block_applying_operations")
            .value(internal_metrics.block_applying_operations)?;
        f.begin_metric("ton_indexer_next_block_applying_operations")
            .value(internal_metrics.next_block_applying_operations)?;
        f.begin_metric("ton_indexer_download_block_operations")
            .value(internal_metrics.download_block_operations)?;

        // TON indexer network
        let network_metrics = self.engine.network_metrics();

        f.begin_metric("network_adnl_peer_count")
            .value(network_metrics.adnl.peer_count)?;
        f.begin_metric("network_adnl_channels_by_id_len")
            .value(network_metrics.adnl.channels_by_peers_len)?;
        f.begin_metric("network_adnl_channels_by_peers_len")
            .value(network_metrics.adnl.channels_by_peers_len)?;
        f.begin_metric("network_adnl_incoming_transfers_len")
            .value(network_metrics.adnl.incoming_transfers_len)?;
        f.begin_metric("network_adnl_query_count")
            .value(network_metrics.adnl.query_count)?;

        f.begin_metric("network_dht_peers_cache_len")
            .value(network_metrics.dht.peers_cache_len)?;
        f.begin_metric("network_dht_bucket_peer_count")
            .value(network_metrics.dht.bucket_peer_count)?;
        f.begin_metric("network_dht_storage_len")
            .value(network_metrics.dht.storage_len)?;

        f.begin_metric("network_rldp_peer_count")
            .value(network_metrics.rldp.peer_count)?;
        f.begin_metric("network_rldp_transfers_cache_len")
            .value(network_metrics.rldp.transfers_cache_len)?;

        // RPC

        f.begin_metric("rpc_requests_processed")
            .value(self.rpc_metrics.requests_processed.load(Ordering::Acquire))?;
        f.begin_metric("rpc_errors")
            .value(self.rpc_metrics.errors.load(Ordering::Acquire))?;

        // jemalloc

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

        f.begin_metric("jemalloc_allocated_bytes")
            .value(allocated)?;
        f.begin_metric("jemalloc_active_bytes").value(active)?;
        f.begin_metric("jemalloc_metadata_bytes").value(metadata)?;
        f.begin_metric("jemalloc_resident_bytes").value(resident)?;
        f.begin_metric("jemalloc_mapped_bytes").value(mapped)?;
        f.begin_metric("jemalloc_retained_bytes").value(retained)?;
        f.begin_metric("jemalloc_dirty_bytes").value(dirty)?;
        f.begin_metric("jemalloc_fragmentation_bytes")
            .value(fragmentation)?;

        // RocksDB

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

        f.begin_metric("rocksdb_uncompressed_block_cache_usage_bytes")
            .value(uncompressed_block_cache_usage)?;
        f.begin_metric("rocksdb_uncompressed_block_cache_pined_usage_bytes")
            .value(uncompressed_block_cache_pined_usage)?;
        f.begin_metric("rocksdb_compressed_block_cache_usage_bytes")
            .value(compressed_block_cache_usage)?;
        f.begin_metric("rocksdb_compressed_block_cache_pined_usage_bytes")
            .value(compressed_block_cache_pined_usage)?;
        f.begin_metric("rocksdb_memtable_total_size_bytes")
            .value(whole_db_stats.mem_table_total)?;
        f.begin_metric("rocksdb_memtable_unflushed_size_bytes")
            .value(whole_db_stats.mem_table_unflushed)?;
        f.begin_metric("rocksdb_memtable_cache_bytes")
            .value(whole_db_stats.cache_total)?;

        Ok(())
    }
}
