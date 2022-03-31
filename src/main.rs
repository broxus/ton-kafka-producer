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
    countme::enable(true);

    tokio::spawn(memory_profiler());
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

    config::Config::builder()
        .add_source(config::File::from_str(
            result.as_ref(),
            config::FileFormat::Yaml,
        ))
        .build()
        .context("Failed to load config")?
        .try_deserialize()
        .context("Failed to parse config")
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
        let indexer = self.engine.indexer();

        // TON indexer
        let indexer_metrics = indexer.metrics();

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
        let internal_metrics = indexer.internal_metrics();

        f.begin_metric("ton_indexer_shard_states_operations_len")
            .value(internal_metrics.shard_states_operations_len)?;
        f.begin_metric("ton_indexer_block_applying_operations_len")
            .value(internal_metrics.block_applying_operations_len)?;
        f.begin_metric("ton_indexer_next_block_applying_operations_len")
            .value(internal_metrics.next_block_applying_operations_len)?;
        f.begin_metric("ton_indexer_download_block_operations")
            .value(internal_metrics.download_block_operations_len)?;

        // TON indexer network
        let network_metrics = indexer.network_metrics();

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
        f.begin_metric("network_dht_storage_total_size")
            .value(network_metrics.dht.storage_total_size)?;

        f.begin_metric("network_rldp_peer_count")
            .value(network_metrics.rldp.peer_count)?;
        f.begin_metric("network_rldp_transfers_cache_len")
            .value(network_metrics.rldp.transfers_cache_len)?;

        for (overlay_id, overlay_metrics) in indexer.network_overlay_metrics() {
            const OVERLAY_ID: &str = "overlay_id";

            let overlay_id = base64::encode(overlay_id.as_ref());

            f.begin_metric("overlay_owned_broadcasts_len")
                .label(OVERLAY_ID, &overlay_id)
                .value(overlay_metrics.owned_broadcasts_len)?;
            f.begin_metric("overlay_finished_broadcasts_len")
                .label(OVERLAY_ID, &overlay_id)
                .value(overlay_metrics.finished_broadcasts_len)?;
            f.begin_metric("overlay_node_count")
                .label(OVERLAY_ID, &overlay_id)
                .value(overlay_metrics.node_count)?;
            f.begin_metric("overlay_known_peers_len")
                .label(OVERLAY_ID, &overlay_id)
                .value(overlay_metrics.known_peers_len)?;
            f.begin_metric("overlay_random_peers_len")
                .label(OVERLAY_ID, &overlay_id)
                .value(overlay_metrics.random_peers_len)?;
            f.begin_metric("overlay_neighbours")
                .label(OVERLAY_ID, &overlay_id)
                .value(overlay_metrics.neighbours)?;
        }

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

        // DB
        let db = indexer.get_db_metrics();
        f.begin_metric("db_shard_state_storage_max_new_mc_cell_count")
            .value(db.shard_state_storage.max_new_mc_cell_count)?;
        f.begin_metric("db_shard_state_storage_max_new_sc_cell_count")
            .value(db.shard_state_storage.max_new_sc_cell_count)?;

        // RocksDB

        let ton_indexer::RocksdbStats {
            whole_db_stats,
            uncompressed_block_cache_usage,
            uncompressed_block_cache_pined_usage,
            compressed_block_cache_usage,
            compressed_block_cache_pined_usage,
        } = indexer.get_memory_usage_stats().map_err(|e| {
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

        let storage_cell = countme::get::<ton_indexer::StorageCell>();
        let cell = countme::get::<ton_types::Cell>();

        f.begin_metric("object_live")
            .label("type", "storage_cell")
            .value(storage_cell.live)?;
        f.begin_metric("object_max_live")
            .label("type", "storage_cell")
            .value(storage_cell.max_live)?;

        f.begin_metric("object_live")
            .label("type", "cell")
            .value(cell.live)?;
        f.begin_metric("object_max_live")
            .label("type", "cell")
            .value(cell.max_live)?;

        Ok(())
    }
}

async fn memory_profiler() {
    use tokio::signal::unix;
    use ton_indexer::alloc;

    let signal = unix::SignalKind::user_defined1();
    let mut stream = unix::signal(signal).expect("failed to create signal stream");
    let path = std::env::var("MEMORY_PROFILER_PATH").unwrap_or_else(|_| "memory.prof".to_string());
    let mut is_active = false;
    while stream.recv().await.is_some() {
        log::info!("Memory profiler signal received");
        if !is_active {
            log::info!("Activating memory profiler");
            if let Err(e) = alloc::activate_prof() {
                log::error!("Failed to activate memory profiler: {}", e);
            }
        } else {
            let invocation_time = chrono::Local::now();
            let path = format!("{}_{}", path, invocation_time.format("%Y-%m-%d_%H-%M-%S"));
            if let Err(e) = alloc::dump_prof(&path) {
                log::error!("Failed to dump prof: {:?}", e);
            }
            if let Err(e) = alloc::deactivate_prof() {
                log::error!("Failed to deactivate memory profiler: {}", e);
            }
        }

        is_active = !is_active;
    }
}
