use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use anyhow::{Context, Result};
use argh::FromArgs;
use broxus_util::alloc::profiling;
use everscale_jrpc_server::{JrpcServer, JrpcState};
use is_terminal::IsTerminal;
use pomfrit::formatter::*;
use tracing_subscriber::EnvFilter;

use ton_kafka_producer::archives_scanner::*;
use ton_kafka_producer::config::*;
use ton_kafka_producer::network_scanner::*;
use ton_kafka_producer::s3_scanner::S3Scanner;

#[global_allocator]
static GLOBAL: broxus_util::alloc::Allocator = ton_indexer::alloc::allocator();

#[tokio::main(worker_threads = 16)]
async fn main() -> Result<()> {
    let logger = tracing_subscriber::fmt().with_env_filter(
        EnvFilter::builder()
            .with_default_directive(tracing::Level::INFO.into())
            .from_env_lossy(),
    );
    if std::io::stdout().is_terminal() {
        logger.init();
    } else {
        logger.without_time().init();
    }

    let any_signal = broxus_util::any_signal(broxus_util::TERMINATION_SIGNALS);

    let app = broxus_util::read_args_with_version!(_);
    let run = run(app);

    tokio::select! {
        result = run => result,
        signal = any_signal => {
            if let Ok(signal) = signal {
                tracing::warn!(?signal, "received termination signal, flushing state...");
            }
            // NOTE: engine future is safely dropped here so rocksdb method
            // `rocksdb_close` is called in DB object destructor
            Ok(())
        }
    }
}

async fn run(app: App) -> Result<()> {
    tracing::info!(version = env!("CARGO_PKG_VERSION"));

    let config: AppConfig = broxus_util::read_config(app.config)?;
    countme::enable(true);

    tokio::spawn(memory_profiler());
    match config.scan_type {
        ScanType::FromNetwork { node_config } => {
            let panicked = Arc::new(AtomicBool::default());
            let orig_hook = std::panic::take_hook();
            std::panic::set_hook({
                let panicked = panicked.clone();
                Box::new(move |panic_info| {
                    panicked.store(true, Ordering::Release);
                    orig_hook(panic_info);
                })
            });

            let global_config = ton_indexer::GlobalConfig::from_file(
                &app.global_config.context("Global config not found")?,
            )
            .context("Failed to open global config")?;

            tracing::info!("initializing producer");

            let jrpc_state = Arc::new(JrpcState::new(false));

            let engine = NetworkScanner::new(
                config.kafka_settings,
                node_config,
                global_config,
                jrpc_state.clone(),
            )
            .await
            .context("Failed to create engine")?;

            let (_exporter, metrics_writer) =
                pomfrit::create_exporter(config.metrics_settings).await?;

            metrics_writer.spawn({
                let jrpc_state = jrpc_state.clone();
                let engine = engine.clone();
                move |buf| {
                    buf.write(Metrics {
                        jrpc_state: &jrpc_state,
                        engine: &engine,
                        panicked: &panicked,
                    });
                }
            });
            tracing::info!("initialized exporter");

            engine.start().await.context("Failed to start engine")?;
            tracing::info!("initialized engine");

            if let Some(config) = config.rpc_config {
                let jrpc = JrpcServer::with_state(jrpc_state)
                    .build(engine.indexer(), config.address)
                    .await?;
                tokio::spawn(jrpc);
                tracing::info!("initialized RPC");
            }

            tracing::info!("initialized producer");
            futures_util::future::pending().await
        }
        ScanType::FromArchives { list_path } => {
            let kafka_settings = config
                .kafka_settings
                .context("No kafka settings provided for archives scan")?;

            let scanner = ArchivesScanner::new(kafka_settings, list_path)
                .context("Failed to create scanner")?;

            scanner.run().await.context("Failed to scan archives")
        }
        ScanType::FromS3(scanner_config) => {
            let kafka_settings = config
                .kafka_settings
                .context("No kafka settings provided for s3 archives scan")?;

            let scanner = S3Scanner::new(kafka_settings, scanner_config)
                .await
                .context("Failed to create scanner")?;

            scanner.run().await.context("Failed to scan archives")
        }
    }
}

#[derive(Debug, FromArgs)]
#[argh(description = "A simple service to stream TON data into Kafka")]
struct App {
    /// path to config file ('config.yaml' by default)
    #[argh(option, short = 'c', default = "String::from(\"config.yaml\")")]
    config: String,

    /// path to global config file
    #[argh(option, short = 'g')]
    global_config: Option<String>,
}

struct Metrics<'a> {
    jrpc_state: &'a JrpcState,
    engine: &'a NetworkScanner,
    panicked: &'a AtomicBool,
}

impl std::fmt::Display for Metrics<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let panicked = self.panicked.load(Ordering::Acquire) as u8;
        f.begin_metric("panicked").value(panicked)?;

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
            .value(network_metrics.dht.known_peers_len)?;
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

        const OVERLAY_ID: &str = "overlay_id";

        for (overlay_id, neighbour_metrics) in indexer.network_neighbour_metrics() {
            f.begin_metric("overlay_peer_search_task_count")
                .label(OVERLAY_ID, overlay_id)
                .value(neighbour_metrics.peer_search_task_count)?;
        }

        for (overlay_id, overlay_metrics) in indexer.network_overlay_metrics() {
            let overlay_id = base64::encode(overlay_id.as_slice());

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
                .value(overlay_metrics.known_peers)?;
            f.begin_metric("overlay_neighbours")
                .label(OVERLAY_ID, &overlay_id)
                .value(overlay_metrics.neighbours)?;
            f.begin_metric("overlay_received_broadcasts_data_len")
                .label(OVERLAY_ID, &overlay_id)
                .value(overlay_metrics.received_broadcasts_data_len)?;
            f.begin_metric("overlay_received_broadcasts_barrier_count")
                .label(OVERLAY_ID, &overlay_id)
                .value(overlay_metrics.received_broadcasts_barrier_count)?;
        }

        // RPC

        let jrpc = self.jrpc_state.metrics();
        f.begin_metric("jrpc_total").value(jrpc.total)?;
        f.begin_metric("jrpc_errors").value(jrpc.errors)?;
        f.begin_metric("jrpc_not_found").value(jrpc.not_found)?;

        // jemalloc

        let profiling::JemallocStats {
            allocated,
            active,
            metadata,
            resident,
            mapped,
            retained,
            dirty,
            fragmentation,
        } = profiling::fetch_stats().map_err(|e| {
            tracing::error!("failed to fetch allocator stats: {e:?}");
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
            block_cache_usage,
            block_cache_pined_usage,
        } = indexer.get_memory_usage_stats().map_err(|e| {
            tracing::error!("failed to fetch rocksdb stats: {e:?}");
            std::fmt::Error
        })?;

        f.begin_metric("rocksdb_block_cache_usage_bytes")
            .value(block_cache_usage)?;
        f.begin_metric("rocksdb_block_cache_pined_usage_bytes")
            .value(block_cache_pined_usage)?;
        f.begin_metric("rocksdb_memtable_total_size_bytes")
            .value(whole_db_stats.mem_table_total)?;
        f.begin_metric("rocksdb_memtable_unflushed_size_bytes")
            .value(whole_db_stats.mem_table_unflushed)?;
        f.begin_metric("rocksdb_memtable_cache_bytes")
            .value(whole_db_stats.cache_total)?;

        Ok(())
    }
}

async fn memory_profiler() {
    use tokio::signal::unix;

    let signal = unix::SignalKind::user_defined1();
    let mut stream = unix::signal(signal).expect("failed to create signal stream");
    let path = std::env::var("MEMORY_PROFILER_PATH").unwrap_or_else(|_| "memory.prof".to_string());

    let mut is_active = false;
    while stream.recv().await.is_some() {
        tracing::info!("memory profiler signal received");
        if !is_active {
            tracing::info!("activating memory profiler");
            if let Err(e) = profiling::start() {
                tracing::error!("failed to activate memory profiler: {e:?}");
            }
        } else {
            let invocation_time = chrono::Local::now();
            let path = format!("{}_{}", path, invocation_time.format("%Y-%m-%d_%H-%M-%S"));
            if let Err(e) = profiling::dump(&path) {
                tracing::error!("failed to dump prof: {e:?}");
            }
            if let Err(e) = profiling::stop() {
                tracing::error!("failed to deactivate memory profiler: {e:?}");
            }
        }

        is_active = !is_active;
    }
}
