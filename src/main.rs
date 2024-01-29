use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use anyhow::{Context, Result};
use argh::FromArgs;
use broxus_util::alloc::profiling;
use everscale_rpc_server::RpcState;
use std::io::IsTerminal;
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

            let rpc_state = config
                .rpc_config
                .map(RpcState::new)
                .transpose()
                .context("Failed to create server state")?
                .map(Arc::new);

            let engine = NetworkScanner::new(
                config.kafka_settings,
                node_config,
                global_config,
                rpc_state.clone(),
            )
            .await
            .context("Failed to create engine")?;
            if app.run_compaction {
                tracing::warn!("compacting database");
                engine.indexer().trigger_compaction().await;
                return Ok(());
            }

            if app.print_memory_usage {
                print_disk_usage_stats(&engine);

                return Ok(());
            }
            if let Some(s) = config.metrics_settings {
                install_monitoring(s.listen_address).context("Failed to install monitoring")?;
                tokio::spawn(profiling::allocator_metrics_loop());
            }

            tracing::info!("initialized exporter");

            engine.start().await.context("Failed to start engine")?;
            tracing::info!("initialized engine");

            if let Some(rpc_state) = rpc_state {
                rpc_state.initialize(engine.indexer()).await?;
                tokio::spawn(rpc_state.serve()?);
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

fn print_disk_usage_stats(engine: &Arc<NetworkScanner>) {
    let stats = engine.indexer().db_usage_stats().unwrap();
    let longest_table_name = stats
        .iter()
        .map(|s| s.cf_name.len())
        .max()
        .unwrap_or_default();
    println!("{}", "=".repeat(80));
    for stat in &stats {
        let padded_name = stat
            .cf_name
            .chars()
            .chain(std::iter::repeat(' ').take(longest_table_name - stat.cf_name.len()))
            .collect::<String>();
        println!(
            "{padded_name} KEYS: {:12} VALUES: {:12} SUM: {:12}",
            stat.keys_total,
            stat.values_total,
            stat.keys_total + stat.values_total
        );
    }
    let total_keys = stats.iter().map(|s| s.keys_total.as_u64()).sum::<_>();
    let total_values = stats.iter().map(|s| s.values_total.as_u64()).sum::<_>();
    println!("{}", "=".repeat(80));
    println!(
        "TOTAL KEYS: {} TOTAL VALUES: {} TOTAL: {}",
        bytesize::to_string(total_keys, true),
        bytesize::to_string(total_values, true),
        bytesize::to_string(total_keys + total_values, true)
    );
}

fn install_monitoring(metrics_addr: SocketAddr) -> Result<()> {
    use metrics_exporter_prometheus::Matcher;
    const EXPONENTIAL_SECONDS: &[f64] = &[
        0.000001, 0.0001, 0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
        60.0, 120.0, 300.0, 600.0, 3600.0,
    ];
    metrics_exporter_prometheus::PrometheusBuilder::new()
        .set_buckets_for_metric(Matcher::Prefix("time".to_string()), EXPONENTIAL_SECONDS)?
        .with_http_listener(metrics_addr)
        .install()
        .context("Failed installing metrics exporter")
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

    /// compact database and exit
    #[argh(switch)]
    run_compaction: bool,

    /// print memory usage statistics and exit
    #[argh(switch)]
    print_memory_usage: bool,
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
