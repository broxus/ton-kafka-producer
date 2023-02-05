use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use broxus_util::serde_string_array;
use everscale_network::{adnl, dht, overlay, rldp};
use rand::Rng;
use serde::Deserialize;
use ton_indexer::OldBlocksPolicy;

/// Main application config (full)
#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AppConfig {
    /// serve states
    #[serde(default)]
    pub rpc_config: Option<StatesConfig>,

    /// Prometheus metrics exporter settings.
    /// Completely disable when not specified
    #[serde(default)]
    pub metrics_settings: Option<pomfrit::Config>,

    /// Scan type
    pub scan_type: ScanType,

    /// Kafka topics settings
    #[serde(default)]
    pub kafka_settings: Option<KafkaConfig>,

    /// Options for TxTree producer
    #[serde(default)]
    pub tx_tree_settings: Option<TxTreeSettings>,

    /// log4rs settings.
    /// See [docs](https://docs.rs/log4rs/1.0.0/log4rs/) for more details
    #[serde(default = "default_logger_settings")]
    pub logger_settings: serde_yaml::Value,
}

#[allow(clippy::large_enum_variant)]
#[derive(Clone, Deserialize)]
#[serde(tag = "kind", deny_unknown_fields)]
pub enum ScanType {
    FromNetwork {
        /// TON node settings
        #[serde(default)]
        node_config: NodeConfig,
    },
    FromArchives {
        list_path: PathBuf,
    },
    FromS3(S3ScannerConfig),
}

impl Default for ScanType {
    fn default() -> Self {
        Self::FromNetwork {
            node_config: Default::default(),
        }
    }
}

/// TON node settings
#[derive(Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct NodeConfig {
    /// Node public ip. Automatically determines if None
    pub adnl_public_ip: Option<Ipv4Addr>,

    /// Node port. Default: 30303
    pub adnl_port: u16,

    /// Path to the DB directory. Default: `./db`
    pub db_path: PathBuf,

    /// Path to the ADNL keys. Default: `./adnl-keys.json`.
    /// NOTE: generates new keys if specified path doesn't exist
    pub temp_keys_path: PathBuf,

    /// Allowed DB size in bytes. Default: one third of all machine RAM
    pub max_db_memory_usage: usize,

    /// Archives map queue. Default: 16
    pub parallel_archive_downloads: usize,

    /// Archives GC and uploader options
    pub archive_options: Option<ton_indexer::ArchiveOptions>,

    pub start_from: Option<u32>,

    #[serde(default)]
    pub adnl_options: adnl::NodeOptions,
    #[serde(default)]
    pub rldp_options: rldp::NodeOptions,
    #[serde(default)]
    pub dht_options: dht::NodeOptions,
    #[serde(default)]
    pub overlay_shard_options: overlay::OverlayOptions,
    #[serde(default)]
    pub neighbours_options: ton_indexer::NeighboursOptions,
}

impl NodeConfig {
    pub async fn build_indexer_config(self) -> Result<ton_indexer::NodeConfig> {
        // Determine public ip
        let ip_address = broxus_util::resolve_public_ip(self.adnl_public_ip).await?;
        tracing::info!(?ip_address, "using public ip");

        // Generate temp keys
        let adnl_keys = ton_indexer::NodeKeys::load(self.temp_keys_path, false)
            .context("Failed to load temp keys")?;

        // Prepare DB folder
        std::fs::create_dir_all(&self.db_path)?;

        let old_blocks_policy = match self.start_from {
            None => OldBlocksPolicy::Ignore,
            Some(a) => OldBlocksPolicy::Sync { from_seqno: a },
        };

        // Done
        Ok(ton_indexer::NodeConfig {
            ip_address: SocketAddrV4::new(ip_address, self.adnl_port),
            adnl_keys,
            rocks_db_path: self.db_path.join("rocksdb"),
            file_db_path: self.db_path.join("files"),
            state_gc_options: Some(ton_indexer::StateGcOptions {
                offset_sec: rand::thread_rng().gen_range(0..3600),
                interval_sec: 3600,
            }),
            blocks_gc_options: Some(ton_indexer::BlocksGcOptions {
                kind: ton_indexer::BlocksGcKind::BeforePreviousKeyBlock,
                enable_for_sync: true,
                ..Default::default()
            }),
            shard_state_cache_options: None, // until state cache GC will be improved
            max_db_memory_usage: self.max_db_memory_usage,
            archive_options: self.archive_options,
            sync_options: ton_indexer::SyncOptions {
                old_blocks_policy,
                parallel_archive_downloads: self.parallel_archive_downloads,
                ..Default::default()
            },
            adnl_options: self.adnl_options,
            rldp_options: self.rldp_options,
            dht_options: self.dht_options,
            neighbours_options: self.neighbours_options,
            overlay_shard_options: self.overlay_shard_options,
        })
    }
}

impl Default for NodeConfig {
    fn default() -> Self {
        Self {
            adnl_public_ip: None,
            adnl_port: 30303,
            db_path: "db".into(),
            temp_keys_path: "adnl-keys.json".into(),
            max_db_memory_usage: ton_indexer::default_max_db_memory_usage(),
            parallel_archive_downloads: 16,
            archive_options: Some(Default::default()),
            start_from: None,
            adnl_options: Default::default(),
            rldp_options: Default::default(),
            dht_options: Default::default(),
            neighbours_options: Default::default(),
            overlay_shard_options: Default::default(),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct S3ScannerConfig {
    /// Archive downloader config
    pub s3_config: archive_downloader::ArchiveDownloaderConfig,

    /// Whether to retry block handler in case of error
    #[serde(default = "default_retry_on_error")]
    pub retry_on_error: bool,
}

fn default_retry_on_error() -> bool {
    true
}

#[derive(Debug, Clone, Deserialize)]
pub struct StatesConfig {
    pub address: SocketAddr,
}

#[allow(clippy::large_enum_variant)]
#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "mode", rename_all = "camelCase")]
pub enum KafkaConfig {
    Broxus(BroxusKafkaConfig),
    Gql(GqlKafkaConfig),
    TxTree(TxTreeProducerConfig),
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct BroxusKafkaConfig {
    /// Compressed raw transactions producer
    pub raw_transaction_producer: KafkaProducerConfig,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct TxTreeProducerConfig {
    pub raw_transaction_producer: Option<KafkaProducerConfig>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct GqlKafkaConfig {
    /// Messages to send
    pub requests_consumer: Option<KafkaConsumerConfig>,
    /// Parsed blocks producer
    pub block_producer: Option<KafkaProducerConfig>,
    /// Raw blocks producer
    pub raw_block_producer: Option<KafkaProducerConfig>,
    /// Parsed messages producer
    pub message_producer: Option<KafkaProducerConfig>,
    /// Parsed transactions producer
    pub transaction_producer: Option<KafkaProducerConfig>,
    /// Account states producer
    pub account_producer: Option<KafkaProducerConfig>,
    /// Raw block proofs producer
    pub block_proof_producer: Option<KafkaProducerConfig>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct KafkaConsumerConfig {
    pub topic: String,
    pub brokers: String,
    pub group_id: String,
    #[cfg(feature = "sasl")]
    #[serde(default)]
    pub security_config: Option<SecurityConfig>,
    pub session_timeout_ms: u32,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct KafkaProducerConfig {
    pub topic: String,
    pub brokers: String,
    pub message_timeout_ms: Option<u32>,
    pub message_max_size: Option<usize>,
    pub attempt_interval_ms: u64,
    #[cfg(feature = "sasl")]
    #[serde(default)]
    pub security_config: Option<SecurityConfig>,
    #[serde(default = "default_batch_flush_threshold_size")]
    pub batch_flush_threshold_size: usize,
    #[serde(default = "default_batch_flush_threshold_ms")]
    pub batch_flush_threshold_ms: u64,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct TxTreeSettings {
    pub max_transaction_depth: Option<u32>,
    pub max_transaction_width: Option<u32>,
    #[serde(default, with = "serde_string_array")]
    pub ignored_senders: Vec<String>,
    #[serde(default, with = "serde_string_array")]
    pub ignored_receivers: Vec<String>,
    pub db_path: String,
    pub assemble_interval_secs: u64,
}

impl Default for TxTreeSettings {
    fn default() -> Self {
        Self {
            max_transaction_depth: Some(1000),
            max_transaction_width: Some(1000),
            ignored_senders: vec![],
            ignored_receivers: vec![],
            db_path: "../../db".to_string(),
            assemble_interval_secs: 5,
        }
    }
}

fn default_batch_flush_threshold_size() -> usize {
    2000
}

fn default_batch_flush_threshold_ms() -> u64 {
    200
}

#[derive(Debug, Clone, Deserialize)]
pub enum SecurityConfig {
    Sasl(SaslConfig),
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SaslConfig {
    pub security_protocol: String,
    pub ssl_ca_location: String,
    #[serde(default)]
    pub ssl_keystore_location: Option<String>,
    #[serde(default)]
    pub ssl_keystore_password: Option<String>,
    pub sasl_mechanism: String,
    pub sasl_username: String,
    pub sasl_password: String,
}

impl ConfigExt for ton_indexer::GlobalConfig {
    fn from_file<P>(path: &P) -> Result<Self>
    where
        P: AsRef<Path>,
    {
        let file = std::fs::File::open(path)?;
        let reader = std::io::BufReader::new(file);
        let config = serde_json::from_reader(reader)?;
        Ok(config)
    }
}

pub trait ConfigExt: Sized {
    fn from_file<P>(path: &P) -> Result<Self>
    where
        P: AsRef<Path>;
}

fn default_logger_settings() -> serde_yaml::Value {
    const DEFAULT_LOG4RS_SETTINGS: &str = r##"
    appenders:
      stdout:
        kind: console
        encoder:
          pattern: "{d(%Y-%m-%d %H:%M:%S %Z)(utc)} - {h({l})} {M} = {m} {n}"
    root:
      level: error
      appenders:
        - stdout
    loggers:
      ton_kafka_producer:
        level: info
        appenders:
          - stdout
        additive: false
    "##;
    serde_yaml::from_str(DEFAULT_LOG4RS_SETTINGS).unwrap()
}
