use std::net::{Ipv4Addr, SocketAddrV4};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
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
    pub rpc_config: Option<everscale_jrpc_server::Config>,

    /// Prometheus metrics exporter settings.
    /// Completely disable when not specified
    #[serde(default)]
    pub metrics_settings: Option<pomfrit::Config>,

    /// Scan type
    pub scan_type: ScanType,

    /// Kafka topics settings
    #[serde(default)]
    pub kafka_settings: Option<KafkaConfig>,
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

    /// Persistent state keeper configuration.
    pub persistent_state_options: ton_indexer::PersistentStateOptions,

    /// Internal DB options.
    pub db_options: ton_indexer::DbOptions,

    /// Archives map queue. Default: 16
    pub parallel_archive_downloads: usize,

    /// Archives GC and uploader options
    pub archive_options: Option<ton_indexer::ArchiveOptions>,

    /// Shard state GC options
    pub state_gc_options: Option<ton_indexer::StateGcOptions>,

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
            state_gc_options: self.state_gc_options,
            blocks_gc_options: Some(ton_indexer::BlocksGcOptions {
                kind: ton_indexer::BlocksGcKind::BeforePreviousKeyBlock,
                enable_for_sync: true,
                ..Default::default()
            }),
            shard_state_cache_options: None, // until state cache GC will be improved
            db_options: self.db_options,
            persistent_state_options: self.persistent_state_options,
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
            persistent_state_options: Default::default(),
            db_options: Default::default(),
            parallel_archive_downloads: 16,
            archive_options: Some(Default::default()),
            state_gc_options: Some(ton_indexer::StateGcOptions {
                offset_sec: rand::thread_rng().gen_range(0..3600),
                interval_sec: 3600,
            }),
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

#[allow(clippy::large_enum_variant)]
#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "mode", rename_all = "camelCase")]
pub enum KafkaConfig {
    Broxus(BroxusKafkaConfig),
    Gql(GqlKafkaConfig),
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct BroxusKafkaConfig {
    /// Compressed raw transactions producer
    pub raw_transaction_producer: KafkaProducerConfig,
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
