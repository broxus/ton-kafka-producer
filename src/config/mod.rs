use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use rand::Rng;
use serde::Deserialize;
use ton_indexer::{OldBlocksPolicy, ShardStateCacheOptions};

/// Main application config (full)
#[derive(Clone, Deserialize)]
pub struct AppConfig {
    /// serve states
    #[serde(default)]
    pub rpc_config: Option<StatesConfig>,

    #[serde(default = "default_metrics_path")]
    pub metrics_path: SocketAddr,

    /// Scan type
    pub scan_type: ScanType,

    /// Kafka topics settings
    pub kafka_settings: KafkaConfig,

    /// log4rs settings.
    /// See [docs](https://docs.rs/log4rs/1.0.0/log4rs/) for more details
    #[serde(default = "default_logger_settings")]
    pub logger_settings: serde_yaml::Value,
}

fn default_metrics_path() -> SocketAddr {
    "0.0.0.0:12345".parse().unwrap()
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
    pub parallel_archive_downloads: u32,

    pub start_from: Option<u32>,

    #[serde(default)]
    pub adnl_options: tiny_adnl::AdnlNodeOptions,
    #[serde(default)]
    pub rldp_options: tiny_adnl::RldpNodeOptions,
    #[serde(default)]
    pub dht_options: tiny_adnl::DhtNodeOptions,
    #[serde(default)]
    pub neighbours_options: tiny_adnl::NeighboursOptions,
    #[serde(default)]
    pub overlay_shard_options: tiny_adnl::OverlayShardOptions,
}

impl NodeConfig {
    pub async fn build_indexer_config(self) -> Result<ton_indexer::NodeConfig> {
        // Determine public ip
        let ip_address = match self.adnl_public_ip {
            Some(address) => address,
            None => public_ip::addr_v4()
                .await
                .ok_or(ConfigError::PublicIpNotFound)?,
        };
        log::info!("Using public ip: {}", ip_address);

        // Generate temp keys
        let adnl_keys = ton_indexer::NodeKeys::load(self.temp_keys_path, false)
            .context("Failed to load temp keys")?;

        // Prepare DB folder
        std::fs::create_dir_all(&self.db_path)?;

        let old_blocks = match self.start_from {
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
            shard_state_cache_options: Some(ShardStateCacheOptions::default()),
            archives_enabled: false,
            old_blocks_policy: old_blocks,
            max_db_memory_usage: self.max_db_memory_usage,
            parallel_archive_downloads: self.parallel_archive_downloads,
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
pub struct StatesConfig {
    pub address: SocketAddr,
}

#[allow(clippy::large_enum_variant)]
#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "mode", rename_all = "camelCase")]
pub enum KafkaConfig {
    Broxus {
        raw_transaction_producer: KafkaProducerConfig,
    },
    Gql(GqlKafkaConfig),
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
pub struct GqlKafkaConfig {
    /// Messages to send
    pub requests_consumer: Option<KafkaConsumerConfig>,
    /// Parsed blocks producer
    pub block_producer: Option<KafkaProducerConfig>,
    /// Raw blocks producer
    pub raw_block_producer: Option<KafkaProducerConfig>,
    /// Parsed messages producer
    pub message_producer: Option<KafkaProducerConfig>,
    /// Parsed transactions producers
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

#[derive(thiserror::Error, Debug)]
enum ConfigError {
    #[error("Failed to find public ip")]
    PublicIpNotFound,
}
