use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use serde::Deserialize;
use ton_indexer::{OldBlocksPolicy, ShardStateCacheOptions};

use self::temp_keys::*;

mod temp_keys;

/// Main application config (full). Used to run relay
#[derive(Deserialize, Default)]
pub struct AppConfig {
    /// serve states
    #[serde(default)]
    pub rpc_config: Option<StatesConfig>,

    /// Scan type
    pub scan_type: ScanType,

    /// Kafka topics settings
    pub kafka_settings: KafkaConfig,

    /// log4rs settings.
    /// See [docs](https://docs.rs/log4rs/1.0.0/log4rs/) for more details
    #[serde(default = "default_logger_settings")]
    pub logger_settings: serde_yaml::Value,
}

#[derive(Deserialize)]
#[serde(tag = "kind")]
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
#[derive(Deserialize, Clone)]
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
        // TODO: add param to generate new temp keys
        let temp_keys =
            TempKeys::load(self.temp_keys_path, false).context("Failed to load temp keys")?;

        // Prepare DB folder
        std::fs::create_dir_all(&self.db_path)?;

        let old_blocks = match self.start_from {
            None => OldBlocksPolicy::Ignore,
            Some(a) => OldBlocksPolicy::Sync { from_seqno: a },
        };

        // Done
        Ok(ton_indexer::NodeConfig {
            ip_address: SocketAddrV4::new(ip_address, self.adnl_port),
            adnl_keys: temp_keys.into(),
            rocks_db_path: self.db_path.join("rocksdb"),
            file_db_path: self.db_path.join("files"),
            // NOTE: State GC is disabled until it is fully tested
            state_gc_options: None,
            blocks_gc_options: None,
            shard_state_cache_options: Some(ShardStateCacheOptions::default()),
            archives_enabled: false,
            old_blocks_policy: old_blocks,
            max_db_memory_usage: self.max_db_memory_usage,
            parallel_archive_downloads: self.parallel_archive_downloads,
            adnl_options: Default::default(),
            rldp_options: Default::default(),
            dht_options: Default::default(),
            neighbours_options: Default::default(),
            overlay_shard_options: Default::default(),
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
        }
    }
}

#[derive(Debug, Deserialize, Clone)]
pub struct StatesConfig {
    pub address: SocketAddr,
}

#[derive(Debug, Default, Deserialize, Clone)]
#[serde(default)]
pub struct KafkaConfig {
    pub raw_transaction_producer: KafkaProducerConfig,
}

#[derive(Debug, Default, Deserialize, Clone)]
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
    1000
}

fn default_batch_flush_threshold_ms() -> u64 {
    60
}

#[derive(Deserialize, Debug, Clone)]
pub enum SecurityConfig {
    Sasl(SaslConfig),
}

#[derive(Deserialize, Default, Debug, Clone)]
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
