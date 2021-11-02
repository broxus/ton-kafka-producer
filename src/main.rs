use anyhow::{Context, Result};
use argh::FromArgs;
use serde::Deserialize;
use std::sync::Arc;
use ton_kafka_producer::config::*;
use ton_kafka_producer::engine::shard_accounts_subscriber::ShardAccountsSubscriber;
use ton_kafka_producer::engine::*;
use ton_kafka_producer::rpc;

#[tokio::main]
async fn main() -> Result<()> {
    run(argh::from_env()).await
}

async fn run(app: App) -> Result<()> {
    let config: AppConfig = read_config(app.config)?;
    let _global_config = ton_indexer::GlobalConfig::from_file(&app.global_config)
        .context("Failed to open global config")?;

    init_logger(&config.logger_settings).context("Failed to init logger")?;

    log::info!("Initializing producer...");

    let shard_accounts_subscriber = Arc::new(ShardAccountsSubscriber::default());

    let engine = Engine::new(config, _global_config, shard_accounts_subscriber.clone())
        .await
        .context("Failed to create engine")?;

    engine.start().await.context("Failed to start engine")?;

    rpc::serve(shard_accounts_subscriber, "0.0.0.0:12345".parse().unwrap()).await;

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
    global_config: String,
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
