use crate::config::HandlerConfig;
use crate::models::TransactionNode;
use crate::output_handlers::api_handler::ApiHandler;
use crate::output_handlers::kafka_handler::KafkaOutputHandler;
use crate::output_handlers::simple_handler::ExampleHandler;
use anyhow::anyhow;
use anyhow::Result;
use async_trait::async_trait;

mod api_handler;
mod kafka_handler;
mod simple_handler;

#[async_trait]
pub trait OutputHandler: Send + Sync {
    async fn handle_output(&self, trees: &[TransactionNode]) -> Result<()>;
}

pub fn prepare_handlers(
    handler_configurations: Vec<HandlerConfig>,
) -> Result<Vec<Box<dyn OutputHandler>>> {
    let mut handlers: Vec<Box<dyn OutputHandler>> = Vec::new();

    for h in handler_configurations {
        match h {
            HandlerConfig::Example => {
                let handler = Box::<ExampleHandler>::default();
                handlers.push(handler);
            }
            HandlerConfig::Kafka(config) => match KafkaOutputHandler::new(config) {
                Ok(handler) => {
                    handlers.push(Box::new(handler));
                }
                Err(e) => {
                    tracing::error!("Failed to prepare kafka handler. Err: {e}");
                }
            },
            HandlerConfig::Api(config) => {
                let handler = ApiHandler::new(config);
                handlers.push(Box::new(handler));
            }
        }
    }

    if handlers.is_empty() {
        return Err(anyhow!("Not output handlers specified"));
    }

    Ok(handlers)
}
