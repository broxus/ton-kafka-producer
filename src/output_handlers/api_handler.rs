use anyhow::{Context, Result};
use async_trait::async_trait;
use base64::Engine;
use reqwest::StatusCode;
use serde::Serialize;
use ton_types::serialize_toc;

use crate::config::ApiProducerConfig;
use crate::models::TransactionNode;
use crate::output_handlers::OutputHandler;
use crate::tree_packer::TreePacker;

#[derive(Serialize, Debug, Clone)]
pub struct BocRequest {
    pub boc: String,
}

pub struct ApiHandler {
    request_url: String,
    client: reqwest::Client,
}

impl ApiHandler {
    pub fn new(config: ApiProducerConfig) -> Self {
        let client = reqwest::Client::new();

        Self {
            request_url: config.request_url,
            client,
        }
    }
}

#[async_trait]
impl OutputHandler for ApiHandler {
    async fn handle_output(&self, trees: &[TransactionNode]) -> Result<()> {
        let packer = TreePacker::default();
        for tree in trees {
            let cell = packer.pack(tree)?;
            if let Ok(boc) = serialize_toc(&cell) {
                tracing::info!(
                    "Sending transaction tree with root {:x} of size {}",
                    tree.hash(),
                    boc.len()
                );
                let base64 = base64::engine::general_purpose::STANDARD.encode(boc);
                if let Err(e) = send_request(&self.client, &self.request_url, base64).await {
                    tracing::warn!("{e:?}")
                }
            }
        }

        Ok(())
    }
}

async fn send_request(client: &reqwest::Client, url: &str, boc: String) -> Result<()> {
    let ok_status = StatusCode::from_u16(200)?;
    let body = BocRequest { boc };

    let res = client
        .post(url)
        .json(&body)
        .send()
        .await
        .context("Failed to send request")?;

    if res.status() != ok_status {
        return Err(anyhow::Error::msg(format!(
            "Remote API {} responded with Non OK Status",
            url
        )));
    }

    Ok(())
}
