use anyhow::Result;

mod auth;
mod client;
mod commands;
mod transport;

pub use client::client;
pub use commands::*;

pub fn random_secret() -> Result<String> {
    let mut bytes = [0; 32];
    aws_lc_rs::rand::fill(&mut bytes).map_err(|_| anyhow::anyhow!("randomness unavailable"))?;
    Ok(hex::encode(bytes))
}

pub fn deployment_key() -> Result<String> {
    let mut bytes = [0; 48];
    aws_lc_rs::rand::fill(&mut bytes).map_err(|_| anyhow::anyhow!("randomness unavailable"))?;
    use base64::Engine;
    Ok(base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bytes))
}
