use anyhow::{Context, Result};
use clap::Args;
use jsonrpsee::http_server::HttpServerBuilder;
use std::path::PathBuf;
use std::time::Duration;

use crate::common::utils::duration_from_str;
use crate::indexer::api::{IndexerServer, IndexerServerImpl};
use crate::indexer::{
    file_location_cache::{DefaultFileLocationCache, FileLocationCacheConfig},
    ip_location::{DefaultIPLocationManager, IPLocationConfig},
    node_manager::{DefaultNodeManger, NodeManagerConfig},
};

#[derive(Args)]
pub struct IndexerArgs {
    #[arg(long, default_value = "12345", help = "Indexer service endpoint")]
    pub endpoint: String,

    #[arg(long, value_delimiter = ',', help = "Trusted storage node URLs separated by comma")]
    pub trusted: Vec<String>,

    #[arg(long, help = "Storage node to discover peers in P2P network")]
    pub node: Option<String>,

    #[arg(
        long,
        default_value = "600",
        value_parser = duration_from_str,
        help = "Interval to discover peers in network (seconds)"
    )]
    pub discover_interval: Duration,

    #[arg(
        long,
        default_value = "600",
        value_parser = duration_from_str,
        help = "Interval to update shard config of discovered peers (seconds)"
    )]
    pub update_interval: Duration,

    #[arg(
        long,
        default_value = "5678",
        value_delimiter = ',',
        help = "Ports to try for discovered nodes"
    )]
    pub discover_ports: Vec<u16>,

    #[arg(
        long,
        default_value = ".ip-location-cache.json",
        help = "File name to cache IP locations"
    )]
    pub ip_location_cache_file: PathBuf,

    #[arg(
        long,
        default_value = "600",
        value_parser = duration_from_str,
        help = "Interval to write IP locations to cache file (seconds)"
    )]
    pub ip_location_cache_interval: Duration,

    #[arg(long, help = "Access token to retrieve IP location from ipinfo.io")]
    pub ip_location_token: Option<String>,

    #[arg(
        long,
        default_value = "86400",
        value_parser = duration_from_str,
        help = "Validity period of location information (seconds)"
    )]
    pub file_location_cache_expiry: Duration,

    #[arg(long, default_value = "100000", help = "Size of file location cache")]
    pub file_location_cache_size: u64,

    #[arg(
        long,
        default_value = "104857600",
        help = "Maximum file size in bytes to download"
    )]
    pub max_download_file_size: u64,

    #[arg(
        long,
        default_value = "1",
        help = "expected number of replications to upload"
    )]
    pub expected_replica: u64,
}

pub async fn run_indexer(args: &IndexerArgs) -> Result<()> {
    let node_config = NodeManagerConfig {
        trusted_nodes: args.trusted.clone(),
        discovery_node: args.node.clone().unwrap_or_default(),
        discovery_interval: args.discover_interval,
        discovery_ports: args.discover_ports.clone(),
        update_interval: args.update_interval,
    };

    let location_config = IPLocationConfig {
        cache_file: args
            .ip_location_cache_file
            .to_str()
            .unwrap_or_default()
            .to_string(),
        cache_write_interval: args.ip_location_cache_interval,
        access_token: args.ip_location_token.clone().unwrap_or_default(),
    };

    let location_cache_config = FileLocationCacheConfig {
        discovery_node: args.node.clone().unwrap_or_default(),
        discovery_ports: args.discover_ports.clone(),
        expiry: args.file_location_cache_expiry,
        cache_size: args.file_location_cache_size,
    };

    log::info!(
        "Starting indexer service ... trusted: {}, discover: {}",
        args.trusted.len(),
        args.node.is_some()
    );

    DefaultIPLocationManager::init(location_config).await?;
    DefaultNodeManger::init(node_config).await?;
    DefaultFileLocationCache::init(location_cache_config).await?;

    log::info!("Starting server at 127.0.0.1:{}", args.endpoint);
    let server = HttpServerBuilder::default()
        .build(format!("127.0.0.1:{}", args.endpoint))
        .await
        .context("Failed to build HTTP server")?;

    let server_addr = server.local_addr()?;
    log::info!("Indexer service running at {}", server_addr);

    let server_handle = server.start(IndexerServerImpl.into_rpc())?;

    match tokio::signal::ctrl_c().await {
        Ok(()) => {
            log::info!("Received shutdown signal, stopping server...");
            server_handle.stop()?;
        }
        Err(err) => {
            log::error!("Unable to listen for shutdown signal: {}", err);
            server_handle.stop()?;
            return Err(err.into());
        }
    }

    Ok(())
}
