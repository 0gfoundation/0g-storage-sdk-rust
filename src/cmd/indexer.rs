use anyhow::{Context, Result};
use clap::Args;
use tokio::signal;
use jsonrpsee::http_server::HttpServerBuilder;
use std::path::PathBuf;
use std::time::Duration;

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

    #[arg(long, help = "Trusted storage node URLs that separated by comma")]
    pub trusted: Vec<String>,

    #[arg(long, help = "Storage node to discover peers in P2P network")]
    pub node: Option<String>,

    #[arg(
        long,
        default_value = "600",
        help = "Interval to discover peers in network (seconds)"
    )]
    pub discover_interval: u64,

    #[arg(
        long,
        default_value = "600",
        help = "Interval to update shard config of discovered peers (seconds)"
    )]
    pub update_interval: u64,

    #[arg(
        long,
        default_value = "5678",
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
        help = "Interval to write ip locations to cache file (seconds)"
    )]
    pub ip_location_cache_interval: u64,

    #[arg(long, help = "Access token to retrieve IP location from ipinfo.io")]
    pub ip_location_token: Option<String>,

    #[arg(
        long,
        default_value = "86400",
        help = "Validity period of location information (seconds)"
    )]
    pub file_location_cache_expiry: u64,

    #[arg(long, default_value = "100000", help = "Size of file location cache")]
    pub file_location_cache_size: usize,

    #[arg(
        long,
        default_value = "104857600",
        help = "Maximum file size in bytes to download"
    )]
    pub max_download_file_size: u64,
}

pub async fn run_indexer(args: &IndexerArgs) -> Result<()> {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    let node_config = NodeManagerConfig {
        trusted_nodes: args.trusted.clone(),
        discovery_node: args.node.clone().unwrap_or_default(),
        discovery_interval: Duration::from_secs(args.discover_interval),
        discovery_ports: args.discover_ports.clone(),
        update_interval: Duration::from_secs(args.update_interval),
    };

    let location_config = IPLocationConfig {
        cache_file: args.ip_location_cache_file.to_str().unwrap().to_string(),
        cache_write_interval: Duration::from_secs(args.ip_location_cache_interval),
        access_token: args.ip_location_token.clone().unwrap_or_default(),
    };

    let location_cache_config = FileLocationCacheConfig {
        discovery_node: args.node.clone().unwrap_or_default(),
        discovery_ports: args.discover_ports.clone(),
        expiry: Duration::from_secs(args.file_location_cache_expiry),
        cache_size: args.file_location_cache_size,
    };

    let _ = DefaultIPLocationManager::init(location_config).await;

    let _ = DefaultNodeManger::init(node_config).await;

    let _ = DefaultFileLocationCache::init(location_cache_config).await;

    log::info!(
        "Starting indexer service ... trusted: {}, discover: {}",
        args.trusted.len(),
        args.node.is_some()
    );

    // Start server
    let server = HttpServerBuilder::default()
        .build(format!("127.0.0.1:{}", args.endpoint))
        .await
        .context("Failed to build HTTP server")?;

    let server_addr = server.local_addr()?;

    // Start the server and handle errors gracefully
    let server_handle = server.start(IndexerServerImpl.into_rpc())?;
    log::info!("Indexer service running at {}", server_addr);

    // 5. 等待关闭信号
    signal::ctrl_c().await?;
    log::info!("Received shutdown signal");

    // 6. 关闭服务器
    server_handle.stop()?;
    log::info!("Server shutdown complete");

    Ok(())
}
