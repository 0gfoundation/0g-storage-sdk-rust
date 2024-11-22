use anyhow::{anyhow, Result};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};
use tokio::sync::Mutex;
use url::Url;

use super::ip_location::DefaultIPLocationManager;
use crate::common::shard::ShardedNode;
use crate::common::utils::schedule::Scheduler;
use crate::node::{client_admin::AdminClient, client_zgs::ZgsClient};

lazy_static::lazy_static! {
    pub static ref NODE_MANAGER: Arc<Mutex<Option<NodeManager>>> = Arc::new(Mutex::new(None));
}

#[derive(Clone, Debug)]
pub struct NodeManagerConfig {
    pub trusted_nodes: Vec<String>,
    pub discovery_node: String,
    pub discovery_interval: Duration,
    pub discovery_ports: Vec<u16>,
    pub update_interval: Duration,
}

#[derive(Debug)]
pub struct NodeManager {
    trusted: RwLock<HashMap<String, Arc<ZgsClient>>>,
    discovery_node: Option<AdminClient>,
    discovery_ports: Vec<u16>,
    discovered: RwLock<HashMap<String, ShardedNode>>,
}

impl NodeManager {
    pub fn trusted_clients(&self) -> Vec<Arc<ZgsClient>> {
        self.trusted.read().unwrap().values().cloned().collect()
    }

    pub async fn trusted(&self) -> Result<Vec<ShardedNode>> {
        let clients = self.trusted_clients();
        let mut nodes = Vec::new();

        for client in clients {
            let start = Instant::now();
            match client.get_shard_config().await {
                Ok(config) => {
                    if config.is_valid() {
                        nodes.push(ShardedNode {
                            url: client.url.to_string(),
                            config,
                            latency: start.elapsed().as_millis() as i64,
                            since: chrono::Utc::now().timestamp(),
                        });
                    } else {
                        log::warn!(
                            "Invalid shard config retrieved from trusted storage node {}: {:?}",
                            client.url, config
                        );
                    }
                }
                Err(e) => log::error!(
                    "Failed to retrieve shard config from trusted storage node {}, error: {}",
                    client.url, e
                ),
            }
        }

        Ok(nodes)
    }

    pub fn discovered(&self) -> Vec<ShardedNode> {
        self.discovered.read().unwrap().values().cloned().collect()
    }

    pub async fn add_trusted_nodes(&self, nodes: &[String]) -> Result<()> {
        for node in nodes {
            let ip = parse_ip(node);
            if let Err(e) = DefaultIPLocationManager::query(&ip).await {
                log::warn!("Failed to query IP location for {}: {}", ip, e);
            }

            let client = Arc::new(ZgsClient::new(node).await?);
            self.trusted.write().unwrap().insert(node.clone(), client);
        }
        Ok(())
    }

    async fn discover(&self) -> Result<()> {
        let start = Instant::now();
        let peers = self
            .discovery_node
            .as_ref()
            .ok_or_else(|| anyhow!("Discovery node not set"))?
            .get_peers()
            .await?;

        if let Some(peers) = peers {
            log::info!(
                "Succeeded to retrieve {} peers from storage node in {:?}",
                peers.len(),
                start.elapsed()
            );

            let mut num_new = 0;
            for (_, v) in peers {
                if v.seen_ips.is_empty() {
                    continue;
                }

                if v.seen_ips.len() > 1 {
                    log::warn!("More than one seen IPs: {:?}", v.seen_ips);
                }

                for port in &self.discovery_ports {
                    let url = format!("http://{}:{}", v.seen_ips.iter().next().unwrap(), port);

                    if self.trusted.read().unwrap().contains_key(&url) {
                        continue;
                    }

                    if self.discovered.read().unwrap().contains_key(&url) {
                        continue;
                    }

                    match self.update_node(&url).await {
                        Ok(node) => {
                            log::info!(
                                "New peer discovered: url={}, shard={:?}, latency={}",
                                url, node.config, node.latency
                            );
                            num_new += 1;
                        }
                        Err(e) => log::error!("Failed to add new peer {}: {}", url, e),
                    }

                    break;
                }
            }

            if num_new > 0 {
                log::info!("{} new peers discovered", num_new);
            }
        }

        Ok(())
    }

    async fn update_node(&self, url: &str) -> Result<ShardedNode> {
        let ip = parse_ip(url);
        if let Err(e) = DefaultIPLocationManager::query(&ip).await {
            log::error!("Failed to query IP location for {}: {}", ip, e);
        }

        let client = ZgsClient::new(url).await?;
        let start = Instant::now();
        let config = client.get_shard_config().await?;

        if !config.is_valid() {
            anyhow::bail!("Invalid shard config retrieved: {:?}", config);
        }

        let node = ShardedNode {
            url: url.to_string(),
            config,
            latency: start.elapsed().as_millis() as i64,
            since: chrono::Utc::now().timestamp(),
        };

        self.discovered
            .write()
            .unwrap()
            .insert(url.to_string(), node.clone());

        Ok(node)
    }

    async fn update(&self) -> Result<()> {
        let urls: Vec<String> = self.discovered.read().unwrap().keys().cloned().collect();

        if urls.is_empty() {
            return Ok(());
        }

        log::info!("Begin to update shard config for {} nodes", urls.len());

        let start = Instant::now();

        for url in &urls {
            match self.update_node(&url).await {
                Ok(_) => {}
                Err(e) => {
                    log::error!(
                        "Failed to update shard config for {}, removing from cache: {}",
                        url, e
                    );
                    self.discovered.write().unwrap().remove(url);
                }
            }
        }

        log::info!(
            "Completed updating shard config for {} nodes in {:?}",
            urls.len(),
            start.elapsed()
        );

        Ok(())
    }
}

fn parse_ip(url: &str) -> String {
    let url = url.to_lowercase();
    if let Ok(parsed_url) = Url::parse(&url) {
        parsed_url.host_str().unwrap_or(&url).to_string()
    } else {
        url
    }
}

pub struct DefaultNodeManger;

impl DefaultNodeManger {
    pub async fn init(config: NodeManagerConfig) -> Result<()> {
        let mut manager_guard = NODE_MANAGER.lock().await;

        if manager_guard.is_none() {
            *manager_guard = Some(NodeManager {
                trusted: RwLock::new(HashMap::new()),
                discovery_node: None,
                discovery_ports: vec![],
                discovered: RwLock::new(HashMap::new()),
            });
        }

        if let Some(manager) = manager_guard.as_mut() {
            if !config.discovery_node.is_empty() {
                manager.discovery_node = Some(AdminClient::new(&config.discovery_node).await?);
            }

            manager.discovery_ports = config.discovery_ports.clone();
            manager.add_trusted_nodes(&config.trusted_nodes).await?;

            if !config.discovery_node.is_empty() {
                let node_manager = NODE_MANAGER.clone();
                let discovery_interval = config.discovery_interval;
                let update_interval = config.update_interval;

                let scheduler = Scheduler::new();

                let node_manager_clone = node_manager.clone();
                scheduler.schedule_now(
                    move || {
                        let node_manager = node_manager_clone.clone();
                        async move {
                            let mut guard = node_manager.lock().await;
                            if let Some(manager) = guard.as_mut() {
                                manager.discover().await
                            } else {
                                anyhow::bail!("Discover node at once failed")
                            }
                        }
                    },
                    discovery_interval,
                    "Failed to discover storage nodes once",
                );

                let node_manager_clone = node_manager.clone();
                scheduler.schedule(
                    move || {
                        let node_manager = node_manager_clone.clone();
                        async move {
                            let mut guard = node_manager.lock().await;
                            if let Some(manager) = guard.as_mut() {
                                manager.update().await
                            } else {
                                anyhow::bail!("Discover node periodically failed")
                            }
                        }
                    },
                    update_interval,
                    "Failed to update shard configs once",
                );
            }
        }

        Ok(())
    }

    pub async fn trusted_clients() -> Result<Vec<Arc<ZgsClient>>> {
        let manager = NODE_MANAGER.lock().await;

        if let Some(manager) = manager.as_ref() {
            Ok(manager.trusted_clients())
        } else {
            anyhow::bail!("NODE_MANAGER is not initialized")
        }
    }

    pub async fn trusted() -> Result<Vec<ShardedNode>> {
        let manager = NODE_MANAGER.lock().await;

        if let Some(manager) = manager.as_ref() {
            manager.trusted().await
        } else {
            anyhow::bail!("NODE_MANAGER is not initialized")
        }
    }

    pub async fn discovered() -> Result<Vec<ShardedNode>> {
        let manager = NODE_MANAGER.lock().await;

        if let Some(manager) = manager.as_ref() {
            Ok(manager.discovered())
        } else {
            anyhow::bail!("NODE_MANAGER is not initialized")
        }
    }

    pub async fn add_trusted_nodes(nodes: &[String]) -> Result<()> {
        let manager = NODE_MANAGER.lock().await;

        if let Some(manager) = manager.as_ref() {
            manager.add_trusted_nodes(nodes).await
        } else {
            anyhow::bail!("NODE_MANAGER is not initialized")
        }
    }
}
