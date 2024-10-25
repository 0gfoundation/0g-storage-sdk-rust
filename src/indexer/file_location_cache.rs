use anyhow::Result;
use dashmap::DashMap;
use ethers::middleware::gas_oracle::cache;
use lru_time_cache::LruCache;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::sync::RwLock;
use tokio::time::{Duration, Instant};

use super::node_manager::{DefaultNodeManger, NodeManagerConfig};
use crate::common::shard::{select, ShardedNode};
use crate::node::client_admin::AdminClient;
use crate::node::client_zgs::ZgsClient;

const DEFAULT_FIND_FILE_COOLDOWN: Duration = Duration::from_secs(3600); // 60 minutes
const DEFAULT_DISCOVERED_URL_RETRY_INTERVAL: Duration = Duration::from_secs(600); // 10 minutes
const DEFAULT_SUCCESS_CALL_LIFETIME: Duration = Duration::from_secs(600); // 10 mi

lazy_static::lazy_static! {
    pub static ref FILE_LOCATION_CACHE: Arc<Mutex<Option<FileLocationCache>>> = Arc::new(Mutex::new(None));

}

#[derive(Clone)]
pub struct FileLocationCacheConfig {
    pub cache_size: usize,
    pub expiry: Duration,
    pub discovery_node: String,
    pub discovery_ports: Vec<u16>,
}

#[derive(Clone)]
struct SuccessCall {
    node: ShardedNode,
    timestamp: Instant,
}

#[derive(Clone)]
pub struct FileLocationCache {
    cache: Arc<RwLock<LruCache<u64, Vec<ShardedNode>>>>,
    latest_find_file: DashMap<u64, Instant>,
    latest_failed_call: DashMap<String, Instant>,
    latest_success_call: DashMap<String, SuccessCall>,
    discover_node: Option<AdminClient>,
    discovery_ports: Vec<u16>,
}

impl FileLocationCache {
    pub async fn new(config: FileLocationCacheConfig) -> Result<Self> {
        let discover_node = if !config.discovery_node.is_empty() {
            Some(AdminClient::new(&config.discovery_node)?)
        } else {
            None
        };

        let cache = Arc::new(RwLock::new(LruCache::with_expiry_duration_and_capacity(
            config.expiry,
            config.cache_size,
        )));

        Ok(Self {
            cache,
            latest_find_file: DashMap::new(),
            latest_failed_call: DashMap::new(),
            latest_success_call: DashMap::new(),
            discover_node,
            discovery_ports: config.discovery_ports,
        })
    }

    pub async fn get_file_locations(&self, tx_seq: u64) -> Result<Option<Vec<ShardedNode>>> {
        // Check cache first
        if let Some(nodes) = self.cache.write().await.get_mut(&tx_seq) {
            if select(nodes.as_mut_slice(), 1, false).1 {
                return Ok(Some(nodes.to_vec()));
            }
        }

        let mut nodes = Vec::new();
        let mut selected = HashMap::new();

        // Fetch from trusted nodes
        let trusted_clients = DefaultNodeManger::trusted_clients().await?;
        for client in trusted_clients {
            let start = Instant::now();

            if let Ok(Some(file_info)) = client.get_file_info_by_tx_seq(tx_seq).await {
                if !file_info.finalized {
                    continue;
                }

                if let Ok(config) = client.get_shard_config().await {
                    if !config.is_valid() {
                        continue;
                    }

                    nodes.push(ShardedNode {
                        url: client.url.clone(),
                        config,
                        latency: start.elapsed().as_millis() as i64,
                        since: 0,
                    });
                    selected.insert(client.url.clone(), ());
                }
            }
        }

        log::debug!(
            "find file #{} from trusted nodes, got {} nodes holding the file",
            tx_seq,
            nodes.len()
        );

        if select(nodes.as_mut(), 1, false).1 {
            return Ok(Some(nodes));
        }

        if let Some(discover_node) = &self.discover_node {
            if let Ok(locations) = discover_node.get_file_location(tx_seq, false).await {
                log::debug!(
                    "find file #{} from location cache, got {} nodes holding the file",
                    tx_seq,
                    locations.len()
                );

                for location in locations {
                    for port in &self.discovery_ports {
                        let url = format!("http://{}:{}", location.ip, port);

                        if selected.contains_key(&url) {
                            break;
                        }

                        if let Some(success_call) = self.latest_success_call.get(&url) {
                            if success_call.timestamp.elapsed() < DEFAULT_SUCCESS_CALL_LIFETIME {
                                nodes.push(success_call.node.clone());
                                break;
                            }
                        }

                        if let Some(failed_time) = self.latest_failed_call.get(&url) {
                            if failed_time.elapsed() < DEFAULT_DISCOVERED_URL_RETRY_INTERVAL {
                                continue;
                            }
                        }

                        match ZgsClient::new(&url) {
                            Ok(client) => {
                                let start = Instant::now();

                                match client.get_file_info_by_tx_seq(tx_seq).await {
                                    Ok(Some(file_info)) if file_info.finalized => {
                                        if let Ok(config) = client.get_shard_config().await {
                                            if config.is_valid() {
                                                let node = ShardedNode {
                                                    url: url.clone(),
                                                    config,
                                                    latency: start.elapsed().as_millis() as i64,
                                                    since: 0,
                                                };

                                                nodes.push(node.clone());
                                                self.latest_success_call.insert(
                                                    url.clone(),
                                                    SuccessCall {
                                                        node,
                                                        timestamp: Instant::now(),
                                                    },
                                                );
                                                selected.insert(url, ());
                                                break;
                                            }
                                        }
                                    }
                                    Err(_) => {
                                        self.latest_failed_call.insert(url, Instant::now());
                                    }
                                    _ => {}
                                }
                            }
                            Err(_) => continue,
                        }
                    }
                }
                if select(nodes.as_mut(), 1, false).1 {
                    return Ok(Some(nodes));
                }

                if let Some(last_find) = self.latest_find_file.get(&tx_seq) {
                    if last_find.elapsed() < DEFAULT_FIND_FILE_COOLDOWN {
                        return Ok(None);
                    }
                }

                log::debug!("triggering FindFile for tx seq {}", tx_seq);
                if let Err(e) = discover_node.find_file(tx_seq).await {
                    log::error!("Failed to trigger find file: {:?}", e);
                }
                self.latest_find_file.insert(tx_seq, Instant::now());
            }
        }
        Ok(None)
    }
}

pub struct DefaultFileLocationCache;

impl DefaultFileLocationCache {
    pub async fn init(config: FileLocationCacheConfig) -> Result<()> {
        if let Ok(cache) = FileLocationCache::new(config).await {
            let mut guard = FILE_LOCATION_CACHE.lock().await;
            *guard = Some(cache);

            Ok(())
        } else {
            anyhow::bail!("Failed to init file location cache")
        }
    }

    pub async fn get_file_locations(tx_seq: u64) -> Result<Option<Vec<ShardedNode>>> {
        let cache = FILE_LOCATION_CACHE.lock().await;
        cache
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("FILE_LOCATION_CACHE not initialized"))?
            .get_file_locations(tx_seq)
            .await

        // if let Some(cache) = FILE_LOCATION_CACHE.lock().await.as_ref() {
        //     cache.get_file_locations(tx_seq).await
        // } else {
        //     anyhow::bail!("FILE_LOCATION_CACHE is not initialized")
        // }
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use mockito::mock;

    #[tokio::test]
    async fn test_file_location_cache_init() {
        let config = FileLocationCacheConfig {
            cache_size: 10,
            expiry: Duration::from_secs(60),
            discovery_node: "http://localhost:8000".to_string(),
            discovery_ports: vec![8080],
        };

        let result = DefaultFileLocationCache::init(config).await;
        assert!(result.is_ok());

        let cache = FILE_LOCATION_CACHE.lock().await;
        assert!(cache.is_some());
    }

    #[tokio::test]
    async fn test_get_file_locations() {

        let node_config = NodeManagerConfig {
            trusted_nodes: vec!["http://127.0.0.1:5678".to_string()],
            discovery_node: "".to_string(),
            discovery_interval: Duration::from_secs(10),
            discovery_ports: vec![],
            update_interval: Duration::from_secs(5),
        };
        let _ = DefaultNodeManger::init(node_config).await;

        let config = FileLocationCacheConfig {
            cache_size: 10,
            expiry: Duration::from_secs(60),
            discovery_node: "http://localhost:8000".to_string(),
            discovery_ports: vec![8080],
        };

        DefaultFileLocationCache::init(config).await.unwrap();

        // Set up a mock server for discovery node
        let _mock = mock("GET", "/file_location")
            .with_status(200)
            .with_body(r#"{"finalized": true, "config": {"valid": true}}"#)
            .create();

        let tx_seq = 1;
        let result = DefaultFileLocationCache::get_file_locations(tx_seq).await;
        println!("result: {:?}", result);
        // assert!(result.is_ok());
        // assert!(result.unwrap().is_some());
    }

    #[tokio::test]
    async fn test_get_file_locations_cached() {
        let config = FileLocationCacheConfig {
            cache_size: 10,
            expiry: Duration::from_secs(60),
            discovery_node: "http://localhost:8000".to_string(),
            discovery_ports: vec![8080],
        };

        DefaultFileLocationCache::init(config).await.unwrap();

        let tx_seq = 1;
        
        // First call should not hit the cache
        let _mock = mock("GET", "/file_location")
            .with_status(200)
            .with_body(r#"{"finalized": true, "config": {"valid": true}}"#)
            .create();

        let result = DefaultFileLocationCache::get_file_locations(tx_seq).await;
        assert!(result.is_ok());
        assert!(result.unwrap().is_some());

        // Now we should hit the cache
        let cached_result = DefaultFileLocationCache::get_file_locations(tx_seq).await;
        assert!(cached_result.is_ok());
        assert!(cached_result.unwrap().is_some());
    }

    #[tokio::test]
    async fn test_get_file_locations_with_failure() {
        let config = FileLocationCacheConfig {
            cache_size: 10,
            expiry: Duration::from_secs(60),
            discovery_node: "http://localhost:8000".to_string(),
            discovery_ports: vec![8080],
        };

        DefaultFileLocationCache::init(config).await.unwrap();

        let tx_seq = 2;

        // Mock a failure response for the discovery node
        let _mock = mock("GET", "/file_location")
            .with_status(500)
            .create();

        let result = DefaultFileLocationCache::get_file_locations(tx_seq).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_find_file_trigger() {
        let config = FileLocationCacheConfig {
            cache_size: 10,
            expiry: Duration::from_secs(60),
            discovery_node: "http://localhost:8000".to_string(),
            discovery_ports: vec![8080],
        };

        DefaultFileLocationCache::init(config).await.unwrap();

        let tx_seq = 3;

        // Triggering find_file without a valid response should log and not cache
        let result = DefaultFileLocationCache::get_file_locations(tx_seq).await;
        assert!(result.is_ok());
        assert!(result.unwrap().is_none());

        // Verify the log entry here if necessary
    }
}
