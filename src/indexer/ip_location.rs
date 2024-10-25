use crate::common::utils::schedule::Scheduler;
use anyhow::{anyhow, Context, Result};
use log::{debug, info, warn};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs;
use std::sync::{Arc, RwLock};
use tokio::sync::Mutex;
use tokio::time::{Duration};

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct IPLocation {
    pub city: String,
    pub region: String,
    pub country: String,
    pub loc: String,
    pub timezone: String,
}

#[derive(Clone, Debug)]
pub struct IPLocationConfig {
    pub cache_file: String,
    pub cache_write_interval: Duration,
    pub access_token: String,
}

lazy_static::lazy_static! {
    pub static ref IP_LOCATION_MANAGER: Arc<Mutex<Option<IPLocationManager>>> = Arc::new(Mutex::new(None));
}

pub struct IPLocationManager {
    config: IPLocationConfig,
    items: Arc<RwLock<HashMap<String, IPLocation>>>,
    base_url: String,
}

impl IPLocationManager {
    fn build_url(&self, ip: &str) -> String {
        if ip == "127.0.0.1" || ip.eq_ignore_ascii_case("localhost") {
            return format!("{}/json", self.base_url);
        }
    
        if self.config.access_token.is_empty() {
            format!("{}/{}/json", self.base_url, ip)
        } else {
            format!(
                "{}/{}/json?token={}",
                self.base_url, ip, self.config.access_token
            )
        }
    }

    // for test
    #[cfg(test)]
    pub fn set_base_url(&mut self, url: &str) {
        self.base_url = url.to_string();
    }
}

pub struct DefaultIPLocationManager;

impl DefaultIPLocationManager {
    pub async fn init(config: IPLocationConfig) -> Result<()> {
        let items = Arc::new(RwLock::new(Self::read(&config.cache_file)?));

        let manager = IPLocationManager {
            config: config.clone(),
            items: items.clone(),
            base_url: "http://ipinfo.io".to_string(),
        };

        info!(
            "Succeeded to read {} cached IP locations",
            items.read().unwrap().len()
        );

        let mut guard = IP_LOCATION_MANAGER.lock().await;
        *guard = Some(manager);

        let cache_file = config.cache_file.clone();
        let items_clone = items.clone();
        let scheduler = Scheduler::new();
        scheduler.schedule_sync(
            move || Self::write(&cache_file, &items_clone.read().unwrap()).map_err(|e| e.into()),
            config.cache_write_interval,
            "Failed to write IP locations",
        );

        Ok(())
    }

    pub async fn all() -> Result<HashMap<String, IPLocation>> {
        let manager = IP_LOCATION_MANAGER.lock().await;
        manager
            .as_ref()
            .ok_or_else(|| anyhow!("IPLocationManager not initialized"))?
            .items
            .read()
            .map_err(|e| anyhow!("Failed to acquire read lock: {}", e))
            .map(|items| items.clone())
    }

    pub async fn query(ip: &str) -> Result<IPLocation> {
        debug!("Starting query for IP: {}", ip);

        // check cache
        if let Ok(items) = DefaultIPLocationManager::all().await {
            debug!("Successfully retrieved cached items");

            if let Some(cached_loc) = items.get(ip) {
                debug!("Cache hit for IP: {}", ip);
                return Ok(cached_loc.clone());
            }
            debug!("Cache miss for IP: {}", ip);
        } else {
            warn!("Failed to retrieve cached items");
        }

        let url = {
            let manager = IP_LOCATION_MANAGER.lock().await;
            let manager = manager
                .as_ref()
                .ok_or_else(|| anyhow!("IPLocationManager not initialized"))?;
            manager.build_url(ip)
        };

        let client = reqwest::Client::new();
        let resp = client.get(&url).send().await?;
        match resp.status() {
            reqwest::StatusCode::OK => {
                let loc: IPLocation = resp
                    .json()
                    .await
                    .context("Failed to deserialize response")?;

                let manager = IP_LOCATION_MANAGER.lock().await;
                if let Some(manager) = manager.as_ref() {
                    if let Ok(mut items) = manager.items.write() {
                        items.insert(ip.to_string(), loc.clone());
                        debug!("Updated cache for IP: {}", ip);
                    } else {
                        warn!("Failed to acquire write lock for cache update");
                    }
                }

                Ok(loc)
            }
            status => {
                let error_msg = format!("Unexpected status code: {}, failed url: {}", status, url);
                Err(anyhow!(error_msg))
            }
        }
    }

    fn read(cache_file: &str) -> Result<HashMap<String, IPLocation>> {
        match fs::read_to_string(cache_file) {
            Ok(data) => serde_json::from_str(&data).context("Failed to parse cached data"),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(HashMap::new()),
            Err(e) => Err(e).context("Failed to read cache file"),
        }
    }

    fn write(cache_file: &str, items: &HashMap<String, IPLocation>) -> Result<()> {
        if items.is_empty() {
            return Ok(());
        }

        let data =
            serde_json::to_string_pretty(items).context("Failed to serialize IP locations")?;
        fs::write(cache_file, data).context("Failed to write IP locations to file")?;
        info!("Succeeded to write {} IP locations to file", items.len());

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mockito;
    use tempfile::tempdir;
    use once_cell::sync::OnceCell;

    static INIT: OnceCell<Mutex<()>> = OnceCell::new();

    async fn global_setup_async() {
        let _ = INIT.get_or_init(|| Mutex::new(())).lock().await;

        let dir = tempdir().unwrap();
        let cache_file = dir.path().join("cache.json");

        let config = IPLocationConfig {
            cache_file: cache_file.to_str().unwrap().to_string(),
            cache_write_interval: Duration::from_secs(60),
            access_token: "test_token".to_string(),
        };

        // initialize ip location manager
        let mut location = HashMap::new();
        location.insert(
            "2.2.2.2".to_string(),
            IPLocation {
                city: "Cached City".to_string(),
                region: "Cached Region".to_string(),
                country: "Cached Country".to_string(),
                loc: "0,0".to_string(),
                timezone: "UTC".to_string(),
            },
        );
        DefaultIPLocationManager::write(&config.cache_file, &location).unwrap();
        let _ = DefaultIPLocationManager::init(config.clone()).await;

        let cached_locations = DefaultIPLocationManager::all().await.unwrap();
        assert!(cached_locations.contains_key("2.2.2.2"));
    }

    #[tokio::test]
    async fn test_init_and_query() {
        global_setup_async().await;

        // mock http://ipinfo.io service
        // let mock = mockito::mock("GET", "/1.1.1.1/json")
        //     .match_query(mockito::Matcher::Any)
        //     .with_status(200)
        //     .with_header("content-type", "application/json")
        //     .with_body(
        //         r#"{
        //         "ip": "1.1.1.1",
        //         "city": "Test City",
        //         "region": "Test Region",
        //         "country": "Test Country",
        //         "loc": "0,0",
        //         "timezone": "UTC"
        //         }"#,
        //     )
        //     .create();

        // // just for test, rewrite IP_LOCATION_MANAGER's base_url
        // {
        //     let mut manager = IP_LOCATION_MANAGER.lock().await;
        //     if let Some(ref mut m) = *manager {
        //         m.set_base_url(&mockito::server_url());
        //     }
        // }

        // query ip 
        let location = DefaultIPLocationManager::query("127.0.0.1").await.unwrap();
        // assert_eq!(location.city, "Test City");
        // assert_eq!(location.country, "Test Country");

        // check if the loction cached
        // let cached_locations = DefaultIPLocationManager::all().await.unwrap();
        // assert!(cached_locations.contains_key("1.1.1.1"));

        // mock.assert();
    }

    #[tokio::test]
    async fn test_query_cached_ip() {
        global_setup_async().await;

        // query the cached ip
        let location = DefaultIPLocationManager::query("2.2.2.2").await.unwrap();

        assert_eq!(location.city, "Cached City");
        assert_eq!(location.country, "Cached Country");
    }
}
