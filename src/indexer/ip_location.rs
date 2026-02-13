use crate::common::utils::schedule::Scheduler;
use anyhow::{anyhow, Context, Result};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs;
use std::sync::{Arc, RwLock};
use std::time::Duration;
use tokio::sync::Mutex;

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

        log::info!(
            "Succeeded to read {} cached IP locations",
            items.read().unwrap().len()
        );

        let mut guard = IP_LOCATION_MANAGER.lock().await;
        *guard = Some(manager);

        let cache_file = config.cache_file.clone();
        let items_clone = items.clone();
        let scheduler = Scheduler::new();
        scheduler.schedule_sync(
            move || Self::write(&cache_file, &items_clone.read().unwrap()),
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
        log::info!("Starting query for IP: {}", ip);

        // Skip external lookup for localhost/loopback addresses
        if ip == "127.0.0.1" || ip.eq_ignore_ascii_case("localhost") || ip.starts_with("127.") {
            log::info!("Returning localhost location for IP: {}", ip);
            return Ok(IPLocation {
                city: "localhost".to_string(),
                region: "localhost".to_string(),
                country: "localhost".to_string(),
                loc: "0,0".to_string(),
                timezone: "UTC".to_string(),
            });
        }

        // check cache
        if let Ok(items) = DefaultIPLocationManager::all().await {
            log::info!("Successfully retrieved cached items");

            if let Some(cached_loc) = items.get(ip) {
                log::info!("Cache hit for IP: {}", ip);
                return Ok(cached_loc.clone());
            }
            log::info!("Cache miss for IP: {}", ip);
        } else {
            log::warn!("Failed to retrieve cached items");
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
                        log::info!("Updated cache for IP: {}", ip);
                    } else {
                        log::warn!("Failed to acquire write lock for cache update");
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
        log::info!("Succeeded to write {} IP locations to file", items.len());

        Ok(())
    }
}
