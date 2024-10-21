use std::collections::HashMap;
use std::fs;
use std::sync::{Arc, RwLock, Mutex};
use std::time::Duration;
use serde::{Deserialize, Serialize};
use reqwest::blocking::Client;
use log::{warn, info, debug};
use anyhow::{Result, Context, anyhow};
use crate::common::utils::schedule::Scheduler; 

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
    pub static ref IP_LOCATION_MANAGER: Mutex<Option<IPLocationManager>> = Mutex::new(None);
}

pub struct IPLocationManager {
    config: IPLocationConfig,
    items: Arc<RwLock<HashMap<String, IPLocation>>>,
    base_url: String,
}

impl IPLocationManager {
    pub fn init(config: IPLocationConfig) -> Result<()> {
        let items = Arc::new(RwLock::new(Self::read(&config.cache_file)?));

        let manager = IPLocationManager {
            config: config.clone(),
            items: items.clone(),
            base_url: "http://ipinfo.io".to_string(),
        };
        
        info!("Succeeded to read {} cached IP locations", items.read().unwrap().len());

        let mut guard = IP_LOCATION_MANAGER.lock().unwrap();
        *guard = Some(manager);

        let cache_file = config.cache_file.clone();
        let items_clone = items.clone();
        let scheduler = Scheduler::new();
        scheduler.schedule_sync(
            move || Self::write(&cache_file, &items_clone.read().unwrap())
                .map_err(|e| e.into()),
            config.cache_write_interval,
            "Failed to write IP locations"
        );

        Ok(())
    }

    pub fn all() -> Result<HashMap<String, IPLocation>> {
        let manager = IP_LOCATION_MANAGER.lock().unwrap();
        manager.as_ref()
            .ok_or_else(|| anyhow!("IPLocationManager not initialized"))?
            .items.read().map_err(|e| anyhow!("Failed to acquire read lock: {}", e))
            .map(|items| items.clone())
    }

    pub fn query(ip: &str) -> Result<IPLocation> {
        let manager = IP_LOCATION_MANAGER.lock().unwrap();
        let manager = manager.as_ref().ok_or_else(|| anyhow!("IPLocationManager not initialized"))?;

        if let Some(loc) = manager.items.read().unwrap().get(ip) {
            return Ok(loc.clone());
        }

        let url = manager.build_url(ip);
        let client = Client::new();
        let resp = client.get(&url).send().context("Failed to send HTTP request")?;
        let loc: IPLocation = resp.json().context("Failed to parse JSON response")?;

        if !loc.timezone.is_empty() && !loc.region.is_empty() && !loc.country.is_empty() 
           && !loc.city.is_empty() && !loc.loc.is_empty() {
            manager.items.write().unwrap().insert(ip.to_string(), loc.clone());
            debug!("New IP location detected: {:?}", loc);
        } else {
            warn!("New IP location detected with partial fields: {:?}", loc);
        }

        Ok(loc)
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

        let data = serde_json::to_string_pretty(items).context("Failed to serialize IP locations")?;
        fs::write(cache_file, data).context("Failed to write IP locations to file")?;
        info!("Succeeded to write {} IP locations to file", items.len());

        Ok(())
    }

    fn build_url(&self, ip: &str) -> String {
        if self.config.access_token.is_empty() {
            format!("{}/{}/json", self.base_url, ip)
        } else {
            format!("{}/{}/json?token={}", self.base_url, ip, self.config.access_token)
        }
    }

    // for test
    #[cfg(test)]
    pub fn set_base_url(&mut self, url: &str) {
        self.base_url = url.to_string();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::tempdir;
    use mockito;

    #[test]
    fn test_init_and_query() {
        // 设置 mock，匹配完整的路径和查询参数
        let mock = mockito::mock("GET", "/1.1.1.1/json")
            .match_query(mockito::Matcher::AllOf(vec![
                mockito::Matcher::UrlEncoded("token".into(), "test_token".into())
            ]))
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(r#"
                {
                    "ip": "1.1.1.1",
                    "city": "Test City",
                    "region": "Test Region",
                    "country": "Test Country",
                    "loc": "0,0",
                    "timezone": "UTC"
                }
            "#)
            .create();

        let dir = tempdir().unwrap();
        let cache_file = dir.path().join("cache.json");

        let config = IPLocationConfig {
            cache_file: cache_file.to_str().unwrap().to_string(),
            cache_write_interval: Duration::from_secs(60),
            access_token: "test_token".to_string(),
        };

        // 初始化管理器
        IPLocationManager::init(config).unwrap();

        // 重写 MANAGER 中的 base_url
        {
            let mut manager = IP_LOCATION_MANAGER.lock().unwrap();
            if let Some(ref mut m) = *manager {
                // 假设 IPLocationManager 有一个方法来设置基础 URL
                m.set_base_url(&mockito::server_url());
            }
        }

        // 查询 IP
        let location = IPLocationManager::query("1.1.1.1").unwrap();

        assert_eq!(location.city, "Test City");
        assert_eq!(location.country, "Test Country");

        // 检查位置是否被缓存
        let cached_locations = IPLocationManager::all().unwrap();
        assert_eq!(cached_locations.len(), 1);
        assert!(cached_locations.contains_key("1.1.1.1"));

        mock.assert();
    }

    #[test]
    fn test_read_write_cache() {
        let dir = tempdir().unwrap();
        let cache_file = dir.path().join("cache.json");

        let mut locations = HashMap::new();
        locations.insert("1.1.1.1".to_string(), IPLocation {
            city: "Test City".to_string(),
            region: "Test Region".to_string(),
            country: "Test Country".to_string(),
            loc: "0,0".to_string(),
            timezone: "UTC".to_string(),
        });

        // Write to cache
        IPLocationManager::write(cache_file.to_str().unwrap(), &locations).unwrap();

        // Read from cache
        let read_locations = IPLocationManager::read(cache_file.to_str().unwrap()).unwrap();

        assert_eq!(locations.len(), read_locations.len());
        assert_eq!(locations.get("1.1.1.1"), read_locations.get("1.1.1.1"));
    }

    #[test]
    fn test_query_cached_ip() {
        let dir = tempdir().unwrap();
        let cache_file = dir.path().join("cache.json");

        let config = IPLocationConfig {
            cache_file: cache_file.to_str().unwrap().to_string(),
            cache_write_interval: Duration::from_secs(60),
            access_token: "test_token".to_string(),
        };

        // Initialize the manager with a pre-populated cache
        let mut initial_cache = HashMap::new();
        initial_cache.insert("2.2.2.2".to_string(), IPLocation {
            city: "Cached City".to_string(),
            region: "Cached Region".to_string(),
            country: "Cached Country".to_string(),
            loc: "0,0".to_string(),
            timezone: "UTC".to_string(),
        });
        IPLocationManager::write(&config.cache_file, &initial_cache).unwrap();

        IPLocationManager::init(config).unwrap();

        // Query the cached IP
        let location = IPLocationManager::query("2.2.2.2").unwrap();

        assert_eq!(location.city, "Cached City");
        assert_eq!(location.country, "Cached Country");

        // Clean up
        fs::remove_file(cache_file).unwrap();
    }
}