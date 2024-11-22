use std::time::Duration;
use anyhow::Result;
use log::LevelFilter;
use tokio::sync::Mutex;
use lazy_static::lazy_static;

#[derive(Debug, Clone, Default)]
pub struct RpcOption {
    pub retry_count: u32,
    pub retry_interval: Duration,
    pub timeout: Duration,
}

#[derive(Debug, Clone, Default)]
pub struct GlobalOption {
    pub gas_price: Option<u64>,
    pub gas_limit: Option<u64>,
    pub web3_log_enabled: bool,
    pub rpc_config: RpcOption,
}

lazy_static! {
    pub static ref GLOBAL_OPTION: Mutex<GlobalOption> = Mutex::new(GlobalOption {
        gas_price: None,
        gas_limit: None,
        web3_log_enabled: false,
        rpc_config: RpcOption {
            retry_count: 5,
            retry_interval: Duration::from_secs(5),
            timeout: Duration::from_secs(30),
        },
    });
}

pub fn init_logging(log_level: &str, enable_colors: bool) -> Result<()> {
    let log_level = match log_level.to_string().as_str() {
        "error" => LevelFilter::Error,
        "warn" => LevelFilter::Warn,
        "info" => LevelFilter::Info,
        "debug" => LevelFilter::Debug,
        "trace" => LevelFilter::Trace,
        _ => LevelFilter::Info,
    };

    let mut builder = env_logger::Builder::new();

    builder.filter_level(log_level);

    // set colorful log
    builder.format(move |buf, record| {
        use env_logger::fmt::Color;
        use std::io::Write;

        let level_color = match record.level() {
            log::Level::Error => Color::Red,
            log::Level::Warn => Color::Yellow,
            log::Level::Info => Color::Green,
            log::Level::Debug => Color::Blue,
            log::Level::Trace => Color::Cyan,
        };

        let mut level_style = buf.style();
        if enable_colors {
            level_style.set_color(level_color).set_bold(true);
        }

        writeln!(
            buf,
            "[{} {} {}] {}",
            chrono::Local::now().format("%Y-%m-%d %H:%M:%S%.3f"),
            level_style.value(record.level()),
            record.target(),
            record.args()
        )
    });

    builder.try_init()?;
    Ok(())
}

pub async fn init_global_config(
    gas_price: Option<u64>,
    gas_limit: Option<u64>,
    web3_log_enabled: bool,
    rpc_retry_count: i32,
    rpc_retry_interval: Duration,
    rpc_timeout: Duration,
) -> Result<()> {
    let mut config = GLOBAL_OPTION.lock().await;
    config.gas_price = gas_price;
    config.gas_limit = gas_limit;
    config.web3_log_enabled = web3_log_enabled;
    config.rpc_config = RpcOption {
        retry_count: rpc_retry_count as u32,
        retry_interval: rpc_retry_interval,
        timeout: rpc_timeout,
    };

    Ok(())
}