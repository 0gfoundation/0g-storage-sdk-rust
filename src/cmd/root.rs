use clap::{Parser, Subcommand};
use super::upload::UploadArgs;

#[derive(Parser)]
#[command(author, version, about = "ZeroGStorage client to interact with ZeroGStorage network")]
#[command(propagate_version = true)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Option<Commands>,

    #[arg(long, value_name = "UINT", help = "Custom gas limit to send transaction")]
    gas_limit: Option<u64>,

    #[arg(long, value_name = "UINT", help = "Custom gas price to send transaction")]
    gas_price: Option<u64>,

    #[arg(long, help = "Force to disable colorful logs")]
    log_color_disabled: bool,

    #[arg(long, value_name = "STRING", default_value = "info", help = "Log level (default \"info\")")]
    log_level: String,

    #[arg(long, value_name = "INT", default_value = "5", help = "Retry count for rpc request (default 5)")]
    rpc_retry_count: i32,

    #[arg(long, value_name = "DURATION", default_value = "5s", help = "Retry interval for rpc request (default 5s)")]
    rpc_retry_interval: String,

    #[arg(long, value_name = "DURATION", default_value = "30s", help = "Timeout for single rpc request (default 30s)")]
    rpc_timeout: String,

    #[arg(long, help = "Enable log for web3 RPC")]
    web3_log_enabled: bool,
}

#[derive(Subcommand)]
pub enum Commands {
    #[command(about = "Generate the autocompletion script for the specified shell")]
    Completion {
        #[arg(value_name = "SHELL")]
        shell: String,
    },
    #[command(about = "Deploy ZeroGStorage contract to specified blockchain")]
    Deploy {
        #[arg(value_name = "BLOCKCHAIN")]
        blockchain: String,
    },
    #[command(about = "Download file from ZeroGStorage network")]
    Download,
    #[command(about = "Start gateway service")]
    Gateway,
    #[command(about = "Generate a temp file for test purpose")]
    Gen,
    #[command(about = "Help about any command")]
    Help {
        #[arg(value_name = "COMMAND")]
        command: Option<String>,
    },
    #[command(about = "Start indexer service")]
    Indexer,
    #[command(about = "read kv streams")]
    KvRead,
    #[command(about = "write to kv streams")]
    KvWrite,
    #[command(about = "Upload file to ZeroGStorage network")]
    Upload(UploadArgs),
}