use clap::{Parser, Subcommand};
use std::time::Duration;

use super::download::DownloadArgs;
use super::generate_file::GenerateArgs;
use super::indexer::IndexerArgs;
use super::kv_read::KvReadArgs;
use super::kv_write::KvWriteArgs;
use super::upload::UploadArgs;
use crate::common::utils::duration_from_str;

#[derive(Parser)]
#[command(
    author,
    version,
    about = "ZeroGStorage client to interact with ZeroGStorage network"
)]
#[command(propagate_version = true)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Option<Commands>,

    #[arg(
        long,
        global = true,
        value_name = "UINT",
        help = "Custom gas limit to send transaction"
    )]
    pub gas_limit: Option<u64>,

    #[arg(
        long,
        global = true,
        value_name = "UINT",
        help = "Custom gas price to send transaction"
    )]
    pub gas_price: Option<u64>,

    #[arg(
        long,
        global = true,
        default_value = "false",
        help = "Force to disable colorful logs"
    )]
    pub log_color_disabled: bool,

    #[arg(
        long,
        global = true,
        value_name = "STRING",
        default_value = "info",
        help = "Log level (default info)"
    )]
    pub log_level: String,

    #[arg(
        long,
        global = true,
        value_name = "INT",
        default_value = "5",
        help = "Retry count for rpc request (default 5)"
    )]
    pub rpc_retry_count: i32,

    #[arg(long, global = true, value_name = "DURATION", value_parser = duration_from_str, default_value = "5", help = "Retry interval for rpc request (default 5s)")]
    pub rpc_retry_interval: Duration,

    #[arg(long, global = true, value_name = "DURATION", value_parser = duration_from_str, default_value = "30", help = "Timeout for single rpc request (default 30s)")]
    pub rpc_timeout: Duration,

    #[arg(
        long,
        global = true,
        default_value = "false",
        help = "Enable log for web3 RPC"
    )]
    pub web3_log_enabled: bool,
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
    Download(DownloadArgs),
    #[command(about = "Start gateway service")]
    Gateway,
    #[command(about = "Generate a temp file for test purpose")]
    Gen(GenerateArgs),
    #[command(about = "Help about any command")]
    Help {
        #[arg(value_name = "COMMAND")]
        command: Option<String>,
    },
    #[command(about = "Start indexer service")]
    Indexer(IndexerArgs),
    #[command(about = "read kv streams")]
    KvRead(KvReadArgs),
    #[command(about = "write to kv streams")]
    KvWrite(KvWriteArgs),
    #[command(about = "Upload file to ZeroGStorage network")]
    Upload(UploadArgs),
}
