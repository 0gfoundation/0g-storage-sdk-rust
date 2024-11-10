
use clap::Parser;
use tokio;
use zg_storage_client::cmd::{root::{Cli, Commands}, upload, download, generate_file, indexer, kv_write, kv_read};
use anyhow::Result;

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    match &cli.command {
        Some(Commands::Completion { shell }) => {
            println!("Generating completion script for {}", shell);
            Ok(())
        }
        Some(Commands::Deploy { blockchain }) => {
            println!("Deploying ZeroGStorage contract to {}", blockchain);
            Ok(())
        }
        Some(Commands::Download(download_args)) => {
            log::info!("Downloading file from ZeroGStorage network");
            download::run_download(download_args).await
        }
        Some(Commands::Gateway) => {
            println!("Starting gateway service");
            Ok(())
        }
        Some(Commands::Gen(generate_args)) => {
            log::info!("Generating a temp file for test purpose");
            generate_file::run_generate_file(generate_args).await
        }
        Some(Commands::Help { command }) => {
            if let Some(cmd) = command {
                println!("Showing help for command: {}", cmd);
            } else {
                println!("Showing general help");
            }
            Ok(())
        }
        Some(Commands::Indexer(indexer_args)) => {
            log::info!("Starting indexer service");
            indexer::run_indexer(indexer_args).await
        }
        Some(Commands::KvRead(kv_read_args)) => {
            let result = kv_read::run_kv_read(kv_read_args).await?;
            println!("Reading kv streams: {}", result);
            Ok(())
        }
        Some(Commands::KvWrite(kv_write_args)) => {
            kv_write::run_kv_write(kv_write_args).await
        },
        Some(Commands::Upload(upload_args)) => {
            upload::run_upload(upload_args).await
        },
        None => {
            println!("No command was used");
            Ok(())
        }
    }
}