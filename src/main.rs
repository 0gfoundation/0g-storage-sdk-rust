use clap::Parser;
use tokio;
use zg_storage_client::cmd::{root::{Cli, Commands}, upload, download, generate_file, indexer};
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
        Some(Commands::KvRead) => {
            println!("Reading kv streams");
            Ok(())
        }
        Some(Commands::KvWrite) => {
            println!("Writing to kv streams");
            Ok(())
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