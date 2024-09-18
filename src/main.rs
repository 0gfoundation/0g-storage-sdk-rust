use clap::Parser;
use tokio;
use zg_storage_client::cmd::{root::{Cli, Commands}, upload};
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
        Some(Commands::Download) => {
            println!("Downloading file from ZeroGStorage network");
            Ok(())
        }
        Some(Commands::Gateway) => {
            println!("Starting gateway service");
            Ok(())
        }
        Some(Commands::Gen) => {
            println!("Generating a temp file for test purpose");
            Ok(())
        }
        Some(Commands::Help { command }) => {
            if let Some(cmd) = command {
                println!("Showing help for command: {}", cmd);
            } else {
                println!("Showing general help");
            }
            Ok(())
        }
        Some(Commands::Indexer) => {
            println!("Starting indexer service");
            Ok(())
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