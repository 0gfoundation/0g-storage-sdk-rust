use anyhow::Result;
use clap::Parser;
use tokio;
use zg_storage_client::cmd::{
    download, generate_file, indexer, kv_read, kv_write,
    root::{Cli, Commands},
    upload,
};
use std::collections::HashMap;

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
            let mut string_result: HashMap<String, String> = HashMap::new();

            for (key, data) in result.iter() {
                // Try to convert data from Vec<u8> to a UTF-8 String
                let value_as_string = match String::from_utf8(data.clone()) {
                    Ok(string) => string,
                    Err(_) => "[Invalid UTF-8 data]".to_string(),
                };
                string_result.insert(key.clone(), value_as_string);
            }

            let json_string =
                serde_json::to_string(&string_result).expect("Failed to serialize to JSON");

            println!("{}", json_string);
            Ok(())
        }
        Some(Commands::KvWrite(kv_write_args)) => kv_write::run_kv_write(kv_write_args).await,
        Some(Commands::Upload(upload_args)) => upload::run_upload(upload_args).await,
        None => {
            println!("No command was used");
            Ok(())
        }
    }
}
