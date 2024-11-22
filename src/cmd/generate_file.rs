use crate::core::dataflow::merkle_tree;
use crate::core::file::File;
use anyhow::{Context, Result};
use clap::Args;
use rand::Rng;
use std::fs::OpenOptions;
use std::io::Write;
use std::path::PathBuf;
use std::sync::Arc;

#[derive(Args)]
pub struct GenerateArgs {
    #[arg(long, default_value = "tmp123456", help = "File name to upload")]
    pub file: PathBuf,

    #[arg(
        long,
        default_value = "false",
        help = "Whether to overwrite existing file"
    )]
    pub overwrite: bool,

    #[arg(long, help = "File size in bytes (default \"[1M, 10M)\")")]
    pub size: Option<usize>,
}

pub async fn run_generate_file(args: &GenerateArgs) -> Result<()> {
    log::info!("Generating file: {:?}", args.file);
    
    let file_name = args
        .file
        .file_name()
        .context("File name is not specified")?;

    if args.file.exists() && !args.overwrite {
        anyhow::bail!("File already exists: {:?}", file_name);
    }

    let size = match args.size {
        Some(s) => s,
        None => {
            let mut rng = rand::thread_rng();
            rng.gen_range(1_048_576..10_485_760) // [1M, 10M)
        }
    };

    let mut file = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(args.file.clone())
        .expect("Failed to create file");

    let mut rng = rand::thread_rng();
    let mut buffer = vec![0u8; 8192]; // 8KB buffer for writing
    let mut remaining = size;

    while remaining > 0 {
        let to_write = std::cmp::min(remaining, buffer.len());
        rng.fill(&mut buffer[..to_write]);
        file.write_all(&buffer[..to_write])
            .expect("Failed to write to file");
        remaining -= to_write;
    }

    file.flush().expect("Failed to flush file");

    let data = File::open(args.file.clone()).context("Failed to open file")?;
    let tree = merkle_tree(Arc::new(data)).await?;
    log::info!(
        "Succeeded to write file. root: {:?}, file: {:?}",
        tree.root(),
        file_name
    );
    Ok(())
}
