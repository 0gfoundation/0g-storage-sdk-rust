pub mod cmd;
pub mod common;
pub mod contract;
pub mod core;
pub mod indexer;
pub mod node;
pub mod transfer;
pub mod kv;

#[cfg(test)]
mod tests {
    // use ctor::ctor;

    // #[ctor]
    // fn setup() {
    //     env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();
    // }
}
