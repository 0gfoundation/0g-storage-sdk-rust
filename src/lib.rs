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
    use ctor::ctor;
    use crate::common::options::init_logging;

    #[ctor]
    fn setup() {
        init_logging("info", true).unwrap();
    }
}
