pub mod cmd;
pub mod common;
pub mod contracts;
pub mod core;
pub mod indexer;
pub mod kv;
pub mod node;
pub mod transfer;

#[cfg(test)]
mod tests {
    use crate::common::options::init_logging;
    use ctor::ctor;

    #[ctor]
    fn setup() {
        init_logging("info", true).unwrap();
    }
}
