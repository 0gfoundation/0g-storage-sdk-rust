#[derive(Debug, Clone, Copy, PartialEq, PartialOrd, Eq)]
pub enum LogLevel {
    Error = 0, 
    Warning = 1,   
    Info = 2, 
    Debug = 3,       
}

#[derive(Debug, Clone, Copy, PartialEq, PartialOrd, Eq)]
pub struct LogOption {
    pub level: log::LevelFilter
}

impl Default for LogOption {
    fn default() -> Self {
        LogOption {
            level: log::LevelFilter::Info,
        }
    }
}