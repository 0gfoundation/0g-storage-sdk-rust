#[derive(Debug, Clone, Copy, PartialEq, PartialOrd, Eq)]
pub enum LogLevel {
    Error = 0, 
    Warning = 1,   
    Info = 2, 
    Debug = 3,       
}

#[derive(Debug, Clone, Copy, PartialEq, PartialOrd, Eq)]
pub struct LogOption {
    pub level: LogLevel
}

impl Default for LogOption {
    fn default() -> Self {
        LogOption {
            level: LogLevel::Info,
        }
    }
}

impl LogLevel {
    pub fn as_str(&self) -> &'static str {
        match self {
            LogLevel::Error => "error",
            LogLevel::Info => "info",
            LogLevel::Debug => "debug",
            LogLevel::Warning => "warning",
        }
    }
}