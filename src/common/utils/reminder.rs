use clap::builder::Str;
use log::{Level, Log, Metadata, Record};
use std::collections::HashMap;
use std::fmt::Arguments;
use std::time::{Duration, Instant};

pub struct Reminder {
    start: Instant,
    interval: Duration,
    logger: Box<dyn Log>,
    level: Level,
}

impl Reminder {
    pub fn new(logger: Option<Box<dyn Log>>, interval: Duration, level: Level) -> Self {
        let logger = logger.unwrap_or_else(|| Box::new(NullLogger));
        Reminder {
            start: Instant::now(),
            interval,
            logger,
            level,
        }
    }

    pub fn remind_with(&mut self, message: &str, key: &str, value: &dyn std::fmt::Debug) {
        let mut fields = HashMap::new();
        fields.insert(key.to_string(), format!("{:?}", value));
        self.remind(message, Some(fields));
    }

    pub fn remind(&mut self, message: &str, fields: Option<HashMap<String, String>>) {
        let elapsed = self.start.elapsed();
        let level = if elapsed > self.interval {
            self.start = Instant::now();
            Level::Warn
        } else {
            self.level
        };

        self.log(level, message, fields);
    }

    fn log(&self, level: Level, message: &str, fields: Option<HashMap<String, String>>) {
        let metadata = Metadata::builder().level(level).build();

        let args: Arguments;
        let fields_str: String;
        if let Some(fields) = fields {
            fields_str = fields
                .iter()
                .map(|(k, v)| format!("{}={}", k, v))
                .collect::<Vec<String>>()
                .join(" ");

            args = format_args!("{} {}", message, fields_str);
        } else {
            args = format_args!("{}", message);
        }

        let record = Record::builder().args(args).metadata(metadata).build();

        self.logger.log(&record);
    }
}

struct NullLogger;

impl Log for NullLogger {
    fn enabled(&self, _metadata: &Metadata) -> bool {
        false
    }

    fn log(&self, _record: &Record) {}

    fn flush(&self) {}
}

#[cfg(test)]
mod tests {
    use super::*;

    struct TestLogger;

    impl Log for TestLogger {
        fn enabled(&self, _metadata: &Metadata) -> bool {
            true
        }

        fn log(&self, record: &Record) {
            println!("{} - {}", record.level(), record.args());
        }

        fn flush(&self) {}
    }

    #[test]
    fn test_reminder() {
        let logger = TestLogger;
        let mut reminder =
            Reminder::new(Some(Box::new(logger)), Duration::from_secs(1), Level::Info);

        reminder.remind("Test message", None);
        reminder.remind_with("Test message with field", "key", &"value");

        // Sleep to trigger warn level
        std::thread::sleep(Duration::from_secs(2));

        reminder.remind("This should be a warning", None);
    }
}
