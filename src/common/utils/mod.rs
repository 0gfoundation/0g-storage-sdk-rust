pub mod schedule;

use std::time::Duration;
use std::str::FromStr;

pub fn duration_from_str(s: &str) -> Result<Duration, std::num::ParseIntError> {
    let secs = u64::from_str(s)?;
    Ok(Duration::from_secs(secs))
}