use chrono::prelude::*;


#[derive(Debug, Clone, Copy, PartialEq)]
pub struct TimestampKey(u64);

impl From<TimestampKey> for u64 {
    fn from(source: TimestampKey) -> u64 {
        source.0
    }
}

impl From<u64> for TimestampKey {
    fn from(source: u64) -> TimestampKey {
        TimestampKey(source)
    }
}

pub trait ToTimestampMicros {
    fn to_timestamp_micros(&self) -> u64;
}

impl<T: TimeZone> ToTimestampMicros for DateTime<T> {
    fn to_timestamp_micros(&self) -> u64 {
        (self.timestamp() * 1_000_000 + self.timestamp_subsec_micros() as i64) as u64
    }
}
