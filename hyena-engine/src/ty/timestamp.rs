use chrono::prelude::*;
use std::fmt::{Display, Formatter, Error as FmtError};
use std::ops::Deref;


#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct Timestamp(u64);

impl From<Timestamp> for u64 {
    fn from(source: Timestamp) -> u64 {
        source.0
    }
}

impl From<u64> for Timestamp {
    fn from(source: u64) -> Timestamp {
        Timestamp(source)
    }
}

impl From<Timestamp> for DateTime<Utc> {
    fn from(timestamp: Timestamp) -> DateTime<Utc> {
        let tsmic = timestamp.from_timestamp_micros();
        Utc.timestamp(tsmic.0 as i64, tsmic.1)
    }
}

impl From<Timestamp> for NaiveDateTime {
    fn from(timestamp: Timestamp) -> NaiveDateTime {
        let tsmic = timestamp.from_timestamp_micros();
        NaiveDateTime::from_timestamp(tsmic.0 as i64, tsmic.1)
    }
}

impl<'ts> From<&'ts Timestamp> for DateTime<Utc> {
    fn from(timestamp: &Timestamp) -> DateTime<Utc> {
        (*timestamp).into()
    }
}

impl<'ts> From<&'ts Timestamp> for NaiveDateTime {
    fn from(timestamp: &Timestamp) -> NaiveDateTime {
        (*timestamp).into()
    }
}

impl<T: ToTimestampMicros> From<T> for Timestamp {
    fn from(source: T) -> Timestamp {
        source.to_timestamp_micros().into()
    }
}

pub trait FromTimestampMicros {
    fn from_timestamp_micros(&self) -> (u64, u32);
}

impl FromTimestampMicros for u64 {
    fn from_timestamp_micros(&self) -> (u64, u32) {
        (self / 1_000_000, (self % 1_000_000) as u32)
    }
}

impl FromTimestampMicros for Timestamp {
    fn from_timestamp_micros(&self) -> (u64, u32) {
        self.0.from_timestamp_micros()
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

impl Default for Timestamp {
    fn default() -> Timestamp {
        Utc::now().into()
    }
}

impl Display for Timestamp {
    fn fmt(&self, f: &mut Formatter) -> Result<(), FmtError> {
        let date: DateTime<Utc> = self.into();

        write!(f, "{}", date)
    }
}

impl Deref for Timestamp {
    type Target = u64;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}


#[cfg(test)]
mod tests {
    use super::*;



}
