use chrono::prelude::*;
use std::fmt::{Display, Error as FmtError, Formatter};
use std::ops::Deref;
use num::{NumCast, One, ToPrimitive, Zero};
use std::ops::{Add, Mul};
use std::i64;
use rand::{Rand, Rng};

pub const MIN_TIMESTAMP_VALUE: u64 = 1_u64;
pub const MAX_TIMESTAMP_VALUE: u64 = 8_210_298_326_400_000_000_u64;

pub const MIN_TIMESTAMP: Timestamp = Timestamp(MIN_TIMESTAMP_VALUE);
pub const MAX_TIMESTAMP: Timestamp = Timestamp(MAX_TIMESTAMP_VALUE);

#[derive(Debug, Clone, Copy, PartialEq, PartialOrd, Eq, Ord, Hash, Serialize, Deserialize)]
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
        (self.timestamp() * 1_000_000
            + <i64 as ::std::convert::From<_>>::from(self.timestamp_subsec_micros())) as u64
    }
}

impl ToTimestampMicros for NaiveDate {
    fn to_timestamp_micros(&self) -> u64 {
        self.and_time(NaiveTime::from_hms(0, 0, 0))
            .to_timestamp_micros()
    }
}

impl ToTimestampMicros for NaiveDateTime {
    fn to_timestamp_micros(&self) -> u64 {
        (self.timestamp() * 1_000_000
            + <i64 as ::std::convert::From<_>>::from(self.timestamp_subsec_micros())) as u64
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

impl ToPrimitive for Timestamp {
    fn to_i64(&self) -> Option<i64> {
        self.0.to_i64()
    }

    fn to_u64(&self) -> Option<u64> {
        Some(self.0)
    }
}

impl NumCast for Timestamp {
    fn from<T>(n: T) -> Option<Self>
    where
        T: ToPrimitive,
    {
        if let Some(v) = n.to_u64() {
            Some(Timestamp(v))
        } else {
            None
        }
    }
}

impl Zero for Timestamp {
    fn zero() -> Timestamp {
        0.into()
    }

    fn is_zero(&self) -> bool {
        self.0 == 0
    }
}

impl One for Timestamp {
    fn one() -> Timestamp {
        MIN_TIMESTAMP
    }
}

impl Add<Timestamp> for Timestamp {
    type Output = Timestamp;

    fn add(self, rhs: Timestamp) -> Self::Output {
        (self.0 + rhs.0).into()
    }
}

impl Mul<Timestamp> for Timestamp {
    type Output = Timestamp;

    fn mul(self, rhs: Timestamp) -> Self::Output {
        (self.0 * rhs.0).into()
    }
}

impl Timestamp {
    pub fn as_micros(&self) -> u64 {
        self.0
    }
}

impl Rand for Timestamp {
    fn rand<R: Rng>(rng: &mut R) -> Self {
        rng.gen_range(MIN_TIMESTAMP_VALUE, MAX_TIMESTAMP_VALUE)
            .into()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn u64() {
        let src: Timestamp = Default::default();

        let to_u64 = src.as_micros();

        assert_eq!(src, to_u64.into());
    }

    #[test]
    fn min() {
        assert_eq!(
            <Timestamp as From<_>>::from(1_u64).as_micros(),
            MIN_TIMESTAMP_VALUE
        );
    }

    #[test]
    fn max() {
        use chrono::naive::MAX_DATE;

        assert_eq!(
            <Timestamp as From<_>>::from(MAX_DATE).as_micros(),
            MAX_TIMESTAMP_VALUE
        );
    }
}
