pub(crate) mod timestamp {
    use rand::{Rng, thread_rng};
    use ty::timestamp::{Timestamp, ToTimestampMicros};
    use chrono::prelude::*;
    use chrono::naive::{MIN_DATE, MAX_DATE};
    use std::iter::repeat;

    // these should be moved associated consts stabilize
    // https://github.com/rust-lang/rust/issues/29646

    /// minimal UNIX timestamp, used in the random generator
    const TS_MIN: u64 = 1_u64;
    // arbitrary upper bound, maximal UNIX timestamp, used in the random generator
    const TS_MAX: u64 = 2_147_472_000_000_u64;

    pub(crate) trait RandomTimestamp {
        fn random<T>() -> T
        where
            T: From<Timestamp>,
            Timestamp: From<T>,
        {
            Self::random_range::<T, u64>(TS_MIN, TS_MAX)
        }

        fn random_range<T, TS>(from: TS, to: TS) -> T
        where
            T: From<Timestamp>,
            Timestamp: From<T> + From<TS>,
        {
            Self::random_range_rng(&mut thread_rng(), from, to)
        }

        fn random_rng<T, R>(rng: &mut R) -> T
        where
            T: From<Timestamp>,
            Timestamp: From<T>,
            R: Rng,
        {
            let ts: Timestamp = rng.gen_range(TS_MIN, TS_MAX).into();
            ts.into()
        }

        fn random_range_rng<T, TS, R>(rng: &mut R, from: TS, to: TS) -> T
        where
            T: From<Timestamp>,
            Timestamp: From<T> + From<TS>,
            R: Rng,
        {
            let ts: Timestamp = Timestamp::from(rng.gen_range(
                Timestamp::from(from).as_micros(),
                Timestamp::from(to).as_micros(),
            ));
            ts.into()
        }
    }

    #[derive(Debug, Clone, Copy, PartialEq)]
    pub(crate) struct RandomTimestampGen;

    impl RandomTimestampGen {
        pub(crate) fn iter<T>() -> Box<Iterator<Item = T>>
        where
            T: From<Timestamp>,
            Timestamp: From<T>,
        {
            Box::new(repeat(RandomTimestampGen).map(|_| Self::random()))
        }

        pub(crate) fn iter_range<T, TS>(from: TS, to: TS) -> Box<Iterator<Item = T>>
        where
            T: From<Timestamp>,
            Timestamp: From<T> + From<TS>,
            TS: Clone + 'static,
        {
            Box::new(
                repeat(RandomTimestampGen)
                    .map(move |_| Self::random_range(from.clone(), to.clone())),
            )
        }

        pub(crate) fn iter_rng<T, R>(rng: R) -> Box<Iterator<Item = T>>
        where
            T: From<Timestamp>,
            Timestamp: From<T>,
            R: Rng + Sized + Clone + 'static,
        {
            Box::new(
                repeat(RandomTimestampGen).map(move |_| Self::random_rng(&mut rng.clone())),
            )
        }

        pub(crate) fn iter_range_rng<T, TS, R>(rng: R, from: TS, to: TS) -> Box<Iterator<Item = T>>
        where
            T: From<Timestamp>,
            Timestamp: From<T> + From<TS>,
            TS: Clone + 'static,
            Timestamp: From<&'static TS>,
            R: Rng + Sized + Clone + 'static,
        {
            Box::new(repeat(RandomTimestampGen).map(move |_| {
                Self::random_range_rng(&mut rng.clone(), from.clone(), to.clone())
            }))
        }
    }

    impl RandomTimestamp for RandomTimestampGen {}
    impl RandomTimestamp for Timestamp {}


    #[cfg(test)]
    mod tests {
        use super::*;

        #[test]
        fn ts_timestamp() {
            let ts = Timestamp::random::<Timestamp>();
        }

        #[test]
        fn ts_u64() {
            let ts = Timestamp::random::<u64>();
        }

        #[test]
        fn ts_datetime() {
            let ts = Timestamp::random::<DateTime<Utc>>();
        }

        #[test]
        fn ts_naive_datetime() {
            let ts = Timestamp::random::<NaiveDateTime>();
        }

        #[test]
        fn ts_range_u64() {
            let lo = 140_000_000_u64;
            let hi = 150_000_000_u64;
            let ts: u64 = RandomTimestampGen::random_range(lo, hi);

            assert!(ts > lo && ts <= hi);
        }

        #[test]
        fn ts_range_datetime() {
            let lo = NaiveDate::from_ymd(2016, 04, 01).and_hms(12, 40, 00);
            let hi = NaiveDate::from_ymd(2017, 08, 12).and_hms(15, 40, 00);
            let ts: NaiveDateTime = RandomTimestampGen::random_range(lo, hi);

            assert!(ts > lo && ts <= hi);
        }

        #[test]
        fn ts_gen_u64() {
            let lo = 140_000_000_u64;
            let hi = 150_000_000_u64;
            let len = 100;
            let count = RandomTimestampGen::iter_range::<u64, _>(lo, hi)
                .take(len)
                .filter(|v| v > &lo && v <= &hi)
                .count();

            assert_eq!(count, len);
        }

        #[test]
        fn ts_gen_datetime() {
            let lo = NaiveDate::from_ymd(2016, 04, 01).and_hms(12, 40, 00);
            let hi = NaiveDate::from_ymd(2017, 08, 12).and_hms(15, 40, 00);
            let len = 100;
            let count = RandomTimestampGen::iter_range::<NaiveDateTime, _>(lo, hi)
                .take(len)
                .filter(|v| v > &lo && v <= &hi)
                .count();

            assert_eq!(count, len);
        }
    }
}
