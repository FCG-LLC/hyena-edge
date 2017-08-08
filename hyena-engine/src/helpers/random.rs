macro_rules! random {
    ($ty: ty) => {{
        use rand::random;

        random::<$ty>()
    }};

    (gen $ty: ty, $count: expr) => {{
        use rand::{thread_rng, Rng};
        use std::iter::repeat;

        let mut rng = thread_rng();

        rng.gen_iter::<$ty>().take($count).collect::<Vec<$ty>>()
    }};

}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn single_typed() {
        let v = random!(u64);
    }

    #[test]
    fn gen_typed() {
        let v = random!(gen u64, 100);

        assert_eq!(v.len(), 100);
    }

    #[test]
    fn gen_long() {
        let v = random!(gen u32, 1 << 20);

        assert_eq!(v.len(), 1 << 20);
    }
}


pub(crate) mod timestamp {
    use rand::{random, thread_rng, Rand, Rng};
    use ty::timestamp::{Timestamp, ToTimestampMicros};
    use chrono::prelude::*;
    use chrono::naive::{MAX_DATE, MIN_DATE};
    use std::iter::repeat;

    // these should be moved associated when consts stabilize
    // https://github.com/rust-lang/rust/issues/29646

    /// minimal UNIX timestamp, used in the random generator
    pub const TS_MIN: u64 = 1_u64;
    // arbitrary upper bound, maximal UNIX timestamp, used in the random generator
    pub const TS_MAX: u64 = 2_147_472_000_000_000_u64;

    pub(crate) trait RandomTimestamp {
        fn random<T>() -> T
        where
            T: From<Timestamp>,
            Timestamp: From<T>,
        {
            Self::random_range::<T, u64>(TS_MIN, TS_MAX)
        }

        fn random_from<T, TS>(from: TS) -> T
        where
            T: From<Timestamp>,
            Timestamp: From<T> + From<TS>,
        {
            let ts = Timestamp::from(from);

            Self::random_range::<T, u64>(*ts, TS_MAX)
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

        pub(crate) fn iter_range_from<T, TS>(from: TS) -> Box<Iterator<Item = T>>
        where
            T: From<Timestamp>,
            Timestamp: From<T> + From<TS>,
            TS: Clone + 'static,
        {
            Box::new(repeat(RandomTimestampGen).map(move |_| {
                Self::random_range(Timestamp::from(from.clone()), Timestamp::from(TS_MAX))
            }))
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

        pub(crate) fn pairs<T>(count: usize) -> Vec<(T, T)>
        where
            T: From<Timestamp> + Ord + Clone,
            Timestamp: From<T>,
        {
            Self::range_pairs::<T, u64>(TS_MIN, TS_MAX, count)
        }

        pub(crate) fn pairs_rng<T, R>(rng: R, count: usize) -> Vec<(T, T)>
        where
            T: From<Timestamp> + Ord + Clone,
            Timestamp: From<T>,
            R: Rng + Sized + Clone + 'static,
        {
            Self::range_pairs_rng::<T, u64, _>(rng, TS_MIN, TS_MAX, count)
        }

        pub(crate) fn range_pairs<T, TS>(from: TS, to: TS, count: usize) -> Vec<(T, T)>
        where
            T: From<Timestamp> + Ord + Clone,
            Timestamp: From<T> + From<TS>,
            TS: Clone + 'static,
        {
            Self::range_pairs_rng(thread_rng(), from, to, count)
        }

        pub(crate) fn range_pairs_rng<T, TS, R>(
            rng: R,
            from: TS,
            to: TS,
            count: usize,
        ) -> Vec<(T, T)>
        where
            T: From<Timestamp> + Ord + Clone,
            Timestamp: From<T> + From<TS>,
            TS: Clone + 'static,
            R: Rng + Sized + Clone + 'static,
        {
            let from = Timestamp::from(from);
            let to = Timestamp::from(to);

            let mut tsvec = repeat(RandomTimestampGen)
                .take(count * 2)
                .map(|_| Self::random_range_rng(&mut rng.clone(), from, to))
                .collect::<Vec<T>>();

            tsvec.sort();

            tsvec
                .as_slice()
                .chunks(2)
                .map(|c| (c[0].clone(), c[1].clone()))
                .collect()
        }
    }

    impl RandomTimestamp for RandomTimestampGen {}
    impl RandomTimestamp for Timestamp {}

    impl Rand for Timestamp {
        fn rand<R: Rng>(rng: &mut R) -> Self {
            Self::random_rng(rng)
        }
    }


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
        fn ts_from_timestamp() {
            let lo = 140_000_000_u64;

            let ts: u64 = Timestamp::random_from(lo);

            assert!(ts > lo);
        }

        #[test]
        fn ts_from_datetime() {
            let lo = NaiveDate::from_ymd(2016, 04, 01).and_hms(12, 40, 00);
            let ts: NaiveDateTime = Timestamp::random_from(lo);

            assert!(ts > lo);
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

        #[test]
        fn pairs_u64() {
            let count = 100;

            let p = RandomTimestampGen::pairs::<u64>(count);

            assert_eq!(p.len(), count);

            let mut plo = &(p[0].0);

            for r in &p {
                let lo = &r.0;
                let hi = &r.1;
                assert!(lo <= hi);
                assert!(hi >= plo);
                plo = &hi;
            }
        }

        #[test]
        fn pairs_datetime() {
            let count = 100;

            let p = RandomTimestampGen::pairs::<NaiveDateTime>(count);

            assert_eq!(p.len(), count);

            let mut plo = &(p[0].0);

            for r in &p {
                let lo = &r.0;
                let hi = &r.1;
                assert!(lo <= hi);
                assert!(hi >= plo);
                plo = &hi;
            }
        }

        #[test]
        fn pairs_range_u64() {
            let rlo = 140_000_000_u64;
            let rhi = 150_000_000_u64;
            let count = 100;

            let p = RandomTimestampGen::range_pairs::<u64, _>(rlo, rhi, count);

            assert_eq!(p.len(), count);

            let mut plo = &(p[0].0);

            for &(ref lo, ref hi) in &p {
                assert!(lo <= hi);
                assert!(hi >= plo);
                assert!(lo >= &rlo && hi <= &rhi);
                plo = &hi;
            }
        }

        #[test]
        fn pairs_range_datetime() {
            let rlo = NaiveDate::from_ymd(2016, 04, 01).and_hms(12, 40, 00);
            let rhi = NaiveDate::from_ymd(2017, 08, 12).and_hms(15, 40, 00);
            let count = 100;

            let p = RandomTimestampGen::range_pairs::<NaiveDateTime, _>(rlo, rhi, count);

            assert_eq!(p.len(), count);

            let mut plo = &(p[0].0);

            for &(ref lo, ref hi) in &p {
                assert!(lo <= hi);
                assert!(hi >= plo);
                assert!(lo >= &rlo && hi <= &rhi);
                plo = &hi;
            }
        }

        #[test]
        fn with_rand() {
            let v = random::<Timestamp>();

            assert!(*v >= TS_MIN);
            assert!(*v <= TS_MAX);
        }

        #[test]
        fn with_rand_iter() {
            let mut rng = thread_rng();
            let v = rng.gen_iter::<Timestamp>().take(100).collect::<Vec<_>>();

            for t in &v {
                assert!(**t >= TS_MIN);
                assert!(**t <= TS_MAX);
            }
        }
    }
}
