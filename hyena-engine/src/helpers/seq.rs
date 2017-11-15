
macro_rules! seqfill {
    (vec $ty: ty as $asty: ty, $count: expr, $start: expr, $step: expr) => {{
        seqfill!(iter $ty as $asty, $count, $start, $step).collect::<Vec<$ty>>()
    }};

    (vec $ty: ty as $asty: ty, $count: expr, $start: expr) => {
        seqfill!(vec $ty as $asty, $count, $start, 1)
    };

    (vec $ty: ty as $asty: ty, $count: expr) => {
        seqfill!(vec $ty as $asty, $count, 0, 1)
    };

    (vec $ty: ty, $count: expr, $start: expr, $step: expr) => {{
        seqfill!(iter $ty, $count, $start, $step).collect::<Vec<$ty>>()
    }};

    (vec $ty: ty, $count: expr, $start: expr) => {
        seqfill!(vec $ty, $count, $start, 1)
    };

    (vec $ty: ty, $count: expr) => {
        seqfill!(vec $ty, $count, 0, 1)
    };

    (vec $ty: ty) => {
        seqfill!(vec $ty, 0, 0, 1)
    };

    (iter $ty: ty, $count: expr, $start: expr, $step: expr) => {{
        use num::NumCast;

        let start = $start;
        let step = $step;
        let count: usize = $count;

        (0..count)
            .into_iter()
            .enumerate()
            .map(move |(idx, _)| {
                let v = seqfill!(@ as start, u64)
                        + seqfill!(@ as idx, u64)
                        * seqfill!(@ as step, u64);

                <$ty as NumCast>::from(v)
                    .unwrap_or_else(|| {v as $ty})
            })
    }};

    (iter $ty: ty, $count: expr, $start: expr) => {
        seqfill!(iter $ty, $count, $start, 1)
    };

    (iter $ty: ty, $count: expr) => {
        seqfill!(iter $ty, $count, 0, 1)
    };

    (@ as $v: expr, $ty: ty ) => {
        <$ty as NumCast>::from($v)
            .ok_or_else(|| "Unable to convert seq value")
            .unwrap()
    };

    ($ty: ty, $slice: expr, $start: expr, $step: expr) => {{
        use num::NumCast;

        let start = $start;
        let step = $step;

        for (idx, ref mut el) in $slice.iter_mut()
            .enumerate() {
                let v = seqfill!(@ as start, u64)
                        + seqfill!(@ as idx, u64)
                        * seqfill!(@ as step, u64);

                **el = <$ty as NumCast>::from(v)
                    .ok_or_else(|| "Unable to convert seq value")
                    .unwrap();
        }

        seqfill!(@ as seqfill!(@ as $slice.len(), usize)
            + (seqfill!(@ as start, usize) * seqfill!(@ as step, usize)), $ty)
    }};

    ($ty: ty, $slice: expr, $start: expr) => {{
        use num::One;

        seqfill!($ty, $slice, $start, <$ty as One>::one())
    }};

    ($ty: ty, $slice: expr) => {{
        use num::{Zero, One};

        seqfill!($ty, $slice, <$ty as Zero>::zero(), <$ty as One>::one())
    }};
}

#[cfg(test)]
mod tests {

    #[test]
    fn simple() {
        let mut v = vec![0_u32; 10];

        let cont = seqfill!(u32, &mut v[..]);

        assert_eq!(&v[..], &[0, 1, 2, 3, 4, 5, 6, 7, 8, 9][..]);
        assert_eq!(cont, 10);
    }

    #[test]
    fn simple_start() {
        let mut v = vec![0_u32; 10];

        let cont = seqfill!(u32, &mut v[..], 10);

        assert_eq!(&v[..], &[10, 11, 12, 13, 14, 15, 16, 17, 18, 19][..]);
        assert_eq!(cont, 20);
    }

    #[test]
    fn simple_frag() {
        let mut v = vec![0_u32; 10];

        let cont = seqfill!(u32, &mut v[2..4], 20);

        assert_eq!(&v[..], &[0, 0, 20, 21, 0, 0, 0, 0, 0, 0][..]);
        assert_eq!(cont, 22);
    }

    #[test]
    fn simple_step() {
        let mut v = vec![0_u32; 10];

        let cont = seqfill!(u32, &mut v[..], 10, 2);

        assert_eq!(&v[..], &[10, 12, 14, 16, 18, 20, 22, 24, 26, 28][..]);
        assert_eq!(cont, 30);
    }

    #[test]
    fn as_vec() {
        let v = seqfill!(iter u32, 10, 10, 2).collect::<Vec<_>>();

        assert_eq!(&v[..], &[10, 12, 14, 16, 18, 20, 22, 24, 26, 28][..]);
    }

    #[test]
    fn as_long_vec() {
        let count = 131072_usize;

        let v = seqfill!(vec u8, 131072);

        let v2 = (0..count).into_iter().map(|e| e as u8).collect::<Vec<u8>>();

        assert_eq!(v.len(), count);

        assert_eq!(v, v2);
    }

    #[test]
    fn as_ty() {
        let count = 1000;

        let v = seqfill!(vec u64 as u8, count);

        let v2 = (0..count)
            .into_iter()
            .map(|e| e as u8 as u64)
            .collect::<Vec<u64>>();

        assert_eq!(v, v2);
    }

    #[cfg(all(feature = "nightly", test))]
    mod benches {
        use test::Bencher;
        use super::*;

        #[bench]
        fn simple(b: &mut Bencher) {
            let mut v = vec![0_u32; 10];

            b.iter(|| seqfill!(u32, &mut v[..]));
        }

        #[bench]
        fn as_long_vec(b: &mut Bencher) {
            b.iter(|| seqfill!(vec u8, 131072));
        }
    }

    mod ts {
        use ty::timestamp::Timestamp;

        #[test]
        fn simple() {
            use num::Zero;

            let mut v = vec![Timestamp::zero(); 10];
            let s = <Timestamp as Default>::default();

            let cont = seqfill!(Timestamp, &mut v[..], s);

            let seq = s.as_micros();
            let c = Timestamp::from(seq + 10);
            let seq = (seq..seq + 10).map(Timestamp::from).collect::<Vec<_>>();

            assert_eq!(&v[..], &seq[..]);
            assert_eq!(cont, c);
        }
    }

}
