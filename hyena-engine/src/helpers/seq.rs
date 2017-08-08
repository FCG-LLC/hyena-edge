use error::*;

macro_rules! seqfill {
    (vec $ty: ty, $count: expr, $start: expr, $step: expr) => {{
        let v: Vec<$ty> = seqfill!(gen $ty, $count, $start, $step);

        v
    }};

    (vec $ty: ty, $count: expr, $start: expr) => {
        seqfill!(vec $ty, $count, $start, 1)
    };

    (vec $ty: ty, $count: expr) => {
        seqfill!(vec $ty, $count, 0, 1)
    };

    (gen $ty: ty, $count: expr, $start: expr, $step: expr) => {{
        use rayon::prelude::*;

        let count: usize = $count;

        (0..count)
            .into_par_iter()
            .enumerate()
            .map(|(idx, _)| {
                ($start + (idx * $step)) as $ty
            })
            .collect()
    }};

    (gen $ty: ty, $count: expr, $start: expr) => {
        seqfill!(gen $ty, $count, $start, 1)
    };

    (gen $ty: ty, $count: expr) => {
        seqfill!(gen $ty, $count, 0, 1)
    };

    ($ty: ty, $slice: expr, $start: expr, $step: expr) => {{
        use rayon::prelude::*;

        $slice.par_iter_mut()
            .enumerate()
            .for_each(|(idx, ref mut el)| {
                **el = ($start + (idx * $step)) as $ty;
            });

        $slice.len() + ($start * $step)
    }};

    ($ty: ty, $slice: expr, $start: expr) => {
        seqfill!($ty, $slice, $start, 1)
    };

    ($ty: ty, $slice: expr) => {
        seqfill!($ty, $slice, 0, 1)
    };
}

#[cfg(test)]
mod tests {
    use super::*;

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
        let v: Vec<_> = seqfill!(gen u32, 10, 10, 2);

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
}
