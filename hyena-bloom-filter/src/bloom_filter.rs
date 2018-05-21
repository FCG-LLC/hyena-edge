use std::hash::{Hash, Hasher, BuildHasher};
use std::fmt::{Display, Formatter, self};
use std::marker::PhantomData;
use bloom_value::{BloomValue, BIT_LENGTH};
use ngram::{Ngram, Trigram};


#[derive(Debug, Copy, Clone, PartialEq, Default)]
pub struct BloomFilter<H1, H2, N = Trigram>
where
    H1: BuildHasher + Default,
    H2: BuildHasher + Default,
    N: Ngram,
{
    value: BloomValue,
    _h1: PhantomData<H1>,
    _h2: PhantomData<H2>,
    _n: PhantomData<N>,
}

impl<H1, H2, N> BloomFilter<H1, H2, N>
where
    H1: BuildHasher + Default,
    H2: BuildHasher + Default,
    N: Ngram,
{
    pub fn new(value: BloomValue) -> BloomFilter<H1, H2, N> {
        BloomFilter {
            value,
            _h1: PhantomData,
            _h2: PhantomData,
            _n: PhantomData,
        }
    }

    pub fn with_data(data: impl AsRef<[u8]>) -> BloomFilter<H1, H2, N> {
        Self::new(Self::encode(data))
    }

    #[inline]
    fn hash<H, T>(value: T) -> u64
    where
        H: BuildHasher + Default,
        T: Hash,
    {
        let mut state = H::default().build_hasher();
        value.hash(&mut state);
        state.finish()
    }

    #[inline]
    pub fn encode(data: impl AsRef<[u8]>) -> BloomValue {
        N::ngrams(&data)
            .flat_map(|value| {
                let h1 = Self::hash::<H1, _>(value) % BIT_LENGTH as u64;
                let h2 = Self::hash::<H2, _>(value) % BIT_LENGTH as u64;

                ::std::iter::once(h1).chain(::std::iter::once(h2))
            })
            .fold(BloomValue::default(), |mut bloom, bit| {
                bloom.set(bit as usize);
                bloom
            })
    }

    #[inline]
    pub fn store(&mut self, data: impl AsRef<[u8]>) {
        self.store_encoded(Self::encode(data))
    }

    #[inline]
    pub fn lookup(&self, data: impl AsRef<[u8]>) -> bool {
        self.lookup_encoded(Self::encode(data))
    }

    #[inline]
    pub fn lookup_encoded(&self, value: BloomValue) -> bool {
        !bool::from(self.value & value ^ value)
    }

    #[inline]
    pub fn store_encoded(&mut self, value: BloomValue) {
        self.value = self.value | value;
    }
}

impl<H1, H2, N> Display for BloomFilter<H1, H2, N>
where
    H1: BuildHasher + Default,
    H2: BuildHasher + Default,
    N: Ngram,
{
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        <BloomValue as Display>::fmt(&self.value, f)
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use fnv::FnvBuildHasher;
    use fxhash::FxBuildHasher;


    type Filter = BloomFilter<FnvBuildHasher, FxBuildHasher>;

    #[test]
    fn lookup() {
        let data = "a quick brown fox";
        let needle1 = "quick";
        let needle2 = "wn fox";
        let needle3 = "a quick";

        let bf = Filter::with_data(&data);

        assert!(bf.lookup(&needle1));
        assert!(bf.lookup(&needle2));
        assert!(bf.lookup(&needle3));

        let enc1 = Filter::encode(&needle1);
        let enc2 = Filter::encode(&needle2);
        let enc3 = Filter::encode(&needle3);

        assert!(bf.lookup_encoded(enc1));
        assert!(bf.lookup_encoded(enc2));
        assert!(bf.lookup_encoded(enc3));
    }

    #[test]
    fn lookup_negative() {
        let data = "a quick brown fox";
        let needle1 = "fast";
        let needle2 = "wn tox";
        let needle3 = "aquick";

        let bf = Filter::with_data(&data);

        assert!(!bf.lookup(&needle1));
        assert!(!bf.lookup(&needle2));
        assert!(!bf.lookup(&needle3));

        let enc1 = Filter::encode(&needle1);
        let enc2 = Filter::encode(&needle2);
        let enc3 = Filter::encode(&needle3);

        assert!(!bf.lookup_encoded(enc1));
        assert!(!bf.lookup_encoded(enc2));
        assert!(!bf.lookup_encoded(enc3));
    }

    #[cfg(all(feature = "nightly", test))]
    mod benches {
        use test::{Bencher, black_box};
        use super::*;

        static LOREM: &str = r##"
        Lorem ipsum dolor sit amet consectetur adipiscing elit, dapibus
        fames volutpat vulputate a mattis pharetra, gravida quis commodo pellentesque fusce
        viverra. Habitant massa euismod dis netus nostra diam ante viverra, aliquet fermentum leo
        curae auctor montes mus hac, primis pharetra eget curabitur ullamcorper nullam velit.
        Nostra dui risus senectus imperdiet cubilia ridiculus lacinia, tempor convallis etiam
        venenatis aptent lobortis pellentesque, aliquam erat rutrum bibendum suspendisse purus.
        Conubia eleme."##;

        fn prepare_data(length: usize) -> impl AsRef<[u8]> {
            &LOREM[..length]
        }


        macro_rules! bloom_bench_impl {
            (
                $filter: ident,
                [ $( $data_length: expr, $name:ident
                    [
                        $(
                            $lookup_name: ident,
                            $needle: expr
                        ),+ $(,)*
                    ]
                    ),+ $(,)*
                ]
            )
            => {
                $(
                mod $name {
                    use super::*;

                    #[bench]
                    fn create(b: &mut Bencher) {
                        let data = prepare_data($data_length);

                        b.iter(|| {
                            let filter = $filter::with_data(&data);
                            black_box(filter);
                        });
                    }

                    mod raw {
                        use super::*;

                        $(
                        #[bench]
                        fn $lookup_name(b: &mut Bencher) {
                            let data = prepare_data($data_length);
                            let filter = $filter::with_data(data);

                            b.iter(|| {
                                black_box(filter.lookup($needle));
                            });
                        }
                        )+
                    }

                    mod precomputed {
                        use super::*;

                        $(
                        #[bench]
                        fn $lookup_name(b: &mut Bencher) {
                            let data = prepare_data($data_length);
                            let filter = $filter::with_data(data);
                            let needle = $filter::encode($needle);

                            b.iter(|| {
                                black_box(filter.lookup_encoded(needle));
                            });
                        }
                        )+
                    }
                }
                )+
            };
        }


        bloom_bench_impl! {
            Filter,
            [ 10, data_10 [
                lookup_small_4, "amet",
                lookup_medium_9, "vulputate",
                lookup_medium_plus_18, "senectus imperdiet",
                lookup_large_26, "eget curabitur ullamcorper"
            ],
            20, data_20 [
                lookup_small_4, "amet",
                lookup_medium_9, "vulputate",
                lookup_medium_plus_18, "senectus imperdiet",
                lookup_large_26, "eget curabitur ullamcorper"
            ],
            32, data_32 [
                lookup_small_4, "amet",
                lookup_medium_9, "vulputate",
                lookup_medium_plus_18, "senectus imperdiet",
                lookup_large_26, "eget curabitur ullamcorper"
            ],
            64, data_64 [
                lookup_small_4, "amet",
                lookup_medium_9, "vulputate",
                lookup_medium_plus_18, "senectus imperdiet",
                lookup_large_26, "eget curabitur ullamcorper"
            ],
            128, data_128 [
                lookup_small_4, "amet",
                lookup_medium_9, "vulputate",
                lookup_medium_plus_18, "senectus imperdiet",
                lookup_large_26, "eget curabitur ullamcorper"
            ],
            255, data_255 [
                lookup_small_4, "amet",
                lookup_medium_9, "vulputate",
                lookup_medium_plus_18, "senectus imperdiet",
                lookup_large_26, "eget curabitur ullamcorper"
            ]]
        }
    }
}
