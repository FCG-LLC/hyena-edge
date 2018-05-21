use std::hash::{Hash, Hasher, BuildHasher};
use std::fmt::{Display, Formatter, self};
use std::marker::PhantomData;
use bloom::{Bloom, BIT_LENGTH};
use ngram::{Ngram, Trigram, ngrams};

#[derive(Debug, Copy, Clone, PartialEq)]
pub struct BloomFilter<H1, H2, N = Trigram>
where
    H1: BuildHasher + Default,
    H2: BuildHasher + Default,
    N: Ngram,
{
    value: Bloom,
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
    pub fn new(value: Bloom) -> BloomFilter<H1, H2, N> {
        BloomFilter {
            value,
            _h1: PhantomData,
            _h2: PhantomData,
            _n: PhantomData,
        }
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
    pub fn encode(value: impl AsRef<[u8]>) -> Bloom {
        ngrams(&value, N::SIZE)
            .flat_map(|value| {
                let h1 = Self::hash::<H1, _>(value) % BIT_LENGTH as u64;
                let h2 = Self::hash::<H2, _>(value) % BIT_LENGTH as u64;

                ::std::iter::once(h1).chain(::std::iter::once(h2))
            })
            .fold(Bloom::default(), |mut bloom, bit| {
                bloom.set(bit as usize);
                bloom
            })
    }

    #[inline]
    pub fn store(&mut self, value: impl AsRef<[u8]>) {
        self.store_encoded(Self::encode(value))
    }

    #[inline]
    pub fn lookup(&self, value: impl AsRef<[u8]>) -> bool {
//             self.lookup_encoded(Self::encode(value))
        ngrams(&value, N::SIZE)
            .flat_map(|value| {
                let h1 = Self::hash::<H1, _>(value) % BIT_LENGTH as u64;
                let h2 = Self::hash::<H2, _>(value) % BIT_LENGTH as u64;

                ::std::iter::once(h1).chain(::std::iter::once(h2))
            })
            .all(|bit| self.value.get(bit as usize))
    }

    #[inline]
    pub fn lookup_encoded(&self, value: Bloom) -> bool {
        !bool::from(self.value & value ^ value)
    }

    #[inline]
    pub fn store_encoded(&mut self, value: Bloom) {
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
        <Bloom as Display>::fmt(&self.value, f)
    }
}
