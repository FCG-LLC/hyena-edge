
#[inline]
pub(crate) fn ngrams<'t, S>(source: &'t S, size: usize) -> impl Iterator<Item = &'t [u8]>
where
    S: 't + AsRef<[u8]>
{
    source.as_ref().windows(size)
}

pub trait Ngram {
    const SIZE: usize;
}

#[derive(Debug)]
pub struct Trigram;

impl Ngram for Trigram {
    const SIZE: usize = 3;
}
