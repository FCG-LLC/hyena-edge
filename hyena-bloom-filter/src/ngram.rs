use std::slice::Windows;
use std::hash::Hash;


pub trait NgramChar: Sized + Hash {}

impl NgramChar for u8 {}
impl NgramChar for char {}


pub trait Ngram<C = u8>
where
    C: NgramChar
{
    const SIZE: usize;

    // FIXME: until impl Trait is allowed within trait definitions
    // this has to be a concrete type
    #[inline]
    fn ngrams<'t, S>(source: &'t S) -> Windows<'t, C>   // -> impl Iterator<Item = &'t [C]>
    where
        S: 't + AsRef<[C]>
    {
        source.as_ref().windows(Self::SIZE)
    }
}


#[derive(Debug, Copy, Clone, PartialEq, PartialOrd, Hash)]
pub struct Trigram;

impl Ngram for Trigram {
    const SIZE: usize = 3;
}


#[cfg(test)]
mod tests {
    mod trigram {
        use super::super::*;

        #[test]
        fn empty() {
            let input = "";

            assert_eq!(Trigram::ngrams(&input).count(), 0);
        }

        #[test]
        fn small() {
            let input = "ab";

            assert_eq!(Trigram::ngrams(&input).count(), 0);

        }

        #[test]
        fn even() {
            let input = "abc de";

            let v = Trigram::ngrams(&input).collect::<Vec<_>>();

            assert_eq!(&v[..], &[
                &b"abc"[..],
                &b"bc "[..],
                &b"c d"[..],
                &b" de"[..],
            ][..]);

        }

        #[test]
        fn odd() {
            let input = "abc def";

            let v = Trigram::ngrams(&input).collect::<Vec<_>>();

            assert_eq!(&v[..], &[
                &b"abc"[..],
                &b"bc "[..],
                &b"c d"[..],
                &b" de"[..],
                &b"def"[..],
            ][..]);

        }
    }

}
