use std::collections::HashSet as StdHashSet;
use std::iter::FromIterator;
use std::hash::Hash;
use std::ops::{Deref, DerefMut};
use super::hash::Hasher;


#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HashSet<V: Hash + Eq>(StdHashSet<V, Hasher>);


impl<V> HashSet<V>
where V: Hash + Eq
{
    pub fn new() -> HashSet<V> {
        HashSet::from(StdHashSet::with_hasher(Hasher::default()))
    }

    pub fn with_capacity(capacity: usize) -> HashSet<V> {
        HashSet::from(StdHashSet::with_capacity_and_hasher(capacity, Hasher::default()))
    }
}

impl<V> From<StdHashSet<V, Hasher>> for HashSet<V>
where V: Hash + Eq
{
    fn from(source: StdHashSet<V, Hasher>) -> Self {
        HashSet(source)
    }
}

impl<V> Default for HashSet<V>
where V: Hash + Eq
{
    fn default() -> Self {
        Self::from(<StdHashSet<V, Hasher> as Default>::default())
    }
}

impl<V> FromIterator<V> for HashSet<V>
where V: Hash + Eq
{
    fn from_iter<I: IntoIterator<Item = V>>(iter: I) -> Self {
        Self::from(<StdHashSet<V, Hasher> as FromIterator<V>>::from_iter(iter))
    }
}

impl<V> IntoIterator for HashSet<V>
where V: Hash + Eq
{
    type Item = <StdHashSet<V, Hasher> as IntoIterator>::Item;
    type IntoIter = <StdHashSet<V, Hasher> as IntoIterator>::IntoIter;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

impl<'a, V> IntoIterator for &'a HashSet<V>
where V: Hash + Eq
{
    type Item = <&'a StdHashSet<V, Hasher> as IntoIterator>::Item;
    type IntoIter = <&'a StdHashSet<V, Hasher> as IntoIterator>::IntoIter;

    fn into_iter(self) -> Self::IntoIter {
        self.0.iter()
    }
}

impl<V> Deref for HashSet<V>
where V: Hash + Eq
{
    type Target = StdHashSet<V, Hasher>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<V> DerefMut for HashSet<V>
where V: Hash + Eq
{
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(all(feature = "nightly", test))]
    mod benches {
        use super::*;
        use test::{Bencher, black_box};

        fn data(count: usize, distance: usize) -> Vec<usize> {
            (0..count)
                .map(|v| v * distance)
                .collect()
        }

        #[inline]
        fn single_impl<H>(b: &mut Bencher, count: usize, distance: usize)
        where H: FromIterator<usize>
        {
            let d = data(count, distance);

            b.iter(||
                black_box(d
                    .iter()
                    .cloned()
                    .collect::<H>())
            )
        }

        macro_rules! hset_bench_impl {
            (
                $(
                $name: ident,
                $count: expr,
                $distance: expr
                ),+ $(,)*
            ) => {

                mod single {
                    use super::*;

                    mod cur {
                        use super::*;

                        $(
                        #[bench]
                        fn $name(b: &mut Bencher) {
                            single_impl::<HashSet<_>>(b, $count, $distance);
                        }
                        )+
                    }

                    mod std {
                        use super::*;

                        $(
                        #[bench]
                        fn $name(b: &mut Bencher) {
                            single_impl::<StdHashSet<_>>(b, $count, $distance);
                        }
                        )+
                    }
                }
            };
        }

        hset_bench_impl!(
            single_100, 100, 1,
            single_10_000, 10_000, 4,
            single_100_000, 100_000, 12,
            single_1_000_000, 1_000_000, 24,
        );
    }
}
