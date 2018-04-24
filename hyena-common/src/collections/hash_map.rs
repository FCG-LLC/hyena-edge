use std::collections::HashMap as StdHashMap;
use std::iter::FromIterator;
use std::hash::Hash;
use std::ops::{Deref, DerefMut};
use rayon::prelude::*;
use super::hash::Hasher;


#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HashMap<K: Hash + Eq, V>(StdHashMap<K, V, Hasher>);


impl<K, V> HashMap<K, V>
where K: Hash + Eq
{
    pub fn new() -> HashMap<K, V> {
        HashMap::from(StdHashMap::with_hasher(Hasher::default()))
    }

    pub fn with_capacity(capacity: usize) -> HashMap<K, V> {
        HashMap::from(StdHashMap::with_capacity_and_hasher(capacity, Hasher::default()))
    }
}

impl<K, V> From<StdHashMap<K, V, Hasher>> for HashMap<K, V>
where K: Hash + Eq
{
    fn from(source: StdHashMap<K, V, Hasher>) -> Self {
        HashMap(source)
    }
}

impl<K, V> IntoIterator for HashMap<K, V>
where K: Hash + Eq
{
    type Item = <StdHashMap<K, V, Hasher> as IntoIterator>::Item;
    type IntoIter = <StdHashMap<K, V, Hasher> as IntoIterator>::IntoIter;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

impl<'a, K, V> IntoIterator for &'a mut HashMap<K, V>
where K: Hash + Eq
{
    type Item = <&'a mut StdHashMap<K, V, Hasher> as IntoIterator>::Item;
    type IntoIter = <&'a mut StdHashMap<K, V, Hasher> as IntoIterator>::IntoIter;

    fn into_iter(self) -> Self::IntoIter {
        self.0.iter_mut()
    }
}

impl<'a, K, V> IntoIterator for &'a HashMap<K, V>
where K: Hash + Eq
{
    type Item = <&'a StdHashMap<K, V, Hasher> as IntoIterator>::Item;
    type IntoIter = <&'a StdHashMap<K, V, Hasher> as IntoIterator>::IntoIter;

    fn into_iter(self) -> Self::IntoIter {
        self.0.iter()
    }
}

impl<K, V> FromIterator<(K, V)> for HashMap<K, V>
where K: Hash + Eq
{
    fn from_iter<T: IntoIterator<Item = (K, V)>>(iter: T) -> Self {
        Self::from(<StdHashMap<K, V, Hasher> as FromIterator<(K, V)>>::from_iter(iter))
    }
}

impl<K, V> Default for HashMap<K, V>
where K: Hash + Eq
{
    fn default() -> Self {
        Self::from(<StdHashMap<K, V, Hasher> as Default>::default())
    }
}

impl<K, V> Deref for HashMap<K, V>
where K: Hash + Eq
{
    type Target = StdHashMap<K, V, Hasher>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<K, V> DerefMut for HashMap<K, V>
where K: Hash + Eq
{
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

// rayon

impl<K, V> FromParallelIterator<(K, V)> for HashMap<K, V>
    where K: Hash + Eq + Send,
          V: Send,
{
    fn from_par_iter<I>(par_iter: I) -> Self
        where I: IntoParallelIterator<Item = (K, V)>
    {
        Self::from(<StdHashMap<K, V, Hasher> as FromParallelIterator<(K,
V)>>::from_par_iter(par_iter))
    }
}


#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(all(feature = "nightly", test))]
    mod benches {
        use super::*;
        use test::{Bencher, black_box};

        fn data(count: usize, distance: usize, value_range: usize) -> Vec<(usize, u64)> {
            (0..count)
                .map(|v| {
                    (v * distance, (value_range % (v + 1)) as u64)
                })
                .collect()
        }

        #[inline]
        fn single_impl<H>(b: &mut Bencher, count: usize, distance: usize, value_range: usize)
        where H: FromIterator<(usize, u64)>
        {
            let d = data(count, distance, value_range);

            b.iter(||
                black_box(d
                    .iter()
                    .cloned()
                    .collect::<H>())
            )
        }

        #[inline]
        fn par_impl<H>(b: &mut Bencher, count: usize, distance: usize, value_range: usize)
        where H: FromParallelIterator<(usize, u64)>
        {
            let d = data(count, distance, value_range);

            b.iter(||
                black_box(d
                    .par_iter()
                    .cloned()
                    .collect::<H>())
            )
        }

        macro_rules! hmap_bench_impl {
            (
                $(
                $name: ident,
                $count: expr,
                $distance: expr,
                $value_range: expr
                ),+ $(,)*
            ) => {

                mod single {
                    use super::*;

                    mod cur {
                        use super::*;

                        $(
                        #[bench]
                        fn $name(b: &mut Bencher) {
                            single_impl::<HashMap<_, _>>(b, $count, $distance, $value_range);
                        }
                        )+
                    }

                    mod std {
                        use super::*;

                        $(
                        #[bench]
                        fn $name(b: &mut Bencher) {
                            single_impl::<StdHashMap<_, _>>(b, $count, $distance, $value_range);
                        }
                        )+
                    }
                }

                mod parallel {
                    use super::*;

                    mod cur {
                        use super::*;

                        $(
                        #[bench]
                        fn $name(b: &mut Bencher) {
                            par_impl::<HashMap<_, _>>(b, $count, $distance, $value_range);
                        }
                        )+
                    }

                    mod std {
                        use super::*;

                        $(
                        #[bench]
                        fn $name(b: &mut Bencher) {
                            par_impl::<StdHashMap<_, _>>(b, $count, $distance, $value_range);
                        }
                        )+
                    }
                }
            };
        }

        hmap_bench_impl!(
            single_100, 100, 1, 20,
            single_10_000, 10_000, 4, 50,
            single_100_000, 100_000, 12, 500,
            single_1_000_000, 1_000_000, 24, 1000,
        );
    }
}
