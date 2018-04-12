use std::iter::{Peekable, Enumerate};
use block::SparseIndex;

/// Iterator for sparse blocks / sparse fragments
///
/// The purpose of this struct is to make fetching records of given `rowid` easier.
/// This is primarily used in `Partition::materialize` for retrieving filtered out records for
/// materialization.
/// The main difference between `SparseIter` and `FragmentIter` is that `SparseIter` doesn't try
/// to fill in missing dense records.
///
/// # Panics
///
/// To properly work, `SparseIter` assumes that the lengths of `data` and `index` are equal.
/// Violating this assumption can result in panic.
pub(crate) struct SparseIter<'data, D: 'data, I: Iterator<Item = SparseIndex>> {
    data: &'data [D],
    index: Peekable<Enumerate<I>>,
}

pub(crate) trait SparseIterator {
    type Item;

    /// Skip to element with given index
    ///
    /// This function behaves similarily to `Iterator::next()` in that
    /// it returns an `Option` with `Some(el)` when the iterator was able to produce a value
    /// and `None` when iteration ended.
    ///
    /// The type of `el` is `Option<Self::Item>`, meaning that if the element with index of `index`
    /// was present in the sparse set, `Some(Self::Item)` will be returned, and `None` otherwise.
    fn next_to(&mut self, index: SparseIndex) -> Option<Option<Self::Item>>;
}

impl<'data, D: 'data, I: Iterator<Item = SparseIndex>> SparseIterator for SparseIter<'data, D, I> {
    type Item = (&'data D, SparseIndex);

    fn next_to(&mut self, index: SparseIndex) -> Option<Option<Self::Item>> {
        loop {
            if let Some(&(_, idx)) = self.index.peek() {
                if idx < index {
                    // try further
                    self.index.next();
                }
                else if idx == index {
                    // hit
                    let (row, idx) = self.index.next().unwrap();

                    break Some(Some((
                        self.data_by_row(row),
                        idx
                    )));
                } else {
                    // too far, stop
                    break Some(None);
                }
            } else {
                break None;
            }
        }
    }
}

impl<'data, D: 'data, I: Iterator<Item = SparseIndex>> SparseIter<'data, D, I> {
    pub(crate) fn new(data: &'data [D], index: I) -> SparseIter<'data, D, I> {
        SparseIter {
            data,
            index: index.enumerate().peekable(),
        }
    }

    #[inline]
    fn data_by_row(&self, row: usize) -> &'data D {
        self.data.get(row)
            .expect("invalid index length for data slice")
    }
}

impl<'data, D: 'data, I: Iterator<Item = SparseIndex>> Iterator for SparseIter<'data, D, I> {
    type Item = (&'data D, SparseIndex);

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        if let Some((row, idx)) = self.index.next() {
            Some((
                self.data_by_row(row),
                idx
            ))
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn first() {
        let data = vec![1, 2, 3, 4];
        let index = vec![0, 2, 4, 6];

        let mut iter = SparseIter::new(&data, index.into_iter());

        assert_eq!(Some(Some((&data[0], 0))), iter.next_to(0));
    }

    #[test]
    fn second() {
        let data = vec![1, 2, 3, 4];
        let index = vec![0, 2, 4, 6];

        let mut iter = SparseIter::new(&data, index.into_iter());

        assert_eq!(Some(Some((&data[1], 2))), iter.next_to(2));
    }

    #[test]
    fn last() {
        let data = vec![1, 2, 3, 4];
        let index = vec![0, 2, 4, 6];

        let mut iter = SparseIter::new(&data, index.into_iter());

        assert_eq!(Some(Some((&data[3], 6))), iter.next_to(6));
    }

    #[test]
    fn nonexistent() {
        let data = vec![1, 2, 3, 4];
        let index = vec![0, 2, 4, 6];

        let mut iter = SparseIter::new(&data, index.into_iter());

        assert_eq!(Some(None), iter.next_to(5));
    }

    #[test]
    fn enditer() {
        let data = vec![1, 2, 3, 4];
        let index = vec![0, 2, 4, 6];

        let mut iter = SparseIter::new(&data, index.into_iter());

        assert_eq!(None, iter.next_to(7));
    }
}
