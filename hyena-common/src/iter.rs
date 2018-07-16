use std::fmt;
use std::iter::FusedIterator;


pub trait IteratorExt: Iterator {
    fn threshold_take_while<P>(self, predicate: P) -> ThresholdTakeWhile<Self, P>
    where
        Self: Sized,
        P: FnMut(&Self::Item) -> Option<bool>,
    {
        ThresholdTakeWhile {
            iter: self,
            flag: false,
            predicate,
        }
    }
}

impl<I: Iterator> IteratorExt for I {}


#[derive(Clone)]
pub struct ThresholdTakeWhile<I, P> {
    iter: I,
    flag: bool,
    predicate: P,
}

impl<I: fmt::Debug, P> fmt::Debug for ThresholdTakeWhile<I, P> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("ThresholdTakeWhile")
            .field("iter", &self.iter)
            .field("flag", &self.flag)
            .finish()
    }
}

// Some(true) -> continue
// Some(false) -> the last one
// None -> filter this one out and stop

impl<I: Iterator, P> Iterator for ThresholdTakeWhile<I, P>
where
    P: FnMut(&I::Item) -> Option<bool>,
{
    type Item = I::Item;

    #[inline]
    fn next(&mut self) -> Option<I::Item> {
        if self.flag {
            None
        } else {
            self.iter.next().and_then(|x| match (self.predicate)(&x) {
                Some(true) => Some(x),
                Some(false) => {
                    self.flag = true;
                    Some(x)
                }
                None => {
                    self.flag = true;
                    None
                }
            })
        }
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        let (_, upper) = self.iter.size_hint();
        (0, upper) // can't know a lower bound, due to the predicate
    }
}

impl<I, P> FusedIterator for ThresholdTakeWhile<I, P>
where
    I: FusedIterator,
    P: FnMut(&I::Item) -> Option<bool>,
{}
