use std::fmt::{Display, Formatter, self};
use std::ops::{BitAnd, BitOr, BitXor, Not};


pub(crate) const BIT_LENGTH: usize = ::std::mem::size_of::<BloomValue>() * 8;
pub(crate) const BASE_BIT_LENGTH: usize = ::std::mem::size_of::<u128>() * 8;

#[derive(Debug, Copy, Clone, PartialEq, PartialOrd, Hash, Default)]
pub struct BloomValue([u128; 2]);

impl BloomValue {
    #[inline]
    pub fn get(&self, bit: usize) -> bool {
        debug_assert!(bit < BIT_LENGTH);

        let word = bit / BASE_BIT_LENGTH;
        let bit = bit % BASE_BIT_LENGTH;
        let block = self.0[word];
        (block & (1 << bit)) != 0
    }

    #[inline]
    pub fn set(&mut self, bit: usize) {
        debug_assert!(bit < BIT_LENGTH);

        let word = bit / BASE_BIT_LENGTH;
        let bit = bit % BASE_BIT_LENGTH;
        let flag = 1 << bit;
        let val = self.0[word] | flag;
        self.0[word] = val;
    }

    pub fn iter<'bloom>(&'bloom self) -> impl Iterator<Item = bool> + 'bloom {
        (0..BIT_LENGTH)
            .map(move |bit| self.get(bit))
    }

    #[inline]
    pub fn contains(&self, other: BloomValue) -> bool {
        !bool::from(*self & other ^ other)
    }

    #[inline]
    pub fn zero() -> BloomValue {
        BloomValue::default()
    }

    #[inline]
    pub fn one() -> BloomValue {
        !BloomValue::default()
    }
}

impl Not for BloomValue {
    type Output = Self;

    fn not(self) -> Self {
        BloomValue([
            !self.0[0],
            !self.0[1]
        ])
    }
}

impl BitAnd for BloomValue {
    type Output = Self;

    fn bitand(self, rhs: Self) -> Self {
        BloomValue([
            self.0[0] & rhs.0[0],
            self.0[1] & rhs.0[1]
        ])
    }
}

impl BitOr for BloomValue {
    type Output = Self;

    fn bitor(self, rhs: Self) -> Self {
        BloomValue([
            self.0[0] | rhs.0[0],
            self.0[1] | rhs.0[1]
        ])
    }
}

impl BitXor for BloomValue {
    type Output = Self;

    fn bitxor(self, rhs: Self) -> Self {
        BloomValue([
            self.0[0] ^ rhs.0[0],
            self.0[1] ^ rhs.0[1]
        ])
    }
}

impl From<BloomValue> for bool {
    fn from(source: BloomValue) -> bool {
        source.0[0] != 0 || source.0[1] != 0
    }
}

impl<'b> From<&'b BloomValue> for bool {
    fn from(source: &'b BloomValue) -> bool {
        source.0[0] != 0 || source.0[1] != 0
    }
}

impl Display for BloomValue {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        let mut s = self.iter().map(|bit| if bit { "1" } else { "0" }).collect::<Vec<_>>();
        s.reverse();

        let mut output = String::with_capacity(BIT_LENGTH + BIT_LENGTH / 4);

        for words in s.chunks(32) {

            for bits in words.chunks(4) {

                for bit in bits {
                    output.push_str(bit);
                }

                output.push_str(" ");
            }

            output.push_str("\n");
        }

        f.write_str(&output)
    }
}


#[cfg(test)]
mod tests {
    use super::*;


    #[test]
    fn get() {
        let bv = BloomValue([1 << 12, 0]);

        assert!(bv.get(12));
        assert!(!bv.get(11));
        assert!(!bv.get(13));
        assert!(!bv.get(140));

        let bv = BloomValue([0, 1 << 12]);

        assert!(!bv.get(12));
        assert!(!bv.get(11));
        assert!(!bv.get(13));
        assert!(bv.get(140));
    }

    #[test]
    fn set() {
        let mut bv = BloomValue([0, 0]);

        bv.set(12);

        assert_eq!(bv.0[0] & 1 << 12 ^ 1 << 12, 0);
        assert_eq!(bv.0[1], 0);

        let mut bv = BloomValue([0, 0]);

        bv.set(140);

        assert_eq!(bv.0[0], 0);
        assert_eq!(bv.0[1] & 1 << 12 ^ 1 << 12, 0);
    }
}
