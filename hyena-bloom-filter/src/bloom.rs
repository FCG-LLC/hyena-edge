use std::fmt::{Display, Formatter, self};
use std::ops::{BitAnd, BitOr, BitXor, Not};


pub(crate) const BIT_LENGTH: usize = ::std::mem::size_of::<Bloom>() * 8;
pub(crate) const BASE_BIT_LENGTH: usize = ::std::mem::size_of::<u128>() * 8;

#[derive(Debug, Copy, Clone, PartialEq, PartialOrd, Hash, Default)]
pub struct Bloom([u128; 2]);

impl Bloom {
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
}

impl Not for Bloom {
    type Output = Self;

    fn not(self) -> Self {
        Bloom([
            !self.0[0],
            !self.0[1]
        ])
    }
}

impl BitAnd for Bloom {
    type Output = Self;

    fn bitand(self, rhs: Self) -> Self {
        Bloom([
            self.0[0] & rhs.0[0],
            self.0[1] & rhs.0[1]
        ])
    }
}

impl BitOr for Bloom {
    type Output = Self;

    fn bitor(self, rhs: Self) -> Self {
        Bloom([
            self.0[0] | rhs.0[0],
            self.0[1] | rhs.0[1]
        ])
    }
}

impl BitXor for Bloom {
    type Output = Self;

    fn bitxor(self, rhs: Self) -> Self {
        Bloom([
            self.0[0] ^ rhs.0[0],
            self.0[1] ^ rhs.0[1]
        ])
    }
}

impl From<Bloom> for bool {
    fn from(source: Bloom) -> bool {
        source.0[0] != 0 || source.0[1] != 0
    }
}

impl<'b> From<&'b Bloom> for bool {
    fn from(source: &'b Bloom) -> bool {
        source.0[0] != 0 || source.0[1] != 0
    }
}

impl Display for Bloom {
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
