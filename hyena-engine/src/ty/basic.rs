use error::*;

macro_rules! basic_ty_impl {
    ($($tyname: ident, $ty: ty;)* $(;)*) => {
        use std::fmt::{Display, Formatter, Error};
        use std::ops::Deref;
        use std::result::Result;


        $(
            #[derive(Debug, Copy, Clone, PartialOrd, Eq, Ord, Hash, Serialize, Deserialize)]
            pub struct $tyname($ty);

            impl Deref for $tyname {
                type Target = $ty;

                fn deref(&self) -> &Self::Target {
                    &self.0
                }
            }

            impl From<$tyname> for $ty {
                fn from(source: $tyname) -> $ty {
                    source.0
                }
            }

            impl From<$ty> for $tyname {
                fn from(source: $ty) -> $tyname {
                    $tyname(source)
                }
            }

            impl<'a> From<&'a $ty> for $tyname {
                fn from(source: &$ty) -> $tyname {
                    $tyname(*source)
                }
            }

            impl Display for $tyname {
                fn fmt(&self, f: &mut Formatter) -> Result<(), Error> {
                    <$ty as Display>::fmt(&self.0, f)
                }
            }

            impl<T> PartialEq<T> for $tyname
            where $ty: From<T>, T: Copy {
                fn eq(&self, other: &T) -> bool {
                    <$ty as PartialEq>::eq(&*self, &<$ty as From<T>>::from(*other))
                }
            }
        )*
    };
}

basic_ty_impl!(I8, i8;
               I16, i16;
               I32, i32;
               I64, i64;
               U8, u8;
               U16, u16;
               U32, u32;
               U64, u64;);

#[cfg(feature = "block_128")]
mod t128 {

    #[derive(Debug, Copy, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
    pub struct I128(i128);

    #[derive(Debug, Copy, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
    pub struct U128(u128);

}

#[cfg(test)]
mod tests {
    use super::*;

    macro_rules! basic_test_impl {
        ($($mod: ident, $ty: ident, $b: ty);* $(;)*) => {
            $(
                mod $mod {
                    use super::*;

                    #[test]
                    fn has_eq_ty() {
                        assert_eq!($ty(12), 12 as $b);
                        assert_ne!($ty(13), 12 as $b);
                    }

                    #[test]
                    fn has_display() {
                        let v = $ty(100);

                        assert_eq!(v.to_string(), "100");
                    }

                    #[test]
                    fn has_deref() {
                        assert!(*$ty(12) == 12);
                    }
                }
            )*
        }
    }

    basic_test_impl!(nt_i8, I8, i8;
                     nt_i16, I16, i16;
                     nt_i32, I32, i32;
                     nt_i64, I64, i64;
                     nt_u8, U8, u8;
                     nt_u16, U16, u16;
                     nt_u32, U32, u32;
                     nt_u64, U64, u64;);
}
