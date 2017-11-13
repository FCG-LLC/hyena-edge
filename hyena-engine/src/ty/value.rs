use error::*;
use std::fmt;
use ty::timestamp::Timestamp;

macro_rules! value_impl {
    ($( $variant: ident, $ty: ty ),+ $(,)*) => {
        #[derive(Debug, Clone, PartialEq)]
        pub enum Value {
            Null,
            $(
            $variant($ty),
            )+
        }

        $(
            impl From<$ty> for Value {
                fn from(v: $ty) -> Value {
                    Value::$variant(v)
                }
            }
        )+

        impl fmt::Display for Value {
            fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
                match *self {
                    Value::Null => write!(f, ""),
                    $(
                    Value::$variant(v) => write!(f, "{}", v),
                    )+
                }
            }
        }
    };
}

value_impl!(U8, u8,
            U16, u16,
            U32, u32,
            U64, u64,
            I8, i8,
            I16, i16,
            I32, i32,
            I64, i64,
            Usize, usize,
            Timestamp, Timestamp);
