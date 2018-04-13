/// helper facilitating calling expressions that utilize `std::ops::Carrier`
/// in functions with no `Result` return type
#[macro_export]
macro_rules! carry {
    ($what: expr) => {
        (|| {
            $what
        })()
    };
}

#[macro_export]
macro_rules! hashmap {
    () => {{
        use $crate::collections::HashMap;

        HashMap::new()
    }};

    ( $($key:expr => $value:expr),+ $(,)* ) => {{
        use $crate::collections::HashMap;

        let capacity = count!($($value),+);

        let mut hash = HashMap::with_capacity(capacity);
        $(
            hash.insert($key, $value);
        )*

        hash
    }};
}

#[macro_export]
macro_rules! hashset {
    () => {{
        use $crate::collections::HashSet;

        HashSet::new()
    }};

    ( $($value:expr),+ $(,)* ) => {{
        use $crate::collections::HashSet;

        let capacity = count!($($value),+);

        let mut hash = HashSet::with_capacity(capacity);
        $(
            hash.insert($value);
        )*

        hash
    }};
}

#[macro_export]
macro_rules! multiply_vec {
    ($vec: expr, $count: expr) => {{
        let count = $count;
        let vec = $vec;

        let mut v = Vec::from($vec);
        for _ in 1..count {
            v.extend(&vec);
        }

        v
    }};
}

#[macro_export]
macro_rules! merge_iter {
    (into $ty: ty, $base: expr, $( $it: expr ),* $(,)*) => {{
        let it = $base;

        $(
            let it = it.chain($it);
        )*

        it.collect::<$ty>()
    }};

    ($base: expr, $( $it: expr ),* $(,)*) => {{
        let it = $base;

        $(
            let it = it.chain($it);
        )*

        it.collect()
    }};
}

#[macro_export]
macro_rules! count {
    ($cur: tt $(, $tail: tt)* $(,)*) => {
        1 + count!($($tail,)*)
    };

    () => { 0 };
}
