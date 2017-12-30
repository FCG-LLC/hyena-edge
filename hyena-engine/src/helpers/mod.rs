#[macro_use]
pub(crate) mod lock;

#[macro_use]
#[cfg(test)]
pub(crate) mod random;

// #[macro_use]
// #[cfg(test)]
// pub(crate) use hyena_test::tempfile;
//
// #[macro_use]
// #[cfg(test)]
// pub(crate) use hyena_test::seq;

#[macro_use]
pub(crate) mod block;

#[cfg(test)]
pub(crate) mod table;

/// helper facilitating calling expressions that utilize `std::ops::Carrier`
/// in functions with no `Result` return type
#[allow(unused_macros)]
macro_rules! carry {
    ($what: expr) => {
        (|| {
            $what
        })()
    };
}


#[macro_use]
#[cfg(test)]
pub(crate) mod tests {

    macro_rules! hashmap {
        () => {{
            use std::collections::hash_map::HashMap;

            HashMap::new()
        }};

        ( $($key:expr => $value:expr),+ $(,)* ) => {{
            use std::collections::hash_map::HashMap;

            let capacity = count!($($value),+);

            let mut hash = HashMap::with_capacity(capacity);
            $(
                hash.insert($key, $value);
            )*

            hash
        }};
    }

    macro_rules! hashset {
        () => {{
            use std::collections::hash_set::HashSet;

            HashSet::new()
        }};

        ( $($value:expr),+ $(,)* ) => {{
            use std::collections::hash_set::HashSet;

            let capacity = count!($($value),+);

            let mut hash = HashSet::with_capacity(capacity);
            $(
                hash.insert($value);
            )*

            hash
        }};
    }

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

    macro_rules! count {
        ($cur: tt $(, $tail: tt)* $(,)*) => {
            1 + count!($($tail,)*)
        };

        () => { 0 };
    }

    macro_rules! assert_file_size {
        ($file: expr, $size: expr) => {{
            let metadata = $file.metadata()
                .with_context(|_| "failed to retrieve metadata")
                .unwrap();

            assert_eq!(metadata.len(), $size as u64);
        }};
    }

    macro_rules! assert_variant {
        ($what: expr, $variant: pat, $test: expr) => {{
            let e = $what;

            if let $variant = e {
                assert!($test);
            } else {
                panic!("assert_variant failed: {:?}", e);
            }
        }};

        ($what: expr, $variant: pat) => {
            assert_variant!($what, $variant, ());
        };
    }

    macro_rules! ensure_read {
        ($file: expr, $buf: expr, $size: expr) => {{
            use fs::ensure_file;
            use std::io::Read;


            let mut buf = $buf;
            let mut file = ensure_file(&$file, $size)
                .with_context(|_| "unable to create file")
                .unwrap();

            file.read_exact(&mut buf)
                .with_context(|_| "unable to read test data")
                .unwrap();
            buf
        }};
    }

    macro_rules! ensure_write {
        ($file: expr, $w: expr, $size: expr) => {{
            use fs::ensure_file;
            use std::io::Write;


            let mut file = ensure_file(&$file, $size)
                .with_context(|_| "unable to create file")
                .unwrap();

            file.write(&$w)
                .with_context(|_| "unable to write test data")
                .unwrap();
        }};
    }
}
