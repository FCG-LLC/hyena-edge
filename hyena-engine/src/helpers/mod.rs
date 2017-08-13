#[macro_use]
pub(crate) mod flame;

#[macro_use]
pub(crate) mod lock;

#[macro_use]
#[cfg(test)]
pub(crate) mod random;

#[macro_use]
#[cfg(test)]
pub(crate) mod seq;

#[macro_use]
#[cfg(test)]
pub(crate) mod tempfile;

#[macro_use]
pub(crate) mod block;


/// helper facilitating calling expressions that utilize `std::ops::Carrier`
/// in functions with no `Result` return type

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
    pub(crate) const DEFAULT_FILE_SIZE: usize = 1 << 20; // 1 MiB


    macro_rules! hashmap {
        ( $($key:expr => $value:expr),* $(,)* ) => {{
            use std::collections::hash_map::HashMap;

            let mut hash = HashMap::new();
            $(
                hash.insert($key, $value);
            )*

            hash
        }};
    }

    macro_rules! count {
        ($cur: ident $(, $tail: ident)* $(,)*) => {
            1 + count!($($tail,)*)
        };

        () => { 0 };
    }

    macro_rules! assert_file_size {
        ($file: expr, $size: expr) => {{
            let metadata = $file.metadata()
                .chain_err(|| "failed to retrieve metadata")
                .unwrap();

            assert_eq!(metadata.len(), $size as u64);
        }};

        ($file: expr) => {
            assert_file_size!($file, $DEFAULT_FILE_SIZE);
        };
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
                .chain_err(|| "unable to create file")
                .unwrap();

            file.read_exact(&mut buf)
                .chain_err(|| "unable to read test data")
                .unwrap();
            buf
        }};

        ($file: expr, $buf: expr) => {
            ensure_read!($file, $buf, DEFAULT_FILE_SIZE)
        };
    }

    macro_rules! ensure_write {
        ($file: expr, $w: expr, $size: expr) => {{
            use fs::ensure_file;
            use std::io::Write;


            let mut file = ensure_file(&$file, $size)
                .chain_err(|| "unable to create file")
                .unwrap();

            file.write(&$w)
                .chain_err(|| "unable to write test data")
                .unwrap();
        }};

        ($file: expr, $w: expr) => {
            ensure_write!($file, $w, DEFAULT_FILE_SIZE);
        };
    }
}
