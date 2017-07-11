#[macro_use]
#[cfg(test)]
pub(crate) mod tests {
    pub(crate) const DEFAULT_TEMPDIR_PREFIX: &str = "hyena-test";
    pub(crate) const DEFAULT_TEMPFILE_NAME: &str = "tempfile.bin";
    pub(crate) const DEFAULT_FILE_SIZE: usize = 1 << 20; // 1 MiB


    pub(crate) mod persistent_tempdir {
        use tempdir;
        use std::io::Result;
        use std::path::Path;
        use std::mem::{forget, replace};

        #[derive(Debug)]
        pub struct TempDir(Option<tempdir::TempDir>);

        impl TempDir {
            pub fn new(prefix: &str) -> Result<TempDir> {
                tempdir::TempDir::new(prefix).map(|td| TempDir(Some(td)))
            }

            pub fn path(&self) -> &Path {
                if let Some(ref td) = self.0 {
                    td.path()
                } else {
                    unreachable!("TempDir::path called with empty inner")
                }
            }
        }

        impl AsRef<Path> for TempDir {
            fn as_ref(&self) -> &Path {
                self.path()
            }
        }

        impl Drop for TempDir {
            fn drop(&mut self) {
                // leak tempdir, leaving files on disk
                forget(replace(&mut self.0, None));
            }
        }
    }

    macro_rules! hashmap {
        // support invocations without trailing comma

        ( $($key:expr => $value:expr),* ) => {
            hashmap! { $( $key => $value,)* }
        };

        ( $($key:expr => $value:expr,)* ) => {{
            use std::collections::hash_map::HashMap;

            let mut hash = HashMap::new();
            $(
                hash.insert($key, $value);
            )*

            hash
        }};
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

    macro_rules! tempdir {
        (@ $tdir: expr) => {
            $tdir
                .chain_err(|| "unable to create temporary directory")
                .unwrap()
        };

        (persistent $prefix: expr) => {{
            use ::helpers::tests::persistent_tempdir::TempDir;

            tempdir!(@ TempDir::new($prefix))
        }};

        (persistent) => {
            tempdir!(persistent ::helpers::tests::DEFAULT_TEMPDIR_PREFIX)
        };

        ($prefix: expr) => {{
            use tempdir::TempDir;

            tempdir!(@ TempDir::new($prefix))
        }};

        () => {
            tempdir!(::helpers::tests::DEFAULT_TEMPDIR_PREFIX)
        };
    }

    /// Create temporary file
    ///
    /// add persistent as first keyword to keep test files
    /// add prefix as first (after persistent) keyword to use prefix other
    /// than default system temp

    macro_rules! tempfile {
        (@ $prefix: expr, $tdir: expr, $($name: expr,)* ) => {{
            let dir = $tdir;

            let pb = dir.path().to_path_buf();
            let p = pb.as_path();

            (dir, $(p.join($name),)*)
        }};

        (persistent prefix $prefix: expr, $($name: expr,)*) => {
            tempfile!(@ $prefix, tempdir!(persistent $prefix), $($name,)*)
        };

        (persistent prefix $prefix: expr, $($name: expr),*) => {
            tempfile!(persistent prefix $prefix, $($name,)*)
        };

        (persistent prefix $prefix: expr) => {
            tempfile!(persistent prefix $prefix, ::helpers::tests::DEFAULT_TEMPFILE_NAME)
        };

        (persistent $($name: expr,)*) => {
            tempfile!(persistent prefix ::helpers::tests::DEFAULT_TEMPDIR_PREFIX, $($name,)*)
        };

        (persistent $($name: expr),*) => {
            tempfile!(persistent $($name,)*)
        };

        (persistent) => {
            tempfile!(persistent ::helpers::tests::DEFAULT_TEMPFILE_NAME)
        };

        (prefix $prefix: expr, $($name: expr,)*) => {
            tempfile!(@ $prefix, tempdir!($prefix), $($name,)*)
        };

        (prefix $prefix: expr, $($name: expr),*) => {
            tempfile!(prefix $prefix, $($name,)*)
        };

        (prefix $prefix: expr) => {
            tempfile!(prefix $prefix, ::helpers::tests::DEFAULT_TEMPFILE_NAME)
        };

        ($($name: expr,)*) => {
            tempfile!(prefix ::helpers::tests::DEFAULT_TEMPDIR_PREFIX, $($name,)*)
        };

        ($($name: expr),*) => {
            tempfile!($($name,)*)
        };

        () => {
            tempfile!(::helpers::tests::DEFAULT_TEMPFILE_NAME)
        };
    }
}
