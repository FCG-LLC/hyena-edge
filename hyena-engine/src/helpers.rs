#[macro_use]
#[cfg(test)]
pub(crate) mod tests {
    pub(crate) const DEFAULT_TEMPDIR_PREFIX: &str = "hyena-test";
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

        impl Drop for TempDir {
            fn drop(&mut self) {
                // leak tempdir, leaving files on disk
                forget(replace(&mut self.0, None));
            }
        }
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
            use storage::file::ensure_file;
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
            use storage::file::ensure_file;
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

    macro_rules! tempfile {
        ($name: expr, $prefix: expr, $tdir: expr) => {{
            let dir = $tdir
                .chain_err(|| "unable to create temporary directory")
                .unwrap();

            let file = dir.path().join($name);

            (dir, file)
        }};

        (persistent $name: expr, $prefix: expr) => {{
            use ::helpers::tests::persistent_tempdir::TempDir;

            tempfile!($name, $prefix, TempDir::new($prefix))
        }};

        (persistent prefix $prefix: expr) => {
            tempfile!(persistent "test_file.bin", $prefix)
        };

        (persistent $name: expr) => {
            tempfile!(persistent $name, ::helpers::tests::DEFAULT_TEMPDIR_PREFIX)
        };

        (persistent) => {
            tempfile!(persistent "test_file.bin")
        };

        ($name: expr, $prefix: expr) => {{
            use tempdir;

            tempfile!($name, $prefix, tempdir::TempDir::new($prefix))
        }};

        ($name: expr) => {
            tempfile!($name, ::helpers::tests::DEFAULT_TEMPDIR_PREFIX)
        };

        (prefix $prefix: expr) => {
            tempfile!("test_file.bin", $prefix)
        };

        () => {
            tempfile!("test_file.bin")
        };
    }
}