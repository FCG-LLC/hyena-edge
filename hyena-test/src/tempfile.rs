pub const DEFAULT_TEMPDIR_PREFIX: &str = "hyena-test";
pub const DEFAULT_TEMPFILE_NAME: &str = "tempfile.bin";

#[cfg(not(feature = "persistent_test_data"))]
pub use tempdir::TempDir as TempDir;
#[cfg(feature = "persistent_test_data")]
pub use self::persistent_tempdir::TempDir as TempDir;

pub use self::tempdir_tools::{TempDirExt, ThingWithTempDir};
pub use self::persistent_tempdir::TempDir as PersistentTempDir;
pub use tempdir::TempDir as VolatileTempDir;


pub(crate) mod tempdir_tools {
    use failure::{err_msg, Error, ResultExt};
    use std::path::{Path, PathBuf};
    use std::fs::File;
    use std::io::Read;
    use std::marker::PhantomData;
    use std::fmt::Debug;
    use std::ops::{Deref, DerefMut};
    use tempdir;

    pub trait TempDirExt: AsRef<Path> {
        fn exists<P: AsRef<Path>>(&self, path: P) -> bool {
            self.relative_path(path).exists()
        }

        fn exists_file<P: AsRef<Path>>(&self, path: P) -> bool {
            let p = self.relative_path(path);

            p.exists() && p.is_file()
        }

        fn exists_dir<P: AsRef<Path>>(&self, path: P) -> bool {
            let p = self.relative_path(path);

            p.exists() && p.is_dir()
        }

        fn read_vec<P: AsRef<Path>>(&self, path: P) -> Result<Vec<u8>, Error> {
            let p = self.relative_path(path);

            if p.exists() && p.is_file() {
                let mut f = File::open(p).context("unable to open file")?;
                let mut buf = Vec::new();

                f.read_to_end(&mut buf).context("unable to read from file")?;

                Ok(buf)
            } else {
                Err(err_msg("file doesn't exist or is not a file"))
            }
        }

        #[inline]
        fn relative_path<P: AsRef<Path>>(&self, path: P) -> PathBuf {
            self.as_ref().join(path)
        }
    }

    impl TempDirExt for tempdir::TempDir {}

    /// Helper struct for easy binding things that use tempdir and rely on its lifetime
    /// while not encapsulating TempDir directly (e.g. `hyena_engine::Catalog`)
    #[derive(Debug)]
    pub struct ThingWithTempDir<'td, T, TD=super::TempDir>
    where
        T: Debug + 'td,
        TD: TempDirExt + 'td,
    {
        thing: T,
        _tempdir: TD,
        _marker: PhantomData<&'td T>,
    }

    impl<'td, T, TD> ThingWithTempDir<'td, T, TD>
    where
        T: Debug + 'td,
        TD: TempDirExt + 'td,
    {
        pub fn new(thing: T, tempdir: TD) -> ThingWithTempDir<'td, T, TD> {
            ThingWithTempDir {
                thing,
                _tempdir: tempdir,
                _marker: PhantomData,
            }
        }
    }

    impl<'td, T, TD> AsRef<T> for ThingWithTempDir<'td, T, TD>
    where
        T: Debug + 'td,
        TD: TempDirExt + 'td,
    {
        fn as_ref(&self) -> &T {
            &self.thing
        }
    }

    impl<'td, T, TD> AsMut<T> for ThingWithTempDir<'td, T, TD>
    where
        T: Debug + 'td,
        TD: TempDirExt + 'td,
    {
        fn as_mut(&mut self) -> &mut T {
            &mut self.thing
        }
    }

    impl<'td, T, TD> Deref for ThingWithTempDir<'td, T, TD>
    where
        T: Debug + 'td,
        TD: TempDirExt + 'td,
    {
        type Target = T;

        fn deref(&self) -> &T {
            self.as_ref()
        }
    }

    impl<'td, T, TD> DerefMut for ThingWithTempDir<'td, T, TD>
    where
        T: Debug + 'td,
        TD: TempDirExt + 'td,
    {
        fn deref_mut(&mut self) -> &mut T {
            self.as_mut()
        }
    }
}

pub(crate) mod persistent_tempdir {
    use tempdir;
    use std::io::Result;
    use std::path::Path;
    use std::mem::{forget, replace};
    use super::tempdir_tools::TempDirExt;

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

    impl TempDirExt for TempDir {}

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

#[macro_export]
macro_rules! tempdir {
    (@ $tdir: expr) => {
        $tdir
            .expect("unable to create temporary directory");
    };

    (persistent $prefix: expr) => {{
        use $crate::tempfile::PersistentTempDir as TempDir;

        tempdir!(@ TempDir::new($prefix))
    }};

    (persistent) => {
        tempdir!(persistent $crate::tempfile::DEFAULT_TEMPDIR_PREFIX)
    };

    (volatile $prefix: expr) => {{
        use $crate::tempfile::VolatileTempDir as TempDir;

        tempdir!(@ TempDir::new($prefix))
    }};

    (volatile) => {
        tempdir!(volatile $crate::tempfile::DEFAULT_TEMPDIR_PREFIX)
    };

    ($prefix: expr) => {{
        // import feature-flagged TempDir
        // so if `persistent_test_data` is set this makes this a persistent version
        use $crate::tempfile::TempDir;

        tempdir!(@ TempDir::new($prefix))
    }};

    () => {
        tempdir!($crate::tempfile::DEFAULT_TEMPDIR_PREFIX)
    };
}

/// Create temporary file
///
/// add persistent as first keyword to keep test files
/// add prefix as first (after persistent) keyword to use prefix other
/// than default system temp

#[macro_export]
macro_rules! tempfile {
    (@ $tdir: expr, $($name: expr,)* ) => {{
        let dir = $tdir;

        let pb = dir.path().to_path_buf();
        let p = pb.as_path();

        (dir, $(p.join($name),)*)
    }};

    (persistent prefix $prefix: expr, $($name: expr),+ $(,)*) => {
        tempfile!(@ tempdir!(persistent $prefix), $($name,)*)
    };

    (persistent prefix $prefix: expr) => {
        tempfile!(persistent prefix $prefix, $crate::tempfile::DEFAULT_TEMPFILE_NAME)
    };

    (persistent $($name: expr),+ $(,)*) => {
        tempfile!(persistent prefix $crate::tempfile::DEFAULT_TEMPDIR_PREFIX, $($name,)*)
    };

    (persistent) => {
        tempfile!(persistent $crate::tempfile::DEFAULT_TEMPFILE_NAME)
    };

    (prefix $prefix: expr, $($name: expr),+ $(,)*) => {
        tempfile!(@ tempdir!($prefix), $($name,)*)
    };

    (prefix $prefix: expr) => {
        tempfile!(prefix $prefix, $crate::tempfile::DEFAULT_TEMPFILE_NAME)
    };

    ($($name: expr),+ $(,)*) => {
        tempfile!(prefix $crate::tempfile::DEFAULT_TEMPDIR_PREFIX, $($name,)*)
    };

    () => {
        tempfile!($crate::tempfile::DEFAULT_TEMPFILE_NAME)
    };
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;
    use std::fs::remove_dir;

    #[cfg(not(feature = "persistent_test_data"))]
    const DEFAULT_PERSISTENT: bool = false;
    #[cfg(feature = "persistent_test_data")]
    const DEFAULT_PERSISTENT: bool = true;

    macro_rules! path_name {
        ($path: expr) => {
            $path.file_name()
                 .expect("Unable to get path name")
                 .to_str()
                 .expect("Unable to convert path name to string")
        };
    }

    fn assert_tempfile<D: AsRef<Path>, F: AsRef<Path>>(
        path_tuple: (D, F),
        dirname: &str,
        filename: &str,
        persistent: bool,
    ) {
        let td = {
            let td = path_tuple;

            let dir: &Path = td.0.as_ref();
            let file: &Path = td.1.as_ref();

            assert!(dir.exists());
            assert!(dir.is_dir());
            assert!(path_name!(dir).starts_with(dirname));
            assert!(!file.exists());
            assert_eq!(path_name!(file), filename);

            dir.to_path_buf()
        };

        if persistent {
            let p = td.as_path();
            assert!(p.exists());
            remove_dir(p).expect("Unable to remove temporary directory");
        } else {
            assert!(!td.as_path().exists());
        }
    }

    #[test]
    fn tempfile_noargs() {
        assert_tempfile(
            tempfile!(),
            DEFAULT_TEMPDIR_PREFIX,
            DEFAULT_TEMPFILE_NAME,
            DEFAULT_PERSISTENT,
        );
    }

    #[test]
    fn tempfile_name() {
        assert_tempfile(
            tempfile!("test"),
            DEFAULT_TEMPDIR_PREFIX,
            "test",
            DEFAULT_PERSISTENT,
        );
    }

    #[test]
    fn tempfile_prefix() {
        assert_tempfile(
            tempfile!(prefix "test"),
            "test",
            DEFAULT_TEMPFILE_NAME,
            DEFAULT_PERSISTENT,
        );
    }

    #[test]
    fn tempfile_prefix_name() {
        assert_tempfile(
            tempfile!(prefix "test", "testfile"),
            "test",
            "testfile",
            DEFAULT_PERSISTENT,
        );
    }

    #[test]
    fn tempfile_persistent_noargs() {
        assert_tempfile(
            tempfile!(persistent),
            DEFAULT_TEMPDIR_PREFIX,
            DEFAULT_TEMPFILE_NAME,
            true,
        );
    }

    #[test]
    fn tempfile_persistent_name() {
        assert_tempfile(
            tempfile!(persistent "test"),
            DEFAULT_TEMPDIR_PREFIX,
            "test",
            true,
        );
    }

    #[test]
    fn tempfile_persistent_prefix() {
        assert_tempfile(
            tempfile!(persistent prefix "test"),
            "test",
            DEFAULT_TEMPFILE_NAME,
            true,
        );
    }

    #[test]
    fn tempfile_persistent_prefix_name() {
        assert_tempfile(
            tempfile!(persistent prefix "test", "testfile"),
            "test",
            "testfile",
            true,
        );
    }

    mod tempdir_tools {
        use super::super::tempdir_tools::TempDirExt;
        use std::fs::{create_dir, remove_dir, remove_file, File};
        use std::io::Write;

        fn exists<T: TempDirExt>(td: T, cleanup: bool) {
            assert!(td.exists(""));

            if cleanup {
                remove_dir(td).expect("unable to remove directory");
            }
        }

        fn exists_file<T: TempDirExt>(td: T, cleanup: bool) {
            let p = td.as_ref().join("testfile");

            {
                let _f = File::create(&p).expect("unable to create file");
            }

            assert!(td.exists_file("testfile"));
            assert!(!td.exists_dir("testfile"));

            if cleanup {
                remove_file(p).expect("unable to remove file");
                remove_dir(td).expect("unable to remove directory");
            }
        }

        fn exists_dir<T: TempDirExt>(td: T, cleanup: bool) {
            let p = td.as_ref().join("testdir");

            {
                create_dir(&p).expect("unable to create directory");
            }

            assert!(td.exists_dir("testdir"));
            assert!(!td.exists_file("testdir"));

            if cleanup {
                remove_dir(p).expect("unable to remove directory");
                remove_dir(td).expect("unable to remove directory");
            }
        }

        fn read_vec<T: TempDirExt>(td: T, cleanup: bool) {
            let p = td.as_ref().join("testfile");
            let testdata = (1..100).collect::<Vec<u8>>();

            {
                let mut f = File::create(&p).expect("unable to create file");

                f.write_all(&testdata).expect("unable to write test data");
            }

            let _rdata = td.read_vec(&p).expect("unable to read test data");

            assert_eq!(&testdata[..], &testdata[..]);

            if cleanup {
                remove_file(p).expect("unable to remove file");
                remove_dir(td).expect("unable to remove directory");
            }
        }

        mod volatile {
            #[test]
            fn exists() {
                super::exists(tempdir!(), false)
            }

            #[test]
            fn exists_file() {
                super::exists_file(tempdir!(), false)
            }

            #[test]
            fn exists_dir() {
                super::exists_dir(tempdir!(), false)
            }

            #[test]
            fn read_vec() {
                super::read_vec(tempdir!(), false)
            }
        }

        mod persistent {
            #[test]
            fn exists() {
                super::exists(tempdir!(persistent), true)
            }

            #[test]
            fn exists_file() {
                super::exists_file(tempdir!(persistent), true)
            }

            #[test]
            fn exists_dir() {
                super::exists_dir(tempdir!(persistent), true)
            }

            #[test]
            fn read_vec() {
                super::read_vec(tempdir!(persistent), true)
            }
        }
    }
}
