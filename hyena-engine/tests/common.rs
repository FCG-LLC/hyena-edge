extern crate hyena_engine;
extern crate hyena_test;
extern crate failure;

use self::failure::ResultExt;

use self::hyena_test::tempfile;

// change this to PersistentTempDir for int easy tests debugging

pub use self::tempfile::VolatileTempDir as TempDir;
// pub use self::tempfile::PersistentTempDir as TempDir;

use hyena_engine::{Catalog, Result};


const TEMPDIR_PREFIX: &str = "hyena-int-test";

/// Create temporary directory helper for test code
pub fn catalog_dir() -> Result<TempDir> {
    TempDir::new(TEMPDIR_PREFIX)
        .with_context(|_| "unable to create temporary directory")
        .map_err(|e| e.into())
}

/// A helper that allows facilitates try (?) operator use within test functions
pub fn wrap_result<F>(cl: F)
where
    F: Fn() -> Result<()>,
{
    cl().with_context(|_| "test execution failed").unwrap()
}

macro_rules! wrap_result {
    ($cl: block) => {
        wrap_result(|| {
            $cl;
            Ok(())
        })
    };
}

// dead_code warning has to be silenced because of how integration-style tests work in Rust
// This module is used in all other test modules and it's also treated as a standalone test
// and for each test module it's linked and linted separately
// So, if we use this function in all but one test module it will generate dead_code
// during the compilation of that single module

/// Get column index, column name pairs from the `Catalog`

#[allow(dead_code)]
pub fn get_columns(catalog: &Catalog) -> Vec<(usize, String)> {
    let mut columns = catalog
        .as_ref()
        .iter()
        .map(|(colid, col)| (*colid, col.to_string()))
        .collect::<Vec<_>>();

    columns.sort_unstable_by_key(|&(colidx, _)| colidx);

    columns
}

#[test]
fn it_wraps_result() {
    // fake use of wrap_result macro
    // to prevent rustc from complaining about unused macro
    wrap_result!({{

    }})
}