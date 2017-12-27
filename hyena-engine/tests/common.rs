extern crate hyena_engine;
extern crate tempdir;
use tempdir::TempDir;

use hyena_engine::{Catalog, Result, ResultExt};


const TEMPDIR_PREFIX: &str = "hyena-int-test";

/// Create temporary directory helper for test code
pub fn catalog_dir() -> Result<TempDir> {
    TempDir::new(TEMPDIR_PREFIX).chain_err(|| "unable to create temporary directory")
}

/// A helper that allows facilitates try (?) operator use within test functions
pub fn wrap_result<F>(cl: F)
where
    F: Fn() -> Result<()>,
{
    cl().chain_err(|| "test execution failed").unwrap()
}

macro_rules! wrap_result {
    ($cl: block) => {
        wrap_result(|| {
            $cl;
            Ok(())
        })
    };
}

/// Get column index, column name pairs from the `Catalog`

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