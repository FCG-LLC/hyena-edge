use hyena_engine::Result;
pub(crate) use hyena_test::tempfile::VolatileTempDir as TempDir;
use failure::ResultExt;

const TEMPDIR_PREFIX: &str = "hyena-criterion-bench";

/// Create temporary directory helper for test code
pub fn catalog_dir() -> Result<TempDir> {
    TempDir::new(TEMPDIR_PREFIX)
        .with_context(|_| "unable to create temporary directory")
        .map_err(|e| e.into())
}
