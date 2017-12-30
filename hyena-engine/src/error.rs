pub use failure::{Fail, Error, ResultExt, err_msg};

pub type Result<T> = ::std::result::Result<T, Error>;
