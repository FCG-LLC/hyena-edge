pub use failure::{err_msg, Error, Fail, ResultExt};

pub type Result<T> = ::std::result::Result<T, Error>;
