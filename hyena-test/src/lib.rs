extern crate chrono;
extern crate failure;
#[cfg(feature = "perf")]
extern crate flame;
extern crate hyena_common;
extern crate num;
extern crate rand;
extern crate tempdir;
extern crate lipsum;

pub mod perf;
pub mod random;
pub mod seq;
pub mod tempfile;
pub mod string;
mod helpers;

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
