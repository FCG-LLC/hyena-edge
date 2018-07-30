#[macro_use]
extern crate criterion;
extern crate failure;
extern crate hyena_engine;
extern crate hyena_test;
#[macro_use]
extern crate hyena_common;

mod config;
mod scan;

criterion_main!(scan::numeric_benches, scan::string_benches);
