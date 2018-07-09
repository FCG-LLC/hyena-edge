#[macro_use]
extern crate criterion;
extern crate failure;
extern crate hyena_engine;
extern crate hyena_test;
extern crate hyena_common;

mod config;
mod scan;

criterion_main!(scan::benches);
