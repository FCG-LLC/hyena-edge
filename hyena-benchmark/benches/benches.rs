#[macro_use]
extern crate criterion;
#[macro_use]
extern crate hyena_common;

mod config;
mod scan;

criterion_main!(scan::numeric_benches, scan::string_benches);
