#[cfg(test)]
#[macro_use]
extern crate hyena_engine;

#[cfg(not(test))]
extern crate hyena_engine;

extern crate bincode;
#[macro_use]
extern crate log;
#[macro_use]
extern crate serde;

pub mod api;
