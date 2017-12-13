#[macro_use] extern crate clap;
#[macro_use] extern crate log;

extern crate bytes;
extern crate colored_logger;
extern crate dotenv;
extern crate flexi_logger;
extern crate futures;
extern crate nanomsg;
extern crate nanomsg_tokio;
extern crate tokio_core;
extern crate tokio_io;
extern crate tokio_proto;
extern crate tokio_service;
extern crate hyena_api;
extern crate hyena_engine;
extern crate bincode;

use dotenv::dotenv;

mod cli;
mod nanoserver;

fn main() {
    dotenv().ok();

    let options = cli::app().get_matches();

    flexi_logger::Logger::with_env()
        .format(colored_logger::formatter)
        .start()
        .unwrap_or_else(|e| panic!("Logger initialization failed with {}", e));
    info!("Starting Hyena");

    let data_dir = options.value_of("data_dir").unwrap();
    debug!("Data directory: {}", data_dir);

    nanoserver::run(&options);
}
