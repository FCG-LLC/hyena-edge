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
extern crate hyena_engine;
extern crate hyena_api;
extern crate bincode;
extern crate nanomsg_multi_server;

use dotenv::dotenv;

mod cli;
mod nanoserver;

fn main() {
    dotenv().ok();

    let options = cli::app().get_matches();

    flexi_logger::Logger::with_env()
        .format(colored_logger::formatter)
        .start()
        .expect("Logger initialization failed");

    info!("Starting Hyena");

    debug!("Data directory: {}", options.value_of("data_dir").unwrap());

    nanoserver::run(options);
}
