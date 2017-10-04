#[macro_use] extern crate clap;
#[macro_use] extern crate log;

extern crate colored_logger;
extern crate dotenv;
extern crate flexi_logger;

use dotenv::dotenv;

mod cli;

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
}
