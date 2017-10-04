extern crate flexi_logger;
extern crate colored_logger;
#[macro_use]
extern crate log;
extern crate dotenv;

use dotenv::dotenv;

fn main() {
    dotenv().ok();

    flexi_logger::Logger::with_env()
        .format(colored_logger::formatter)
        .start()
        .unwrap_or_else(|e| panic!("Logger initialization failed with {}", e));

    info!("Starting Hyena");
}
