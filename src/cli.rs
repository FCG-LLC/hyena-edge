use clap::{App, Arg};

fn validate_port(port_string: String) -> Result<(), String> {
    match port_string.parse::<u16>() {
        Ok(val) => if val == 0 {
            Err("Port number must be between 1 and 65535".into())
        } else {
            Ok(())
        },
        Err(_) => return Err("Port number must be between 1 and 65535".into())
    }
}

pub fn app() -> App<'static, 'static> {
    app_from_crate!()
        .arg(Arg::with_name("data_dir")
             .takes_value(true)
             .help("Directory to store data in.")
             .required(false)
             .default_value("/tmp/hyena-data-dir")
             .short("d")
             .long("data-directory"))
        .arg(Arg::with_name("transport")
             .takes_value(true)
             .help("Nanomsg transport")
             .required(false)
             .default_value("tcp")
             .possible_values(&["tcp", "ipc", "ws"])
             .short("t")
             .long("transport"))
        .arg(Arg::with_name("hostname")
             .takes_value(true)
             .help("Address to bind nanomsg socket")
             .required(false)
             .default_value("*")
             .short("h")
             .long("hostname"))
        .arg(Arg::with_name("port")
             .takes_value(true)
             .help("Port number to bind nanomsg socket")
             .required(false)
             .default_value("4433")
             .short("p")
             .long("port")
             .validator(validate_port))
        .arg(Arg::with_name("ipc_path")
             .takes_value(true)
             .help("Path of nanomsg ipc socket")
             .required(false)
             .default_value("/tmp/hyena-ipc")
             .short("i")
             .long("ipc-path"))
}
