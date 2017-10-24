use clap::{App, Arg};

static PORT_NUMBER_ERROR_STRING: &'static str = "Port number must be between 1 and 65535";

fn validate_port(port_string: String) -> Result<(), String> {
    match port_string.parse::<u16>() {
        Ok(val) => if val == 0 {
            Err(PORT_NUMBER_ERROR_STRING.into())
        } else {
            Ok(())
        },
        Err(_) => return Err(PORT_NUMBER_ERROR_STRING.into())
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

#[cfg(test)]
mod tests {

    mod validate_port {
        use cli::validate_port;

        #[test]
        fn rejects_empty_string() {
            assert!(validate_port("".into()).is_err())
        }

        #[test]
        fn rejects_non_numbers() {
            assert!(validate_port("test".into()).is_err());
            assert!(validate_port("----".into()).is_err());
            assert!(validate_port("  ".into()).is_err());
            assert!(validate_port("@#$%".into()).is_err());
            assert!(validate_port("99beers_on_the_wall".into()).is_err());
        }

        #[test]
        fn rejects_negative_numbers() {
            assert!(validate_port("-4321".into()).is_err());
            assert!(validate_port("-0".into()).is_err());
        }

        #[test]
        fn rejects_big_numbers() {
            assert!(validate_port("655350".into()).is_err());
            assert!(validate_port("65536".into()).is_err());
        }

        #[test]
        fn rejects_float_numbers() {
            assert!(validate_port("4321.1".into()).is_err());
        }

        #[test]
        fn rejects_zero() {
            assert!(validate_port("0".into()).is_err());
        }

        #[test]
        fn allows_valid_port_numbers() {
            assert!(validate_port("65535".into()).is_ok());
            assert!(validate_port("1".into()).is_ok());
            assert!(validate_port("4321".into()).is_ok());
        }
    }
}
