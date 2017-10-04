use clap::{App, Arg};

pub fn app() -> App<'static, 'static> {
    app_from_crate!()
        .arg(Arg::with_name("data_dir")
             .takes_value(true)
             .help("Directory to store data in.")
             .required(false)
             .default_value("/tmp/hyena-data-dir")
             .short("d")
             .long("data-directory")
        )
}
