/// This is a binary used by automatic protocol tests. Its job is to generate a Hyena reply,
/// serialize it and put into a file
///
/// More info at https://github.com/FCG-LLC/hyena-proto-test#hyena-verifier.
///
extern crate bincode;
extern crate clap;
extern crate hyena_api;
extern crate hyena_engine;

use bincode::{serialize, Infinite};
use clap::{App, Arg, ArgMatches};
use hyena_api::{Reply, ReplyColumn};
use hyena_engine::BlockType;
use std::fs::File;
use std::io::Write;

struct Options {
    output: String,
    column_names: Vec<String>,
    column_ids: Vec<usize>,
    column_types: Vec<BlockType>,
}

fn match_args<F, T>(args: &ArgMatches, arg_name: &str, convert: F) -> Vec<T>
    where F: Fn(&str) -> T
{
    match args.values_of(arg_name) {
        None => vec![],
        Some(vals) => vals.map(|x| convert(x)).collect(),
    }
}

impl Options {
    fn new(args: &ArgMatches) -> Self {
        Options {
            output: args.value_of("output").unwrap().into(),
            column_names: match_args(args, "column name", |x| x.into()),
            column_ids: match_args(args, "column id", |x| x.parse().unwrap()),
            column_types: match_args(args, "column type", |x| x.parse().unwrap()),
        }
    }
}

fn get_args() -> App<'static, 'static> {
    App::new("Protocol test generator")
        .version("0.1")
        .about("Generates serialized messages for automated protocol tests.")
        .arg(Arg::with_name("output")
            .help("The file to put the serialized message to")
            .short("o")
            .long("output")
            .takes_value(true)
            .required(true))
        .arg(Arg::with_name("command")
            .help("The command")
            .short("c")
            .long("command")
            .takes_value(true)
            .possible_values(&["columns", "catalog", "addcolumn", "insert", "scan", "error"])
            .required(true))
        .arg(Arg::with_name("column name")
            .help("Column name")
            .short("n")
            .long("column-name")
            .takes_value(true)
            .multiple(true))
        .arg(Arg::with_name("column id")
            .help("Column id")
            .short("i")
            .long("column-id")
            .takes_value(true)
            .multiple(true))
        .arg(Arg::with_name("column type")
            .help("Column type")
            .short("t")
            .long("column-type")
            .takes_value(true)
            .multiple(true))
}

fn write(filename: &String, data: &Vec<u8>) {
    let mut f = File::create(filename).unwrap();
    f.write(data).unwrap();
}

fn gen_columns(options: &Options) {
    if options.column_names.len() != options.column_ids.len() ||
       options.column_names.len() != options.column_types.len() {
        println!{"Number of -n, -i and -t options must be the same!"};
        std::process::exit(1);
    }
    let columns = (0..options.column_ids.len())
        .map(|index| {
            ReplyColumn::new(options.column_types[index],
                             options.column_ids[index],
                             options.column_names[index].clone())
        })
        .collect();
    let reply = Reply::ListColumns(columns);
    let serialized = serialize(&reply, Infinite).unwrap();

    write(&options.output, &serialized);
}

fn main() {
    let args = get_args().get_matches();
    let command = args.value_of("command").unwrap();
    let options = Options::new(&args);

    match command {
        "columns" => gen_columns(&options),
        _ => {}
    }
}
