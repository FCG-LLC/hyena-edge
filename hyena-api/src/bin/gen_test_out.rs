/// This is a binary used by automatic protocol tests. Its job is to generate a Hyena reply,
/// serialize it and put into a file
///
/// More info at https://github.com/FCG-LLC/hyena-proto-test#hyena-verifier.
///
extern crate bincode;
extern crate clap;
extern crate hyena_api;
extern crate hyena_engine;
extern crate rand;
extern crate uuid;

use bincode::{serialize, Infinite};
use clap::{App, Arg, ArgMatches};
use hyena_api::{Reply, ReplyColumn, RefreshCatalogResponse, PartitionInfo, Error};
use hyena_engine::BlockType;
use rand::Rng;
use std::fs::File;
use std::io::Write;
use uuid::Uuid;

struct Options {
    output: String,
    column_names: Vec<String>,
    column_ids: Vec<usize>,
    column_types: Vec<BlockType>,
    partitions: u32,
    error: Option<Error>,
}

fn match_args<F, T>(args: &ArgMatches, arg_name: &str, convert: F) -> Vec<T>
    where F: Fn(&str) -> T
{
    match args.values_of(arg_name) {
        None => vec![],
        Some(vals) => vals.map(|x| convert(x)).collect(),
    }
}

fn parse_error(args: &ArgMatches) -> Option<Error> {
    match args.value_of("error type") {
        None => None,
        Some(type_string) => {
            Some(match type_string {
                "ColumnNameAlreadyExists" => {
                    Error::ColumnNameAlreadyExists(args.value_of("error param").unwrap().into())
                }
                "ColumnIdAlreadyExists" => {
                    Error::ColumnIdAlreadyExists(args.value_of("error param")
                        .unwrap()
                        .parse()
                        .unwrap())
                }
                "ColumnNameCannotBeEmpty" => Error::ColumnNameCannotBeEmpty,
                "NoData" => Error::NoData(args.value_of("error param").unwrap().into()),
                "InconsistentData" => {
                    Error::InconsistentData(args.value_of("error param").unwrap().into())
                }
                "InvalidScanRequest" => {
                    Error::InvalidScanRequest(args.value_of("error param").unwrap().into())
                }
                "CatalogError" => Error::CatalogError(args.value_of("error param").unwrap().into()),
                "ScanError" => Error::ScanError(args.value_of("error param").unwrap().into()),
                "Unknown" => Error::Unknown(args.value_of("error param").unwrap().into()),
                _ => {
                    println!("Unknown error type {}", type_string);
                    std::process::exit(1);
                }
            })
        }
    }
}

impl Options {
    fn new(args: &ArgMatches) -> Self {
        Options {
            output: args.value_of("output").unwrap().into(),
            column_names: match_args(args, "column name", |x| x.into()),
            column_ids: match_args(args, "column id", |x| x.parse().unwrap()),
            column_types: match_args(args, "column type", |x| x.parse().unwrap()),
            partitions: args.value_of("partitions").unwrap().parse().unwrap(),
            error: parse_error(args),
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
        .arg(Arg::with_name("partitions")
            .help("Number of partitions")
            .short("p")
            .long("partition")
            .takes_value(true)
            .default_value("0"))
        .arg(Arg::with_name("error type")
            .help("Error type")
            .short("e")
            .long("error-type")
            .takes_value(true))
        .arg(Arg::with_name("error param")
            .help("Error parameter")
            .short("w")
            .long("error-param")
            .takes_value(true))
}

fn write(filename: &String, data: &Vec<u8>) {
    let mut f = File::create(filename).unwrap();
    f.write(data).unwrap();
}

fn verify_columns(options: &Options) {
    if options.column_names.len() != options.column_ids.len() ||
       options.column_names.len() != options.column_types.len() {
        println!{"Number of -n, -i and -t options must be the same!"};
        std::process::exit(1);
    }
}

fn gen_columns_vec(options: &Options) -> Vec<ReplyColumn> {
    (0..options.column_ids.len())
        .map(|index| {
            ReplyColumn::new(options.column_types[index],
                             options.column_ids[index],
                             options.column_names[index].clone())
        })
        .collect()
}

fn gen_columns(options: Options) {
    verify_columns(&options);
    let columns = gen_columns_vec(&options);
    let reply = Reply::ListColumns(columns);
    let serialized = serialize(&reply, Infinite).unwrap();

    write(&options.output, &serialized);
}

fn gen_partition_vec(options: &Options) -> Vec<PartitionInfo> {
    let mut rng = rand::thread_rng();

    (0..options.partitions)
        .map(|_| {
            let string_length = rng.gen_range(0, 1024);
            PartitionInfo::new(rand::random(),
                               rand::random(),
                               Uuid::new_v4().into(),
                               rng.gen_ascii_chars().take(string_length).collect())
        })
        .collect()
}

fn gen_catalog(options: Options) {
    verify_columns(&options);
    let response = RefreshCatalogResponse {
        columns: gen_columns_vec(&options),
        available_partitions: gen_partition_vec(&options),
    };
    let reply = Reply::RefreshCatalog(response);
    let serialized = serialize(&reply, Infinite).unwrap();

    write(&options.output, &serialized);
}

fn gen_add_column(options: Options) {
    if options.column_ids.len() != 1 && options.error.is_none() {
        println!("addcolumn requires exactly one column id (-i) or an error (-e & -w)");
        std::process::exit(1);
    }
    let reply = Reply::AddColumn(if options.column_ids.len() == 0 {
        Err(options.error.unwrap())
    } else {
        Ok(options.column_ids[0])
    });
    let serialized = serialize(&reply, Infinite).unwrap();

    write(&options.output, &serialized);
}

fn main() {
    let args = get_args().get_matches();
    let command = args.value_of("command").unwrap();
    let options = Options::new(&args);

    match command {
        "columns" => gen_columns(options),
        "catalog" => gen_catalog(options),
        "addcolumn" => gen_add_column(options),
        _ => {}
    }
}
