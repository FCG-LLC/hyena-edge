/// This is a binary used by automatic protocol tests. Its job is to generate a Hyena reply,
/// serialize it and put into a file
///
/// More info at https://github.com/FCG-LLC/hyena-proto-test#hyena-generator.
///
extern crate bincode;
extern crate clap;
extern crate hyena_api;
extern crate hyena_engine;
extern crate rand;
extern crate uuid;

use bincode::{serialize, Infinite};
use clap::{App, Arg, ArgMatches};
use hyena_api::{Reply, ReplyColumn, RefreshCatalogResponse, PartitionInfo, Error, DataTriple,
                ScanResultMessage};
use hyena_engine::{BlockType, Fragment};
use rand::Rng;
use std::fs::File;
use std::io::Write;
use uuid::Uuid;

struct Options {
    output: String,
    column_names: Vec<String>,
    column_ids: Vec<usize>,
    column_types: Vec<BlockType>,
    column_data: Vec<usize>,
    partitions: u32,
    error: Option<Error>,
    rows: Option<usize>,
    error_text: Option<String>,
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
    args.value_of("error type").map(|type_string| match type_string {
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
        "InconsistentData" => Error::InconsistentData(args.value_of("error param").unwrap().into()),
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

impl Options {
    fn new(args: &ArgMatches) -> Self {
        Options {
            output: args.value_of("output").unwrap().into(),
            column_names: match_args(args, "column name", |x| x.into()),
            column_ids: match_args(args, "column id", |x| x.parse().unwrap()),
            column_types: match_args(args, "column type", |x| x.parse().unwrap()),
            column_data: match_args(args, "column data", |x| x.parse().unwrap()),
            partitions: args.value_of("partitions").unwrap().parse().unwrap(),
            error: parse_error(args),
            rows: args.value_of("rows").map(|str| str.parse().unwrap()),
            error_text: args.value_of("error param").map(|x| x.into()),
        }
    }
}

fn get_args() -> App<'static, 'static> {
    App::new("Protocol test generator")
        .version("0.1")
        .about("Generates serialized messages for automated protocol tests.")
        .set_term_width(100)
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
            .possible_values(&["columns",
                               "catalog",
                               "addcolumn",
                               "insert",
                               "scan",
                               "serializeerror"])
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
        .arg(Arg::with_name("column data")
            .help("Number of data rows returned from scan for the column")
            .short("d")
            .long("column-data")
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
        .arg(Arg::with_name("rows")
            .help("Number of inserted rows")
            .short("r")
            .long("rows")
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

fn verify_column_data(options: &Options) {
    if options.column_data.len() != options.column_ids.len() ||
       options.column_data.len() != options.column_types.len() {
        println!{"Number of -d, -i and -t options must be the same!"};
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

fn gen_insert(options: Options) {
    if options.rows.is_none() && options.error.is_none() {
        println!("insert requires a numeric value (-r) or an error (-e & -w)");
        std::process::exit(1);
    }
    let reply = Reply::Insert(if options.rows.is_some() {
        Ok(options.rows.unwrap())
    } else {
        Err(options.error.unwrap())
    });
    let serialized = serialize(&reply, Infinite).unwrap();

    write(&options.output, &serialized);
}

fn gen_serialize_error(options: Options) {
    if options.error_text.is_none() {
        println!("serializerror requires error string (-w)");
        std::process::exit(1);
    }
    let reply = Reply::SerializeError(options.error_text.unwrap());
    let serialized = serialize(&reply, Infinite).unwrap();

    write(&options.output, &serialized);
}

fn gen_vec<T: rand::Rand>(rows: usize) -> Vec<T> {
    (0..rows).map(|_| rand::thread_rng().gen()).collect()
}

fn gen_fragment(rows: usize, block_type: BlockType) -> Fragment {

    match block_type {
        BlockType::I8Dense => Fragment::I8Dense(gen_vec(rows)),
        BlockType::I16Dense => Fragment::I16Dense(gen_vec(rows)),
        BlockType::I32Dense => Fragment::I32Dense(gen_vec(rows)),
        BlockType::I64Dense => Fragment::I64Dense(gen_vec(rows)),
        BlockType::I128Dense => Fragment::I128Dense(gen_vec(rows)),
        BlockType::U8Dense => Fragment::U8Dense(gen_vec(rows)),
        BlockType::U16Dense => Fragment::U16Dense(gen_vec(rows)),
        BlockType::U32Dense => Fragment::U32Dense(gen_vec(rows)),
        BlockType::U64Dense => Fragment::U64Dense(gen_vec(rows)),
        BlockType::U128Dense => Fragment::U128Dense(gen_vec(rows)),
        BlockType::I8Sparse => Fragment::I8Sparse(gen_vec(rows), gen_vec(rows)),
        BlockType::I16Sparse => Fragment::I16Sparse(gen_vec(rows), gen_vec(rows)),
        BlockType::I32Sparse => Fragment::I32Sparse(gen_vec(rows), gen_vec(rows)),
        BlockType::I64Sparse => Fragment::I64Sparse(gen_vec(rows), gen_vec(rows)),
        BlockType::I128Sparse => Fragment::I128Sparse(gen_vec(rows), gen_vec(rows)),
        BlockType::U8Sparse => Fragment::U8Sparse(gen_vec(rows), gen_vec(rows)),
        BlockType::U16Sparse => Fragment::U16Sparse(gen_vec(rows), gen_vec(rows)),
        BlockType::U32Sparse => Fragment::U32Sparse(gen_vec(rows), gen_vec(rows)),
        BlockType::U64Sparse => Fragment::U64Sparse(gen_vec(rows), gen_vec(rows)),
        BlockType::U128Sparse => Fragment::U128Sparse(gen_vec(rows), gen_vec(rows)),
    }
}

fn gen_scan_result(options: &Options) -> ScanResultMessage {
    let mut result = ScanResultMessage::new();
    let mut data = (0..options.column_data.len())
        .map(|index| {
            let data = if options.column_data[index] == 0 {
                None
            } else {
                Some(gen_fragment(options.column_data[index], options.column_types[index]))
            };

            DataTriple::new(options.column_ids[index], options.column_types[index], data)
        })
        .collect();

    result.append(&mut data);
    result
}

fn gen_scan(options: Options) {
    verify_column_data(&options);

    let reply = Reply::Scan(if options.error.is_some() {
        Err(options.error.unwrap())
    } else {
        Ok(gen_scan_result(&options))
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
        "insert" => gen_insert(options),
        "scan" => gen_scan(options),
        "serializeerror" => gen_serialize_error(options),
        _ => {}
    }
}
