

extern crate hyena_api;
extern crate regex;

use hyena_api::Request;
use regex::Regex;
use std::env;
use std::fs::File;
use std::io::Read;

trait TestPrinter {
    fn test_print(&self);
}

impl TestPrinter for Request {
    fn test_print(&self) {
        match *self {
            Request::ListColumns => 
                println!("{:?}", self),
            Request::RefreshCatalog =>
                println!("{:?}", self),
            Request::AddColumn(ref req) => {
                println!("AddColumn {{column_name: {:?}, column_type: {:?}}}", req.column_name, req.column_type);
            },
            Request::Insert(ref req) => {
                let r = Regex::new(r"(\d+): (\w+).\[([^\]]+)\].").unwrap();
                print!("Insert {{source: {}, timestamps: #{}, columns: [",
                         req.source, 
                         req.timestamps.len());
                let columns = req.columns.iter().fold("".to_owned(), |acc, column| {
                    let col_string = format!("{:?}", column);
                    let lala = r.captures_iter(&col_string).fold("".to_owned(), |acc2, cap| {
                        let col = format!("{}: {} #{}", &cap[1], &cap[2], column.values().next().unwrap().len());
                        if acc2 == "" { col } else { acc2 + ", " + &col }
                    });
                    if acc == "" { lala } else { acc + ", " + &lala }
                });
                println!("{} ]}}", columns);
            },
            Request::Scan(ref req) => {
                println!("Scan {{min_ts: {}, max_ts: {}, partition_ids: {:?}, projection: {:?}, filters: {:?}}}", 
                         req.min_ts,
                         req.max_ts,
                         req.partition_ids,
                         req.projection,
                         req.filters
                         );
            }
            _ => {
                println!{"Request type not implemented"}
            }
        }
    }
}

fn main() {
    let args: Vec<String> = env::args().collect();

    if args.len() < 2 {
        panic!("usage: {} <filename>", &args[0]);
    }
    let filename = &args[1];
    let mut msg: Vec<u8> = vec![];
    File::open(filename).unwrap().read_to_end(&mut msg).unwrap();

    let operation = Request::parse(msg).unwrap();
    operation.test_print();
}
