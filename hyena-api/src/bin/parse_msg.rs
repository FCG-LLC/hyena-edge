/// This is a binary used by automatic protocol tests. Its job is to take a file with a serialized
/// Hyena request, read and parse it, and print the result.
///
/// More info at https://github.com/FCG-LLC/hyena-proto-test#hyena-verifier.

use clap::{App, Arg};
use hyena_api::Request;
use nanomsg_multi_server::proto::{deserialize, PeerRequest};
use regex::Regex;
use std::fs::File;
use std::io::Read;

fn test_write(request: &Request) -> String {
    match *request {
        Request::ListColumns => format!("{:?}", request),
        Request::RefreshCatalog => format!("{:?}", request),
        Request::AddColumn(ref req) => {
            format!("AddColumn {{column_name: {:?}, column_type: {:?}}}",
                     req.column_name,
                     req.column_type)
        }
        Request::Insert(ref req) => {
            let r = Regex::new(r"(\d+): (\w+).\[([^\]]+)\].").unwrap();
            let pre = format!("Insert {{source: {}, timestamps: #{}, columns: [",
                   req.source(),
                   req.timestamps().len());
            let columns = req.columns().iter().fold("".to_owned(), |acc, column| {
                let col_string = format!("{:?}", column);
                let lala = r.captures_iter(&col_string).fold("".to_owned(), |acc2, cap| {
                    let col = format!("{}: {} #{}",
                                      &cap[1],
                                      &cap[2],
                                      column.values().next().unwrap().len());
                    if acc2 == "" { col } else { acc2 + ", " + &col }
                });
                if acc == "" { lala } else { acc + ", " + &lala }
            });
            format!("{}{} ]}}", pre, columns)
        }
        Request::Scan(ref req) => {
            format!("Scan {{min_ts: {}, max_ts: {}, partition_ids: #{}, projection: {:?}, \
                      filters: {:?}}}",
                     req.min_ts,
                     req.max_ts,
                     req.partition_ids.len(),
                     req.projection,
                     req.filters)
        }
        _ => format!{"Request type not implemented"},
    }
}

fn test_print_peer_request(pr: &PeerRequest) {
    match *pr {
        PeerRequest::Request(ref msg_id, Some(ref msg)) => {
            let operation = Request::parse(&msg).unwrap();
            println!("Request: {}, {}", msg_id, test_write(&operation));
        },
        ref msg => println!("{:?}", msg)
    }
}

fn main() {
    let app = App::new("Protocol test verifier")
        .version("0.1")
        .arg(Arg::with_name("filename")
            .help("The file with serialized message")
            .required(true)
            .index(1))
        .get_matches();

    let filename = app.value_of("filename").unwrap();
    let mut msg: Vec<u8> = vec![];
    File::open(filename).unwrap().read_to_end(&mut msg).unwrap();

    let operation: PeerRequest = deserialize(&msg).unwrap();
    test_print_peer_request(&operation);
}
