use clap;
use std::str;
use futures::{future, Future, Stream};
use tokio_core::reactor::Core;
use nanomsg_tokio::Socket as NanoSocket;
use nanomsg::Protocol;
use bincode::{serialize, Infinite};

use hyena_engine::api::{ApiRequest, run_request};
use hyena_engine::catalog::Catalog;

fn get_address(matches: &clap::ArgMatches) -> String {
    let transport = matches.value_of("transport").unwrap();
    let hostname  = matches.value_of("hostname").unwrap();
    let port      = matches.value_of("port").unwrap();
    let ipc_path  = matches.value_of("ipc_path").unwrap();

    match transport {
        "ipc" => format!("{}://{}", transport, ipc_path),
        _     => format!("{}://{}:{}", transport, hostname, port)
    }
}

fn process_message(msg: Vec<u8>, catalog: &Catalog) -> Vec<u8> {
    println!("Got: {:?}", msg);
    let operation = ApiRequest::parse(msg);
    println!("Operation: {:?}", operation);
    let reply = run_request(operation, catalog);
    println!("Returning: {:?}", reply);

    serialize(&reply, Infinite).unwrap()
}

pub fn run(matches: &clap::ArgMatches) {
    let mut core = Core::new().expect("Could not create Core");
    let handle = core.handle();

    let mut nano_socket = NanoSocket::new(Protocol::Rep, &handle)
        .expect("Unable to create nanomsg socket");

    let address = get_address(matches);
    debug!("Starting nanomsg server on {}", address);

    nano_socket
        .bind(address.as_str())
        .expect("Unable to bind nanomsg endpoint");

    let (writer, reader) = nano_socket.split();
    let catalog = Catalog::open_or_create(matches.value_of("data_dir").unwrap());

    let server = reader.map(move |msg| {
        process_message(msg, &catalog)
    }).forward(writer).then(|_| future::ok::<(), ()>(()));

    handle.spawn(server);

    loop {core.turn(None);}
}
