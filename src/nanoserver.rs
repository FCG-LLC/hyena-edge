use clap;
use std::str;
use futures::{future, Future, Stream};
use tokio_core::reactor::Core;
use nanomsg_tokio::Socket as NanoSocket;
use nanomsg::Protocol;

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

    let responses = reader.map(|mut msg| {
        println!("Got: {:?}", msg);
        msg.reverse();
        msg
    });

    let server = responses.forward(writer).then(|_| future::ok::<(), ()>(()));

    handle.spawn(server);

    loop {core.turn(None);}
}
