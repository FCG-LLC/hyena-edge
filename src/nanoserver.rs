use clap;
use std::fs;
use futures::{Future, Stream};
use tokio_core::reactor::Core;
use bincode::{serialize, Infinite};

use nanomsg_multi_server::{MultiServer, MultiServerFutures};
use nanomsg_multi_server::proto::{PeerReply, PeerRequest, PeerError};
use nanomsg_multi_server::config::{GcInterval, SessionTimeout};

use hyena_api::{Request, Reply, run_request};
use hyena_engine::Catalog;

use std::rc::Rc;
use std::cell::RefCell;


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

fn process_message(msg: Vec<u8>, catalog: &mut Catalog) -> Vec<u8> {
    let operation = Request::parse(&msg);

    let reply = match operation {
        Ok(request) => {
            trace!("Operation: {}", request);

            run_request(request, catalog)
        },
        Err(error) => Reply::SerializeError(format!("{:?}", error)),
    };

    trace!("Returning: {}", reply);

    serialize(&reply, Infinite)
        .expect("Reply serialization error")
}

pub fn run(matches: &clap::ArgMatches) {
    let mut core = Core::new().expect("Could not create Core");
    let handle = core.handle();

    let dir = matches.value_of("data_dir").unwrap();
    fs::create_dir_all(dir).expect("Could not create data_dir");
    let catalog = Catalog::open_or_create(dir).expect("Unable to initialize catalog");

    let address = get_address(matches);

    info!("Starting socket server");

    let server = MultiServer::new(
        address.as_ref(),
        SessionTimeout::default(),
        GcInterval::default(),
        handle.clone(),
    );

    info!("Listening on {}", address);

    let MultiServerFutures { server, sessions } = server.into_futures().expect("server error");

    let catalog = Rc::new(RefCell::new(catalog));

    handle.clone().spawn(sessions.for_each(move |session| {
        let connid = session.connid;

        info!("[{connid}] Incoming new session {:?}", session, connid=connid);
        let catalog = catalog.clone();

        let (writer, reader) = session.connection.split();

        handle.spawn(reader
            .map(move |msg| {
                use self::PeerRequest::*;

                match msg {
                    Request(msgid, Some(msg)) => {
                        info!(
                            "[{connid}@{msgid}] Incoming Request",
                            connid=connid, msgid=msgid
                        );

                        let reply = process_message(msg, &mut catalog.borrow_mut());

                        Ok(PeerReply::Response(msgid, Ok(Some(reply))))
                    }
                    Abort(msgid) => {
                        info!(
                            "[{connid}@{msgid}] Abort Request",
                            connid=connid, msgid=msgid
                        );

                        Ok(PeerReply::Response(msgid, Ok(None)))
                    }
                    KeepAlive => {
                        debug!(
                            "[{connid}] Keepalive Request",
                            connid=connid
                        );

                        Ok(PeerReply::KeepAlive)
                    }
                    _ => Err(PeerError::BadMessage),
                }
            })
            .and_then(|reply| reply)
            .forward(writer)
            .map(|_| ())
            .map_err(|error| error!("Session connection error {:?}", error)));

            Ok(())
    }));

    core.run(server).unwrap();
}

#[cfg(test)]
mod tests {

    mod get_address {
        use nanoserver::get_address;
        use cli::app;

        #[test]
        fn get_tcp_address() {
            let args = vec!["bin_name", "-ttcp", "--hostname", "a.host.com", "--port", "1234"];
            let m = app().get_matches_from_safe(args)
                .unwrap_or_else( |e| { panic!("Can't parse arguments: {}", e) });
            let address = get_address(&m);
            assert_eq!("tcp://a.host.com:1234", address);
        }

        #[test]
        fn get_ipc_address() {
            let args = vec!["bin_name", "--transport", "ipc", "--ipc-path", "/some/path"];
            let m = app().get_matches_from_safe(args)
                .unwrap_or_else( |e| { panic!("Can't parse arguments: {}", e) });
            let address = get_address(&m);
            assert_eq!("ipc:///some/path", address);
        }

        #[test]
        fn get_ws_address() {
            let args = vec!["bin_name", "--transport", "ws", "--hostname", "a.host.com", "--port", "1234"];
            let m = app().get_matches_from_safe(args)
                .unwrap_or_else( |e| { panic!("Can't parse arguments: {}", e) });
            let address = get_address(&m);
            assert_eq!("ws://a.host.com:1234", address);
        }
    }
}
