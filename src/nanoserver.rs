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
use std::thread;
use std::path::PathBuf;
use futures::sync::mpsc::UnboundedSender;

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

pub fn run(matches: clap::ArgMatches) {
    let mut core = Core::new().expect("Could not create Core");
    let handle = core.handle();

    let dir = matches.value_of("data_dir").unwrap();
    fs::create_dir_all(dir).expect("Could not create data_dir");

    let address = get_address(&matches);

    let session_ipc_path = PathBuf::from(matches.value_of("session_ipc_path")
        .expect("Session ipc path not specified"));

    info!("Starting socket server");

    let server = {
        let mut server = MultiServer::new(
            address.as_ref(),
            SessionTimeout::default(),
            GcInterval::default(),
            move |session_id| {
                let path = session_ipc_path.join(format!("hyena-session-{}.ipc", session_id));
                format!("ipc://{}", path.display())
            },
            handle.clone(),
        );

        // socket permissions

        let owner = matches
            .value_of("ipc_socket_owner")
            .map(|uid| uid.parse().expect("Invalid UID provided"));

        let group = matches
            .value_of("ipc_socket_group")
            .map(|gid| gid.parse().expect("Invalid GID provided"));

        let permissions = matches
            .value_of("ipc_socket_permissions")
            .map(|perms| u32::from_str_radix(perms, 8).expect("Invalid permissions provided"));

        server.set_socket_permissions(owner, group, permissions);

        server
    };

    info!("Listening on {}", address);

    let MultiServerFutures { server, sessions } = server.into_futures().expect("server error");

    // for sending to the catalog thread
    // this in-memory channel is unbounded, so requests sent through it will queue up
    // and be processed synchronously within the thread
    let (cat_writer, cat_reader) = ::std::sync::mpsc::channel();

    let catalog_thread = {
        let dir = dir.to_owned();

        thread::Builder::new()
            .spawn(move || {
                let catalog = Rc::new(RefCell::new(Catalog::open_or_create(dir)
                    .expect("Unable to initialize catalog")));

                cat_reader
                    .iter()
                    .for_each(|msg: (_, _, _, UnboundedSender<_>)| {
                        let (msg, connid, msgid, output_channel) = msg;

                        let reply = process_message(msg, &mut catalog.borrow_mut());

                        debug!("[{}@{}] Sending reply", connid, msgid);

                        output_channel
                            .unbounded_send(Ok(PeerReply::Response(msgid, Ok(Some(reply)))))
                            .unwrap_or_else(|_| error!(
                                "session was already closed before reply could be sent"
                            ));
                });
            })
            .expect("unable to start catalog thread")
    };

    {
        let cat_writer = cat_writer.clone();

        handle.clone().spawn(sessions.for_each(move |session| {
            let connid = session.connid;

            info!("[{connid}] Incoming new session {:?}", session, connid=connid);

            // for outputting to writer
            let (out_writer, out_reader) = ::futures::sync::mpsc::unbounded();

            let (writer, reader) = session.connection.split();

            handle.spawn(
                out_reader
                    .map_err(|_| PeerError::Unknown)
                    .and_then(|reply| reply)
                    .forward(writer)
                    .map(|_| ())
                    .map_err(|error| error!("Session connection error {:?}", error))
            );

            let cat_writer = cat_writer.clone();

            handle.spawn(reader
                .for_each(move |msg| {
                    use self::PeerRequest::*;

                    match msg {
                        Request(msgid, Some(msg)) => {
                            debug!(
                                "[{connid}@{msgid}] Incoming Request",
                                connid=connid, msgid=msgid
                            );

                            let msg = (msg, connid, msgid, out_writer.clone());

                            cat_writer
                                .send(msg)
                                .unwrap_or_else(|_| error!("catalog thread was unavailable"));
                        }
                        Abort(msgid) => {
                            debug!(
                                "[{connid}@{msgid}] Abort Request",
                                connid=connid, msgid=msgid
                            );

                            out_writer
                                .unbounded_send(Ok(PeerReply::Response(msgid, Ok(None))))
                                .unwrap_or_else(|_| error!(
                                    "session was already closed before reply could be sent"
                                ));
                        }
                        KeepAlive => {
                            debug!(
                                "[{connid}] Keepalive Request",
                                connid=connid
                            );

                            out_writer
                                .unbounded_send(Ok(PeerReply::KeepAlive))
                                .unwrap_or_else(|_| error!(
                                    "session was already closed before reply could be sent"
                                ));
                        }
                        _ => {
                            out_writer
                                .unbounded_send(Err(PeerError::BadMessage))
                                .unwrap_or_else(|_| error!(
                                    "session was already closed before reply could be sent"
                                ));
                        }
                    };

                    Ok(())
                })
                .map(|_| ())
                .map_err(|error| error!("Session connection error {:?}", error)));

                Ok(())
        }));
    }

    core.run(server).unwrap();

    if let Err(err) = catalog_thread.join() {
        error!("Catalog thread was poisoned with {:?}", err);
    }
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
