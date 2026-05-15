use std::time::{Duration, SystemTime, UNIX_EPOCH};

use kameo::actor::Spawn;
use signal_core::{Reply as CoreReply, RequestPayload, SubReply};
use tokio::net::UnixStream;

use lojix::wire::{
    CacheRetentionObservationSubscription, GenerationKind, GenerationQuery, LojixFrame,
    LojixFrameBody, Request,
};
use lojix::{Client, Connection, RuntimeRoot, SocketAddress, SocketServer};

fn generation_query() -> Request {
    Request::GenerationQuery(GenerationQuery {
        cluster: None,
        node: None,
        kind: Some(GenerationKind::HomeOnly),
    })
}

#[tokio::test]
async fn socket_round_trip_routes_request_through_runtime_root() {
    let root = RuntimeRoot::spawn(RuntimeRoot::new());
    let (client_stream, server_stream) = UnixStream::pair().expect("unix stream pair");
    let server = SocketServer::handle_stream(Connection::new(server_stream), root);

    let client = async move {
        let mut connection = Connection::new(client_stream);
        let exchange = lojix::socket::ExchangeIdentity::first_connector_exchange();
        let frame = LojixFrame::new(LojixFrameBody::Request {
            exchange: exchange.value(),
            request: generation_query().into_request(),
        });
        connection.write_frame(&frame).await.expect("write request");
        let reply_frame = connection.read_frame().await.expect("read reply");
        match reply_frame.into_body() {
            LojixFrameBody::Reply {
                exchange: reply_exchange,
                reply,
            } => {
                assert_eq!(reply_exchange, exchange.value());
                match reply {
                    CoreReply::Accepted { per_operation, .. } => match per_operation.into_head() {
                        SubReply::Ok {
                            payload: lojix::wire::Reply::GenerationListing(listing),
                            ..
                        } => assert!(listing.generations.is_empty()),
                        other => panic!("expected generation listing, got {other:?}"),
                    },
                    other => panic!("expected accepted reply, got {other:?}"),
                }
            }
            other => panic!("expected reply frame, got {other:?}"),
        }
    };

    let (server_result, ()) = tokio::join!(server, client);
    server_result.expect("server result");
}

#[tokio::test]
async fn subscription_request_receives_stream_open_reply() {
    let root = RuntimeRoot::spawn(RuntimeRoot::new());
    let (client_stream, server_stream) = UnixStream::pair().expect("unix stream pair");
    let server = SocketServer::handle_stream(Connection::new(server_stream), root);

    let client = async move {
        let mut connection = Connection::new(client_stream);
        let exchange = lojix::socket::ExchangeIdentity::first_connector_exchange();
        let request =
            Request::CacheRetentionObservationSubscription(CacheRetentionObservationSubscription {
                generation: None,
            });
        let frame = LojixFrame::new(LojixFrameBody::Request {
            exchange: exchange.value(),
            request: request.into_request(),
        });
        connection.write_frame(&frame).await.expect("write request");
        let reply_frame = connection.read_frame().await.expect("read reply");
        match reply_frame.into_body() {
            LojixFrameBody::Reply { reply, .. } => match reply {
                CoreReply::Accepted { per_operation, .. } => match per_operation.into_head() {
                    SubReply::Ok {
                        payload:
                            lojix::wire::Reply::CacheRetentionObservationSubscriptionOpened(opened),
                        ..
                    } => assert_eq!(opened.token.value(), 1),
                    other => panic!("expected cache-retention stream-open reply, got {other:?}"),
                },
                other => panic!("expected accepted reply, got {other:?}"),
            },
            other => panic!("expected reply frame, got {other:?}"),
        }
    };

    let (server_result, ()) = tokio::join!(server, client);
    server_result.expect("server result");
}

#[tokio::test]
async fn stalled_connection_does_not_block_next_client() {
    let path = temporary_socket_path();
    let _ = std::fs::remove_file(&path);
    let server = SocketServer::new(SocketAddress::new(path.clone()));
    let server_task = tokio::spawn(async move { server.serve_forever().await });
    wait_until_socket_exists(&path).await;

    let stalled_stream = UnixStream::connect(&path)
        .await
        .expect("connect stalled stream");
    let client = Client::new(SocketAddress::new(path.clone()));
    let reply = tokio::time::timeout(Duration::from_secs(1), client.send(generation_query()))
        .await
        .expect("client was not blocked by stalled connection")
        .expect("client reply");

    match reply {
        lojix::wire::Reply::GenerationListing(listing) => assert!(listing.generations.is_empty()),
        other => panic!("expected generation listing, got {other:?}"),
    }

    drop(stalled_stream);
    server_task.abort();
    let _ = std::fs::remove_file(path);
}

fn temporary_socket_path() -> std::path::PathBuf {
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time")
        .as_nanos();
    std::env::temp_dir().join(format!("lojix-{}-{timestamp}.sock", std::process::id()))
}

async fn wait_until_socket_exists(path: &std::path::Path) {
    for _ in 0..100 {
        if path.exists() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("socket was not created at {}", path.display());
}
