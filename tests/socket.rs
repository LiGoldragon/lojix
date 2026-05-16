use std::time::{Duration, SystemTime, UNIX_EPOCH};

use kameo::actor::Spawn;
use lojix::deploy::{AppendDeploymentObservation, CountDeploymentObservationSubscriptions};
use signal_core::{
    ExchangeIdentifier, ExchangeLane, LaneSequence, Reply as CoreReply, RequestPayload,
    SessionEpoch, SubReply,
};
use tokio::net::UnixStream;

use lojix::wire::{
    BuildResult, BuilderSelection, CacheRetentionObservationSubscription, ClusterName,
    DeploymentBuilding, DeploymentBuilt, DeploymentFailed, DeploymentId, DeploymentObservation,
    DeploymentObservationSubscription, DeploymentObservationToken, DeploymentPhase,
    DeploymentSubmitted, DispatcherChoosesBuilder, Event, FailureText, GenerationKind,
    GenerationQuery, LojixFrame, LojixFrameBody, NodeName, RealizedStorePath, Request, StorePath,
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
async fn deployment_observation_subscription_receives_live_stream_sequence_and_closes() {
    let cluster = ClusterName::from_text("goldragon").expect("cluster name");
    let node = NodeName::from_text("zeus").expect("node name");
    let deployment = DeploymentId::from_text("deployment_live").expect("deployment id");
    let observations = deployment_observation_sequence(&deployment);

    let root_state = RuntimeRoot::new();
    let deployment_ledger = root_state.deployment_ledger().clone();
    let root = RuntimeRoot::spawn(root_state);
    let (client_stream, server_stream) = UnixStream::pair().expect("unix stream pair");
    let server = SocketServer::handle_stream(Connection::new(server_stream), root);

    let client = async move {
        let mut connection = Connection::new(client_stream);
        let exchange = lojix::socket::ExchangeIdentity::first_connector_exchange();
        let frame = LojixFrame::new(LojixFrameBody::Request {
            exchange: exchange.value(),
            request: Request::DeploymentObservationSubscription(
                DeploymentObservationSubscription {
                    cluster: Some(cluster.clone()),
                    node: Some(node.clone()),
                    deployment: Some(deployment.clone()),
                },
            )
            .into_request(),
        });
        connection.write_frame(&frame).await.expect("write request");
        let token = read_deployment_observation_opened(&mut connection, exchange.value()).await;

        for observation in &observations {
            deployment_ledger
                .ask(AppendDeploymentObservation {
                    cluster: cluster.clone(),
                    node: node.clone(),
                    observation: observation.clone(),
                })
                .await
                .expect("append deployment observation");

            let event_observation =
                read_deployment_observation_event(&mut connection, &token).await;
            assert_eq!(event_observation, *observation);
        }

        let close_exchange = ExchangeIdentifier::new(
            SessionEpoch::new(1),
            ExchangeLane::Connector,
            LaneSequence::new(1),
        );
        let close_frame = LojixFrame::new(LojixFrameBody::Request {
            exchange: close_exchange,
            request: Request::DeploymentObservationRetraction(token.clone()).into_request(),
        });
        connection
            .write_frame(&close_frame)
            .await
            .expect("write close request");
        read_deployment_observation_closed(&mut connection, close_exchange, token).await;
        let remaining_subscriptions = deployment_ledger
            .ask(CountDeploymentObservationSubscriptions)
            .await
            .expect("count deployment observation subscriptions");
        assert_eq!(remaining_subscriptions, 0);
    };

    let (server_result, ()) = tokio::join!(server, client);
    server_result.expect("server result");
}

#[tokio::test]
async fn deployment_observation_subscription_retracts_when_client_disconnects() {
    let root_state = RuntimeRoot::new();
    let deployment_ledger = root_state.deployment_ledger().clone();
    let root = RuntimeRoot::spawn(root_state);
    let (client_stream, server_stream) = UnixStream::pair().expect("unix stream pair");
    let server = SocketServer::handle_stream(Connection::new(server_stream), root);

    let client = async move {
        let mut connection = Connection::new(client_stream);
        let exchange = lojix::socket::ExchangeIdentity::first_connector_exchange();
        let frame = LojixFrame::new(LojixFrameBody::Request {
            exchange: exchange.value(),
            request: Request::DeploymentObservationSubscription(
                DeploymentObservationSubscription {
                    cluster: None,
                    node: None,
                    deployment: None,
                },
            )
            .into_request(),
        });
        connection.write_frame(&frame).await.expect("write request");
        read_deployment_observation_opened(&mut connection, exchange.value()).await
    };

    let (server_result, token) = tokio::join!(server, client);
    server_result.expect("server result");
    assert!(token.value() > 0);
    let remaining_subscriptions = deployment_ledger
        .ask(CountDeploymentObservationSubscriptions)
        .await
        .expect("count deployment observation subscriptions");
    assert_eq!(remaining_subscriptions, 0);
}

fn deployment_observation_sequence(deployment: &DeploymentId) -> Vec<DeploymentObservation> {
    vec![
        DeploymentObservation {
            phase: DeploymentPhase::DeploymentSubmitted(DeploymentSubmitted {
                deployment: deployment.clone(),
            }),
        },
        DeploymentObservation {
            phase: DeploymentPhase::DeploymentBuilding(DeploymentBuilding {
                deployment: deployment.clone(),
                builder: BuilderSelection::DispatcherChoosesBuilder(DispatcherChoosesBuilder {}),
            }),
        },
        DeploymentObservation {
            phase: DeploymentPhase::DeploymentBuilt(DeploymentBuilt {
                deployment: deployment.clone(),
                result: BuildResult::RealizedStorePath(RealizedStorePath {
                    store_path: StorePath::from_text(
                        "/nix/store/00000000000000000000000000000000-built-system",
                    )
                    .expect("store path"),
                }),
            }),
        },
        DeploymentObservation {
            phase: DeploymentPhase::DeploymentFailed(DeploymentFailed {
                deployment: deployment.clone(),
                reason: FailureText::from_text("forced failure after stream witness")
                    .expect("failure text"),
            }),
        },
    ]
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

async fn read_deployment_observation_opened(
    connection: &mut Connection<UnixStream>,
    exchange: ExchangeIdentifier,
) -> DeploymentObservationToken {
    let reply_frame = connection.read_frame().await.expect("read reply");
    match reply_frame.into_body() {
        LojixFrameBody::Reply {
            exchange: reply_exchange,
            reply,
        } => {
            assert_eq!(reply_exchange, exchange);
            match reply {
                CoreReply::Accepted { per_operation, .. } => match per_operation.into_head() {
                    SubReply::Ok {
                        payload: lojix::wire::Reply::DeploymentObservationSubscriptionOpened(opened),
                        ..
                    } => opened.token,
                    other => panic!("expected deployment observation open reply, got {other:?}"),
                },
                other => panic!("expected accepted reply, got {other:?}"),
            }
        }
        other => panic!("expected reply frame, got {other:?}"),
    }
}

async fn read_deployment_observation_event(
    connection: &mut Connection<UnixStream>,
    token: &DeploymentObservationToken,
) -> DeploymentObservation {
    let event_frame = connection.read_frame().await.expect("read stream event");
    match event_frame.into_body() {
        LojixFrameBody::SubscriptionEvent {
            event_identifier,
            token: inner_token,
            event: Event::DeploymentObservation(observation),
        } => {
            assert_eq!(event_identifier.lane, ExchangeLane::Acceptor);
            assert_eq!(inner_token.value(), token.value());
            observation
        }
        other => panic!("expected deployment observation stream event, got {other:?}"),
    }
}

async fn read_deployment_observation_closed(
    connection: &mut Connection<UnixStream>,
    exchange: ExchangeIdentifier,
    expected: DeploymentObservationToken,
) {
    let reply_frame = connection.read_frame().await.expect("read close reply");
    match reply_frame.into_body() {
        LojixFrameBody::Reply {
            exchange: reply_exchange,
            reply,
        } => {
            assert_eq!(reply_exchange, exchange);
            match reply {
                CoreReply::Accepted { per_operation, .. } => match per_operation.into_head() {
                    SubReply::Ok {
                        payload: lojix::wire::Reply::DeploymentObservationSubscriptionClosed(closed),
                        ..
                    } => assert_eq!(closed.token, expected),
                    other => panic!("expected deployment observation close reply, got {other:?}"),
                },
                other => panic!("expected accepted close reply, got {other:?}"),
            }
        }
        other => panic!("expected close reply frame, got {other:?}"),
    }
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
