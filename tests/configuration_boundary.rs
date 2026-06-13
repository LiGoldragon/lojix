use std::path::Path;

#[test]
fn production_code_has_no_socket_environment_control_plane() {
    for relative_path in [
        "src/bin/lojix.rs",
        "src/bin/lojix-daemon.rs",
        "src/client.rs",
        "src/socket.rs",
    ] {
        let source = read_source(relative_path);
        for forbidden in [
            "LOJIX_SOCKET_PATH",
            "std::env::var(",
            "std::env::var_os(",
            "from_environment",
        ] {
            assert!(
                !source.contains(forbidden),
                "{relative_path} must not use {forbidden} as a production control-plane path"
            );
        }
    }
}

#[test]
fn production_binaries_decode_typed_configuration_sources() {
    let cli_source = read_source("src/bin/lojix.rs");
    let daemon_source = read_source("src/bin/lojix-daemon.rs");

    assert!(cli_source.contains("ConfigurationSource::from_argv_nth(0)"));
    assert!(cli_source.contains("decode::<lojix::wire::LojixCliConfiguration>()"));
    assert!(daemon_source.contains("ConfigurationSource::from_argv()"));
    assert!(daemon_source.contains("decode::<lojix::wire::LojixDaemonConfiguration>()"));
}

#[test]
fn daemon_runtime_gets_horizon_source_from_typed_configuration() {
    let runtime_source = read_source("src/runtime.rs");

    assert!(runtime_source.contains("configuration.horizon_configuration_source.as_str()"));
    assert!(!runtime_source.contains("DEFAULT_HORIZON_CONFIGURATION_SOURCE"));
    assert!(!runtime_source.contains("/git/github.com/LiGoldragon/criomos-horizon-config"));
}

#[test]
fn cli_has_exactly_one_runtime_peer_the_daemon_socket() {
    for relative_path in ["src/bin/lojix.rs", "src/client.rs"] {
        let source = read_source(relative_path);
        for forbidden in [
            "horizon_lib",
            "horizon_nota_codec",
            "HorizonProposal",
            "NotaSource",
            "ClusterProposal",
            "Viewpoint",
            "ProcessToolchain",
            "ProcessInvocation",
            "tokio::process",
            "Command::new",
            "sema_engine",
            "RuntimeRoot",
            "SocketServer",
            "DeploymentActor",
            "BuildOnlyRequest",
            "std::fs::",
        ] {
            assert!(
                !source.contains(forbidden),
                "{relative_path} must not reach around lojix-daemon through {forbidden}"
            );
        }
    }

    let cli_source = read_source("src/bin/lojix.rs");
    let client_source = read_source("src/client.rs");

    assert!(cli_source.contains("Client::from_configuration"));
    assert!(client_source.contains("UnixStream::connect"));
    assert!(client_source.contains("StreamingFrameBody::Request"));
    assert!(client_source.contains("write_frame"));
    assert!(client_source.contains("read_frame"));
}

#[test]
fn daemon_deployment_path_owns_horizon_projection() {
    let deploy_source = read_source("src/deploy.rs");

    for required in [
        "ClusterProposal",
        "NotaSource",
        "Viewpoint",
        "ProposalSource",
        "NotaSource::new(&text).parse()",
        "proposal.project(&viewpoint)",
    ] {
        assert!(
            deploy_source.contains(required),
            "src/deploy.rs must own the daemon-side projection path using {required}"
        );
    }
}

fn read_source(relative_path: &str) -> String {
    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join(relative_path);
    std::fs::read_to_string(&path)
        .unwrap_or_else(|error| panic!("failed to read {}: {error}", path.display()))
}
