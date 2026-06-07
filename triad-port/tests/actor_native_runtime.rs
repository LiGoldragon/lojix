use std::fs;

#[test]
fn daemon_uses_actor_native_listener_runtime() {
    let daemon_source = fs::read_to_string("src/daemon.rs").expect("read daemon source");

    for forbidden in [
        "std::os::unix::net::UnixStream",
        "BoundedWorkers",
        "impl MultiListenerRuntime",
        ".dispatch(",
        "set_read_timeout",
    ] {
        assert!(
            !daemon_source.contains(forbidden),
            "daemon source must not contain the old blocking listener marker `{forbidden}`",
        );
    }

    for required in [
        "ActorMultiListenerDaemon",
        "ActorMultiConnectionRuntime",
        "AcceptedConnection",
        "read_body_async",
        "write_body_async",
        "spawn_blocking",
    ] {
        assert!(
            daemon_source.contains(required),
            "daemon source must contain actor-native marker `{required}`",
        );
    }
}
