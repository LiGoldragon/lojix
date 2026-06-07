use std::fs;

#[test]
fn daemon_uses_actor_native_listener_runtime() {
    let daemon_source = fs::read_to_string("src/daemon.rs").expect("read daemon source");
    let schema_runtime_source =
        fs::read_to_string("src/schema_runtime.rs").expect("read schema runtime source");

    for forbidden in [
        "std::os::unix::net::UnixStream",
        "BoundedWorkers",
        "impl MultiListenerRuntime",
        ".dispatch(",
        "set_read_timeout",
        "spawn_blocking",
    ] {
        assert!(
            !daemon_source.contains(forbidden),
            "daemon source must not contain the old blocking listener marker `{forbidden}`",
        );
    }

    assert!(
        !schema_runtime_source.contains("std::process::Command"),
        "schema runtime must use Tokio process effects, not std::process::Command",
    );

    for required in [
        "ActorMultiListenerDaemon",
        "ActorMultiConnectionRuntime",
        "AcceptedConnection",
        "read_body_async",
        "write_body_async",
        "execute_request",
        "engine.execute(work).await",
    ] {
        assert!(
            daemon_source.contains(required),
            "daemon source must contain actor-native marker `{required}`",
        );
    }

    for required in [
        "tokio::process::Command",
        "async fn run_effect",
        "async fn apply_sema_write",
        "async fn observe_sema_read",
    ] {
        assert!(
            schema_runtime_source.contains(required),
            "schema runtime must contain async engine marker `{required}`",
        );
    }
}
