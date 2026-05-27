//! Topology + no-ZST tests — per `skills/actor-systems.md`
//! §"Test actor density" and §"Zero-sized actors are not actors".

use std::mem::size_of;

use lojix_next::runtime::activator::Activator;
use lojix_next::runtime::authorization::{AuthorizationGate, AuthorizationPolicy};
use lojix_next::runtime::builder::Builder;
use lojix_next::runtime::copier::ClosureCopier;
use lojix_next::runtime::dispatcher::OperationDispatcher;
use lojix_next::runtime::engine::Engine;
use lojix_next::runtime::gc_root::GcRootPinner;
use lojix_next::runtime::observation::ObservationFan;
use lojix_next::runtime::root::LojixRoot;
use lojix_next::runtime::socket::SocketListener;
use lojix_next::runtime::source_stager::SourceStager;
use lojix_next::runtime::store::Store;
use lojix_next::runtime::toolchain::ToolchainMode;
use lojix_next::runtime::trace::TraceLog;
use lojix_next::{StateDirectory, Toolchain};

#[test]
fn lojix_next_no_zst_actors() {
    assert!(size_of::<LojixRoot>() > 0, "LojixRoot must carry data");
    assert!(size_of::<OperationDispatcher>() > 0);
    assert!(size_of::<AuthorizationGate>() > 0);
    assert!(size_of::<SourceStager>() > 0);
    assert!(size_of::<Builder>() > 0);
    assert!(size_of::<ClosureCopier>() > 0);
    assert!(size_of::<Activator>() > 0);
    assert!(size_of::<GcRootPinner>() > 0);
    assert!(size_of::<ObservationFan>() > 0);
    assert!(size_of::<Store>() > 0);
    assert!(size_of::<TraceLog>() > 0);
    assert!(size_of::<SocketListener>() > 0);
}

#[tokio::test(flavor = "current_thread")]
async fn lojix_next_actor_topology_includes_every_plane() {
    let state = tempfile::TempDir::new().expect("state tempdir");
    let engine = Engine::spawn(
        Toolchain::sandbox_default(),
        ToolchainMode::Sandbox,
        AuthorizationPolicy::AllowAll,
        StateDirectory(state.path().to_string_lossy().into_owned()),
    )
    .await;
    let children = engine.children();
    assert_eq!(children.count(), 10);

    // Each ref is alive after spawn.
    assert!(children.authorization.is_alive());
    assert!(children.source_stager.is_alive());
    assert!(children.builder.is_alive());
    assert!(children.copier.is_alive());
    assert!(children.activator.is_alive());
    assert!(children.gc_root.is_alive());
    assert!(children.fan.is_alive());
    assert!(children.store.is_alive());
    assert!(children.trace.is_alive());
    assert!(children.dispatcher.is_alive());
}
