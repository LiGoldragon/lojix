//! Smoke tests for the lojix crate shape.
//!
//! These are the minimum witnesses that the crate compiles and that
//! the substrate dependencies (signal-frame, signal-lojix, sema-engine,
//! nota-codec) link in cleanly. Socket and actor witnesses live in
//! `tests/socket.rs`; subsequent test files cover durable state and the
//! deploy pipeline as those land.

#[test]
fn crate_compiles_and_names_itself() {
    assert_eq!(env!("CARGO_PKG_NAME"), "lojix");
}

#[test]
fn wire_vocabulary_is_reachable_via_re_export() {
    // `lojix::wire` re-exports `signal_lojix`; the typed Request enum
    // is what the daemon's socket loop and the CLI both speak.
    let _ = std::any::type_name::<lojix::wire::Operation>();
}

#[test]
fn signal_frame_is_on_the_dep_path() {
    // The wire kernel comes via signal-frame; signal-lojix's
    // Request/Reply enums build on signal_channel! from this crate.
    let _ = std::any::type_name::<
        signal_frame::StreamingFrame<
            lojix::wire::Operation,
            lojix::wire::LojixReply,
            lojix::wire::LojixEvent,
        >,
    >();
}

#[test]
fn sema_engine_is_on_the_dep_path() {
    // The daemon's durable state lives in a sema-engine Engine.
    let _ = std::any::type_name::<sema_engine::Engine>();
}

#[test]
fn deployment_request_round_trips_via_wire_vocabulary() {
    use lojix::wire::{
        BuildLocally, BuilderSelection, ClusterName, DeploymentPlan, DeploymentRequest,
        FlakeReference, FullOsDeployment, NodeName, Operation, ProposalSource, SystemAction,
    };

    let submission = DeploymentRequest {
        cluster: ClusterName::try_from("goldragon").unwrap(),
        node: NodeName::try_from("prometheus").unwrap(),
        source: ProposalSource::try_from("github:LiGoldragon/goldragon/horizon-leaner-shape")
            .unwrap(),
        flake: FlakeReference::try_from("github:LiGoldragon/CriomOS/horizon-leaner-shape").unwrap(),
        plan: DeploymentPlan::FullOsDeployment(FullOsDeployment {
            action: SystemAction::Switch,
        }),
        builder: BuilderSelection::BuildLocally(BuildLocally {}),
        substituters: Vec::new(),
    };
    let request = Operation::Deploy(submission);

    // The library compiles with the wire vocabulary it will speak;
    // round-trip semantics live in signal-lojix's own tests.
    assert!(matches!(request, Operation::Deploy(_)));
}
