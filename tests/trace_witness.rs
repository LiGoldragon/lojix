//! Trace witnesses — assert that a Submit drove the pipeline through
//! every named plane (Authorization, Plan, Build, GcPin, Copy,
//! Activation, Observation fan, Output).

use lojix_next::runtime::authorization::AuthorizationPolicy;
use lojix_next::runtime::engine::Engine;
use lojix_next::runtime::toolchain::ToolchainMode;
use lojix_next::runtime::trace::{Plane, Snapshot, TraceWitness};
use lojix_next::{
    CriomeAuthorization, DeploymentRequest, HorizonView, Input, StateDirectory, TargetNode,
    Toolchain,
};

#[tokio::test(flavor = "current_thread")]
async fn lojix_next_trace_witnesses_full_pipeline() {
    let state = tempfile::TempDir::new().expect("state tempdir");
    let engine = Engine::spawn(
        Toolchain::sandbox_default(),
        ToolchainMode::Sandbox,
        AuthorizationPolicy::AllowAll,
        StateDirectory(state.path().to_string_lossy().into_owned()),
    )
    .await;
    let _ = engine
        .handle(Input::Submit(DeploymentRequest {
            horizon_view: HorizonView("horizon: trace".to_owned()),
            target_node: TargetNode("nspawn-dune".to_owned()),
            criome_authorization: CriomeAuthorization::OperatorAllowlist,
        }))
        .await
        .expect("submit succeeds");

    let trace = engine
        .children()
        .trace
        .ask(Snapshot)
        .await
        .expect("snapshot trace");

    let planes: Vec<Plane> = trace.iter().map(TraceWitness::plane).collect();

    let required = [
        Plane::OperationDispatcher,
        Plane::AuthorizationGate,
        Plane::SourceStager,
        Plane::Builder,
        Plane::GcRootPinner,
        Plane::ClosureCopier,
        Plane::Activator,
        Plane::ObservationFan,
        Plane::Store,
    ];

    for plane in required {
        assert!(
            planes.contains(&plane),
            "pipeline trace missing plane {plane:?}; observed: {planes:?}"
        );
    }
}
