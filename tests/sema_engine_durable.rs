//! Sema-engine durability test (iteration 2, test #3 of 6).
//!
//! Asserts: write a `RecordPlan` SemaCommand, drop the engine,
//! reopen against the same database path, and verify the plan +
//! the assigned GenerationRecord are still there. The fixture
//! drives Submit through the Engine surface so the whole pipeline
//! (Nexus -> SEMA -> trace) participates.

use lojix_next::runtime::authorization::AuthorizationPolicy;
use lojix_next::runtime::engine::Engine;
use lojix_next::runtime::store::{GenerationsSnapshot, PlansSnapshot};
use lojix_next::runtime::toolchain::ToolchainMode;
use lojix_next::{
    CriomeAuthorization, DeploymentRequest, HorizonView, Input, TargetNode, Toolchain,
};

#[tokio::test(flavor = "current_thread")]
async fn lojix_next_sema_engine_durable_across_restart() {
    let temp = tempfile::tempdir().expect("temp dir");
    let database_path = temp.path().join("sema.redb");

    // First incarnation — submit a deployment so the pipeline lands
    // a plan + generation record into sema-engine.
    {
        let engine = Engine::spawn(
            database_path.clone(),
            Toolchain::sandbox_default(),
            ToolchainMode::Sandbox,
            AuthorizationPolicy::AllowAll,
        )
        .await
        .expect("first engine spawn");
        let _ = engine
            .handle(Input::Submit(DeploymentRequest {
                horizon_view: HorizonView("horizon: durable".to_owned()),
                target_node: TargetNode("nspawn-dune".to_owned()),
                criome_authorization: CriomeAuthorization::OperatorAllowlist,
            }))
            .await
            .expect("submit");
        let plans = engine
            .children()
            .store
            .ask(PlansSnapshot)
            .await
            .expect("plans snapshot");
        let generations = engine
            .children()
            .store
            .ask(GenerationsSnapshot)
            .await
            .expect("generations snapshot");
        assert_eq!(plans.len(), 1, "first incarnation has one plan");
        assert!(
            !generations.is_empty(),
            "first incarnation has at least one generation"
        );
        // Engine drops here; sema-engine closes the redb.
    }

    // Wait for the actors to drop so the redb lock is released. The
    // tokio runtime on the current_thread flavor cleans up actors
    // synchronously when Engine drops, but the underlying file handle
    // may need a beat — re-opening with `Engine::spawn` will fail
    // fast if it's still locked.
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    // Second incarnation — opens the same database file; the prior
    // plan + generations must reappear.
    let engine = Engine::spawn(
        database_path,
        Toolchain::sandbox_default(),
        ToolchainMode::Sandbox,
        AuthorizationPolicy::AllowAll,
    )
    .await
    .expect("second engine spawn");
    let plans = engine
        .children()
        .store
        .ask(PlansSnapshot)
        .await
        .expect("plans snapshot");
    let generations = engine
        .children()
        .store
        .ask(GenerationsSnapshot)
        .await
        .expect("generations snapshot");
    assert_eq!(
        plans.len(),
        1,
        "second incarnation reopens the same plan from sema-engine"
    );
    assert!(
        !generations.is_empty(),
        "second incarnation reopens the generation ledger"
    );
    assert_eq!(plans[0].horizon_view.0, "horizon: durable");
}
