//! Database marker tests (iteration 2, tests #5 and #6 of 6).
//!
//! - `lojix_next_database_marker_in_every_reply` — every Output
//!   variant carries a DatabaseMarker; the transaction-counter
//!   monotonically increases across operations.
//! - `lojix_next_database_marker_state_hash_changes_on_write` —
//!   write operations change the StateHash; reads leave it stable.

use lojix_next::runtime::authorization::AuthorizationPolicy;
use lojix_next::runtime::engine::Engine;
use lojix_next::runtime::toolchain::ToolchainMode;
use lojix_next::{
    CriomeAuthorization, DeploymentRequest, Detail, GenerationSelector, HelpQuery, HorizonView,
    Input, TargetNode, Toolchain,
};

#[tokio::test(flavor = "current_thread")]
async fn lojix_next_database_marker_in_every_reply() {
    let temp = tempfile::tempdir().expect("temp dir");
    let database_path = temp.path().join("sema.redb");
    let engine = Engine::spawn(
        database_path,
        Toolchain::sandbox_default(),
        ToolchainMode::Sandbox,
        AuthorizationPolicy::AllowAll,
    )
    .await
    .expect("engine spawn");

    // Help reply: a forward-only Output that still carries a marker.
    let help_output = engine
        .handle(Input::Help(HelpQuery(Detail("everything".to_owned()))))
        .await
        .expect("help");
    let help_marker = help_output.database_marker().clone();
    let help_counter = help_marker.transaction_counter.0;

    // Submit reply: state-involving Output that carries an updated marker.
    let submit_output = engine
        .handle(Input::Submit(DeploymentRequest {
            horizon_view: HorizonView("horizon: marker".to_owned()),
            target_node: TargetNode("nspawn-dune".to_owned()),
            criome_authorization: CriomeAuthorization::OperatorAllowlist,
        }))
        .await
        .expect("submit");
    let submit_marker = submit_output.database_marker().clone();
    let submit_counter = submit_marker.transaction_counter.0;
    assert!(
        submit_counter > help_counter,
        "Submit must advance counter past help: {submit_counter} > {help_counter}"
    );

    // Query reply: also stamped, reading the latest state.
    let query_output = engine
        .handle(Input::Query(GenerationSelector(
            lojix_next::DeploymentIdentifier(1),
        )))
        .await
        .expect("query");
    let query_marker = query_output.database_marker().clone();
    assert!(
        query_marker.transaction_counter.0 >= submit_counter,
        "Query counter must be >= submit counter: {} >= {submit_counter}",
        query_marker.transaction_counter.0
    );
}

#[tokio::test(flavor = "current_thread")]
async fn lojix_next_database_marker_state_hash_changes_on_write() {
    let temp = tempfile::tempdir().expect("temp dir");
    let database_path = temp.path().join("sema.redb");
    let engine = Engine::spawn(
        database_path,
        Toolchain::sandbox_default(),
        ToolchainMode::Sandbox,
        AuthorizationPolicy::AllowAll,
    )
    .await
    .expect("engine spawn");

    // Two consecutive Help calls should yield the same state-hash
    // because no writes happened between them.
    let help1 = engine
        .handle(Input::Help(HelpQuery(Detail("first".to_owned()))))
        .await
        .expect("help 1");
    let help2 = engine
        .handle(Input::Help(HelpQuery(Detail("second".to_owned()))))
        .await
        .expect("help 2");
    assert_eq!(
        help1.database_marker().state_hash,
        help2.database_marker().state_hash,
        "two consecutive reads must yield the same state hash"
    );

    // A Submit (which is a write) should change the state hash.
    let _ = engine
        .handle(Input::Submit(DeploymentRequest {
            horizon_view: HorizonView("horizon: write-hash".to_owned()),
            target_node: TargetNode("nspawn-dune".to_owned()),
            criome_authorization: CriomeAuthorization::OperatorAllowlist,
        }))
        .await
        .expect("submit");

    let help3 = engine
        .handle(Input::Help(HelpQuery(Detail("third".to_owned()))))
        .await
        .expect("help 3");
    assert_ne!(
        help2.database_marker().state_hash,
        help3.database_marker().state_hash,
        "state hash must change across a write"
    );
}
