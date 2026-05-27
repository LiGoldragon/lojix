//! Source-staging witness — Submit must pass through SourceStager,
//! write a source artifact, and commit a SourceRecord through Store.

use lojix_next::runtime::authorization::AuthorizationPolicy;
use lojix_next::runtime::engine::Engine;
use lojix_next::runtime::store::SourcesSnapshot;
use lojix_next::runtime::toolchain::ToolchainMode;
use lojix_next::{
    CriomeAuthorization, DeploymentRequest, HorizonView, Input, Output, StateDirectory, TargetNode,
    Toolchain,
};

#[tokio::test(flavor = "current_thread")]
async fn lojix_next_submit_stages_sources_before_build() {
    let state = tempfile::TempDir::new().expect("state tempdir");
    let engine = Engine::spawn(
        Toolchain::sandbox_default(),
        ToolchainMode::Sandbox,
        AuthorizationPolicy::AllowAll,
        StateDirectory(state.path().to_string_lossy().into_owned()),
    )
    .await;

    let output = engine
        .handle(Input::Submit(DeploymentRequest {
            horizon_view: HorizonView("horizon: source".to_owned()),
            target_node: TargetNode("nspawn-dune".to_owned()),
            criome_authorization: CriomeAuthorization::OperatorAllowlist,
        }))
        .await
        .expect("submit succeeds");
    assert!(matches!(output, Output::Accepted(_)));

    let sources = engine
        .children()
        .store
        .ask(SourcesSnapshot)
        .await
        .expect("source snapshot");
    assert_eq!(sources.len(), 1);
    let source = sources.first().expect("one source record");
    assert_eq!(source.target_node.0, "nspawn-dune");

    let artifact = state
        .path()
        .join("sources")
        .join(format!("{}.source", source.source_digest.0));
    let artifact_text = std::fs::read_to_string(&artifact)
        .unwrap_or_else(|error| panic!("read {}: {error}", artifact.display()));
    assert!(artifact_text.contains("horizon: source"));
    assert!(artifact_text.contains("nspawn-dune"));
    assert!(artifact_text.contains(&source.source_digest.0));
}
