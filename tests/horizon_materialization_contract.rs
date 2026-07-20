//! Horizon materialization contract tests. These are source-level because the
//! generated Nexus catalog is the load-bearing artifact: production deploys
//! must name materialization as a real engine feature, not hide it behind an
//! inline Rust shortcut.

use horizon_lib::proposal::{NodeService, NodeServiceKind};

#[test]
fn nexus_schema_names_horizon_materialization_and_eval_overrides() {
    let schema = include_str!("../schema/nexus.schema");
    assert!(schema.contains("HorizonMaterializationCommand"));
    assert!(schema.contains("(MaterializeHorizon HorizonMaterializationCommand)"));
    assert!(schema.contains("NixEvalCommand"));
    assert!(schema.contains("overrides.(Vector FlakeInputOverride)"));
}

#[test]
fn runtime_no_longer_rejects_absent_build_attribute_as_unsupported() {
    let runtime = include_str!("../src/schema_runtime.rs");
    assert!(!runtime.contains("deployment.build_attribute.is_some()"));
    assert!(runtime.contains("needs_horizon_materialization"));
    assert!(runtime.contains("EffectCommand::MaterializeHorizon"));
}

#[test]
fn deploy_projection_uses_the_agent_intercom_gateway_and_peer_schema() {
    assert_eq!(
        NodeService::AgentIntercomGateway {}.kind(),
        NodeServiceKind::AgentIntercomGateway
    );
    assert_eq!(
        NodeService::AgentIntercomPeer {}.kind(),
        NodeServiceKind::AgentIntercomPeer
    );
}
