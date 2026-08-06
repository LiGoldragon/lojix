//! Horizon materialization runtime tests. The handwritten Nexus vocabulary is
//! load-bearing: production deploys name materialization as a real engine
//! effect instead of hiding it behind an inline shortcut.

use horizon_lib::proposal::{NodeService, NodeServiceKind};

#[test]
fn handwritten_nexus_names_horizon_materialization_and_eval_overrides() {
    let flow = include_str!("../src/runtime_flow.rs");
    assert!(flow.contains("pub struct HorizonMaterializationCommand"));
    assert!(flow.contains("MaterializeHorizon(HorizonMaterializationCommand)"));
    assert!(flow.contains("pub struct NixEvalCommand"));
    assert!(flow.contains("Vec<FlakeInputOverride>"));
}

#[test]
fn runtime_no_longer_rejects_absent_build_attribute_as_unsupported() {
    let runtime = include_str!("../src/schema_runtime.rs");
    assert!(!runtime.contains("deployment.build_attribute.is_some()"));
    assert!(runtime.contains("needs_horizon_materialization"));
    assert!(runtime.contains("EffectCommand::MaterializeHorizon"));
}

#[test]
fn deploy_projection_uses_local_and_graphical_agent_intercom_capabilities() {
    assert_eq!(
        NodeService::AgentIntercomLocal {}.kind(),
        NodeServiceKind::AgentIntercomLocal
    );
    assert_eq!(
        NodeService::AgentIntercomGraphical {}.kind(),
        NodeServiceKind::AgentIntercomGraphical
    );
}
