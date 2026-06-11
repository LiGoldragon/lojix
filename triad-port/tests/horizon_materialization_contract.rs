//! Horizon materialization contract tests. These are source-level because the
//! generated Nexus catalog is the load-bearing artifact: production deploys
//! must name materialization as a real engine feature, not hide it behind an
//! inline Rust shortcut.

#[test]
fn nexus_schema_names_horizon_materialization_and_eval_overrides() {
    let schema = include_str!("../schema/nexus.schema");
    assert!(schema.contains("HorizonMaterializationCommand"));
    assert!(schema.contains("(MaterializeHorizon HorizonMaterializationCommand)"));
    assert!(schema.contains("NixEvalCommand"));
    assert!(schema.contains("overrides (Vec FlakeInputOverride)"));
}

#[test]
fn runtime_no_longer_rejects_absent_build_attribute_as_unsupported() {
    let runtime = include_str!("../src/schema_runtime.rs");
    assert!(!runtime.contains("deployment.build_attribute.is_some()"));
    assert!(runtime.contains("needs_horizon_materialization"));
    assert!(runtime.contains("EffectCommand::MaterializeHorizon"));
}

#[test]
fn os_only_materialization_matches_legacy_firmware_policy() {
    let runtime = include_str!("../src/schema_runtime.rs");
    assert!(runtime.contains(
        "nexus::MaterializationShape::OsOnly => Some(Self {\n                include_home: false,\n                include_all_firmware: false,\n            })"
    ));
}
