//! Engine-routing tests: drive the generated `NexusEngine::execute` runner
//! over `SchemaRuntime` for the non-IO paths (reads, subscription handshake,
//! and the GC-roots mutations). The deploy pipeline shells out to real `nix`,
//! so it is exercised only in a live environment, not here.

use lojix::schema::nexus::{self, NexusEngine};
use lojix::schema_runtime::SchemaRuntime;
use meta_signal_lojix::schema::lib as meta;
use signal_lojix::schema::lib as ordinary;

fn run(engine: &mut SchemaRuntime, input: nexus::SignalInput) -> nexus::SignalOutput {
    let work = nexus::NexusWork::SignalArrived(input).with_origin_route(nexus::OriginRoute(0));
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime");
    match runtime.block_on(async { engine.execute(work).await.into_root() }) {
        nexus::NexusAction::ReplyToSignal(output) => output,
        other => panic!("expected ReplyToSignal, got {other:?}"),
    }
}

fn ordinary_reply(output: nexus::SignalOutput) -> ordinary::Output {
    match output {
        nexus::SignalOutput::OrdinaryOutput(output) => output,
        nexus::SignalOutput::MetaOutput(output) => panic!("expected ordinary, got {output:?}"),
    }
}

fn meta_reply(output: nexus::SignalOutput) -> meta::Output {
    match output {
        nexus::SignalOutput::MetaOutput(output) => output,
        nexus::SignalOutput::OrdinaryOutput(output) => panic!("expected meta, got {output:?}"),
    }
}

#[test]
fn query_empty_live_set_returns_empty_listing() {
    let mut engine = SchemaRuntime::new();
    let input = nexus::SignalInput::OrdinaryInput(ordinary::Input::Query(
        ordinary::Selection::ByNode(ordinary::NodeSelector {
            cluster_name: "alpha".to_string(),
            node_name: "node-1".to_string(),
            kind: None,
        }),
    ));
    let output = ordinary_reply(run(&mut engine, input));
    match output {
        ordinary::Output::Queried(listing) => assert!(listing.generations.is_empty()),
        other => panic!("expected Queried, got {other:?}"),
    }
}

#[test]
fn watch_deployments_mints_subscription_token() {
    let mut engine = SchemaRuntime::new();
    let input = nexus::SignalInput::OrdinaryInput(ordinary::Input::WatchDeployments(
        ordinary::DeploymentWatch {
            deployment: None,
            cluster: None,
            node: None,
        },
    ));
    let output = ordinary_reply(run(&mut engine, input));
    match output {
        ordinary::Output::Watching(opened) => assert_eq!(opened.subscription_token, 1),
        other => panic!("expected Watching, got {other:?}"),
    }
}

#[test]
fn check_host_key_material_reports_no_mismatches() {
    let mut engine = SchemaRuntime::new();
    let input = nexus::SignalInput::OrdinaryInput(ordinary::Input::CheckHostKeyMaterial(
        ordinary::KeyMaterialQuery {
            cluster_name: "alpha".to_string(),
            node_name: "node-1".to_string(),
            source: "github:owner/repo".to_string(),
        },
    ));
    let output = ordinary_reply(run(&mut engine, input));
    match output {
        ordinary::Output::KeyMaterialChecked(report) => {
            assert_eq!(report.node_name, "node-1");
            assert!(report.mismatches.is_empty());
        }
        other => panic!("expected KeyMaterialChecked, got {other:?}"),
    }
}

#[test]
fn pin_unknown_generation_is_rejected() {
    let mut engine = SchemaRuntime::new();
    let input = nexus::SignalInput::MetaInput(meta::Input::Pin(meta::PinRequest {
        cluster_name: "alpha".to_string(),
        node_name: "node-1".to_string(),
        generation_identifier: 42,
        pin_label: "keep".to_string(),
    }));
    let output = meta_reply(run(&mut engine, input));
    match output {
        meta::Output::PinRejected(_) => {}
        other => panic!("expected PinRejected for unknown generation, got {other:?}"),
    }
}

#[test]
fn retire_unknown_generation_is_rejected() {
    let mut engine = SchemaRuntime::new();
    let input = nexus::SignalInput::MetaInput(meta::Input::Retire(meta::RetireRequest {
        cluster_name: "alpha".to_string(),
        node_name: "node-1".to_string(),
        generation_identifier: 7,
    }));
    let output = meta_reply(run(&mut engine, input));
    match output {
        meta::Output::RetireRejected(_) => {}
        other => panic!("expected RetireRejected for unknown generation, got {other:?}"),
    }
}

/// A System deploy submission with the given `build_attribute` and action.
fn system_deployment(
    build_attribute: Option<&str>,
    action: ordinary::SystemAction,
) -> meta::SystemDeployment {
    meta::SystemDeployment {
        cluster_name: "alpha".to_string(),
        node_name: "node-1".to_string(),
        deployment_kind: ordinary::DeploymentKind::OsOnly,
        source: "/dev/null".to_string(),
        flake: "github:owner/repo".to_string(),
        system_action: action,
        builder: None,
        substituters: Vec::new(),
        build_attribute: build_attribute.map(str::to_string),
    }
}

fn deploy_rejection_reason(output: nexus::SignalOutput) -> meta::DeployRejectionReason {
    match meta_reply(output) {
        meta::Output::DeployRejected(rejected) => rejected.deploy_rejection_reason,
        other => panic!("expected DeployRejected, got {other:?}"),
    }
}

// ---- M1 reject-guard (audit C1): the daemon must reject — never falsely
// accept — a deploy shape it does not yet implement, so the durable live-set
// never records a generation that was not actually deployed. These run without
// `nix` because the guard rejects before any effect.

#[test]
fn activating_deploy_is_rejected_until_activate_lands() {
    let mut engine = SchemaRuntime::new();
    let input = nexus::SignalInput::MetaInput(meta::Input::Deploy(meta::DeployRequest::System(
        system_deployment(Some("dune-nspawn-toplevel"), ordinary::SystemAction::Switch),
    )));
    assert_eq!(
        deploy_rejection_reason(run(&mut engine, input)),
        meta::DeployRejectionReason::UnsupportedDeployAction,
    );
}

#[test]
fn production_deploy_without_build_attribute_is_rejected() {
    // No `build_attribute` means the production `nixosConfigurations.target`
    // path, which needs horizon `--override-input` materialization (M3).
    let mut engine = SchemaRuntime::new();
    let input = nexus::SignalInput::MetaInput(meta::Input::Deploy(meta::DeployRequest::System(
        system_deployment(None, ordinary::SystemAction::Build),
    )));
    assert_eq!(
        deploy_rejection_reason(run(&mut engine, input)),
        meta::DeployRejectionReason::UnsupportedDeployAction,
    );
}

#[test]
fn home_deploy_is_rejected_until_materialization_lands() {
    let mut engine = SchemaRuntime::new();
    let input = nexus::SignalInput::MetaInput(meta::Input::Deploy(meta::DeployRequest::Home(
        meta::HomeDeployment {
            cluster_name: "alpha".to_string(),
            node_name: "node-1".to_string(),
            user_name: "li".to_string(),
            source: "/dev/null".to_string(),
            flake: "github:owner/repo".to_string(),
            home_mode: meta::HomeMode::Build,
            builder: None,
            substituters: Vec::new(),
        },
    )));
    assert_eq!(
        deploy_rejection_reason(run(&mut engine, input)),
        meta::DeployRejectionReason::UnsupportedDeployAction,
    );
}
