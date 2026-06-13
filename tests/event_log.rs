use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

use lojix::deploy::DeploymentLedger;
use lojix::wire::{
    ClusterName, DeploymentObservation, DeploymentPhase, DeploymentSubmitted, GenerationKind,
    GenerationQuery, GenerationState, NodeName, StorePath, WatchDeployments,
};

#[test]
fn deployment_ledger_observations_survive_reopen_through_sema_engine() {
    let state_directory = unique_temporary_directory("lojix-event-log").join("state");
    let cluster = ClusterName::from_text("goldragon").expect("cluster name");
    let node = NodeName::from_text("zeus").expect("node name");
    let other_node = NodeName::from_text("ouranos").expect("other node name");

    let deployment = {
        let ledger = DeploymentLedger::open(&state_directory).expect("open deployment ledger");
        let deployment = ledger
            .allocate_deployment()
            .expect("allocate deployment id");
        ledger
            .append_observation(
                cluster.clone(),
                node.clone(),
                DeploymentObservation {
                    phase: DeploymentPhase::DeploymentSubmitted(DeploymentSubmitted {
                        deployment: deployment.clone(),
                    }),
                },
            )
            .expect("append submitted observation");
        deployment
    };

    let reopened = DeploymentLedger::open(&state_directory).expect("reopen deployment ledger");
    let opened = reopened
        .open_deployment_observation_subscription(WatchDeployments {
            cluster: Some(cluster.clone()),
            node: Some(node.clone()),
            deployment: Some(deployment.clone()),
        })
        .expect("open deployment observation subscription");

    assert!(opened.token.value() > 0);
    assert_eq!(opened.observations.len(), 1);
    assert_eq!(
        opened.observations[0],
        DeploymentObservation {
            phase: DeploymentPhase::DeploymentSubmitted(DeploymentSubmitted {
                deployment: deployment.clone(),
            }),
        }
    );

    let filtered = reopened
        .snapshot_deployment_observations(&WatchDeployments {
            cluster: Some(cluster),
            node: Some(other_node),
            deployment: Some(deployment),
        })
        .expect("filter deployment observations");
    assert!(filtered.is_empty());
}

#[test]
fn deployment_ledger_generations_survive_reopen_through_sema_engine() {
    let state_directory = unique_temporary_directory("lojix-generation-ledger").join("state");
    let cluster = ClusterName::from_text("goldragon").expect("cluster name");
    let node = NodeName::from_text("zeus").expect("node name");
    let other_node = NodeName::from_text("ouranos").expect("other node name");
    let store_path =
        StorePath::from_text("/nix/store/00000000000000000000000000000000-built-system")
            .expect("store path");

    let generation = {
        let ledger = DeploymentLedger::open(&state_directory).expect("open deployment ledger");
        ledger
            .record_built_generation(
                cluster.clone(),
                node.clone(),
                GenerationKind::FullOs,
                store_path,
            )
            .expect("record built generation")
    };

    assert_eq!(generation.state, GenerationState::Built);

    let reopened = DeploymentLedger::open(&state_directory).expect("reopen deployment ledger");
    let listing = reopened
        .query_generations(&GenerationQuery {
            cluster: Some(cluster.clone()),
            node: Some(node.clone()),
            kind: Some(GenerationKind::FullOs),
        })
        .expect("query matching generation");
    assert_eq!(listing, vec![generation]);

    let filtered = reopened
        .query_generations(&GenerationQuery {
            cluster: Some(cluster),
            node: Some(other_node),
            kind: Some(GenerationKind::FullOs),
        })
        .expect("query filtered generation");
    assert!(filtered.is_empty());
}

#[test]
fn deployment_identifiers_do_not_reset_after_reopen() {
    let state_directory = unique_temporary_directory("lojix-event-log-identities").join("state");
    let first = {
        let ledger = DeploymentLedger::open(&state_directory).expect("open deployment ledger");
        ledger
            .allocate_deployment()
            .expect("allocate first deployment")
    };
    let second = {
        let ledger = DeploymentLedger::open(&state_directory).expect("reopen deployment ledger");
        ledger
            .allocate_deployment()
            .expect("allocate second deployment")
    };

    assert_ne!(first, second);
}

#[test]
fn deployment_observation_subscription_retraction_removes_durable_record() {
    let state_directory = unique_temporary_directory("lojix-event-log-subscriptions").join("state");
    let ledger = DeploymentLedger::open(&state_directory).expect("open deployment ledger");
    let opened = ledger
        .open_deployment_observation_subscription(WatchDeployments {
            cluster: None,
            node: None,
            deployment: None,
        })
        .expect("open deployment observation subscription");

    assert_eq!(
        ledger
            .deployment_observation_subscription_count()
            .expect("count subscriptions"),
        1
    );
    ledger
        .close_deployment_observation_subscription(&opened.token)
        .expect("close deployment observation subscription");
    assert_eq!(
        ledger
            .deployment_observation_subscription_count()
            .expect("count subscriptions after close"),
        0
    );
}

fn unique_temporary_directory(prefix: &str) -> PathBuf {
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time after epoch")
        .as_nanos();
    let path = std::env::temp_dir().join(format!("{prefix}-{}-{timestamp}", std::process::id()));
    std::fs::create_dir_all(&path).expect("create temporary directory");
    path
}
