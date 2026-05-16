use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

use lojix::deploy::DeploymentEventLog;
use lojix::wire::{
    ClusterName, DeploymentObservation, DeploymentObservationSubscription, DeploymentPhase,
    DeploymentSubmitted, NodeName,
};

#[test]
fn deployment_event_log_survives_reopen_through_sema_engine() {
    let state_directory = unique_temporary_directory("lojix-event-log").join("state");
    let cluster = ClusterName::from_text("goldragon").expect("cluster name");
    let node = NodeName::from_text("zeus").expect("node name");
    let other_node = NodeName::from_text("ouranos").expect("other node name");

    let deployment = {
        let log = DeploymentEventLog::open(&state_directory).expect("open deployment event log");
        let deployment = log.allocate_deployment().expect("allocate deployment id");
        log.append_observation(
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

    let reopened = DeploymentEventLog::open(&state_directory).expect("reopen deployment event log");
    let opened = reopened
        .open_deployment_observation_subscription(DeploymentObservationSubscription {
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
        .snapshot_deployment_observations(&DeploymentObservationSubscription {
            cluster: Some(cluster),
            node: Some(other_node),
            deployment: Some(deployment),
        })
        .expect("filter deployment observations");
    assert!(filtered.is_empty());
}

#[test]
fn deployment_identifiers_do_not_reset_after_reopen() {
    let state_directory = unique_temporary_directory("lojix-event-log-identities").join("state");
    let first = {
        let log = DeploymentEventLog::open(&state_directory).expect("open deployment event log");
        log.allocate_deployment()
            .expect("allocate first deployment")
    };
    let second = {
        let log = DeploymentEventLog::open(&state_directory).expect("reopen deployment event log");
        log.allocate_deployment()
            .expect("allocate second deployment")
    };

    assert_ne!(first, second);
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
