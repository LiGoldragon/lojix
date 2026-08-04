use lojix::schema::sema::{self as ordinary, EventLogEntry, LoggedEvent};
use lojix::{EventLogRetention, Store};

fn event(position: u64) -> EventLogEntry {
    EventLogEntry {
        event_log_position: ordinary::EventLogPosition::new(position),
        logged_event: LoggedEvent::CacheRetention(ordinary::CacheRetentionTransitionEvent {
            generation_identifier: ordinary::GenerationIdentifier::new(position + 1),
            cluster_name: ordinary::ClusterName::new("alpha"),
            node_name: ordinary::NodeName::new("node"),
            cache_retention_transition: ordinary::CacheRetentionTransition::Pinned,
            generation_slot: ordinary::GenerationSlot::Pinned,
            optional_generation_slot: None,
            optional_pin_label: None,
            event_log_position: ordinary::EventLogPosition::new(position),
        }),
    }
}

fn deployment_event(position: u64) -> EventLogEntry {
    EventLogEntry {
        event_log_position: ordinary::EventLogPosition::new(position),
        logged_event: LoggedEvent::Deployment(ordinary::DeploymentPhaseEvent {
            deployment_identifier: ordinary::DeploymentIdentifier::new(position + 1),
            generation_identifier: ordinary::GenerationIdentifier::new(position + 1),
            cluster_name: ordinary::ClusterName::new("alpha"),
            node_name: ordinary::NodeName::new("node"),
            deployment_phase: ordinary::DeploymentPhase::Submitted,
            event_log_position: ordinary::EventLogPosition::new(position),
            state_marker: ordinary::StateMarker {
                commit_sequence: ordinary::CommitSequence::new(0),
                state_digest: ordinary::StateDigest::new(0),
            },
            optional_immutable_revision: None,
            optional_deployment_terminal: None,
        }),
    }
}

#[test]
fn event_retention_preserves_the_newest_query_window_and_monotonic_positions() {
    let directory = tempfile::tempdir().expect("temporary store");
    let store = Store::open(directory.path().join("lojix.sema")).expect("store opens");
    for position in 0..3 {
        store
            .append_event_log_entry(event(position))
            .expect("event appends");
    }

    assert_eq!(
        store
            .compact_event_history(EventLogRetention::new(1))
            .expect("event history compacts"),
        2
    );
    let retained = store
        .event_log_in_range(0, 3)
        .expect("retained events read");
    assert_eq!(retained, vec![event(2)]);
    assert!(
        store
            .next_event_log_position()
            .expect("position remains durable")
            >= 3,
        "the next position does not reuse compacted event keys"
    );
}

#[test]
fn deployment_events_cannot_bypass_the_transition_intent_protocol() {
    let directory = tempfile::tempdir().expect("temporary store");
    let store = Store::open(directory.path().join("lojix.sema")).expect("store opens");

    assert!(store.append_event_log_entry(deployment_event(0)).is_err());
    assert!(
        store
            .event_log_in_range(0, u64::MAX)
            .expect("event log remains empty")
            .is_empty()
    );
}
