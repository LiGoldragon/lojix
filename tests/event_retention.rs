use lojix::schema::sema::{EventLogEntry, LoggedEvent};
use lojix::{EventLogRetention, Store};
use signal_lojix::schema::lib as ordinary;

fn event(position: u64) -> EventLogEntry {
    EventLogEntry {
        event_log_position: ordinary::EventLogPosition::new(position),
        record: LoggedEvent::Deployment(ordinary::DeploymentPhaseEvent {
            deployment_identifier: ordinary::DeploymentIdentifier::new(position + 1),
            generation_identifier: ordinary::GenerationIdentifier::new(position + 1),
            cluster_name: ordinary::ClusterName::new("alpha"),
            node_name: ordinary::NodeName::new("node"),
            deployment_phase: ordinary::DeploymentPhase::Submitted,
            event_log_position: ordinary::EventLogPosition::new(position),
            detail: None,
            source_revision: None,
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
