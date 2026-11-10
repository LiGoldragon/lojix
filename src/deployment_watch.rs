//! The deployment watches a Nexus serves now, held in memory.
//!
//! A `WatchDeployments` registers one watcher under its subscription token
//! with its `DeploymentWatch` filter, before the `Watching` reply is written.
//! The connection serving it then claims the stream of events matching that
//! filter. When a deployment phase event is committed to the store, the store
//! announces it here and each matching watcher receives it, in commit order.
//!
//! A watch ends when either side closes: the connection that serves it drops
//! its stream when the peer goes, which removes the watcher; an `Unwatch` of a
//! live token removes the watcher, which ends its stream, and the connection
//! closes. Watches are connection state, never durable state: nothing here is
//! stored, and none survives a restart.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, MutexGuard};

use tokio::sync::mpsc;

use crate::runtime_model::{DeploymentPhaseEvent, DeploymentWatch};

/// The live deployment watchers, by subscription token.
#[derive(Clone, Default)]
pub struct DeploymentWatchers {
    watchers: Arc<Mutex<BTreeMap<u64, DeploymentWatcher>>>,
}

/// One live watcher: the filter it was opened with, where its matching events
/// go, and the receiving end until the connection serving it claims it.
struct DeploymentWatcher {
    deployment_watch: DeploymentWatch,
    events: mpsc::UnboundedSender<DeploymentPhaseEvent>,
    unclaimed: Option<mpsc::UnboundedReceiver<DeploymentPhaseEvent>>,
}

/// The events of one watch, held by the connection that serves it. Dropping
/// it ends the watch and removes its watcher.
pub struct DeploymentWatchStream {
    subscription_token: u64,
    events: mpsc::UnboundedReceiver<DeploymentPhaseEvent>,
    watchers: DeploymentWatchers,
}

/// Registering, ending and feeding the live deployment watchers.
pub trait WatchesDeployments {
    /// Register a watcher under `subscription_token` with its filter. Events
    /// committed from now on that match it are held for its stream.
    fn register(&self, subscription_token: u64, deployment_watch: DeploymentWatch);

    /// Hand the registered watcher's stream to the connection that serves
    /// it. `None` when no watcher is registered under the token or its stream
    /// was already claimed.
    fn claim(&self, subscription_token: u64) -> Option<DeploymentWatchStream>;

    /// End the watcher under `subscription_token`, closing its stream. True
    /// when a live watcher was ended.
    fn end(&self, subscription_token: u64) -> bool;

    /// Write a committed phase event to every watcher whose filter it
    /// matches. A watcher whose stream is gone is removed.
    fn announce(&self, event: &DeploymentPhaseEvent);

    /// How many watchers are live.
    fn live_count(&self) -> usize;
}

/// Whether a committed phase event is one a watcher asked for: every position
/// the filter names must equal the event's.
pub trait MatchesDeploymentEvent {
    fn matches(&self, event: &DeploymentPhaseEvent) -> bool;
}

/// Reading the next event of one watch.
pub trait StreamsDeploymentEvents {
    fn subscription_token(&self) -> u64;

    /// The next matching event, or `None` once the watch has been ended.
    fn next_event(
        &mut self,
    ) -> impl std::future::Future<Output = Option<DeploymentPhaseEvent>> + Send;
}

trait HoldsWatchers {
    fn held(&self) -> MutexGuard<'_, BTreeMap<u64, DeploymentWatcher>>;
}

impl HoldsWatchers for DeploymentWatchers {
    fn held(&self) -> MutexGuard<'_, BTreeMap<u64, DeploymentWatcher>> {
        // A panic while the map was held leaves it whole: every operation on
        // it is a single insert, remove or send.
        self.watchers
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

impl MatchesDeploymentEvent for DeploymentWatch {
    fn matches(&self, event: &DeploymentPhaseEvent) -> bool {
        self.optional_deployment_identifier
            .as_ref()
            .is_none_or(|identifier| identifier == &event.deployment_identifier)
            && self
                .optional_cluster_name
                .as_ref()
                .is_none_or(|cluster| cluster == &event.cluster_name)
            && self
                .optional_node_name
                .as_ref()
                .is_none_or(|node| node == &event.node_name)
    }
}

impl WatchesDeployments for DeploymentWatchers {
    fn register(&self, subscription_token: u64, deployment_watch: DeploymentWatch) {
        let (events, receiver) = mpsc::unbounded_channel();
        self.held().insert(
            subscription_token,
            DeploymentWatcher {
                deployment_watch,
                events,
                unclaimed: Some(receiver),
            },
        );
    }

    fn claim(&self, subscription_token: u64) -> Option<DeploymentWatchStream> {
        let events = self.held().get_mut(&subscription_token)?.unclaimed.take()?;
        Some(DeploymentWatchStream {
            subscription_token,
            events,
            watchers: self.clone(),
        })
    }

    fn end(&self, subscription_token: u64) -> bool {
        self.held().remove(&subscription_token).is_some()
    }

    fn announce(&self, event: &DeploymentPhaseEvent) {
        self.held().retain(|_, watcher| {
            !watcher.deployment_watch.matches(event) || watcher.events.send(event.clone()).is_ok()
        });
    }

    fn live_count(&self) -> usize {
        self.held().len()
    }
}

impl StreamsDeploymentEvents for DeploymentWatchStream {
    fn subscription_token(&self) -> u64 {
        self.subscription_token
    }

    fn next_event(
        &mut self,
    ) -> impl std::future::Future<Output = Option<DeploymentPhaseEvent>> + Send {
        self.events.recv()
    }
}

impl Drop for DeploymentWatchStream {
    fn drop(&mut self) {
        self.watchers.end(self.subscription_token);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Payload as _;
    use crate::runtime_model::{
        ClusterName, CommitSequence, DeploymentIdentifier, DeploymentPhase, EventLogPosition,
        GenerationIdentifier, NodeName, StateDigest, StateMarker,
    };

    fn event(deployment: u64, node: &str, phase: DeploymentPhase) -> DeploymentPhaseEvent {
        DeploymentPhaseEvent {
            deployment_identifier: DeploymentIdentifier::new(deployment),
            generation_identifier: GenerationIdentifier::new(1),
            cluster_name: ClusterName::from("alpha"),
            node_name: NodeName::from(node),
            deployment_phase: phase,
            event_log_position: EventLogPosition::new(deployment),
            state_marker: StateMarker {
                commit_sequence: CommitSequence::new(deployment),
                state_digest: StateDigest::new(deployment),
            },
            optional_immutable_revision: None,
            optional_deployment_terminal: None,
        }
    }

    fn watch(node: Option<&str>) -> DeploymentWatch {
        DeploymentWatch {
            optional_deployment_identifier: None,
            optional_cluster_name: Some(ClusterName::from("alpha")),
            optional_node_name: node.map(NodeName::from),
        }
    }

    #[tokio::test]
    async fn a_watcher_receives_only_the_events_its_filter_matches() {
        let watchers = DeploymentWatchers::default();
        watchers.register(1, watch(Some("beacon")));
        let mut stream = watchers.claim(1).expect("a registered watcher is claimed");
        watchers.announce(&event(7, "atlas", DeploymentPhase::Building));
        watchers.announce(&event(8, "beacon", DeploymentPhase::Building));
        watchers.announce(&event(8, "beacon", DeploymentPhase::Completed));
        assert_eq!(
            stream.next_event().await,
            Some(event(8, "beacon", DeploymentPhase::Building))
        );
        assert_eq!(
            stream.next_event().await,
            Some(event(8, "beacon", DeploymentPhase::Completed))
        );
    }

    #[tokio::test]
    async fn ending_a_watcher_ends_its_stream_and_a_dropped_stream_removes_its_watcher() {
        let watchers = DeploymentWatchers::default();
        watchers.register(1, watch(None));
        watchers.register(2, watch(None));
        let mut ended = watchers.claim(1).expect("first watcher");
        let dropped = watchers.claim(2).expect("second watcher");
        assert!(watchers.claim(2).is_none(), "a stream is claimed once");
        assert_eq!(watchers.live_count(), 2);

        assert!(watchers.end(1));
        assert_eq!(ended.next_event().await, None);
        assert!(!watchers.end(1), "an ended watcher is no longer live");

        drop(dropped);
        assert_eq!(watchers.live_count(), 0);
    }
}
