//! ObservationFan — fans out observation events to subscribers.
//!
//! State carries a typed list of subscriber handles. Per
//! `skills/push-not-pull.md`: subscribers don't poll — the fan
//! pushes observations as they arrive.

use kameo::Actor;
use kameo::actor::ActorRef;
use kameo::error::Infallible;
use kameo::message::{Context, Message};
use tokio::sync::mpsc::UnboundedSender;

use crate::generated::ObservationRecord;

/// Subscriber set carried by the ObservationFan. Each subscriber is
/// an mpsc sender that the fan pushes ObservationRecord values into.
#[derive(Default)]
pub struct SubscriberSet {
    handles: Vec<UnboundedSender<ObservationRecord>>,
}

impl SubscriberSet {
    pub fn len(&self) -> usize {
        self.handles.len()
    }

    pub fn is_empty(&self) -> bool {
        self.handles.is_empty()
    }

    pub fn push(&mut self, handle: UnboundedSender<ObservationRecord>) {
        self.handles.push(handle);
    }

    pub fn broadcast(&mut self, record: ObservationRecord) {
        self.handles
            .retain(|handle| handle.send(record.clone()).is_ok());
    }
}

pub struct ObservationFan {
    subscribers: SubscriberSet,
    most_recent: Option<ObservationRecord>,
}

impl Default for ObservationFan {
    fn default() -> Self {
        Self::new()
    }
}

impl ObservationFan {
    pub fn new() -> Self {
        Self {
            subscribers: SubscriberSet::default(),
            most_recent: None,
        }
    }

    pub fn subscriber_count(&self) -> usize {
        self.subscribers.len()
    }

    pub fn most_recent(&self) -> Option<&ObservationRecord> {
        self.most_recent.as_ref()
    }
}

impl Actor for ObservationFan {
    type Args = Self;
    type Error = Infallible;

    async fn on_start(
        state: Self::Args,
        _actor_ref: ActorRef<Self>,
    ) -> std::result::Result<Self, Self::Error> {
        Ok(state)
    }
}

/// Register a subscriber. The fan pushes observation records to it.
pub struct Subscribe(pub UnboundedSender<ObservationRecord>);

impl Message<Subscribe> for ObservationFan {
    type Reply = Result<(), std::convert::Infallible>;

    async fn handle(
        &mut self,
        message: Subscribe,
        _context: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.subscribers.push(message.0);
        Ok(())
    }
}

/// Broadcast an observation to every subscriber.
pub struct BroadcastObservation(pub ObservationRecord);

impl Message<BroadcastObservation> for ObservationFan {
    type Reply = Result<(), std::convert::Infallible>;

    async fn handle(
        &mut self,
        message: BroadcastObservation,
        _context: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.most_recent = Some(message.0.clone());
        self.subscribers.broadcast(message.0);
        Ok(())
    }
}
