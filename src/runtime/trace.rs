//! Trace witnesses for the deploy pipeline.
//!
//! Per `skills/actor-systems.md` §"Traces are required": every named
//! plane that runs emits a trace event. The trace IS the testable
//! claim that the pipeline ran through every plane.

use std::convert::Infallible as StdInfallible;

use kameo::Actor;
use kameo::actor::ActorRef;
use kameo::error::Infallible;
use kameo::message::{Context, Message};

/// A single trace event — names which plane fired and what data it
/// observed. The Plane enum names every actor in the topology, so a
/// test can assert the trace contains a specific path.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TraceWitness {
    PlaneStarted {
        plane: Plane,
    },
    InputReceived {
        plane: Plane,
    },
    AuthorizationDecided {
        plane: Plane,
        decision: AuthorizationDecision,
    },
    PlanMaterialized {
        plane: Plane,
    },
    BuildStarted {
        plane: Plane,
    },
    BuildComplete {
        plane: Plane,
    },
    ClosureCopied {
        plane: Plane,
    },
    ActivationComplete {
        plane: Plane,
    },
    GcPinned {
        plane: Plane,
    },
    ObservationFanned {
        plane: Plane,
    },
    SemaApplied {
        plane: Plane,
        command: SemaCommandTag,
    },
    OutputEmitted {
        plane: Plane,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Plane {
    SocketListener,
    OperationDispatcher,
    AuthorizationGate,
    Builder,
    ClosureCopier,
    Activator,
    GcRootPinner,
    ObservationFan,
    Store,
    LojixRoot,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AuthorizationDecision {
    Granted,
    Denied,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SemaCommandTag {
    RecordPlan,
    RecordBuild,
    RecordCopy,
    RecordActivation,
    RecordObservation,
    QueryGeneration,
}

/// Trace log actor. The `State` field carries the witnessed events
/// so far — the actor IS the log. Tests subscribe via `Snapshot`.
pub struct TraceLog {
    events: Vec<TraceWitness>,
}

impl Default for TraceLog {
    fn default() -> Self {
        Self::new()
    }
}

impl TraceLog {
    pub fn new() -> Self {
        Self { events: Vec::new() }
    }
}

impl Actor for TraceLog {
    type Args = Self;
    type Error = Infallible;

    async fn on_start(
        state: Self::Args,
        _actor_ref: ActorRef<Self>,
    ) -> std::result::Result<Self, Self::Error> {
        Ok(state)
    }
}

/// Record a single witness on the trace log.
pub struct RecordWitness(pub TraceWitness);

impl Message<RecordWitness> for TraceLog {
    type Reply = Result<(), StdInfallible>;

    async fn handle(
        &mut self,
        message: RecordWitness,
        _context: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.events.push(message.0);
        Ok(())
    }
}

/// Snapshot the current trace log; tests assert against the returned vector.
pub struct Snapshot;

impl Message<Snapshot> for TraceLog {
    type Reply = Result<Vec<TraceWitness>, StdInfallible>;

    async fn handle(
        &mut self,
        _message: Snapshot,
        _context: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        Ok(self.events.clone())
    }
}

impl TraceWitness {
    /// Return the plane that emitted this witness — used by tests to
    /// assert pipeline coverage.
    pub fn plane(&self) -> Plane {
        match self {
            Self::PlaneStarted { plane }
            | Self::InputReceived { plane }
            | Self::AuthorizationDecided { plane, .. }
            | Self::PlanMaterialized { plane }
            | Self::BuildStarted { plane }
            | Self::BuildComplete { plane }
            | Self::ClosureCopied { plane }
            | Self::ActivationComplete { plane }
            | Self::GcPinned { plane }
            | Self::ObservationFanned { plane }
            | Self::SemaApplied { plane, .. }
            | Self::OutputEmitted { plane } => *plane,
        }
    }
}
