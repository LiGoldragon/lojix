//! Builder — owns the build plane.
//!
//! Receives a typed SourceRecord, asks Store for a generation id,
//! invokes the ProcessToolchain build, returns a BuildRecord.
//! State carries the most recent in-flight source (Option) and the
//! toolchain handle.

use kameo::Actor;
use kameo::actor::ActorRef;
use kameo::error::Infallible;
use kameo::message::{Context, Message};

use crate::error::{Error, Result};
use crate::generated::{BuildRecord, SourceRecord};
use crate::runtime::toolchain::ProcessToolchain;

/// Builder actor. The State field is the noun: it holds the toolchain
/// reference and tracks whether a build is currently in flight.
pub struct Builder {
    toolchain: ProcessToolchain,
    in_flight: Option<SourceRecord>,
}

impl Builder {
    pub fn new(toolchain: ProcessToolchain) -> Self {
        Self {
            toolchain,
            in_flight: None,
        }
    }

    pub fn toolchain(&self) -> &ProcessToolchain {
        &self.toolchain
    }

    pub fn in_flight(&self) -> Option<&SourceRecord> {
        self.in_flight.as_ref()
    }
}

impl Actor for Builder {
    type Args = Self;
    type Error = Infallible;

    async fn on_start(
        state: Self::Args,
        _actor_ref: ActorRef<Self>,
    ) -> std::result::Result<Self, Self::Error> {
        Ok(state)
    }
}

/// Drive a build for the supplied plan + generation id, returning a BuildRecord.
pub struct DriveBuild {
    pub source: SourceRecord,
    pub generation: crate::generated::GenerationIdentifier,
}

impl Message<DriveBuild> for Builder {
    type Reply = Result<BuildRecord>;

    async fn handle(
        &mut self,
        message: DriveBuild,
        _context: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.in_flight = Some(message.source.clone());
        let outcome = self
            .toolchain
            .execute_build(&message.source, message.generation)
            .await;
        self.in_flight = None;
        outcome.map_err(|error| match error {
            Error::BuildFailed(detail) => Error::BuildFailed(detail),
            other => other,
        })
    }
}
