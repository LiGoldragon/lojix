//! Activator — owns the activation plane.
//!
//! State carries the toolchain plus the most recently activated
//! generation. This is the actor that talks to `nspawn-dune-on-prometheus`
//! (in production) or `echo` (in sandbox) to switch a generation.

use kameo::Actor;
use kameo::actor::ActorRef;
use kameo::error::Infallible;
use kameo::message::{Context, Message};

use crate::error::Result;
use crate::generated::{ActivationKind, ActivationRecord, CopyRecord};
use crate::runtime::toolchain::ProcessToolchain;

#[derive(Clone, Debug, Default)]
pub struct ActiveGeneration {
    record: Option<ActivationRecord>,
}

impl ActiveGeneration {
    pub fn set(&mut self, record: ActivationRecord) {
        self.record = Some(record);
    }

    pub fn current(&self) -> Option<&ActivationRecord> {
        self.record.as_ref()
    }
}

pub struct Activator {
    toolchain: ProcessToolchain,
    active: ActiveGeneration,
}

impl Activator {
    pub fn new(toolchain: ProcessToolchain) -> Self {
        Self {
            toolchain,
            active: ActiveGeneration::default(),
        }
    }

    pub fn toolchain(&self) -> &ProcessToolchain {
        &self.toolchain
    }

    pub fn active(&self) -> &ActiveGeneration {
        &self.active
    }
}

impl Actor for Activator {
    type Args = Self;
    type Error = Infallible;

    async fn on_start(
        state: Self::Args,
        _actor_ref: ActorRef<Self>,
    ) -> std::result::Result<Self, Self::Error> {
        Ok(state)
    }
}

pub struct DriveActivation {
    pub copy: CopyRecord,
    pub activation_kind: ActivationKind,
}

impl Message<DriveActivation> for Activator {
    type Reply = Result<ActivationRecord>;

    async fn handle(
        &mut self,
        message: DriveActivation,
        _context: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        let record = self
            .toolchain
            .execute_activation(&message.copy, message.activation_kind)
            .await?;
        self.active.set(record.clone());
        Ok(record)
    }
}
