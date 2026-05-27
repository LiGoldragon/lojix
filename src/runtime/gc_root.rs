//! GcRootPinner — owns the GC root pinning plane.
//!
//! State carries a typed set of pinned generations. In sandbox mode
//! the pin is in-memory only; in production the toolchain calls
//! `nix-store --add-root`.

use std::collections::BTreeSet;

use kameo::Actor;
use kameo::actor::ActorRef;
use kameo::error::Infallible;
use kameo::message::{Context, Message};

use crate::error::Result;
use crate::generated::{BuildRecord, GenerationIdentifier};
use crate::runtime::toolchain::ProcessToolchain;

#[derive(Clone, Debug, Default)]
pub struct PinnedSet {
    pinned: BTreeSet<u64>,
}

impl PinnedSet {
    pub fn contains(&self, identifier: &GenerationIdentifier) -> bool {
        self.pinned.contains(&identifier.0)
    }

    pub fn insert(&mut self, identifier: &GenerationIdentifier) -> bool {
        self.pinned.insert(identifier.0)
    }

    pub fn len(&self) -> usize {
        self.pinned.len()
    }

    pub fn is_empty(&self) -> bool {
        self.pinned.is_empty()
    }
}

pub struct GcRootPinner {
    toolchain: ProcessToolchain,
    pinned: PinnedSet,
}

impl GcRootPinner {
    pub fn new(toolchain: ProcessToolchain) -> Self {
        Self {
            toolchain,
            pinned: PinnedSet::default(),
        }
    }

    pub fn toolchain(&self) -> &ProcessToolchain {
        &self.toolchain
    }

    pub fn pinned(&self) -> &PinnedSet {
        &self.pinned
    }
}

impl Actor for GcRootPinner {
    type Args = Self;
    type Error = Infallible;

    async fn on_start(
        state: Self::Args,
        _actor_ref: ActorRef<Self>,
    ) -> std::result::Result<Self, Self::Error> {
        Ok(state)
    }
}

pub struct DrivePin(pub BuildRecord);

impl Message<DrivePin> for GcRootPinner {
    type Reply = Result<GenerationIdentifier>;

    async fn handle(
        &mut self,
        message: DrivePin,
        _context: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        let identifier = self.toolchain.execute_pin(&message.0).await?;
        self.pinned.insert(&identifier);
        Ok(identifier)
    }
}
