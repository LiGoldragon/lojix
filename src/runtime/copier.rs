//! ClosureCopier — owns the closure-copy plane.
//!
//! State carries the toolchain plus a typed queue of pending copies.

use kameo::Actor;
use kameo::actor::ActorRef;
use kameo::error::Infallible;
use kameo::message::{Context, Message};

use crate::error::Result;
use crate::generated::{BuildRecord, CopyRecord, TargetNode};
use crate::runtime::toolchain::ProcessToolchain;

#[derive(Clone, Debug, Default)]
pub struct CopyQueue {
    pending: Vec<BuildRecord>,
}

impl CopyQueue {
    pub fn pending_count(&self) -> usize {
        self.pending.len()
    }

    pub fn enqueue(&mut self, build: BuildRecord) {
        self.pending.push(build);
    }

    pub fn dequeue(&mut self) -> Option<BuildRecord> {
        if self.pending.is_empty() {
            None
        } else {
            Some(self.pending.remove(0))
        }
    }
}

pub struct ClosureCopier {
    toolchain: ProcessToolchain,
    queue: CopyQueue,
}

impl ClosureCopier {
    pub fn new(toolchain: ProcessToolchain) -> Self {
        Self {
            toolchain,
            queue: CopyQueue::default(),
        }
    }

    pub fn toolchain(&self) -> &ProcessToolchain {
        &self.toolchain
    }

    pub fn queue(&self) -> &CopyQueue {
        &self.queue
    }
}

impl Actor for ClosureCopier {
    type Args = Self;
    type Error = Infallible;

    async fn on_start(
        state: Self::Args,
        _actor_ref: ActorRef<Self>,
    ) -> std::result::Result<Self, Self::Error> {
        Ok(state)
    }
}

pub struct DriveCopy {
    pub build: BuildRecord,
    pub target_node: TargetNode,
}

impl Message<DriveCopy> for ClosureCopier {
    type Reply = Result<CopyRecord>;

    async fn handle(
        &mut self,
        message: DriveCopy,
        _context: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.queue.enqueue(message.build.clone());
        let outcome = self
            .toolchain
            .execute_copy(&message.build, message.target_node)
            .await;
        let _ = self.queue.dequeue();
        outcome
    }
}
