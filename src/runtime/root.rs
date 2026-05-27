//! LojixRoot — the runtime root actor.
//!
//! Per `skills/actor-systems.md` §"Runtime roots are actors":
//! the daemon's root is itself an actor. State carries the typed
//! child ActorRef set; lifecycle hooks own child spawning + shutdown.

use kameo::Actor;
use kameo::actor::ActorRef;
use kameo::error::Infallible;
use kameo::message::{Context, Message};

use crate::runtime::activator::Activator;
use crate::runtime::authorization::AuthorizationGate;
use crate::runtime::builder::Builder;
use crate::runtime::copier::ClosureCopier;
use crate::runtime::dispatcher::{Dispatch, OperationDispatcher};
use crate::runtime::gc_root::GcRootPinner;
use crate::runtime::observation::ObservationFan;
use crate::runtime::store::Store;
use crate::runtime::trace::TraceLog;

/// Typed child set the LojixRoot owns. This IS the noun — the root
/// actor's job is to keep these refs alive, route Dispatch through
/// the dispatcher, and tear them down on shutdown.
#[derive(Clone)]
pub struct LojixChildSet {
    pub authorization: ActorRef<AuthorizationGate>,
    pub builder: ActorRef<Builder>,
    pub copier: ActorRef<ClosureCopier>,
    pub activator: ActorRef<Activator>,
    pub gc_root: ActorRef<GcRootPinner>,
    pub store: ActorRef<Store>,
    pub fan: ActorRef<ObservationFan>,
    pub trace: ActorRef<TraceLog>,
    pub dispatcher: ActorRef<OperationDispatcher>,
}

impl LojixChildSet {
    pub fn count(&self) -> usize {
        9
    }
}

pub struct LojixRoot {
    children: LojixChildSet,
}

impl LojixRoot {
    pub fn new(children: LojixChildSet) -> Self {
        Self { children }
    }

    pub fn children(&self) -> &LojixChildSet {
        &self.children
    }

    pub fn dispatcher(&self) -> ActorRef<OperationDispatcher> {
        self.children.dispatcher.clone()
    }

    pub fn trace(&self) -> ActorRef<TraceLog> {
        self.children.trace.clone()
    }

    pub fn store(&self) -> ActorRef<Store> {
        self.children.store.clone()
    }

    pub fn observation_fan(&self) -> ActorRef<ObservationFan> {
        self.children.fan.clone()
    }
}

impl Actor for LojixRoot {
    type Args = Self;
    type Error = Infallible;

    async fn on_start(
        state: Self::Args,
        _actor_ref: ActorRef<Self>,
    ) -> std::result::Result<Self, Self::Error> {
        Ok(state)
    }
}

/// Dispatch an Input through the root. Returns the Output.
pub struct DispatchInput(pub crate::generated::Input);

impl Message<DispatchInput> for LojixRoot {
    type Reply = crate::error::Result<crate::generated::Output>;

    async fn handle(
        &mut self,
        message: DispatchInput,
        _context: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        let output = self
            .children
            .dispatcher
            .ask(Dispatch(message.0))
            .await
            .map_err(|error| crate::error::Error::ActorSend(format!("root dispatch: {error}")))?;
        Ok(output)
    }
}
