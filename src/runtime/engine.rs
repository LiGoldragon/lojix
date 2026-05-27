//! Engine — the in-process composition entry point.
//!
//! Spawns the full LojixRoot actor topology and exposes
//! `Engine::handle(Input) -> Output` for tests and the daemon.
//! State carries the LojixRoot ActorRef plus the spawned children;
//! the daemon binary keeps this Engine alive for the process
//! lifetime.

use kameo::actor::{ActorRef, Spawn};

use crate::error::{Error, Result};
use crate::generated::{Input, Output, StateDirectory, Toolchain};
use crate::runtime::activator::Activator;
use crate::runtime::authorization::{AuthorizationGate, AuthorizationPolicy};
use crate::runtime::builder::Builder;
use crate::runtime::copier::ClosureCopier;
use crate::runtime::dispatcher::{ActorReferenceSet, OperationDispatcher};
use crate::runtime::gc_root::GcRootPinner;
use crate::runtime::observation::ObservationFan;
use crate::runtime::root::{DispatchInput, LojixChildSet, LojixRoot};
use crate::runtime::source_stager::SourceStager;
use crate::runtime::store::Store;
use crate::runtime::toolchain::{ProcessToolchain, ToolchainMode};
use crate::runtime::trace::TraceLog;

pub struct Engine {
    root: ActorRef<LojixRoot>,
    children: LojixChildSet,
}

impl Engine {
    /// Spawn the full lojix-next topology and return the Engine handle.
    pub async fn spawn(
        toolchain: Toolchain,
        toolchain_mode: ToolchainMode,
        policy: AuthorizationPolicy,
        state_directory: StateDirectory,
    ) -> Self {
        let process_toolchain = ProcessToolchain::new(toolchain, toolchain_mode);

        let trace = TraceLog::spawn(TraceLog::new());
        let store = Store::spawn(Store::new());
        let authorization = AuthorizationGate::spawn(AuthorizationGate::new(policy));
        let source_stager = SourceStager::spawn(SourceStager::new(state_directory));
        let builder = Builder::spawn(Builder::new(process_toolchain.clone()));
        let copier = ClosureCopier::spawn(ClosureCopier::new(process_toolchain.clone()));
        let activator = Activator::spawn(Activator::new(process_toolchain.clone()));
        let gc_root = GcRootPinner::spawn(GcRootPinner::new(process_toolchain.clone()));
        let fan = ObservationFan::spawn(ObservationFan::new());

        let refs = ActorReferenceSet {
            authorization: authorization.clone(),
            source_stager: source_stager.clone(),
            builder: builder.clone(),
            copier: copier.clone(),
            activator: activator.clone(),
            gc_root: gc_root.clone(),
            store: store.clone(),
            fan: fan.clone(),
            trace: trace.clone(),
        };
        let dispatcher = OperationDispatcher::spawn(OperationDispatcher::new(refs));

        let children = LojixChildSet {
            authorization,
            source_stager,
            builder,
            copier,
            activator,
            gc_root,
            store,
            fan,
            trace,
            dispatcher,
        };

        let root = LojixRoot::spawn(LojixRoot::new(children.clone()));

        Self { root, children }
    }

    pub fn children(&self) -> &LojixChildSet {
        &self.children
    }

    pub fn root(&self) -> &ActorRef<LojixRoot> {
        &self.root
    }

    /// Drive a single Input through the topology and return the Output.
    pub async fn handle(&self, input: Input) -> Result<Output> {
        let output = self
            .root
            .ask(DispatchInput(input))
            .await
            .map_err(|error| Error::ActorSend(format!("engine.handle: {error}")))?;
        Ok(output)
    }
}
