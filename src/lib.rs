//! `lojix-next` runtime.
//!
//! Schema-deep lojix pilot: the public wire types AND every internal
//! actor mailbox / SEMA command / SEMA response come from
//! `schema/lojix.schema` through `schema-next` and `schema-rust-next`.
//! Hand-written code in `runtime/` attaches behavior (methods) to the
//! schema-emitted nouns. The actor topology is Kameo 0.20.
//!
//! Iteration 2 (per `reports/system-designer/37/...`):
//! - the `OperationDispatcher` of iteration 1 became `NexusMailKeeper`,
//!   the mail keeper noun named by psyche records 963-970.
//! - the in-memory `Store` is now backed by `sema-engine` for durable
//!   redb persistence; daemon restart survives.
//! - every `Output` reply variant carries a schema-emitted
//!   `DatabaseMarker` (commit-counter + state-hash) that Nexus
//!   stamps from sema-engine's transaction state.
//! - `Communicate` trait abstracts the wire surface (concrete impl:
//!   `UnixSocketCommunicate`).

#![forbid(unsafe_code)]

pub mod error;
pub mod runtime;

pub mod generated {
    include!(concat!(env!("OUT_DIR"), "/lojix_next_generated.rs"));
}

pub use error::{Error, Result};
pub use generated::{
    AcceptedReply, ActivationCommand, ActivationKind, ActivationRecord, ActorReply, ActorRequest,
    BuildCommand, BuildLog, BuildRecord, ClosurePath, CopyCommand, CopyRecord, CriomeAuthorization,
    DaemonConfiguration, DatabaseMarker, DeploymentIdentifier, DeploymentRequest, Detail,
    GcRootDirectory, GenerationIdentifier, GenerationRecord, GenerationSelector, HelpAnswerReply,
    HelpQuery, HelpReply, HorizonView, Input, InputRoute, MailLifecycle, MessageIdentifier,
    MessageProcessed, MessageProcessedHook, MessageSent, MessageSentHook, NotaDecodeError,
    ObservationIdentifier, ObservationRecord, ObservedReply, Output, OutputRoute, Phase,
    PlanRecord, RejectedReply, RejectionReason, SemaCommand, SemaCommandIdentifier,
    SemaDatabasePath, SemaResponse, SignalFrameError, SnapshotReply, SocketPath, StateDirectory,
    StateHash, Status, TargetNode, Toolchain, TransactionCounter,
};
pub use runtime::{
    Activator, AuthorizationGate, AuthorizationPolicy, Builder, ClosureCopier, Communicate, Engine,
    GcRootPinner, LojixRoot, NexusActorRefs, NexusHooks, NexusMailKeeper, ObservationFan,
    ProcessToolchain, RunDaemon, SocketListener, Store, TraceLog, TraceWitness,
    UnixSocketCommunicate,
};
