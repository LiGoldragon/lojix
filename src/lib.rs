//! `lojix-next` runtime.
//!
//! Schema-deep lojix pilot: the public wire types AND every internal
//! actor mailbox / SEMA command / SEMA response come from
//! `schema/lojix.schema` through `schema-next` and `schema-rust-next`.
//! Hand-written code in `runtime/` attaches behavior (methods) to the
//! schema-emitted nouns. The actor topology is Kameo 0.20.

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
    HelpQuery, HelpReply, HorizonView, Input, InputRoute, NotaDecodeError, ObservationIdentifier,
    ObservationRecord, ObservedReply, Output, OutputRoute, Phase, PlanRecord, RejectedReply,
    RejectionReason, SemaCommand, SemaCommandIdentifier, SemaDatabasePath, SemaResponse,
    SignalFrameError, SnapshotReply, SocketPath, SourceDigest, SourceRecord, StateDirectory,
    StateHash, Status, TargetNode, Toolchain, TransactionCounter,
};
pub use runtime::{
    Activator, AuthorizationGate, AuthorizationPolicy, Builder, ClosureCopier, Engine,
    GcRootPinner, LojixRoot, ObservationFan, OperationDispatcher, ProcessToolchain, RunDaemon,
    SocketListener, SourceStager, Store, TraceLog, TraceWitness,
};
