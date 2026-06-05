//! lojix deploy orchestrator runtime.
//!
//! The daemon owns the live generation set, the GC-roots retention tree, the
//! append-only event log, and the container-lifecycle mirror. It drives the
//! generated Nexus runner: each wire frame becomes a `NexusWork::SignalArrived`
//! tagged by listener role, the engine decides the next step, and the deploy
//! pipeline runs as a chain of effect continuations. The CLI is only a
//! text-to-Signal adapter for this daemon.
//!
//! In-memory `Mutex`-backed state backs the `SemaEngine` for this build,
//! mirroring the `cloud` `Store` shape; sema-engine / redb persistence is a
//! noted follow-on.

extern crate self as lojix;

use std::sync::{Mutex, MutexGuard};

use nota_codec::NotaRecord;
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};

use crate::schema::sema::{ContainerLifecycleTable, EventLogTable, GcRootsTable, LiveSetTable};

pub mod client;
pub mod daemon;
pub mod schema;
pub mod schema_runtime;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    #[error("NOTA decode error: {0}")]
    Nota(#[from] nota_codec::Error),

    #[error("configuration decode error: {0}")]
    Configuration(#[from] nota_config::Error),

    #[error("component argument error: {0}")]
    Argument(#[from] triad_runtime::ArgumentError),

    #[error("signal frame error: {0}")]
    SignalFrame(#[from] triad_runtime::FrameError),

    #[error("ordinary signal frame error: {0}")]
    OrdinaryFrame(signal_lojix::schema::lib::SignalFrameError),

    #[error("meta signal frame error: {0}")]
    MetaFrame(meta_signal_lojix::schema::lib::SignalFrameError),

    #[error("expected exactly one argument")]
    ExpectedSingleArgument,

    #[error("flag-style arguments are not part of component binaries: {0}")]
    FlagArgument(String),

    #[error("unexpected signal frame for this socket")]
    UnexpectedFrame,

    #[error("connection closed before a complete frame arrived")]
    ConnectionClosed,

    #[error("signal request was rejected before execution")]
    SignalRequestRejected,

    #[error("lojix state mutex was poisoned")]
    StorePoisoned,

    #[error("deploy effect failed at stage {stage}: {detail}")]
    EffectFailed { stage: String, detail: String },
}

impl From<signal_lojix::schema::lib::SignalFrameError> for Error {
    fn from(error: signal_lojix::schema::lib::SignalFrameError) -> Self {
        Self::OrdinaryFrame(error)
    }
}

impl From<meta_signal_lojix::schema::lib::SignalFrameError> for Error {
    fn from(error: meta_signal_lojix::schema::lib::SignalFrameError) -> Self {
        Self::MetaFrame(error)
    }
}

pub type Result<T> = std::result::Result<T, Error>;

/// Daemon configuration: the two authority-tiered socket paths and their unix
/// permission modes. Decoded from the single NOTA argument the daemon binary
/// receives (`nota_config::ConfigurationSource`). Mirrors the `cloud`
/// `DaemonConfiguration` shape.
#[derive(Archive, RkyvSerialize, RkyvDeserialize, NotaRecord, Debug, Clone, PartialEq, Eq)]
pub struct DaemonConfiguration {
    pub ordinary_socket_path: String,
    pub ordinary_socket_mode: u32,
    pub owner_socket_path: String,
    pub owner_socket_mode: u32,
}

nota_config::impl_rkyv_configuration!(DaemonConfiguration);

/// The four SEMA tables plus the monotonic sequence counters, held under one
/// lock so a single write commits atomically across the tables. The
/// `SemaEngine` impl on `SchemaRuntime` reads and writes through this state.
#[derive(Debug)]
pub struct StoreState {
    pub live_set: LiveSetTable,
    pub gc_roots: GcRootsTable,
    pub event_log: EventLogTable,
    pub containers: ContainerLifecycleTable,
    pub commit_sequence: u64,
    pub deployment_sequence: u64,
    pub generation_sequence: u64,
    pub subscription_sequence: u64,
}

impl Default for StoreState {
    fn default() -> Self {
        Self {
            live_set: LiveSetTable(Vec::new()),
            gc_roots: GcRootsTable(Vec::new()),
            event_log: EventLogTable(Vec::new()),
            containers: ContainerLifecycleTable(Vec::new()),
            commit_sequence: 0,
            deployment_sequence: 0,
            generation_sequence: 0,
            subscription_sequence: 0,
        }
    }
}

impl StoreState {
    /// Advance the commit sequence and return the new value. The state digest
    /// is modeled as the commit sequence for this in-memory build.
    pub fn next_commit_sequence(&mut self) -> u64 {
        self.commit_sequence += 1;
        self.commit_sequence
    }

    pub fn next_deployment_identifier(&mut self) -> u64 {
        self.deployment_sequence += 1;
        self.deployment_sequence
    }

    pub fn next_generation_identifier(&mut self) -> u64 {
        self.generation_sequence += 1;
        self.generation_sequence
    }

    pub fn next_event_log_position(&self) -> u64 {
        self.event_log.0.len() as u64
    }

    pub fn next_subscription_token(&mut self) -> u64 {
        self.subscription_sequence += 1;
        self.subscription_sequence
    }
}

/// Durable lojix daemon state plane: the four tables the `SemaEngine` reads and
/// writes, behind one `Mutex`. In-memory for this build, matching the `cloud`
/// `Store` shape; the sema-engine / redb durable backing is the noted
/// follow-on.
#[derive(Debug, Default)]
pub struct Store {
    state: Mutex<StoreState>,
}

impl Store {
    pub fn new() -> Self {
        Self::default()
    }

    /// Lock the durable state. Returns `StorePoisoned` if a prior holder
    /// panicked while the lock was held.
    pub fn lock(&self) -> Result<MutexGuard<'_, StoreState>> {
        self.state.lock().map_err(|_| Error::StorePoisoned)
    }

    pub fn commit_sequence(&self) -> Result<u64> {
        Ok(self.lock()?.commit_sequence)
    }
}
