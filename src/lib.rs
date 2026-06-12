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
//! while the daemon socket shell is actor-native. Sema-engine / redb
//! persistence is a noted follow-on.

use std::path::Path;
use std::sync::{Mutex, MutexGuard};

use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};

use crate::schema::sema::{
    ContainerLifecycleRecord, ContainerLifecycleTable, EventLogEntry, EventLogTable, GcRoot,
    GcRootsTable, LiveGeneration, LiveSetTable,
};

pub mod client;
pub mod daemon;
pub mod schema;
pub mod schema_runtime;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    #[error("configuration archive decode error: {0}")]
    ConfigurationArchive(String),

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

    #[error("NOTA request decoding requires the nota-text feature")]
    NotaTextUnsupported,

    #[error("NOTA request did not decode: {0}")]
    NotaRequestText(String),

    #[error(
        "owner socket mode {0:#o} grants other-access; refusing to expose the privileged surface"
    )]
    InsecureOwnerSocketMode(u32),

    #[error(
        "owner socket peer uid/gid mismatch: peer {peer_user_id}:{peer_group_id}, daemon {daemon_user_id}:{daemon_group_id}"
    )]
    UnauthorizedOwnerPeer {
        peer_user_id: u32,
        peer_group_id: u32,
        daemon_user_id: u32,
        daemon_group_id: u32,
    },

    #[error("unexpected signal frame for this socket")]
    UnexpectedFrame,

    #[error("connection closed before a complete frame arrived")]
    ConnectionClosed,

    #[error("request frame read timed out")]
    RequestReadTimedOut,

    #[error("signal request was rejected before execution")]
    SignalRequestRejected,

    #[error("lojix state mutex was poisoned")]
    StorePoisoned,

    #[error("horizon projection error: {0}")]
    Horizon(#[from] horizon_lib::Error),

    #[error("horizon nota decode error: {0}")]
    HorizonNota(#[from] nota_next::NotaDecodeError),

    #[error("horizon json encode error: {0}")]
    HorizonJson(#[from] serde_json::Error),
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
/// permission modes. Decoded only from the single rkyv startup file the daemon
/// binary receives. Mirrors the `cloud` `DaemonConfiguration` shape.
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub struct DaemonConfiguration {
    pub ordinary_socket_path: String,
    pub ordinary_socket_mode: u32,
    pub owner_socket_path: String,
    pub owner_socket_mode: u32,
    pub state_directory_path: String,
}

impl DaemonConfiguration {
    pub fn from_rkyv_file(path: &Path) -> Result<Self> {
        let bytes = std::fs::read(path)?;
        rkyv::from_bytes::<Self, rkyv::rancor::Error>(&bytes)
            .map_err(|error| Error::ConfigurationArchive(error.to_string()))
    }

    pub fn write_rkyv_file(&self, path: &Path) -> Result<()> {
        let bytes = rkyv::to_bytes::<rkyv::rancor::Error>(self)
            .map_err(|error| Error::ConfigurationArchive(error.to_string()))?;
        std::fs::write(path, bytes)?;
        Ok(())
    }
}

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
            live_set: LiveSetTable::new(Vec::new()),
            gc_roots: GcRootsTable::new(Vec::new()),
            event_log: EventLogTable::new(Vec::new()),
            containers: ContainerLifecycleTable::new(Vec::new()),
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
        self.event_log.payload().len() as u64
    }

    pub fn push_event_log_entry(&mut self, entry: EventLogEntry) {
        let mut entries = self.event_log.clone().into_payload();
        entries.push(entry);
        self.event_log = EventLogTable::new(entries);
    }

    pub fn push_live_generation(&mut self, generation: LiveGeneration) {
        let mut generations = self.live_set.clone().into_payload();
        generations.push(generation);
        self.live_set = LiveSetTable::new(generations);
    }

    pub fn push_gc_root(&mut self, root: GcRoot) {
        let mut roots = self.gc_roots.clone().into_payload();
        roots.push(root);
        self.gc_roots = GcRootsTable::new(roots);
    }

    pub fn replace_gc_roots(&mut self, roots: Vec<GcRoot>) {
        self.gc_roots = GcRootsTable::new(roots);
    }

    pub fn push_container_record(&mut self, record: ContainerLifecycleRecord) {
        let mut records = self.containers.clone().into_payload();
        records.push(record);
        self.containers = ContainerLifecycleTable::new(records);
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
