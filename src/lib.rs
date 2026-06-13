//! lojix deploy orchestrator runtime.
//!
//! The daemon owns the live generation set, the GC-roots retention tree, the
//! append-only event log, and the container-lifecycle mirror. It drives the
//! generated Nexus runner: each wire frame becomes a `NexusWork::SignalArrived`
//! tagged by listener role, the engine decides the next step, and the deploy
//! pipeline runs as a chain of effect continuations. The CLI is only a
//! text-to-Signal adapter for this daemon.
//!
//! Durable `sema-engine`-backed state backs the `SemaEngine` for this build,
//! while the daemon socket shell is actor-native. Daemon state — the live
//! generation set, GC-roots, the append-only event log, and the container
//! mirror — persists to a `*.sema` file and self-resumes on restart: opening
//! the engine reads the persisted catalog, commit counter, and records straight
//! back, so the daemon recovers without replay code (Spirit oh9l durable-first,
//! fosp sema-engine-exclusive, ur16 self-resume).

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use sema_engine::{
    Assertion, Engine as SemaDatabase, EngineOpen, EngineRecord, FamilyName, Mutation, QueryPlan,
    RecordKey, Retraction, SchemaHash, SchemaVersion, TableDescriptor, TableName, TableReference,
};

use crate::schema::sema::{
    ContainerLifecycleRecord, EventLogEntry, GcRoot, LiveGeneration,
};

pub mod client;
pub mod daemon;
pub mod schema;
pub mod schema_runtime;

/// The lojix durable-store schema version. The store is a typed, versioned
/// database from the very first write: every future bump is a deliberate HARD
/// migration (the workspace no-backward-compat override), not a soft upgrade —
/// the kernel hard-fails to open a store stamped at a different version.
const LOJIX_SCHEMA_VERSION: SchemaVersion = SchemaVersion::new(1);

/// The four durable table names. One row per element (a keyed record family),
/// not one blob per table — the sema-engine model.
const LIVE_SET_TABLE: TableName = TableName::new("live-set");
const GC_ROOTS_TABLE: TableName = TableName::new("gc-roots");
const EVENT_LOG_TABLE: TableName = TableName::new("event-log");
const CONTAINER_LIFECYCLE_TABLE: TableName = TableName::new("container-lifecycle");

/// The stable per-family schema identities. Each table is its own record
/// family with a distinct, reopen-stable `FamilyName` + `SchemaHash`. The hash
/// only has to be stable across reopens and distinct per family; the leading
/// byte distinguishes the four families. A schema change is a deliberate hard
/// migration, so a fixed value is correct until a version bump.
const LIVE_SET_FAMILY: &str = "LiveSetFamily";
const GC_ROOTS_FAMILY: &str = "GcRootsFamily";
const EVENT_LOG_FAMILY: &str = "EventLogFamily";
const CONTAINER_LIFECYCLE_FAMILY: &str = "ContainerLifecycleFamily";
const LIVE_SET_SCHEMA_HASH: [u8; 32] = [1; 32];
const GC_ROOTS_SCHEMA_HASH: [u8; 32] = [2; 32];
const EVENT_LOG_SCHEMA_HASH: [u8; 32] = [3; 32];
const CONTAINER_LIFECYCLE_SCHEMA_HASH: [u8; 32] = [4; 32];

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

    #[error(
        "owner socket TCP peer {peer_address} has no Unix credentials; refusing the privileged surface"
    )]
    UnauthorizedOwnerTcpPeer { peer_address: String },

    #[error("unexpected signal frame for this socket")]
    UnexpectedFrame,

    #[error("connection closed before a complete frame arrived")]
    ConnectionClosed,

    #[error("request frame read timed out")]
    RequestReadTimedOut,

    #[error("signal request was rejected before execution")]
    SignalRequestRejected,

    #[error("sema database engine error: {0}")]
    Database(#[from] sema_engine::Error),

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

/// The durable lojix daemon state plane: a `sema-engine` keyed table store
/// written to a `*.sema` file, mirroring the spirit `Store` shape. SEMA means
/// database work. `Store` maps the four schema-emitted record families onto
/// sema-engine operations — one row per element, not one blob per table — and
/// sema-engine owns the database handle, the durable commit sequence, and typed
/// rkyv table access. There is no `Mutex`: the engine's redb write transaction
/// is the serialization point and reads are `&self`, so concurrent reads run
/// against the persisted catalog while a single writer commits.
///
/// `Engine::open` IS the self-resume: a fresh file stamps empty counters; a
/// populated file reads the persisted catalog, commit sequence, and records
/// straight back, so the daemon recovers without replay code (ur16). The
/// restart-safe identifier counters (`next_generation_identifier`,
/// `next_deployment_identifier`, `next_event_log_position`) are derived from
/// the persisted rows on every call rather than from RAM counters that reset to
/// zero on restart.
pub struct Store {
    database: SemaDatabase,
    live_set: TableReference<LiveGeneration>,
    gc_roots: TableReference<GcRoot>,
    event_log: TableReference<EventLogEntry>,
    containers: TableReference<ContainerLifecycleRecord>,
    path: PathBuf,
    /// The ephemeral subscription-token counter. Subscriptions are connection
    /// state, not durable state — they do NOT persist across restart — so an
    /// in-memory atomic is the correct issuer (decision 5).
    subscription_sequence: AtomicU64,
}

impl std::fmt::Debug for Store {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("Store")
            .field("path", &self.path)
            .finish_non_exhaustive()
    }
}

impl Store {
    /// Open or create the durable SEMA database at `path`. A fresh file is
    /// created with empty engine counters; an existing file resumes its
    /// persisted commit sequence and records straight back through sema-engine.
    /// The four `register_table` calls are idempotent, so opening doubles as the
    /// resume — there is no separate load path (ur16).
    pub fn open(path: impl Into<PathBuf>) -> Result<Self> {
        let path = path.into();
        let mut database = SemaDatabase::open(EngineOpen::new(path.clone(), LOJIX_SCHEMA_VERSION))?;
        let live_set = database.register_table(TableDescriptor::new(
            LIVE_SET_TABLE,
            FamilyName::new(LIVE_SET_FAMILY),
            SchemaHash::new(LIVE_SET_SCHEMA_HASH),
        ))?;
        let gc_roots = database.register_table(TableDescriptor::new(
            GC_ROOTS_TABLE,
            FamilyName::new(GC_ROOTS_FAMILY),
            SchemaHash::new(GC_ROOTS_SCHEMA_HASH),
        ))?;
        let event_log = database.register_table(TableDescriptor::new(
            EVENT_LOG_TABLE,
            FamilyName::new(EVENT_LOG_FAMILY),
            SchemaHash::new(EVENT_LOG_SCHEMA_HASH),
        ))?;
        let containers = database.register_table(TableDescriptor::new(
            CONTAINER_LIFECYCLE_TABLE,
            FamilyName::new(CONTAINER_LIFECYCLE_FAMILY),
            SchemaHash::new(CONTAINER_LIFECYCLE_SCHEMA_HASH),
        ))?;
        Ok(Self {
            database,
            live_set,
            gc_roots,
            event_log,
            containers,
            path,
            subscription_sequence: AtomicU64::new(0),
        })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    /// The persisted commit sequence — sema-engine's durable write counter,
    /// read back after a write for the reply marker. Survives restart because
    /// the engine owns it (it just deletes from RAM here). `state_digest`
    /// remains the commit-sequence stand-in (decision 6).
    pub fn commit_sequence(&self) -> Result<u64> {
        Ok(self.database.current_commit_sequence()?.value())
    }

    fn live_generations(&self) -> Result<Vec<LiveGeneration>> {
        Ok(self
            .database
            .match_records(QueryPlan::all(self.live_set))?
            .records()
            .to_vec())
    }

    fn gc_root_records(&self) -> Result<Vec<GcRoot>> {
        Ok(self
            .database
            .match_records(QueryPlan::all(self.gc_roots))?
            .records()
            .to_vec())
    }

    fn event_log_entries(&self) -> Result<Vec<EventLogEntry>> {
        Ok(self
            .database
            .match_records(QueryPlan::all(self.event_log))?
            .records()
            .to_vec())
    }

    /// The live generations matching a predicate, projected by the caller.
    pub fn matching_live_generations(
        &self,
        keep: impl Fn(&LiveGeneration) -> bool,
    ) -> Result<Vec<LiveGeneration>> {
        Ok(self
            .live_generations()?
            .into_iter()
            .filter(|generation| keep(generation))
            .collect())
    }

    /// The persisted event-log entries in the half-open position range
    /// `[from, until)`.
    pub fn event_log_in_range(&self, from: u64, until: u64) -> Result<Vec<EventLogEntry>> {
        Ok(self
            .event_log_entries()?
            .into_iter()
            .filter(|entry| {
                let position = *entry.event_log_position.payload();
                position >= from && position < until
            })
            .collect())
    }

    /// The next generation identifier: one past the maximum persisted across
    /// the live set, or 1 when empty. Restart-safe — derived from durable rows,
    /// not a RAM counter (decision 5, the bug this fixes).
    pub fn next_generation_identifier(&self) -> Result<u64> {
        Ok(self
            .live_generations()?
            .iter()
            .map(|generation| *generation.generation_identifier.payload())
            .max()
            .map(|maximum| maximum + 1)
            .unwrap_or(1))
    }

    /// The next deployment identifier: one past the maximum persisted across the
    /// live set, or 1 when empty. Restart-safe (decision 5).
    pub fn next_deployment_identifier(&self) -> Result<u64> {
        Ok(self
            .live_generations()?
            .iter()
            .map(|generation| *generation.deployment_identifier.payload())
            .max()
            .map(|maximum| maximum + 1)
            .unwrap_or(1))
    }

    /// The next event-log position: the count of persisted event-log records.
    /// Restart-safe — derived from the durable rows, the analogue of the old
    /// `event_log.len()` (decision 5).
    pub fn next_event_log_position(&self) -> Result<u64> {
        Ok(self.event_log_entries()?.len() as u64)
    }

    /// The next subscription token: an in-memory atomic fetch-add. Subscriptions
    /// do not survive restart, so an ephemeral counter is correct (decision 5).
    pub fn next_subscription_token(&self) -> u64 {
        self.subscription_sequence.fetch_add(1, Ordering::SeqCst) + 1
    }

    /// Append one event-log entry, keyed by its position (decision 4).
    pub fn append_event_log_entry(&self, entry: EventLogEntry) -> Result<()> {
        self.database.assert(Assertion::new(self.event_log, entry))?;
        Ok(())
    }

    /// Append one live generation, keyed by its generation identifier
    /// (decision 4).
    pub fn append_live_generation(&self, generation: LiveGeneration) -> Result<()> {
        self.database
            .assert(Assertion::new(self.live_set, generation))?;
        Ok(())
    }

    /// Record an activation: write the live generation then the GC-root as TWO
    /// sequential keyed asserts. `CommitRequest` is single-table, so true
    /// cross-table atomicity is NOT available; the sequential write is the
    /// accepted baseline. Each row is keyed by its generation identifier, so the
    /// asserts are fail-safe — a duplicate key errors rather than silently
    /// clobbering — but a crash between the two leaves a torn write (a live row
    /// without its gc-root) that is NOT auto-reconciled on reopen. True
    /// cross-table atomicity needs a sema-engine multi-table commit, tracked as
    /// the follow-on.
    pub fn record_activation(
        &self,
        generation: LiveGeneration,
        root: GcRoot,
    ) -> Result<()> {
        self.append_live_generation(generation)?;
        self.append_gc_root(root)?;
        Ok(())
    }

    /// Append one GC-root, keyed by its generation identifier (decision 4).
    pub fn append_gc_root(&self, root: GcRoot) -> Result<()> {
        self.database.assert(Assertion::new(self.gc_roots, root))?;
        Ok(())
    }

    /// Overwrite one GC-root in place (a slot/label change), keyed by its
    /// generation identifier.
    pub fn mutate_gc_root(&self, root: GcRoot) -> Result<()> {
        self.database.mutate(Mutation::new(self.gc_roots, root))?;
        Ok(())
    }

    /// Drop one GC-root by its generation identifier.
    pub fn retract_gc_root(&self, generation_identifier: u64) -> Result<()> {
        self.database.retract(Retraction::new(
            self.gc_roots,
            RecordKey::new(generation_identifier.to_string()),
        ))?;
        Ok(())
    }

    /// The persisted GC-roots — the retention tree the pin/unpin/retire verbs
    /// search and rewrite.
    pub fn gc_roots(&self) -> Result<Vec<GcRoot>> {
        self.gc_root_records()
    }

    /// Append one container-lifecycle record, keyed by its event-log position,
    /// and a matching event-log entry. Two sequential keyed asserts across two
    /// tables; cross-table atomicity needs the same sema-engine enhancement
    /// noted on [`Self::record_activation`].
    pub fn record_container_transition(
        &self,
        record: ContainerLifecycleRecord,
        entry: EventLogEntry,
    ) -> Result<()> {
        self.database
            .assert(Assertion::new(self.containers, record))?;
        self.append_event_log_entry(entry)?;
        Ok(())
    }

}

impl EngineRecord for LiveGeneration {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(self.generation_identifier.payload().to_string())
    }
}

impl EngineRecord for GcRoot {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(self.generation_identifier.payload().to_string())
    }
}

impl EngineRecord for EventLogEntry {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(self.event_log_position.payload().to_string())
    }
}

impl EngineRecord for ContainerLifecycleRecord {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(self.event_log_position.payload().to_string())
    }
}
