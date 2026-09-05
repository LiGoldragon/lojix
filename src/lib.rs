//! lojix deploy orchestrator runtime.
//!
//! The daemon owns the live generation set, the GC-roots retention tree, the
//! append-only event log, and the container-lifecycle mirror. It drives the
//! handwritten Nexus runner: each wire frame becomes a `NexusWork::SignalArrived`
//! tagged by listener role, the engine decides the next step, and the deploy
//! pipeline runs as a chain of effect continuations. The CLI is only a
//! text-to-Signal adapter for this daemon.
//!
//! Durable `sema-engine`-backed state backs the handwritten runtime model,
//! while the daemon socket shell is actor-native. Daemon state — the live
//! generation set, GC-roots, the append-only event log, and the container
//! mirror — persists to a `*.sema` file and self-resumes on restart: opening
//! the engine reads the persisted catalog, commit counter, and records straight
//! back, so the daemon recovers without replay code (Spirit oh9l durable-first,
//! fosp sema-engine-exclusive, ur16 self-resume).

use std::ffi::OsString;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, MutexGuard};

use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use sema_engine::{
    Assertion, Engine as SemaDatabase, EngineOpen, EngineRecord, FamilyDirectory, FamilyName,
    Mutation, QueryPlan, RecordKey, Retraction, RowMaterializer, SchemaHash, SchemaVersion,
    TableDescriptor, TableName, TableReference, VersionedHistoryAcknowledgement,
    VersionedHistoryRetention, VersionedStoreName, VersioningPolicy,
};

use crate::runtime_model::{
    AdmissionMarker, CommitSequence, ContainerLifecycleRecord, DeployJob, DeploymentLifecycle,
    DeploymentOutboxRecord, DeploymentRecord, DeploymentRequestIdentity, DeploymentTerminal,
    EventLogEntry, GcRoot, IdentifierAllocation, ImmutableRevision, LiveGeneration,
    OutboxDeliveryState, OutboxRetryCount, PendingTransitionIntent, StateDigest, StateMarker,
    StoredTestRun, TerminalMarker, TransitionIntentState, TransitionMarker, TransitionOrdinal,
};

pub mod adapters;
pub mod bootstrap;
pub mod client;
pub mod daemon;
pub mod ingress;
pub mod inspection;
pub mod ingress;
pub mod reconstruction;
pub mod runtime_flow;
pub mod runtime_model;
pub mod schema_runtime;

/// The Lojix durable-store schema version. v4 records exact private deployment
/// routing snapshots and deliberately does not decode or migrate earlier
/// layouts. Use the path-scoped `lojix-reset-store` primitive while the daemon
/// is stopped.
const LOJIX_SCHEMA_VERSION: SchemaVersion = SchemaVersion::new(4);

/// The ten durable table names. One row per element (a keyed record family),
/// not one blob per table — the sema-engine model. `deploy-job` is the
/// in-flight deploy-job mirror (up9q): one row per submitted deploy, rewritten
/// per phase transition, read on daemon start so an in-flight deploy resumes
/// rather than being lost with the connection that submitted it.
const LIVE_SET_TABLE: TableName = TableName::new("live-set");
const GC_ROOTS_TABLE: TableName = TableName::new("gc-roots");
const EVENT_LOG_TABLE: TableName = TableName::new("event-log");
const CONTAINER_LIFECYCLE_TABLE: TableName = TableName::new("container-lifecycle");
const DEPLOY_JOB_TABLE: TableName = TableName::new("deploy-job");
const TEST_RUN_TABLE: TableName = TableName::new("test-run");
const DEPLOYMENT_RECORD_TABLE: TableName = TableName::new("deployment-record");
const IDENTIFIER_ALLOCATION_TABLE: TableName = TableName::new("identifier-allocation");
const DEPLOYMENT_OUTBOX_TABLE: TableName = TableName::new("deployment-outbox");
const PENDING_TRANSITION_INTENT_TABLE: TableName = TableName::new("pending-transition-intent");

/// The stable per-family schema identities. Each table is its own record
/// family with a distinct, reopen-stable `FamilyName` + `SchemaHash`. The hash
/// only has to be stable across reopens and distinct per family; the leading
/// byte distinguishes the eight families. A schema change is a deliberate hard
/// migration, so a fixed value is correct until a version bump.
const LIVE_SET_FAMILY: &str = "LiveSetFamily";
const GC_ROOTS_FAMILY: &str = "GcRootsFamily";
const EVENT_LOG_FAMILY: &str = "EventLogFamily";
const CONTAINER_LIFECYCLE_FAMILY: &str = "ContainerLifecycleFamily";
const DEPLOY_JOB_FAMILY: &str = "DeployJobFamily";
const TEST_RUN_FAMILY: &str = "TestRunFamily";
const DEPLOYMENT_RECORD_FAMILY: &str = "DeploymentRecordFamily";
const IDENTIFIER_ALLOCATION_FAMILY: &str = "IdentifierAllocationFamily";
const DEPLOYMENT_OUTBOX_FAMILY: &str = "DeploymentOutboxFamily";
const PENDING_TRANSITION_INTENT_FAMILY: &str = "PendingTransitionIntentFamily";
const LIVE_SET_SCHEMA_HASH: [u8; 32] = [1; 32];
const GC_ROOTS_SCHEMA_HASH: [u8; 32] = [2; 32];
const EVENT_LOG_SCHEMA_HASH: [u8; 32] = [3; 32];
const CONTAINER_LIFECYCLE_SCHEMA_HASH: [u8; 32] = [4; 32];
const DEPLOY_JOB_SCHEMA_HASH: [u8; 32] = [5; 32];
const TEST_RUN_SCHEMA_HASH: [u8; 32] = [6; 32];
const DEPLOYMENT_RECORD_SCHEMA_HASH: [u8; 32] = [7; 32];
const IDENTIFIER_ALLOCATION_SCHEMA_HASH: [u8; 32] = [8; 32];
const DEPLOYMENT_OUTBOX_SCHEMA_HASH: [u8; 32] = [9; 32];
const PENDING_TRANSITION_INTENT_SCHEMA_HASH: [u8; 32] = [11; 32];

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

    #[error("bound signal frame error: {0}")]
    BoundFrame(#[from] signal_frame::FrameError),

    #[error("expected exactly one argument")]
    ExpectedSingleArgument,

    #[error("flag-style arguments are not part of component binaries: {0}")]
    FlagArgument(String),

    #[error("this CLI accepts exactly one inline Datom object")]
    InlineDatomRequired,

    #[error("required runtime configuration environment variable is unset: {0}")]
    MissingRuntimeConfiguration(String),

    #[error("Datom request did not decode: {0}")]
    DatomRequestText(String),

    #[error("structural wire conversion failed: {0}")]
    Wire(String),

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

    #[error("runtime invariant failed: {0}")]
    Invariant(String),

    #[error("connection closed before a complete frame arrived")]
    ConnectionClosed,

    #[error("request frame read timed out")]
    RequestReadTimedOut,

    #[error("signal request was rejected before execution")]
    SignalRequestRejected,

    #[error("sema database engine error: {0}")]
    Database(#[from] sema_engine::Error),

    #[error("store maintenance rejected: {0}")]
    StoreMaintenance(String),

    #[error(
        "lojix store at {path:?} cannot be used by this daemon's startup schema/layout gate while {stage}; daemon startup stopped before serving requests; inspect it with an inline `lojix-inspect-store '(InspectStore <path>)'` request: {source}"
    )]
    StoreStartupCompatibility {
        path: PathBuf,
        stage: &'static str,
        #[source]
        source: Box<sema_engine::Error>,
    },

    #[error("horizon projection error: {0}")]
    Horizon(#[from] horizon_lib::Error),
    #[error("horizon json encode error: {0}")]
    HorizonJson(#[from] serde_json::Error),

    #[error("cluster secret file name is not valid UTF-8: {0}")]
    SecretFileNameNotUtf8(std::path::PathBuf),

    #[error(
        "two cluster secret files map to the same sopsFiles attribute name {attribute_name:?}: {first} and {second}"
    )]
    SecretAttributeCollision {
        attribute_name: String,
        first: String,
        second: String,
    },
}

pub type Result<T> = std::result::Result<T, Error>;

/// Select one inline Datom operand without classifying filesystem paths.
/// Component operator surfaces use this instead of `ComponentCommand`: that
/// helper intentionally recognises request files, whereas these bounded CLIs
/// must never read a caller-selected path.
pub fn single_inline_datom_argument(
    arguments: impl IntoIterator<Item = OsString>,
) -> Result<String> {
    let mut arguments = arguments.into_iter();
    let Some(argument) = arguments.next() else {
        return Err(Error::ExpectedSingleArgument);
    };
    if arguments.next().is_some() {
        return Err(Error::ExpectedSingleArgument);
    }
    let argument = argument
        .into_string()
        .map_err(|_| Error::InlineDatomRequired)?;
    if argument.starts_with('-') {
        return Err(Error::FlagArgument(argument));
    }
    Ok(argument)
}

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
    /// Exact configured durable Lojix store path. This is intentionally
    /// independent from the state directory so the daemon and reset primitive
    /// share one explicit target rather than deriving a `lojix.sema` basename.
    pub store_path: String,
    /// The node name of the host this daemon runs on. It is
    /// retained as deployment identity, while every immutable generation is
    /// evaluated and built locally, copied to its target, then activated there.
    /// Decoded from the same rkyv startup file — not a flag, not a runtime
    /// `Configure`.
    pub daemon_host: String,
    /// The test-op defaults the daemon fills into a `(Check …)` shorthand at
    /// lowering (report 54). `None` when the daemon was started without a baked
    /// fixture — the production posture: a bare `(Check …)`/`(Run …)` then
    /// rejects with `NoTestDefaults` rather than resolving against a per-node
    /// baked test cluster (the workspace deployment-independence discipline;
    /// test fixtures live only in test code). Decoded from the same rkyv startup
    /// file as the rest of the configuration — NOT a runtime `Configure` op, NOT
    /// a flag, consistent with the daemons-take-binary-config-only override.
    pub test_defaults: Option<TestDefaults>,
}

/// The config-default test-op selection a `(Check <node>)` shorthand expands
/// into (report 54 decision D — cluster, host, and mode all default from
/// config, so `(Check mercury)` is the routine form). `default_vm_host` is the
/// host the test VM is brought up on when the request says `DefaultHost`;
/// `default_mode` is the everyday dispatch mode (`Hermetic`). `TestMode` is the
/// shared signal type, so the wire op, the durable record, and this config
/// default all name one type.
///
/// `test_flake`, `test_nix_system`, and `test_output_selector` make the
/// shorthand's hermetic execution profile fully explicit in startup
/// configuration. `proposal_source` is the canonical ClusterProposal artifact the
/// daemon projects to validate `(OnHost h)` and resolve `All`; it is empty when
/// host-set validation is not configured.
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub struct TestDefaults {
    pub cluster: String,
    pub default_vm_host: String,
    pub default_mode: TestMode,
    pub test_flake: String,
    pub test_nix_system: String,
    pub test_output_selector: String,
    pub proposal_source: String,
}

/// The rkyv-stored test mode. A daemon-local mirror of the shared
/// `signal-lojix` `TestMode`, so the binary startup configuration archives
/// without depending on the wire crate's rkyv layout. Converts to/from the
/// encoded Interface type at the structural adapter boundary.
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, Copy, PartialEq, Eq)]
pub enum TestMode {
    Hermetic,
    Live,
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

/// The explicit bounded-history policy for deployment and container events.
/// Current generations, GC roots, and active deploy jobs are independent keyed
/// state and are never selected by this policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EventLogRetention {
    maximum_entries: u64,
}

impl EventLogRetention {
    pub const DEFAULT_MAXIMUM_ENTRIES: u64 = 4_096;

    pub const fn default_policy() -> Self {
        Self::new(Self::DEFAULT_MAXIMUM_ENTRIES)
    }

    pub const fn new(maximum_entries: u64) -> Self {
        Self { maximum_entries }
    }

    pub const fn maximum_entries(&self) -> u64 {
        self.maximum_entries
    }
}

/// The durable lojix daemon state plane: a `sema-engine` keyed table store
/// written to a `*.sema` file, mirroring the spirit `Store` shape. SEMA means
/// database work. `Store` maps the six schema-emitted record families onto
/// sema-engine operations — one row per element, not one blob per table — and
/// sema-engine owns the database handle, the durable commit sequence, and typed
/// rkyv table access. A process-local write gate serializes Lojix mutations
/// before they reach redb. This is stricter than redb's transaction lock: the
/// admission and terminal markers embedded in durable deployment records name
/// the exact commit that creates the row, and therefore cannot be based on an
/// optimistic sequence read that another local writer might overtake.
///
/// `Engine::open` IS the self-resume: a fresh file stamps empty counters; a
/// populated file reads the persisted catalog, commit sequence, and records
/// straight back, so the daemon recovers without replay code (ur16). The
/// restart-safe identifier counters (`next_generation_identifier`,
/// `next_deployment_identifier`, `next_event_log_position`) live in a durable
/// high-water row rather than in RAM or a scan of retained history.
pub struct Store {
    database: SemaDatabase,
    live_set: TableReference<LiveGeneration>,
    gc_roots: TableReference<GcRoot>,
    event_log: TableReference<EventLogEntry>,
    containers: TableReference<ContainerLifecycleRecord>,
    deploy_jobs: TableReference<DeployJob>,
    test_runs: TableReference<StoredTestRun>,
    deployment_records: TableReference<DeploymentRecord>,
    identifier_allocation: TableReference<IdentifierAllocation>,
    deployment_outbox: TableReference<DeploymentOutboxRecord>,
    pending_transition_intents: TableReference<PendingTransitionIntent>,
    path: PathBuf,
    write_gate: Mutex<()>,
    /// The ephemeral subscription-token counter. Subscriptions are connection
    /// state, not durable state — they do NOT persist across restart — so an
    /// in-memory atomic is the correct issuer (decision 5).
    subscription_sequence: AtomicU64,
}

struct LojixDirectory {
    live_set: TableReference<LiveGeneration>,
    gc_roots: TableReference<GcRoot>,
    event_log: TableReference<EventLogEntry>,
    containers: TableReference<ContainerLifecycleRecord>,
    deploy_jobs: TableReference<DeployJob>,
    test_runs: TableReference<StoredTestRun>,
    deployment_records: TableReference<DeploymentRecord>,
    identifier_allocation: TableReference<IdentifierAllocation>,
    deployment_outbox: TableReference<DeploymentOutboxRecord>,
    pending_transition_intents: TableReference<PendingTransitionIntent>,
}

impl FamilyDirectory for LojixDirectory {
    fn materialize(&self, row: RowMaterializer<'_>) -> sema_engine::Result<()> {
        match row.family().table_name() {
            "live-set" => row.apply(self.live_set),
            "gc-roots" => row.apply(self.gc_roots),
            "event-log" => row.apply(self.event_log),
            "container-lifecycle" => row.apply(self.containers),
            "deploy-job" => row.apply(self.deploy_jobs),
            "test-run" => row.apply(self.test_runs),
            "deployment-record" => row.apply(self.deployment_records),
            "identifier-allocation" => row.apply(self.identifier_allocation),
            "deployment-outbox" => row.apply(self.deployment_outbox),
            "pending-transition-intent" => row.apply(self.pending_transition_intents),
            table => Err(sema_engine::Error::TableNotRegistered {
                table: table.to_owned(),
            }),
        }
    }
}

impl std::fmt::Debug for Store {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("Store")
            .field("path", &self.path)
            .finish_non_exhaustive()
    }
}

fn next_identifier(value: u64) -> Result<u64> {
    value
        .checked_add(1)
        .ok_or_else(|| Error::StoreMaintenance("identifier space exhausted".to_string()))
}

fn state_marker(commit_sequence: u64) -> StateMarker {
    StateMarker {
        commit_sequence: CommitSequence::new(commit_sequence),
        state_digest: StateDigest::new(commit_sequence),
    }
}

/// The only representation we treat as a publicly truthful immutable source
/// revision: a complete 40-hex Git commit.  Nix metadata can contain nar
/// hashes and other opaque values, which must remain private rather than being
/// projected as a source revision.
pub(crate) fn immutable_revision(value: &str) -> Option<ImmutableRevision> {
    (value.len() == 40 && value.bytes().all(|byte| byte.is_ascii_hexdigit()))
        .then(|| ImmutableRevision::new(value))
}

fn canonical_nix_store_root(value: &str) -> bool {
    let Some(item) = value.strip_prefix("/nix/store/") else {
        return false;
    };
    let Some((hash, name)) = item.split_once('-') else {
        return false;
    };
    hash.len() == 32
        && hash.bytes().all(|byte| {
            matches!(byte, b'0'..=b'9' | b'a'..=b'z') && !matches!(byte, b'e' | b'o' | b't' | b'u')
        })
        && !name.is_empty()
        && !name.contains("..")
        && name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'+' | b'_' | b'-'))
        && !credential_like(value)
}

fn validate_fresh_closure_path(value: &str, record_kind: &str) -> Result<()> {
    if canonical_nix_store_root(value) {
        Ok(())
    } else {
        Err(Error::Invariant(format!(
            "fresh {record_kind} requires a canonical immutable store-item root"
        )))
    }
}

fn validate_fresh_deploy_job(job: &DeployJob) -> Result<()> {
    if let Some(closure_path) = job.optional_closure_path.as_ref() {
        validate_fresh_closure_path(closure_path.payload(), "deploy job")?;
    }
    Ok(())
}

fn validate_persisted_deploy_job(job: &DeployJob) -> Result<()> {
    validate_fresh_deploy_job(job)
}

fn validate_fresh_gc_root(root: &GcRoot) -> Result<()> {
    validate_fresh_closure_path(root.closure_path.payload(), "GC root")
}

fn credential_like(value: &str) -> bool {
    let value = value.to_ascii_lowercase();
    [
        "token",
        "secret",
        "password",
        "passwd",
        "credential",
        "apikey",
        "api-key",
        "api_key",
        "auth",
    ]
    .into_iter()
    .any(|term| value.contains(term))
}

impl Store {
    /// Open or create the durable SEMA database at `path`. A fresh file is
    /// created with empty engine counters; an existing file resumes its
    /// persisted commit sequence and records straight back through sema-engine.
    /// The six `register_table` calls are idempotent, so opening doubles as the
    /// resume — there is no separate load path (ur16).
    pub fn open(path: impl Into<PathBuf>) -> Result<Self> {
        let path = path.into();
        let mut database = SemaDatabase::open(
            EngineOpen::new(path.clone(), LOJIX_SCHEMA_VERSION).with_versioning(
                VersioningPolicy::new(VersionedStoreName::new("lojix"))
                    .with_retention(VersionedHistoryRetention::new(4_096)),
            ),
        )
        .map_err(|source| Error::StoreStartupCompatibility {
            path: path.clone(),
            stage: "opening sema-engine",
            source: Box::new(source),
        })?;
        let live_set = database
            .register_table(TableDescriptor::new(
                LIVE_SET_TABLE,
                FamilyName::new(LIVE_SET_FAMILY),
                SchemaHash::new(LIVE_SET_SCHEMA_HASH),
            ))
            .map_err(|source| Error::StoreStartupCompatibility {
                path: path.clone(),
                stage: "registering live-set table",
                source: Box::new(source),
            })?;
        let gc_roots = database
            .register_table(TableDescriptor::new(
                GC_ROOTS_TABLE,
                FamilyName::new(GC_ROOTS_FAMILY),
                SchemaHash::new(GC_ROOTS_SCHEMA_HASH),
            ))
            .map_err(|source| Error::StoreStartupCompatibility {
                path: path.clone(),
                stage: "registering gc-roots table",
                source: Box::new(source),
            })?;
        let event_log = database
            .register_table(TableDescriptor::new(
                EVENT_LOG_TABLE,
                FamilyName::new(EVENT_LOG_FAMILY),
                SchemaHash::new(EVENT_LOG_SCHEMA_HASH),
            ))
            .map_err(|source| Error::StoreStartupCompatibility {
                path: path.clone(),
                stage: "registering event-log table",
                source: Box::new(source),
            })?;
        let containers = database
            .register_table(TableDescriptor::new(
                CONTAINER_LIFECYCLE_TABLE,
                FamilyName::new(CONTAINER_LIFECYCLE_FAMILY),
                SchemaHash::new(CONTAINER_LIFECYCLE_SCHEMA_HASH),
            ))
            .map_err(|source| Error::StoreStartupCompatibility {
                path: path.clone(),
                stage: "registering container-lifecycle table",
                source: Box::new(source),
            })?;
        let deploy_jobs = database
            .register_table(TableDescriptor::new(
                DEPLOY_JOB_TABLE,
                FamilyName::new(DEPLOY_JOB_FAMILY),
                SchemaHash::new(DEPLOY_JOB_SCHEMA_HASH),
            ))
            .map_err(|source| Error::StoreStartupCompatibility {
                path: path.clone(),
                stage: "registering deploy-job table",
                source: Box::new(source),
            })?;
        let test_runs = database
            .register_table(TableDescriptor::new(
                TEST_RUN_TABLE,
                FamilyName::new(TEST_RUN_FAMILY),
                SchemaHash::new(TEST_RUN_SCHEMA_HASH),
            ))
            .map_err(|source| Error::StoreStartupCompatibility {
                path: path.clone(),
                stage: "registering test-run table",
                source: Box::new(source),
            })?;
        let deployment_records = database
            .register_table(TableDescriptor::new(
                DEPLOYMENT_RECORD_TABLE,
                FamilyName::new(DEPLOYMENT_RECORD_FAMILY),
                SchemaHash::new(DEPLOYMENT_RECORD_SCHEMA_HASH),
            ))
            .map_err(|source| Error::StoreStartupCompatibility {
                path: path.clone(),
                stage: "registering deployment-record table",
                source: Box::new(source),
            })?;
        let identifier_allocation = database
            .register_table(TableDescriptor::new(
                IDENTIFIER_ALLOCATION_TABLE,
                FamilyName::new(IDENTIFIER_ALLOCATION_FAMILY),
                SchemaHash::new(IDENTIFIER_ALLOCATION_SCHEMA_HASH),
            ))
            .map_err(|source| Error::StoreStartupCompatibility {
                path: path.clone(),
                stage: "registering identifier-allocation table",
                source: Box::new(source),
            })?;
        let deployment_outbox = database
            .register_table(TableDescriptor::new(
                DEPLOYMENT_OUTBOX_TABLE,
                FamilyName::new(DEPLOYMENT_OUTBOX_FAMILY),
                SchemaHash::new(DEPLOYMENT_OUTBOX_SCHEMA_HASH),
            ))
            .map_err(|source| Error::StoreStartupCompatibility {
                path: path.clone(),
                stage: "registering deployment-outbox table",
                source: Box::new(source),
            })?;
        let pending_transition_intents = database
            .register_table(TableDescriptor::new(
                PENDING_TRANSITION_INTENT_TABLE,
                FamilyName::new(PENDING_TRANSITION_INTENT_FAMILY),
                SchemaHash::new(PENDING_TRANSITION_INTENT_SCHEMA_HASH),
            ))
            .map_err(|source| Error::StoreStartupCompatibility {
                path: path.clone(),
                stage: "registering pending-transition-intent table",
                source: Box::new(source),
            })?;
        let store = Self {
            database,
            live_set,
            gc_roots,
            event_log,
            containers,
            deploy_jobs,
            test_runs,
            deployment_records,
            identifier_allocation,
            deployment_outbox,
            pending_transition_intents,
            path,
            write_gate: Mutex::new(()),
            subscription_sequence: AtomicU64::new(0),
        };
        store.ensure_identifier_allocation()?;
        store.resume_compaction()?;
        store.validate_startup_compatibility()?;
        store.requeue_dispatched_outbox()?;
        store.reconcile_pending_transition_intents()?;
        store.reconcile_deployment_outbox()?;
        Ok(store)
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Read every durable table once during store construction. The daemon must
    /// fail at the storage boundary when an older row layout no longer decodes;
    /// otherwise startup succeeds and the first query reports a misleading
    /// domain miss such as `GenerationUnknown`.
    fn validate_startup_compatibility(&self) -> Result<()> {
        self.live_generations().map(|_| ()).map_err(|source| {
            self.startup_compatibility_error("validating live-set rows", source)
        })?;
        self.gc_root_records().map(|_| ()).map_err(|source| {
            self.startup_compatibility_error("validating gc-roots rows", source)
        })?;
        self.event_log_entries().map(|_| ()).map_err(|source| {
            self.startup_compatibility_error("validating event-log rows", source)
        })?;
        self.container_lifecycle_records()
            .map(|_| ())
            .map_err(|source| {
                self.startup_compatibility_error("validating container-lifecycle rows", source)
            })?;
        self.deploy_jobs().map(|_| ()).map_err(|source| {
            self.startup_compatibility_error("validating deploy-job rows", source)
        })?;
        self.test_runs().map(|_| ()).map_err(|source| {
            self.startup_compatibility_error("validating test-run rows", source)
        })?;
        self.deployment_records().map(|_| ()).map_err(|source| {
            self.startup_compatibility_error("validating deployment-record rows", source)
        })?;
        self.identifier_allocations()
            .map(|_| ())
            .map_err(|source| {
                self.startup_compatibility_error("validating identifier-allocation row", source)
            })?;
        self.deployment_outbox_records()
            .map(|_| ())
            .map_err(|source| {
                self.startup_compatibility_error("validating deployment-outbox rows", source)
            })?;
        self.pending_transition_intents()
            .map(|_| ())
            .map_err(|source| {
                self.startup_compatibility_error(
                    "validating pending-transition-intent rows",
                    source,
                )
            })?;
        Ok(())
    }

    fn startup_compatibility_error(&self, stage: &'static str, error: Error) -> Error {
        match error {
            Error::Database(source) => Error::StoreStartupCompatibility {
                path: self.path.clone(),
                stage,
                source: Box::new(source),
            },
            other => other,
        }
    }

    /// The persisted commit sequence — sema-engine's durable write counter,
    /// read back after a write for the reply marker. Survives restart because
    /// the engine owns it (it just deletes from RAM here). `state_digest`
    /// remains the commit-sequence stand-in (decision 6).
    pub fn commit_sequence(&self) -> Result<u64> {
        Ok(self.database.current_commit_sequence()?.value())
    }

    fn lock_write(&self) -> Result<MutexGuard<'_, ()>> {
        self.write_gate
            .lock()
            .map_err(|_| Error::Invariant("lojix store write gate is poisoned".to_string()))
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

    fn container_lifecycle_records(&self) -> Result<Vec<ContainerLifecycleRecord>> {
        Ok(self
            .database
            .match_records(QueryPlan::all(self.containers))?
            .records()
            .to_vec())
    }

    fn deployment_records_unchecked(&self) -> Result<Vec<DeploymentRecord>> {
        Ok(self
            .database
            .match_records(QueryPlan::all(self.deployment_records))?
            .records()
            .to_vec())
    }

    fn identifier_allocations(&self) -> Result<Vec<IdentifierAllocation>> {
        Ok(self
            .database
            .match_records(QueryPlan::all(self.identifier_allocation))?
            .records()
            .to_vec())
    }

    fn deployment_outbox_records(&self) -> Result<Vec<DeploymentOutboxRecord>> {
        Ok(self
            .database
            .match_records(QueryPlan::all(self.deployment_outbox))?
            .records()
            .to_vec())
    }

    /// The private write-ahead transition records. Public queries never expose
    /// these rows; they exist solely to bridge a committed state transition to
    /// its exactly-once public event/outbox delivery.
    pub fn pending_transition_intents(&self) -> Result<Vec<PendingTransitionIntent>> {
        Ok(self
            .database
            .match_records(QueryPlan::all(self.pending_transition_intents))?
            .records()
            .to_vec())
    }

    fn identifier_allocation(&self) -> Result<IdentifierAllocation> {
        let allocations = self.identifier_allocations()?;
        match allocations.as_slice() {
            [allocation] => Ok(allocation.clone()),
            [] => Err(Error::StoreMaintenance(
                "identifier allocation row is missing after store initialization".to_string(),
            )),
            _ => Err(Error::StoreMaintenance(
                "identifier allocation table contains more than one row".to_string(),
            )),
        }
    }

    /// Initialize the one global high-water record once from the durable v4
    /// records that can issue identities.
    fn ensure_identifier_allocation(&self) -> Result<()> {
        if !self.identifier_allocations()?.is_empty() {
            return self.identifier_allocation().map(|_| ());
        }

        let next_deployment_identifier = self
            .live_generations()?
            .iter()
            .map(|generation| *generation.deployment_identifier.payload())
            .chain(
                self.deploy_jobs()?
                    .iter()
                    .map(|job| *job.deployment_identifier.payload()),
            )
            .max()
            .map(next_identifier)
            .transpose()?
            .unwrap_or(1);
        let next_generation_identifier = self
            .live_generations()?
            .iter()
            .map(|generation| *generation.generation_identifier.payload())
            .chain(
                self.deploy_jobs()?
                    .iter()
                    .map(|job| *job.generation_identifier.payload()),
            )
            .max()
            .map(next_identifier)
            .transpose()?
            .unwrap_or(1);
        let next_event_log_position = self
            .event_log_entries()?
            .iter()
            .map(|entry| *entry.event_log_position.payload())
            .max()
            .map(next_identifier)
            .transpose()?
            .unwrap_or(0);
        self.database.assert(Assertion::new(
            self.identifier_allocation,
            IdentifierAllocation {
                next_deployment_identifier,
                next_generation_identifier,
                next_event_log_position,
            },
        ))?;
        Ok(())
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

    /// The private delivery ledger. A record is uniquely keyed by the durable
    /// deployment identity and the exact transition marker that created its
    /// event; an effect replay therefore cannot create a second delivery for
    /// the same transition.
    pub fn deployment_outbox(&self) -> Result<Vec<DeploymentOutboxRecord>> {
        self.deployment_outbox_records()
    }

    fn outbox_key(
        deployment_identifier: &crate::runtime_model::DeploymentIdentifier,
        transition_marker: &TransitionMarker,
    ) -> RecordKey {
        RecordKey::new(format!(
            "{}:{}:{}",
            deployment_identifier.payload(),
            transition_marker.payload().commit_sequence.payload(),
            transition_marker.payload().state_digest.payload(),
        ))
    }

    fn transition_intent_key(intent: &PendingTransitionIntent) -> RecordKey {
        RecordKey::new(format!(
            "{}:{}",
            intent.deployment_identifier.payload(),
            intent.transition_ordinal.payload()
        ))
    }

    fn transition_intent_event(
        intent: &PendingTransitionIntent,
    ) -> Result<crate::runtime_model::DeploymentPhaseEvent> {
        let marker = intent.optional_transition_marker.clone().ok_or_else(|| {
            Error::Invariant("attempted to deliver an unbound transition intent".to_string())
        })?;
        Ok(crate::runtime_model::DeploymentPhaseEvent {
            deployment_identifier: intent.deployment_identifier.clone(),
            generation_identifier: intent.generation_identifier.clone(),
            cluster_name: intent.cluster_name.clone(),
            node_name: intent.node_name.clone(),
            deployment_phase: intent.deployment_phase,
            event_log_position: intent.event_log_position.clone(),
            state_marker: marker.into_payload(),
            optional_immutable_revision: intent.optional_immutable_revision.clone(),
            optional_deployment_terminal: intent.optional_deployment_terminal.clone(),
        })
    }

    /// Find the exact durable transaction that created `intent`.  The
    /// write-ahead row is not allowed to borrow the current database head: a
    /// later unrelated commit would otherwise forge the public marker.  The
    /// versioned SEMA log names both keyed rows written together, so matching
    /// the intent key and its deployment-record key proves the atomic source.
    fn initial_intent_marker(&self, intent: &PendingTransitionIntent) -> Result<TransitionMarker> {
        let intent_key = Self::transition_intent_key(intent);
        let deployment_key = RecordKey::new(intent.deployment_identifier.payload().to_string());
        let candidates: Vec<_> = self
            .database
            .versioned_commit_log()?
            .into_iter()
            .filter(|entry| {
                let operations = entry.operations();
                let has_intent = operations.head().table_name()
                    == PENDING_TRANSITION_INTENT_TABLE.as_str()
                    && operations.head().key() == Some(&intent_key)
                    || operations.tail().iter().any(|operation| {
                        operation.table_name() == PENDING_TRANSITION_INTENT_TABLE.as_str()
                            && operation.key() == Some(&intent_key)
                    });
                let has_deployment_record = operations.head().table_name()
                    == DEPLOYMENT_RECORD_TABLE.as_str()
                    && operations.head().key() == Some(&deployment_key)
                    || operations.tail().iter().any(|operation| {
                        operation.table_name() == DEPLOYMENT_RECORD_TABLE.as_str()
                            && operation.key() == Some(&deployment_key)
                    });
                has_intent && has_deployment_record
            })
            .collect();
        match candidates.as_slice() {
            [entry] => Ok(TransitionMarker::new(state_marker(
                entry.commit_sequence().value(),
            ))),
            [] => Err(Error::Invariant(
                "pending transition intent has no durable atomic source commit".to_string(),
            )),
            _ => Err(Error::Invariant(
                "pending transition intent has more than one durable source commit".to_string(),
            )),
        }
    }

    fn intent_from_key(
        &self,
        deployment_identifier: u64,
        transition_ordinal: u64,
    ) -> Result<PendingTransitionIntent> {
        self.pending_transition_intents()?
            .into_iter()
            .find(|intent| {
                *intent.deployment_identifier.payload() == deployment_identifier
                    && *intent.transition_ordinal.payload() == transition_ordinal
            })
            .ok_or_else(|| {
                Error::Invariant(format!(
                    "pending transition intent {deployment_identifier}:{transition_ordinal} is missing"
                ))
            })
    }

    /// Bind one freshly committed write-ahead intent to the receipt of its own
    /// durable transaction and create its private delivery row.  This is a
    /// separate commit by design: the marker is learned only from the durable
    /// commit log, never predicted before the initial transaction.
    fn bind_transition_intent(
        &self,
        deployment_identifier: u64,
        transition_ordinal: u64,
    ) -> Result<PendingTransitionIntent> {
        let _write = self.lock_write()?;
        let mut intent = self.intent_from_key(deployment_identifier, transition_ordinal)?;
        if intent.optional_transition_marker.is_some() {
            return Ok(intent);
        }
        let marker = self.initial_intent_marker(&intent)?;
        let event = Self::transition_intent_event(&PendingTransitionIntent {
            optional_transition_marker: Some(marker.clone()),
            ..intent.clone()
        })?;
        let outbox = Self::outbox_record_for_event(event);
        if let Some(existing) = self
            .deployment_outbox_records()?
            .into_iter()
            .find(|existing| {
                existing.deployment_identifier == outbox.deployment_identifier
                    && existing.transition_marker == outbox.transition_marker
            })
        {
            if existing.deployment_phase_event != outbox.deployment_phase_event {
                return Err(Error::Invariant(
                    "transition intent marker is already bound to a different outbox payload"
                        .to_string(),
                ));
            }
            return Err(Error::Invariant(
                "unbound transition intent already has a delivery row".to_string(),
            ));
        }
        intent.optional_transition_marker = Some(marker.clone());
        intent.transition_intent_state = TransitionIntentState::Bound;

        let mut commit = self
            .database
            .begin_atomic_commit()
            .mutate(self.pending_transition_intents, intent.clone())
            .assert(self.deployment_outbox, outbox);
        if matches!(
            intent.deployment_phase,
            crate::runtime_model::DeploymentPhase::Submitted
        ) || intent.optional_deployment_terminal.is_some()
        {
            let mut record = self
                .deployment_records_unchecked()?
                .into_iter()
                .find(|record| *record.deployment_identifier.payload() == deployment_identifier)
                .ok_or_else(|| {
                    Error::Invariant(format!(
                        "deployment record {deployment_identifier} is missing while binding admission"
                    ))
                })?;
            if matches!(
                intent.deployment_phase,
                crate::runtime_model::DeploymentPhase::Submitted
            ) {
                record.optional_admission_marker =
                    Some(AdmissionMarker::new(marker.clone().into_payload()));
            }
            if let Some(terminal) = &intent.optional_deployment_terminal {
                if record.optional_deployment_terminal.as_ref() != Some(terminal) {
                    return Err(Error::Invariant(
                        "terminal intent does not match its durable deployment record".to_string(),
                    ));
                }
                record.optional_terminal_marker = Some(TerminalMarker::new(marker.into_payload()));
            }
            commit = commit.mutate(self.deployment_records, record);
        }
        self.database.commit_atomic(commit)?;
        Ok(intent)
    }

    /// Persist `Pending -> Dispatched` before touching the local journal.  A
    /// restart deliberately requeues this durable state, so a crash cannot
    /// skip the exact-payload journal check.
    fn dispatch_transition_intent(
        &self,
        intent: &PendingTransitionIntent,
    ) -> Result<PendingTransitionIntent> {
        let _write = self.lock_write()?;
        let intent = self.intent_from_key(
            *intent.deployment_identifier.payload(),
            *intent.transition_ordinal.payload(),
        )?;
        let event = Self::transition_intent_event(&intent)?;
        let expected = Self::outbox_record_for_event(event);
        let mut outbox = self
            .deployment_outbox_records()?
            .into_iter()
            .find(|record| {
                record.deployment_identifier == expected.deployment_identifier
                    && record.transition_marker == expected.transition_marker
            })
            .ok_or_else(|| {
                Error::Invariant("bound intent is missing its outbox row".to_string())
            })?;
        if outbox.deployment_phase_event != expected.deployment_phase_event {
            return Err(Error::Invariant(
                "bound intent outbox payload does not match its full journal payload".to_string(),
            ));
        }
        if matches!(
            outbox.outbox_delivery_state,
            OutboxDeliveryState::Acknowledged
        ) {
            // An outbox acknowledgement is not proof of local delivery on its
            // own.  Continue to the exact journal check below; corrupted or
            // legacy rows must not cause a short-circuit around that boundary.
            return Ok(intent);
        }
        if matches!(outbox.outbox_delivery_state, OutboxDeliveryState::Pending) {
            outbox.outbox_delivery_state = OutboxDeliveryState::Dispatched;
            outbox.outbox_retry_count =
                OutboxRetryCount::new(next_identifier(*outbox.outbox_retry_count.payload())?);
            self.database
                .mutate(Mutation::new(self.deployment_outbox, outbox))?;
        }
        Ok(intent)
    }

    /// Append (or prove an exact existing copy of) the public journal event.
    /// An outbox row never counts as delivery: the local journal is checked at
    /// its full typed payload boundary on every recovery attempt.
    fn append_transition_intent_journal(
        &self,
        intent: &PendingTransitionIntent,
    ) -> Result<PendingTransitionIntent> {
        let _write = self.lock_write()?;
        let mut intent = self.intent_from_key(
            *intent.deployment_identifier.payload(),
            *intent.transition_ordinal.payload(),
        )?;
        let event = Self::transition_intent_event(&intent)?;
        let expected_entry = EventLogEntry {
            event_log_position: event.event_log_position.clone(),
            logged_event: crate::runtime_model::LoggedEvent::Deployment(event),
        };
        match self
            .event_log_entries()?
            .into_iter()
            .find(|entry| entry.event_log_position == expected_entry.event_log_position)
        {
            Some(existing) if existing == expected_entry => {}
            Some(_) => {
                return Err(Error::Invariant(
                    "transition intent journal position has a mismatched payload".to_string(),
                ));
            }
            None => {
                intent.transition_intent_state = TransitionIntentState::Appended;
                self.database.commit_atomic(
                    self.database
                        .begin_atomic_commit()
                        .assert(self.event_log, expected_entry)
                        .mutate(self.pending_transition_intents, intent.clone()),
                )?;
                return Ok(intent);
            }
        }
        if !matches!(
            intent.transition_intent_state,
            TransitionIntentState::Appended
        ) {
            intent.transition_intent_state = TransitionIntentState::Appended;
            self.database.mutate(Mutation::new(
                self.pending_transition_intents,
                intent.clone(),
            ))?;
        }
        Ok(intent)
    }

    /// The terminal local acknowledgement is atomic across the private intent
    /// and its outbox row, and is permitted only after the exact journal event
    /// has been proven present.
    fn acknowledge_transition_intent(&self, intent: &PendingTransitionIntent) -> Result<()> {
        let _write = self.lock_write()?;
        let mut intent = self.intent_from_key(
            *intent.deployment_identifier.payload(),
            *intent.transition_ordinal.payload(),
        )?;
        let event = Self::transition_intent_event(&intent)?;
        let expected_entry = EventLogEntry {
            event_log_position: event.event_log_position.clone(),
            logged_event: crate::runtime_model::LoggedEvent::Deployment(event.clone()),
        };
        if !self
            .event_log_entries()?
            .into_iter()
            .any(|entry| entry == expected_entry)
        {
            return Err(Error::Invariant(
                "cannot acknowledge a transition whose exact journal event is absent".to_string(),
            ));
        }
        let mut outbox = self
            .deployment_outbox_records()?
            .into_iter()
            .find(|record| {
                record.deployment_identifier == event.deployment_identifier
                    && record.transition_marker.payload() == &event.state_marker
            })
            .ok_or_else(|| {
                Error::Invariant("journalled transition is missing its outbox row".to_string())
            })?;
        if outbox.deployment_phase_event != event {
            return Err(Error::Invariant(
                "journalled transition outbox payload mismatch".to_string(),
            ));
        }
        if matches!(
            intent.transition_intent_state,
            TransitionIntentState::Acknowledged
        ) && matches!(
            outbox.outbox_delivery_state,
            OutboxDeliveryState::Acknowledged
        ) {
            return Ok(());
        }
        outbox.outbox_delivery_state = OutboxDeliveryState::Acknowledged;
        intent.transition_intent_state = TransitionIntentState::Acknowledged;
        let terminal_job_key = if intent.optional_deployment_terminal.is_some()
            && self
                .deploy_jobs()?
                .into_iter()
                .any(|job| job.deployment_identifier == event.deployment_identifier)
        {
            Some(RecordKey::new(
                event.deployment_identifier.payload().to_string(),
            ))
        } else {
            None
        };
        let mut commit = self
            .database
            .begin_atomic_commit()
            .mutate(self.deployment_outbox, outbox)
            .mutate(self.pending_transition_intents, intent);
        if let Some(job_key) = terminal_job_key {
            commit = commit.retract(self.deploy_jobs, job_key);
        }
        self.database.commit_atomic(commit)?;
        Ok(())
    }

    /// Drive one durable transition all the way to a locally-acknowledged
    /// journal record.  It is idempotent over every crash boundary above.
    pub fn complete_pending_transition_intent(
        &self,
        deployment_identifier: u64,
        transition_ordinal: u64,
    ) -> Result<()> {
        let intent = self.bind_transition_intent(deployment_identifier, transition_ordinal)?;
        let intent = self.dispatch_transition_intent(&intent)?;
        let intent = self.append_transition_intent_journal(&intent)?;
        self.acknowledge_transition_intent(&intent)
    }

    /// Finish a committed write-ahead transition after restart. This runs
    /// before pipelines are reconstructed, so a later phase can never leap an
    /// earlier record mutation whose public transition was not delivered.
    fn reconcile_pending_transition_intents(&self) -> Result<()> {
        for intent in self.pending_transition_intents()? {
            if !matches!(
                intent.transition_intent_state,
                TransitionIntentState::Acknowledged
            ) {
                self.complete_pending_transition_intent(
                    *intent.deployment_identifier.payload(),
                    *intent.transition_ordinal.payload(),
                )?;
            }
        }
        Ok(())
    }

    fn outbox_record_for_event(
        event: crate::runtime_model::DeploymentPhaseEvent,
    ) -> DeploymentOutboxRecord {
        DeploymentOutboxRecord {
            deployment_identifier: event.deployment_identifier.clone(),
            transition_marker: TransitionMarker::new(event.state_marker.clone()),
            deployment_phase_event: event,
            outbox_delivery_state: OutboxDeliveryState::Pending,
            outbox_retry_count: OutboxRetryCount::new(0),
        }
    }

    /// Reopen recovery makes any unacknowledged dispatch available again. An
    /// acknowledged transition is never re-delivered; a dispatched-but-unacked
    /// transition is retried with its original composite key and payload.
    pub fn requeue_dispatched_outbox(&self) -> Result<()> {
        let _write = self.lock_write()?;
        let dispatched: Vec<_> = self
            .deployment_outbox_records()?
            .into_iter()
            .filter(|record| {
                matches!(
                    record.outbox_delivery_state,
                    OutboxDeliveryState::Dispatched
                )
            })
            .collect();
        if dispatched.is_empty() {
            return Ok(());
        }
        let mut commit = self.database.begin_atomic_commit();
        for mut record in dispatched {
            record.outbox_delivery_state = OutboxDeliveryState::Pending;
            commit = commit.mutate(self.deployment_outbox, record);
        }
        self.database.commit_atomic(commit)?;
        Ok(())
    }

    /// Reconstruct delivery rows from already-durable deployment events. Equal
    /// payloads deduplicate; any attempt to reuse a composite key for a
    /// different payload stops recovery.
    pub fn reconcile_deployment_outbox(&self) -> Result<()> {
        let _write = self.lock_write()?;
        let existing = self.deployment_outbox_records()?;
        let mut additions = Vec::new();
        for entry in self.event_log_entries()? {
            let crate::runtime_model::LoggedEvent::Deployment(event) = entry.logged_event else {
                continue;
            };
            let candidate = Self::outbox_record_for_event(event);
            let same_key = existing.iter().chain(additions.iter()).find(|record| {
                record.deployment_identifier == candidate.deployment_identifier
                    && record.transition_marker == candidate.transition_marker
            });
            match same_key {
                Some(record)
                    if record.deployment_phase_event == candidate.deployment_phase_event => {}
                Some(_) => {
                    return Err(Error::Invariant(
                        "deployment outbox composite key has mismatched event payloads".to_string(),
                    ));
                }
                None => additions.push(candidate),
            }
        }
        if additions.is_empty() {
            return Ok(());
        }
        let mut commit = self.database.begin_atomic_commit();
        for record in additions {
            commit = commit.assert(self.deployment_outbox, record);
        }
        self.database.commit_atomic(commit)?;
        Ok(())
    }

    /// Compact only the historical event and container-observation rows. The
    /// caller supplies the query window explicitly; live deploy state and
    /// restart-resume jobs stay outside this historical plane.
    fn resume_compaction(&self) -> Result<()> {
        self.database.resume_compaction(&LojixDirectory {
            live_set: self.live_set,
            gc_roots: self.gc_roots,
            event_log: self.event_log,
            containers: self.containers,
            deploy_jobs: self.deploy_jobs,
            test_runs: self.test_runs,
            deployment_records: self.deployment_records,
            identifier_allocation: self.identifier_allocation,
            deployment_outbox: self.deployment_outbox,
            pending_transition_intents: self.pending_transition_intents,
        })?;
        Ok(())
    }

    pub fn compact_event_history(&self, retention: EventLogRetention) -> Result<u64> {
        let _write = self.lock_write()?;
        let mut events = self.event_log_entries()?;
        events.sort_by_key(|entry| *entry.event_log_position.payload());
        let retired = events
            .len()
            .saturating_sub(retention.maximum_entries() as usize);
        if retired == 0 {
            return Ok(0);
        }
        let retired_positions: std::collections::BTreeSet<u64> = events
            .into_iter()
            .take(retired)
            .map(|entry| *entry.event_log_position.payload())
            .collect();
        self.database.begin_compaction()?;
        let mut group = self.database.begin_atomic_commit();
        for container in self.container_lifecycle_records()? {
            if retired_positions.contains(container.event_log_position.payload()) {
                group = group.retract(
                    self.containers,
                    RecordKey::new(container.event_log_position.payload().to_string()),
                );
            }
        }
        for position in &retired_positions {
            group = group.retract(self.event_log, RecordKey::new(position.to_string()));
        }
        // A pending/dispatched delivery remains the recovery payload even when
        // its historical journal row ages out. Only an acknowledged delivery
        // is eligible for the same compaction pass.
        for outbox in self.deployment_outbox_records()? {
            if retired_positions
                .contains(outbox.deployment_phase_event.event_log_position.payload())
                && matches!(
                    outbox.outbox_delivery_state,
                    OutboxDeliveryState::Acknowledged
                )
            {
                group = group.retract(
                    self.deployment_outbox,
                    Self::outbox_key(&outbox.deployment_identifier, &outbox.transition_marker),
                );
            }
        }
        self.database.commit_atomic(group)?;
        if self.database.park_compaction()? {
            self.resume_compaction()?;
        }
        // Lojix has no external mirror consumer. The verified local checkpoint
        // is therefore the complete recovery artifact once the historical rows
        // are retracted, and compaction reclaims the corresponding raw log and
        // outbox rows rather than leaving a second unbounded on-disk history.
        self.database.compact_versioned_history(
            VersionedHistoryRetention::new(0),
            VersionedHistoryAcknowledgement::LocalCheckpoint,
        )?;
        Ok(retired as u64)
    }

    fn maintain_event_history(&self) -> Result<()> {
        self.compact_event_history(EventLogRetention::default_policy())?;
        self.database.compact_configured_versioned_history()?;
        Ok(())
    }

    /// The next generation identifier comes from the durable global allocator,
    /// never a scan of currently retained rows.
    pub fn next_generation_identifier(&self) -> Result<u64> {
        Ok(self.identifier_allocation()?.next_generation_identifier)
    }

    /// The next deployment identifier comes from the durable global allocator,
    /// never a scan of currently retained rows.
    pub fn next_deployment_identifier(&self) -> Result<u64> {
        Ok(self.identifier_allocation()?.next_deployment_identifier)
    }

    /// The next event-log position comes from the durable global allocator, so
    /// retention cannot cause an event position to be reused.
    pub fn next_event_log_position(&self) -> Result<u64> {
        Ok(self.identifier_allocation()?.next_event_log_position)
    }

    /// Reserve an event-log position before constructing its record. The
    /// reservation itself is durable, so a crash can introduce only a gap;
    /// it can never lead to a reused position.
    pub fn allocate_event_log_position(&self) -> Result<u64> {
        let _write = self.lock_write()?;
        let allocation = self.identifier_allocation()?;
        let position = allocation.next_event_log_position;
        self.database
            .commit_atomic(self.database.begin_atomic_commit().mutate(
                self.identifier_allocation,
                IdentifierAllocation {
                    next_deployment_identifier: allocation.next_deployment_identifier,
                    next_generation_identifier: allocation.next_generation_identifier,
                    next_event_log_position: next_identifier(position)?,
                },
            ))?;
        Ok(position)
    }

    /// Atomically create the admission correlation record *and* the durable
    /// write-ahead intent.  The public receipt is intentionally absent here:
    /// it is bound only after this transaction is visible in sema-engine's
    /// versioned commit log.
    pub fn begin_admission_transition(
        &self,
        deployment_request_identity: DeploymentRequestIdentity,
        mut deploy_job: DeployJob,
    ) -> Result<DeploymentRecord> {
        validate_fresh_deploy_job(&deploy_job)?;
        let _write = self.lock_write()?;
        let allocation = self.identifier_allocation()?;
        let record = DeploymentRecord {
            deployment_identifier: allocation.next_deployment_identifier.into(),
            generation_identifier: allocation.next_generation_identifier.into(),
            deployment_request_identity,
            optional_admission_marker: None,
            deployment_lifecycle: DeploymentLifecycle::Submitted,
            optional_terminal_marker: None,
            optional_deployment_terminal: None,
        };
        let intent = PendingTransitionIntent {
            deployment_identifier: record.deployment_identifier.clone(),
            generation_identifier: record.generation_identifier.clone(),
            cluster_name: record.deployment_request_identity.cluster_name.clone(),
            node_name: record.deployment_request_identity.node_name.clone(),
            deployment_phase: crate::runtime_model::DeploymentPhase::Submitted,
            event_log_position: allocation.next_event_log_position.into(),
            optional_immutable_revision: record
                .deployment_request_identity
                .optional_immutable_revision
                .clone(),
            optional_deployment_terminal: None,
            transition_ordinal: TransitionOrdinal::new(0),
            optional_transition_marker: None,
            transition_intent_state: TransitionIntentState::Pending,
        };
        if !matches!(
            deploy_job.deploy_job_phase,
            crate::runtime_model::DeployJobPhase::Submitted
        ) || deploy_job.cluster_name != record.deployment_request_identity.cluster_name
            || deploy_job.node_name != record.deployment_request_identity.node_name
        {
            return Err(Error::Invariant(
                "admission restart cursor does not match its deployment identity".to_string(),
            ));
        }
        deploy_job.deployment_identifier = record.deployment_identifier.clone();
        deploy_job.generation_identifier = record.generation_identifier.clone();
        self.database.commit_atomic(
            self.database
                .begin_atomic_commit()
                .mutate(
                    self.identifier_allocation,
                    IdentifierAllocation {
                        next_deployment_identifier: next_identifier(
                            allocation.next_deployment_identifier,
                        )?,
                        next_generation_identifier: next_identifier(
                            allocation.next_generation_identifier,
                        )?,
                        next_event_log_position: next_identifier(
                            allocation.next_event_log_position,
                        )?,
                    },
                )
                .assert(self.deployment_records, record.clone())
                .assert(self.deploy_jobs, deploy_job)
                .assert(self.pending_transition_intents, intent),
        )?;
        Ok(record)
    }

    /// The synchronous admission helper used by the runtime.  It makes a
    /// submitted record visible only after its initial transition has reached
    /// the locally acknowledged journal protocol.
    pub fn allocate_deployment_record(
        &self,
        deployment_request_identity: DeploymentRequestIdentity,
        deploy_job: DeployJob,
    ) -> Result<DeploymentRecord> {
        let record = self.begin_admission_transition(deployment_request_identity, deploy_job)?;
        self.complete_pending_transition_intent(*record.deployment_identifier.payload(), 0)?;
        self.deployment_records_unchecked()?
            .into_iter()
            .find(|existing| existing.deployment_identifier == record.deployment_identifier)
            .ok_or_else(|| {
                Error::Invariant("admission record disappeared after acknowledgement".to_string())
            })
    }

    fn terminal_phase(
        deployment_lifecycle: DeploymentLifecycle,
    ) -> Result<crate::runtime_model::DeploymentPhase> {
        match deployment_lifecycle {
            DeploymentLifecycle::Completed => Ok(crate::runtime_model::DeploymentPhase::Completed),
            DeploymentLifecycle::Rejected => Ok(crate::runtime_model::DeploymentPhase::Rejected),
            DeploymentLifecycle::Failed => Ok(crate::runtime_model::DeploymentPhase::Failed),
            _ => Err(Error::Invariant(
                "terminal intent requires a terminal deployment lifecycle".to_string(),
            )),
        }
    }

    fn terminal_job_phase(
        deployment_lifecycle: DeploymentLifecycle,
    ) -> crate::runtime_model::DeployJobPhase {
        match deployment_lifecycle {
            DeploymentLifecycle::Completed => crate::runtime_model::DeployJobPhase::Activated,
            DeploymentLifecycle::Rejected | DeploymentLifecycle::Failed => {
                crate::runtime_model::DeployJobPhase::Failed
            }
            _ => unreachable!("terminal job phase requires a terminal lifecycle"),
        }
    }

    /// Atomically persist the terminal correlation state and its write-ahead
    /// intent.  The job remains durable through dispatch/journal delivery and
    /// is retracted only in the same acknowledgement commit as the intent and
    /// outbox, so a crash cannot turn an unreported terminal into an orphan.
    pub fn begin_terminal_transition(
        &self,
        deployment_identifier: u64,
        deployment_lifecycle: DeploymentLifecycle,
        deployment_terminal: DeploymentTerminal,
    ) -> Result<u64> {
        let deployment_phase = Self::terminal_phase(deployment_lifecycle)?;
        let _write = self.lock_write()?;
        let mut record = self
            .deployment_records_unchecked()?
            .into_iter()
            .find(|record| *record.deployment_identifier.payload() == deployment_identifier)
            .ok_or_else(|| {
                Error::Invariant(format!(
                    "deployment record {deployment_identifier} is missing for terminal transition"
                ))
            })?;
        if record.optional_deployment_terminal.is_some() {
            return Err(Error::Invariant(
                "deployment record is already terminal".to_string(),
            ));
        }
        let intents = self.pending_transition_intents()?;
        if intents.iter().any(|intent| {
            *intent.deployment_identifier.payload() == deployment_identifier
                && !matches!(
                    intent.transition_intent_state,
                    TransitionIntentState::Acknowledged
                )
        }) {
            return Err(Error::Invariant(
                "cannot terminalize deployment while an earlier transition lacks local acknowledgement"
                    .to_string(),
            ));
        }
        let transition_ordinal = intents
            .iter()
            .filter(|intent| *intent.deployment_identifier.payload() == deployment_identifier)
            .map(|intent| *intent.transition_ordinal.payload())
            .max()
            .map(next_identifier)
            .transpose()?
            .unwrap_or(0);
        let allocation = self.identifier_allocation()?;
        record.deployment_lifecycle = deployment_lifecycle;
        record.optional_deployment_terminal = Some(deployment_terminal.clone());
        record.optional_terminal_marker = None;
        let intent = PendingTransitionIntent {
            deployment_identifier: record.deployment_identifier.clone(),
            generation_identifier: record.generation_identifier.clone(),
            cluster_name: record.deployment_request_identity.cluster_name.clone(),
            node_name: record.deployment_request_identity.node_name.clone(),
            deployment_phase,
            event_log_position: allocation.next_event_log_position.into(),
            optional_immutable_revision: record
                .deployment_request_identity
                .optional_immutable_revision
                .clone(),
            optional_deployment_terminal: Some(deployment_terminal),
            transition_ordinal: TransitionOrdinal::new(transition_ordinal),
            optional_transition_marker: None,
            transition_intent_state: TransitionIntentState::Pending,
        };
        let mut commit = self
            .database
            .begin_atomic_commit()
            .mutate(
                self.identifier_allocation,
                IdentifierAllocation {
                    next_deployment_identifier: allocation.next_deployment_identifier,
                    next_generation_identifier: allocation.next_generation_identifier,
                    next_event_log_position: next_identifier(allocation.next_event_log_position)?,
                },
            )
            .mutate(self.deployment_records, record)
            .assert(self.pending_transition_intents, intent);
        if let Some(mut job) = self
            .deploy_jobs()?
            .into_iter()
            .find(|job| *job.deployment_identifier.payload() == deployment_identifier)
        {
            job.deploy_job_phase = Self::terminal_job_phase(deployment_lifecycle);
            job.deploy_resume_stage = crate::runtime_model::DeployResumeStage::FinishDeployment;
            commit = commit.mutate(self.deploy_jobs, job);
        }
        self.database.commit_atomic(commit)?;
        Ok(transition_ordinal)
    }

    /// Complete a terminal transition and return the record only after its
    /// terminal journal/outbox acknowledgement has committed.
    pub fn terminalize_deployment(
        &self,
        deployment_identifier: u64,
        deployment_lifecycle: DeploymentLifecycle,
        deployment_terminal: DeploymentTerminal,
    ) -> Result<DeploymentRecord> {
        let ordinal = self.begin_terminal_transition(
            deployment_identifier,
            deployment_lifecycle,
            deployment_terminal,
        )?;
        self.complete_pending_transition_intent(deployment_identifier, ordinal)?;
        let record = self
            .deployment_records_unchecked()?
            .into_iter()
            .find(|record| *record.deployment_identifier.payload() == deployment_identifier)
            .ok_or_else(|| {
                Error::Invariant("terminal record disappeared after acknowledgement".to_string())
            })?;
        if record.optional_terminal_marker.is_none() {
            return Err(Error::Invariant(
                "acknowledged terminal record lacks its exact transition marker".to_string(),
            ));
        }
        self.maintain_event_history()?;
        Ok(record)
    }

    /// Preflight/policy rejection creates the correlation record and terminal
    /// intent in one transaction.  Unlike an accepted deployment it has no
    /// restart job, but its terminal journal is still locally acknowledged
    /// before a rejection handle can leave the daemon.
    pub fn begin_rejected_deployment_request(
        &self,
        deployment_request_identity: DeploymentRequestIdentity,
        deployment_terminal: DeploymentTerminal,
    ) -> Result<DeploymentRecord> {
        let _write = self.lock_write()?;
        let allocation = self.identifier_allocation()?;
        let record = DeploymentRecord {
            deployment_identifier: allocation.next_deployment_identifier.into(),
            generation_identifier: allocation.next_generation_identifier.into(),
            deployment_request_identity,
            optional_admission_marker: None,
            deployment_lifecycle: DeploymentLifecycle::Rejected,
            optional_terminal_marker: None,
            optional_deployment_terminal: Some(deployment_terminal.clone()),
        };
        let intent = PendingTransitionIntent {
            deployment_identifier: record.deployment_identifier.clone(),
            generation_identifier: record.generation_identifier.clone(),
            cluster_name: record.deployment_request_identity.cluster_name.clone(),
            node_name: record.deployment_request_identity.node_name.clone(),
            deployment_phase: crate::runtime_model::DeploymentPhase::Rejected,
            event_log_position: allocation.next_event_log_position.into(),
            optional_immutable_revision: record
                .deployment_request_identity
                .optional_immutable_revision
                .clone(),
            optional_deployment_terminal: Some(deployment_terminal),
            transition_ordinal: TransitionOrdinal::new(0),
            optional_transition_marker: None,
            transition_intent_state: TransitionIntentState::Pending,
        };
        self.database.commit_atomic(
            self.database
                .begin_atomic_commit()
                .mutate(
                    self.identifier_allocation,
                    IdentifierAllocation {
                        next_deployment_identifier: next_identifier(
                            allocation.next_deployment_identifier,
                        )?,
                        next_generation_identifier: next_identifier(
                            allocation.next_generation_identifier,
                        )?,
                        next_event_log_position: next_identifier(
                            allocation.next_event_log_position,
                        )?,
                    },
                )
                .assert(self.deployment_records, record.clone())
                .assert(self.pending_transition_intents, intent),
        )?;
        Ok(record)
    }

    pub fn reject_deployment_request(
        &self,
        deployment_request_identity: DeploymentRequestIdentity,
        deployment_terminal: DeploymentTerminal,
    ) -> Result<DeploymentRecord> {
        let record = self
            .begin_rejected_deployment_request(deployment_request_identity, deployment_terminal)?;
        self.complete_pending_transition_intent(*record.deployment_identifier.payload(), 0)?;
        self.deployment_records_unchecked()?
            .into_iter()
            .find(|current| current.deployment_identifier == record.deployment_identifier)
            .ok_or_else(|| {
                Error::Invariant("rejected record disappeared after acknowledgement".to_string())
            })
    }

    /// Every admitted, rejected, or failed deployment is retained here for
    /// correlation queries; in-flight job rows are a separate resume
    /// convenience and may be retired at terminal state.
    pub fn deployment_records(&self) -> Result<Vec<DeploymentRecord>> {
        self.deployment_records_unchecked()
    }

    /// Persist the safe immutable revision once flake resolution supplies it.
    /// This updates the correlation record before subsequent generation and
    /// terminal projections consume the same value.
    pub fn set_deployment_immutable_revision(
        &self,
        deployment_identifier: u64,
        immutable_revision: crate::runtime_model::ImmutableRevision,
    ) -> Result<()> {
        let _write = self.lock_write()?;
        let mut record = self
            .deployment_records_unchecked()?
            .into_iter()
            .find(|record| *record.deployment_identifier.payload() == deployment_identifier)
            .ok_or_else(|| {
                Error::Invariant(format!(
                    "deployment record {deployment_identifier} is missing"
                ))
            })?;
        record
            .deployment_request_identity
            .optional_immutable_revision = Some(immutable_revision);
        self.database
            .mutate(Mutation::new(self.deployment_records, record))?;
        Ok(())
    }

    /// Persist the resolved immutable identity and the exact restart cursor in
    /// one commit before the pipeline advances. A crash cannot leave a record
    /// pinned to one revision while its job replays a different mutable ref.
    pub fn record_resolved_source(
        &self,
        deployment_identifier: u64,
        immutable_revision: crate::runtime_model::ImmutableRevision,
        deploy_job: DeployJob,
    ) -> Result<()> {
        validate_fresh_deploy_job(&deploy_job)?;
        let _write = self.lock_write()?;
        let mut record = self
            .deployment_records_unchecked()?
            .into_iter()
            .find(|record| *record.deployment_identifier.payload() == deployment_identifier)
            .ok_or_else(|| {
                Error::Invariant(format!(
                    "deployment record {deployment_identifier} is missing"
                ))
            })?;
        if *deploy_job.deployment_identifier.payload() != deployment_identifier {
            return Err(Error::Invariant(
                "resolved source job has a different deployment identity".to_string(),
            ));
        }
        record
            .deployment_request_identity
            .optional_immutable_revision = Some(immutable_revision);
        self.database.commit_atomic(
            self.database
                .begin_atomic_commit()
                .mutate(self.deployment_records, record)
                .mutate(self.deploy_jobs, deploy_job),
        )?;
        Ok(())
    }

    /// The next subscription token: an in-memory atomic fetch-add. Subscriptions
    /// do not survive restart, so an ephemeral counter is correct (decision 5).
    pub fn next_subscription_token(&self) -> u64 {
        self.subscription_sequence.fetch_add(1, Ordering::SeqCst) + 1
    }

    /// Append one non-deployment event-log entry, keyed by its position.
    ///
    /// Deployment events are deliberately excluded: they must originate from
    /// a `PendingTransitionIntent`, acquire their marker from that exact
    /// commit, and be locally acknowledged through the durable outbox before
    /// a caller can continue the deployment pipeline.
    pub fn append_event_log_entry(&self, entry: EventLogEntry) -> Result<()> {
        if matches!(
            entry.logged_event,
            crate::runtime_model::LoggedEvent::Deployment(_)
        ) {
            return Err(Error::Invariant(
                "deployment journal entries require a pending transition intent".to_string(),
            ));
        }
        {
            let _write = self.lock_write()?;
            let allocation = self.identifier_allocation()?;
            let position = *entry.event_log_position.payload();
            let mut commit = self
                .database
                .begin_atomic_commit()
                .assert(self.event_log, entry);
            if position >= allocation.next_event_log_position {
                commit = commit.mutate(
                    self.identifier_allocation,
                    IdentifierAllocation {
                        next_deployment_identifier: allocation.next_deployment_identifier,
                        next_generation_identifier: allocation.next_generation_identifier,
                        next_event_log_position: next_identifier(position)?,
                    },
                );
            }
            self.database.commit_atomic(commit)?;
        }
        self.maintain_event_history()?;
        Ok(())
    }

    /// Atomically persist one intermediate record/job transition together with
    /// its write-ahead intent.  The public event is deliberately absent from
    /// this commit: its marker is bound from this exact logged transaction and
    /// delivery is acknowledged before this method returns to the runtime.
    pub fn begin_deployment_phase_transition(
        &self,
        deployment_identifier: u64,
        deployment_lifecycle: DeploymentLifecycle,
        deploy_job: DeployJob,
        event: crate::runtime_model::DeploymentPhaseEvent,
    ) -> Result<u64> {
        if matches!(
            deployment_lifecycle,
            DeploymentLifecycle::Completed
                | DeploymentLifecycle::Rejected
                | DeploymentLifecycle::Failed
        ) {
            return Err(Error::Invariant(
                "terminal deployment phases require a terminal correlation transition".to_string(),
            ));
        }
        validate_fresh_deploy_job(&deploy_job)?;
        let _write = self.lock_write()?;
        if *deploy_job.deployment_identifier.payload() != deployment_identifier {
            return Err(Error::Invariant(
                "deployment phase job identity does not match its correlation record".to_string(),
            ));
        }
        if *event.deployment_identifier.payload() != deployment_identifier {
            return Err(Error::Invariant(
                "deployment phase event identity does not match its correlation record".to_string(),
            ));
        }
        let mut record = self
            .deployment_records_unchecked()?
            .into_iter()
            .find(|record| *record.deployment_identifier.payload() == deployment_identifier)
            .ok_or_else(|| {
                Error::Invariant(format!(
                    "deployment record {deployment_identifier} is missing"
                ))
            })?;
        let job_present = self
            .deploy_jobs()?
            .into_iter()
            .any(|job| *job.deployment_identifier.payload() == deployment_identifier);
        if !job_present {
            return Err(Error::Invariant(format!(
                "deployment job {deployment_identifier} is missing for phase commit"
            )));
        }
        if record.generation_identifier != event.generation_identifier
            || record.deployment_request_identity.cluster_name != event.cluster_name
            || record.deployment_request_identity.node_name != event.node_name
        {
            return Err(Error::Invariant(
                "deployment phase event does not match its correlation identity".to_string(),
            ));
        }
        let intents = self.pending_transition_intents()?;
        if intents.iter().any(|intent| {
            *intent.deployment_identifier.payload() == deployment_identifier
                && !matches!(
                    intent.transition_intent_state,
                    TransitionIntentState::Acknowledged
                )
        }) {
            return Err(Error::Invariant(
                "cannot advance deployment while an earlier transition lacks local acknowledgement"
                    .to_string(),
            ));
        }
        if self
            .event_log_entries()?
            .iter()
            .any(|entry| entry.event_log_position == event.event_log_position)
        {
            return Err(Error::Invariant(
                "deployment phase event-log position is already occupied".to_string(),
            ));
        }
        let transition_ordinal = intents
            .iter()
            .filter(|intent| *intent.deployment_identifier.payload() == deployment_identifier)
            .map(|intent| *intent.transition_ordinal.payload())
            .max()
            .map(next_identifier)
            .transpose()?
            .unwrap_or(0);
        record.deployment_lifecycle = deployment_lifecycle;
        record.optional_deployment_terminal = None;
        record.optional_terminal_marker = None;
        let intent = PendingTransitionIntent {
            deployment_identifier: event.deployment_identifier.clone(),
            generation_identifier: event.generation_identifier.clone(),
            cluster_name: event.cluster_name.clone(),
            node_name: event.node_name.clone(),
            deployment_phase: event.deployment_phase,
            event_log_position: event.event_log_position,
            optional_immutable_revision: event.optional_immutable_revision,
            optional_deployment_terminal: None,
            transition_ordinal: TransitionOrdinal::new(transition_ordinal),
            optional_transition_marker: None,
            transition_intent_state: TransitionIntentState::Pending,
        };
        self.database.commit_atomic(
            self.database
                .begin_atomic_commit()
                .mutate(self.deployment_records, record)
                .mutate(self.deploy_jobs, deploy_job)
                .assert(self.pending_transition_intents, intent),
        )?;
        Ok(transition_ordinal)
    }

    /// Complete the acknowledged delivery half of one intermediate phase.  No
    /// caller receives a phase receipt, and therefore no next effect becomes
    /// eligible, until this returns the exact marker from the durable intent.
    pub fn advance_deployment_phase(
        &self,
        deployment_identifier: u64,
        deployment_lifecycle: DeploymentLifecycle,
        deploy_job: DeployJob,
        event: crate::runtime_model::DeploymentPhaseEvent,
    ) -> Result<StateMarker> {
        let transition_ordinal = self.begin_deployment_phase_transition(
            deployment_identifier,
            deployment_lifecycle,
            deploy_job,
            event,
        )?;
        self.complete_pending_transition_intent(deployment_identifier, transition_ordinal)?;
        let intent = self.intent_from_key(deployment_identifier, transition_ordinal)?;
        if !matches!(
            intent.transition_intent_state,
            TransitionIntentState::Acknowledged
        ) {
            return Err(Error::Invariant(
                "deployment phase delivery returned without local acknowledgement".to_string(),
            ));
        }
        self.maintain_event_history()?;
        intent
            .optional_transition_marker
            .map(TransitionMarker::into_payload)
            .ok_or_else(|| Error::Invariant("acknowledged phase intent lacks a marker".to_string()))
    }

    pub fn record_deployment_phase(
        &self,
        mut entry: EventLogEntry,
        deployment_lifecycle: DeploymentLifecycle,
        deploy_job: DeployJob,
    ) -> Result<EventLogEntry> {
        let event = match &entry.logged_event {
            crate::runtime_model::LoggedEvent::Deployment(event) => event.clone(),
            _ => {
                return Err(Error::Invariant(
                    "deployment phase commit requires a deployment event".to_string(),
                ));
            }
        };
        let marker = self.advance_deployment_phase(
            *event.deployment_identifier.payload(),
            deployment_lifecycle,
            deploy_job,
            event,
        )?;
        if let crate::runtime_model::LoggedEvent::Deployment(event) = &mut entry.logged_event {
            event.state_marker = marker;
        }
        Ok(entry)
    }

    /// Append one live generation, keyed by its generation identifier
    /// (decision 4).
    pub fn append_live_generation(&self, generation: LiveGeneration) -> Result<()> {
        if !canonical_nix_store_root(generation.closure_path.payload()) {
            return Err(Error::Invariant(
                "fresh live generation requires a canonical immutable store-item root".to_string(),
            ));
        }
        let _write = self.lock_write()?;
        let allocation = self.identifier_allocation()?;
        let deployment_identifier = *generation.deployment_identifier.payload();
        let generation_identifier = *generation.generation_identifier.payload();
        self.database.commit_atomic(
            self.database
                .begin_atomic_commit()
                .assert(self.live_set, generation)
                .mutate(
                    self.identifier_allocation,
                    IdentifierAllocation {
                        next_deployment_identifier: allocation
                            .next_deployment_identifier
                            .max(next_identifier(deployment_identifier)?),
                        next_generation_identifier: allocation
                            .next_generation_identifier
                            .max(next_identifier(generation_identifier)?),
                        next_event_log_position: allocation.next_event_log_position,
                    },
                ),
        )?;
        Ok(())
    }

    /// Record the live generation and its GC root as one durable commit.
    pub fn record_activation(&self, generation: LiveGeneration, root: GcRoot) -> Result<()> {
        if !canonical_nix_store_root(generation.closure_path.payload())
            || !canonical_nix_store_root(root.closure_path.payload())
        {
            return Err(Error::Invariant(
                "fresh activation requires a canonical immutable store-item root".to_string(),
            ));
        }
        let _write = self.lock_write()?;
        let allocation = self.identifier_allocation()?;
        let deployment_identifier = *generation.deployment_identifier.payload();
        let generation_identifier = *generation.generation_identifier.payload();
        let prior_current: std::collections::BTreeSet<u64> = if matches!(
            generation.generation_slot,
            crate::runtime_model::GenerationSlot::Current
        ) {
            self.live_generations()?
                .into_iter()
                .filter(|existing| {
                    existing.cluster_name == generation.cluster_name
                        && existing.node_name == generation.node_name
                        && existing.deployment_environment == generation.deployment_environment
                        && matches!(
                            existing.generation_slot,
                            crate::runtime_model::GenerationSlot::Current
                        )
                })
                .map(|existing| *existing.generation_identifier.payload())
                .collect()
        } else {
            std::collections::BTreeSet::new()
        };
        let roots = self.gc_root_records()?;
        let mut commit = self
            .database
            .begin_atomic_commit()
            .assert(self.live_set, generation)
            .assert(self.gc_roots, root)
            .mutate(
                self.identifier_allocation,
                IdentifierAllocation {
                    next_deployment_identifier: allocation
                        .next_deployment_identifier
                        .max(next_identifier(deployment_identifier)?),
                    next_generation_identifier: allocation
                        .next_generation_identifier
                        .max(next_identifier(generation_identifier)?),
                    next_event_log_position: allocation.next_event_log_position,
                },
            );
        for mut existing in self.live_generations()? {
            if prior_current.contains(existing.generation_identifier.payload()) {
                existing.generation_slot = crate::runtime_model::GenerationSlot::Recent;
                commit = commit.mutate(self.live_set, existing);
            }
        }
        for mut existing in roots {
            if prior_current.contains(existing.generation_identifier.payload()) {
                existing.generation_slot = crate::runtime_model::GenerationSlot::Recent;
                commit = commit.mutate(self.gc_roots, existing);
            }
        }
        self.database.commit_atomic(commit)?;
        Ok(())
    }

    /// Append one GC-root, keyed by its generation identifier (decision 4).
    pub fn append_gc_root(&self, root: GcRoot) -> Result<()> {
        validate_fresh_gc_root(&root)?;
        let _write = self.lock_write()?;
        self.database.assert(Assertion::new(self.gc_roots, root))?;
        Ok(())
    }

    /// Overwrite one GC-root in place (a slot/label change), keyed by its
    /// generation identifier.
    pub fn mutate_gc_root(&self, root: GcRoot) -> Result<()> {
        validate_fresh_gc_root(&root)?;
        let _write = self.lock_write()?;
        self.database.mutate(Mutation::new(self.gc_roots, root))?;
        Ok(())
    }

    /// Drop one GC-root by its generation identifier.
    pub fn retract_gc_root(&self, generation_identifier: u64) -> Result<()> {
        let _write = self.lock_write()?;
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

    /// Record a container observation and its matching event in one durable commit.
    pub fn record_container_transition(
        &self,
        record: ContainerLifecycleRecord,
        entry: EventLogEntry,
    ) -> Result<()> {
        {
            let _write = self.lock_write()?;
            self.database.commit_atomic(
                self.database
                    .begin_atomic_commit()
                    .assert(self.containers, record)
                    .assert(self.event_log, entry),
            )?;
        }
        self.maintain_event_history()?;
        Ok(())
    }

    /// The persisted in-flight deploy-job rows — read on daemon start so an
    /// in-flight deploy resumes from its recorded phase rather than being lost
    /// with the connection that submitted it (up9q). A finished or failed job
    /// is retracted on completion, so a steady-state store reads back empty.
    pub fn deploy_jobs(&self) -> Result<Vec<DeployJob>> {
        let jobs = self
            .database
            .match_records(QueryPlan::all(self.deploy_jobs))?
            .records()
            .to_vec();
        for job in &jobs {
            validate_persisted_deploy_job(job)?;
        }
        Ok(jobs)
    }

    /// Write or rewrite one deploy-job row, keyed by its deployment identifier.
    /// `assert` on the first (submit) write, `mutate` to overwrite the existing
    /// row on each phase transition — so the persisted phase cursor always
    /// reflects the latest committed step (up9q durable resume).
    pub fn upsert_deploy_job(&self, job: DeployJob) -> Result<()> {
        validate_fresh_deploy_job(&job)?;
        let _write = self.lock_write()?;
        let key = *job.deployment_identifier.payload();
        let present = self
            .deploy_jobs()?
            .iter()
            .any(|existing| *existing.deployment_identifier.payload() == key);
        if present {
            self.database.mutate(Mutation::new(self.deploy_jobs, job))?;
        } else {
            self.database
                .assert(Assertion::new(self.deploy_jobs, job))?;
        }
        Ok(())
    }

    /// Drop one deploy-job row by its deployment identifier — called when the
    /// deploy reaches a terminal phase (Activated or Failed), so the in-flight
    /// mirror tracks only deploys that still need resuming. A no-op when the
    /// row is already absent.
    pub fn retract_deploy_job(&self, deployment_identifier: u64) -> Result<()> {
        let _write = self.lock_write()?;
        let present = self
            .deploy_jobs()?
            .iter()
            .any(|existing| *existing.deployment_identifier.payload() == deployment_identifier);
        if present {
            self.database.retract(Retraction::new(
                self.deploy_jobs,
                RecordKey::new(deployment_identifier.to_string()),
            ))?;
        }
        Ok(())
    }

    /// The persisted test-run rows — read by the ordinary `(ByTestRun …)`
    /// query and on daemon start so an in-flight test reconciles rather than
    /// being lost (report 54 §5.3). A row is keyed by its `TestRunIdentifier`.
    pub fn test_runs(&self) -> Result<Vec<StoredTestRun>> {
        Ok(self
            .database
            .match_records(QueryPlan::all(self.test_runs))?
            .records()
            .to_vec())
    }

    /// The next test-run identifier: one past the maximum persisted, or 1 when
    /// empty. Restart-safe — derived from the durable rows, not a RAM counter,
    /// mirroring `next_deployment_identifier`.
    pub fn next_test_run_identifier(&self) -> Result<u64> {
        Ok(self
            .test_runs()?
            .iter()
            .map(|run| *run.test_run_identifier.payload())
            .max()
            .map(|maximum| maximum + 1)
            .unwrap_or(1))
    }

    /// Write or rewrite one test-run row, keyed by its run identifier. `assert`
    /// on the first (accept) write, `mutate` to overwrite the existing row at
    /// each later phase transition (Unit 2b), so the persisted outcome always
    /// reflects the latest committed step. Mirrors `upsert_deploy_job`.
    pub fn upsert_test_run(&self, run: StoredTestRun) -> Result<()> {
        let _write = self.lock_write()?;
        let key = *run.test_run_identifier.payload();
        let present = self
            .test_runs()?
            .iter()
            .any(|existing| *existing.test_run_identifier.payload() == key);
        if present {
            self.database.mutate(Mutation::new(self.test_runs, run))?;
        } else {
            self.database.assert(Assertion::new(self.test_runs, run))?;
        }
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

impl EngineRecord for DeployJob {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(self.deployment_identifier.payload().to_string())
    }
}

impl EngineRecord for DeploymentRecord {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(self.deployment_identifier.payload().to_string())
    }
}

impl EngineRecord for DeploymentOutboxRecord {
    fn record_key(&self) -> RecordKey {
        Store::outbox_key(&self.deployment_identifier, &self.transition_marker)
    }
}

impl EngineRecord for PendingTransitionIntent {
    fn record_key(&self) -> RecordKey {
        Store::transition_intent_key(self)
    }
}

impl EngineRecord for IdentifierAllocation {
    fn record_key(&self) -> RecordKey {
        RecordKey::new("global")
    }
}

impl EngineRecord for StoredTestRun {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(self.test_run_identifier.payload().to_string())
    }
}

#[cfg(test)]
mod transition_intent_tests {
    use super::*;
    use crate::runtime_model as ordinary;

    fn admission_identity() -> ordinary::DeploymentRequestIdentity {
        ordinary::DeploymentRequestIdentity {
            deployment_environment: ordinary::DeploymentEnvironment::HostEnvironment,
            cluster_name: ordinary::ClusterName::new("cluster"),
            node_name: ordinary::NodeName::new("node"),
            generation_artifact: ordinary::GenerationArtifact::BaseHost,
            requested_deployment_action: ordinary::RequestedDeploymentAction::Host(
                ordinary::HostDeployAction::Evaluate,
            ),
            activation_effect: ordinary::ActivationEffect::ProfileOnly,
            source_revision_policy: ordinary::SourceRevisionPolicy::RequireImmutable,
            optional_immutable_revision: Some(ordinary::ImmutableRevision::new(
                "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            )),
        }
    }

    fn admission_job() -> ordinary::DeployJob {
        ordinary::DeployJob {
            deployment_identifier: ordinary::DeploymentIdentifier::new(0),
            generation_identifier: ordinary::GenerationIdentifier::new(0),
            cluster_name: ordinary::ClusterName::new("cluster"),
            node_name: ordinary::NodeName::new("node"),
            deploy_job_phase: ordinary::DeployJobPhase::Submitted,
            optional_closure_path: None,
            source_revision_policy: ordinary::SourceRevisionPolicy::RequireImmutable,
            flake_reference: ordinary::FlakeReference::new(
                "github:owner/repo?rev=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            ),
            optional_flake_reference: None,
            resolved_revision: None,
            deployment_transport: ordinary::DeploymentTransport {
                nix_store_uri: ordinary::NixStoreUri::new("ssh-ng://fixture-copy.invalid"),
                ssh_destination: ordinary::SshDestination::new(
                    "fixture-login@fixture-activate.invalid",
                ),
            },
            deployment_input_mode: ordinary::DeploymentInputMode::Direct,
            deployment_output_selector: ordinary::DeploymentOutputSelector::new(
                ordinary::FlakeAttribute::new("checks.fixture-a"),
            ),
            activation_backend: ordinary::ActivationBackend::NixosSystemdBootV1,
            optional_nix_builder_spec: None,
            boot_once_unit: None,
            optional_generation_slot: None,
            persisted_flake_input_override_vector: Vec::new(),
            deploy_resume_stage: ordinary::DeployResumeStage::ResolveFlakeAuth,
            optional_phase_receipt: None,
            optional_deploy_submission: None,
        }
    }

    #[test]
    fn fresh_job_and_gc_root_writes_reject_noncanonical_closure_paths() {
        let directory = tempfile::TempDir::new().expect("temporary store directory");
        let store = Store::open(directory.path().join("lojix.sema")).expect("open store");
        let unsafe_path = ordinary::ClosurePath::new(
            "/nix/store/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-private-secret",
        );
        let mut job = admission_job();
        job.optional_closure_path = Some(unsafe_path.clone());
        assert!(
            store
                .begin_admission_transition(admission_identity(), job)
                .is_err()
        );
        let root = ordinary::GcRoot {
            generation_identifier: ordinary::GenerationIdentifier::new(1),
            cluster_name: ordinary::ClusterName::new("cluster"),
            node_name: ordinary::NodeName::new("node"),
            generation_slot: ordinary::GenerationSlot::Current,
            closure_path: unsafe_path,
            optional_pin_label: None,
        };
        assert!(store.append_gc_root(root.clone()).is_err());
        assert!(store.mutate_gc_root(root).is_err());
    }

    #[test]
    fn reopen_rejects_an_unsafe_resumable_job_closure() {
        let directory = tempfile::TempDir::new().expect("temporary store directory");
        let path = directory.path().join("lojix.sema");
        let store = Store::open(&path).expect("open store");
        let record = store
            .allocate_deployment_record(admission_identity(), admission_job())
            .expect("admit deployment");
        let mut job = store
            .deploy_jobs()
            .expect("read restart cursor")
            .into_iter()
            .find(|job| job.deployment_identifier == record.deployment_identifier)
            .expect("one restart cursor");
        job.optional_closure_path = Some(ordinary::ClosurePath::new(
            "/nix/store/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-private-secret",
        ));
        store
            .database
            .mutate(Mutation::new(store.deploy_jobs, job))
            .expect("inject malformed persisted row");
        drop(store);

        assert!(Store::open(&path).is_err());
    }

    fn building_event(
        record: &ordinary::DeploymentRecord,
        position: u64,
    ) -> ordinary::DeploymentPhaseEvent {
        ordinary::DeploymentPhaseEvent {
            deployment_identifier: record.deployment_identifier.clone(),
            generation_identifier: record.generation_identifier.clone(),
            cluster_name: record.deployment_request_identity.cluster_name.clone(),
            node_name: record.deployment_request_identity.node_name.clone(),
            deployment_phase: ordinary::DeploymentPhase::Building,
            event_log_position: ordinary::EventLogPosition::new(position),
            state_marker: state_marker(0),
            optional_immutable_revision: record
                .deployment_request_identity
                .optional_immutable_revision
                .clone(),
            optional_deployment_terminal: None,
        }
    }

    #[test]
    fn admission_intent_recovery_is_exactly_once_at_every_crash_window() {
        for crash_after in ["intent", "bind", "dispatch", "journal"] {
            let directory = tempfile::TempDir::new().expect("temporary store directory");
            let path = directory.path().join("lojix.sema");
            let deployment_identifier = {
                let store = Store::open(&path).expect("open store");
                let record = store
                    .begin_admission_transition(admission_identity(), admission_job())
                    .expect("atomically create admission record and intent");
                let deployment_identifier = *record.deployment_identifier.payload();
                match crash_after {
                    "intent" => {}
                    "bind" => {
                        store
                            .bind_transition_intent(deployment_identifier, 0)
                            .expect("bind exact durable receipt and outbox");
                    }
                    "dispatch" => {
                        let intent = store
                            .bind_transition_intent(deployment_identifier, 0)
                            .expect("bind exact durable receipt and outbox");
                        store
                            .dispatch_transition_intent(&intent)
                            .expect("persist dispatched state");
                    }
                    "journal" => {
                        let intent = store
                            .bind_transition_intent(deployment_identifier, 0)
                            .expect("bind exact durable receipt and outbox");
                        let intent = store
                            .dispatch_transition_intent(&intent)
                            .expect("persist dispatched state");
                        store
                            .append_transition_intent_journal(&intent)
                            .expect("append exact journal row before acknowledgement");
                    }
                    _ => unreachable!(),
                }
                deployment_identifier
            };

            let store = Store::open(&path).expect("reopen recovers admission intent");
            assert_eq!(
                store
                    .deploy_jobs()
                    .expect("read durable restart cursor")
                    .len(),
                1,
                "{crash_after} retains the admission restart cursor"
            );
            let intents = store
                .pending_transition_intents()
                .expect("read recovered transition intent");
            assert!(matches!(
                intents[0].transition_intent_state,
                TransitionIntentState::Acknowledged
            ));
            let marker = intents[0]
                .optional_transition_marker
                .as_ref()
                .expect("intent is bound to its source commit");
            let journal: Vec<_> = store
                .event_log_entries()
                .expect("read journal")
                .into_iter()
                .filter(|entry| match &entry.logged_event {
                    ordinary::LoggedEvent::Deployment(event) => {
                        *event.deployment_identifier.payload() == deployment_identifier
                            && event.state_marker == marker.payload().clone()
                    }
                    _ => false,
                })
                .collect();
            assert_eq!(journal.len(), 1, "{crash_after} replays one exact event");
            let outbox: Vec<_> = store
                .deployment_outbox()
                .expect("read outbox")
                .into_iter()
                .filter(|record| {
                    *record.deployment_identifier.payload() == deployment_identifier
                        && record.transition_marker == marker.clone()
                })
                .collect();
            assert_eq!(outbox.len(), 1, "{crash_after} retains one delivery row");
            assert!(matches!(
                outbox[0].outbox_delivery_state,
                OutboxDeliveryState::Acknowledged
            ));
        }
    }

    #[test]
    fn reopening_after_acknowledged_transition_compaction_does_not_redeliver() {
        let directory = tempfile::TempDir::new().expect("temporary store directory");
        let path = directory.path().join("lojix.sema");
        let store = Store::open(&path).expect("open store");
        let record = store
            .allocate_deployment_record(admission_identity(), admission_job())
            .expect("admit deployment");
        let deployment_identifier = *record.deployment_identifier.payload();
        assert_eq!(
            store
                .compact_event_history(EventLogRetention::new(0))
                .expect("compact acknowledged admission"),
            1
        );
        assert!(
            store
                .event_log_entries()
                .expect("journal compacted")
                .is_empty()
        );
        assert!(
            store
                .deployment_outbox()
                .expect("outbox compacted")
                .is_empty()
        );
        let commit_before_reopen = store.commit_sequence().expect("commit sequence");
        drop(store);

        let reopened = Store::open(&path).expect("reopen compacted store");
        assert_eq!(
            reopened.commit_sequence().expect("commit sequence"),
            commit_before_reopen,
            "reopen must not redeliver an acknowledged compacted transition"
        );
        assert!(
            reopened
                .event_log_entries()
                .expect("journal remains compacted")
                .is_empty()
        );
        assert!(
            reopened
                .deployment_outbox()
                .expect("outbox remains compacted")
                .is_empty()
        );
        assert!(
            reopened
                .pending_transition_intents()
                .expect("intent remains durable")
                .iter()
                .any(|intent| {
                    *intent.deployment_identifier.payload() == deployment_identifier
                        && matches!(
                            intent.transition_intent_state,
                            TransitionIntentState::Acknowledged
                        )
                })
        );
    }

    #[test]
    fn intermediate_phase_intent_recovery_acknowledges_before_continuation() {
        for crash_after in ["intent", "bind", "dispatch", "journal"] {
            let directory = tempfile::TempDir::new().expect("temporary store directory");
            let path = directory.path().join("lojix.sema");
            let deployment_identifier = {
                let store = Store::open(&path).expect("open store");
                let record = store
                    .allocate_deployment_record(admission_identity(), admission_job())
                    .expect("admit deployment and restart cursor");
                let position = store
                    .allocate_event_log_position()
                    .expect("reserve building journal position");
                let mut job = admission_job();
                job.deployment_identifier = record.deployment_identifier.clone();
                job.generation_identifier = record.generation_identifier.clone();
                job.deploy_job_phase = ordinary::DeployJobPhase::Building;
                job.deploy_resume_stage = ordinary::DeployResumeStage::NixEval;
                let ordinal = store
                    .begin_deployment_phase_transition(
                        *record.deployment_identifier.payload(),
                        ordinary::DeploymentLifecycle::Building,
                        job,
                        building_event(&record, position),
                    )
                    .expect("atomically persist phase state and intent");
                assert_eq!(ordinal, 1, "admission owns ordinal zero");
                match crash_after {
                    "intent" => {}
                    "bind" => {
                        store
                            .bind_transition_intent(
                                *record.deployment_identifier.payload(),
                                ordinal,
                            )
                            .expect("bind phase receipt and outbox");
                    }
                    "dispatch" => {
                        let intent = store
                            .bind_transition_intent(
                                *record.deployment_identifier.payload(),
                                ordinal,
                            )
                            .expect("bind phase receipt and outbox");
                        store
                            .dispatch_transition_intent(&intent)
                            .expect("persist dispatched phase delivery");
                    }
                    "journal" => {
                        let intent = store
                            .bind_transition_intent(
                                *record.deployment_identifier.payload(),
                                ordinal,
                            )
                            .expect("bind phase receipt and outbox");
                        let intent = store
                            .dispatch_transition_intent(&intent)
                            .expect("persist dispatched phase delivery");
                        store
                            .append_transition_intent_journal(&intent)
                            .expect("append phase journal before acknowledgement");
                    }
                    _ => unreachable!(),
                }
                *record.deployment_identifier.payload()
            };

            let store = Store::open(&path).expect("reopen recovers phase intent");
            let intents: Vec<_> = store
                .pending_transition_intents()
                .expect("read intents")
                .into_iter()
                .filter(|intent| *intent.deployment_identifier.payload() == deployment_identifier)
                .collect();
            assert_eq!(intents.len(), 2, "admission plus one phase intent");
            let building = intents
                .iter()
                .find(|intent| {
                    matches!(intent.deployment_phase, ordinary::DeploymentPhase::Building)
                })
                .expect("building intent");
            assert!(matches!(
                building.transition_intent_state,
                TransitionIntentState::Acknowledged
            ));
            let marker = building
                .optional_transition_marker
                .as_ref()
                .expect("building intent marker");
            let events: Vec<_> = store
                .event_log_entries()
                .expect("read journal")
                .into_iter()
                .filter(|entry| match &entry.logged_event {
                    ordinary::LoggedEvent::Deployment(event) => {
                        *event.deployment_identifier.payload() == deployment_identifier
                            && matches!(event.deployment_phase, ordinary::DeploymentPhase::Building)
                            && event.state_marker == marker.payload().clone()
                    }
                    _ => false,
                })
                .collect();
            assert_eq!(events.len(), 1, "{crash_after} records one building event");
            assert!(
                store
                    .deployment_outbox()
                    .expect("read outbox")
                    .iter()
                    .any(|outbox| {
                        *outbox.deployment_identifier.payload() == deployment_identifier
                            && outbox.transition_marker
                                == TransitionMarker::new(marker.payload().clone())
                            && matches!(
                                outbox.outbox_delivery_state,
                                OutboxDeliveryState::Acknowledged
                            )
                    })
            );
            let job = store
                .deploy_jobs()
                .expect("read restart cursor")
                .into_iter()
                .find(|job| *job.deployment_identifier.payload() == deployment_identifier)
                .expect("phase restart cursor");
            assert!(matches!(
                job.deploy_job_phase,
                ordinary::DeployJobPhase::Building
            ));
            assert!(matches!(
                job.deploy_resume_stage,
                ordinary::DeployResumeStage::NixEval
            ));
        }
    }

    #[test]
    fn terminal_intent_recovery_keeps_the_job_until_local_acknowledgement() {
        for crash_after in ["intent", "bind", "dispatch", "journal"] {
            let directory = tempfile::TempDir::new().expect("temporary store directory");
            let path = directory.path().join("lojix.sema");
            let deployment_identifier = {
                let store = Store::open(&path).expect("open store");
                let record = store
                    .allocate_deployment_record(admission_identity(), admission_job())
                    .expect("admit deployment");
                let deployment_identifier = *record.deployment_identifier.payload();
                let ordinal = store
                    .begin_terminal_transition(
                        deployment_identifier,
                        ordinary::DeploymentLifecycle::Completed,
                        ordinary::DeploymentTerminal::Succeeded,
                    )
                    .expect("atomically persist completed state and terminal intent");
                assert_eq!(ordinal, 1, "terminal follows admission ordinal zero");
                match crash_after {
                    "intent" => {}
                    "bind" => {
                        store
                            .bind_transition_intent(deployment_identifier, ordinal)
                            .expect("bind terminal receipt and outbox");
                    }
                    "dispatch" => {
                        let intent = store
                            .bind_transition_intent(deployment_identifier, ordinal)
                            .expect("bind terminal receipt and outbox");
                        store
                            .dispatch_transition_intent(&intent)
                            .expect("persist terminal dispatched state");
                    }
                    "journal" => {
                        let intent = store
                            .bind_transition_intent(deployment_identifier, ordinal)
                            .expect("bind terminal receipt and outbox");
                        let intent = store
                            .dispatch_transition_intent(&intent)
                            .expect("persist terminal dispatched state");
                        store
                            .append_transition_intent_journal(&intent)
                            .expect("append terminal journal before acknowledgement");
                    }
                    _ => unreachable!(),
                }
                assert!(
                    store
                        .deploy_jobs()
                        .expect("read pre-ack restart cursor")
                        .iter()
                        .any(|job| *job.deployment_identifier.payload() == deployment_identifier),
                    "terminal job survives until the acknowledgement commit"
                );
                deployment_identifier
            };

            let store = Store::open(&path).expect("reopen terminal recovery");
            let record = store
                .deployment_records()
                .expect("read terminal record")
                .into_iter()
                .find(|record| *record.deployment_identifier.payload() == deployment_identifier)
                .expect("terminal record");
            assert!(matches!(
                record.deployment_lifecycle,
                ordinary::DeploymentLifecycle::Completed
            ));
            let marker = record
                .optional_terminal_marker
                .as_ref()
                .expect("terminal marker is bound from source commit");
            assert!(
                store
                    .event_log_entries()
                    .expect("read terminal journal")
                    .iter()
                    .filter(|entry| match &entry.logged_event {
                        ordinary::LoggedEvent::Deployment(event) => {
                            *event.deployment_identifier.payload() == deployment_identifier
                                && matches!(
                                    event.deployment_phase,
                                    ordinary::DeploymentPhase::Completed
                                )
                                && event.state_marker == marker.payload().clone()
                        }
                        _ => false,
                    })
                    .count()
                    == 1
            );
            assert!(
                store
                    .deployment_outbox()
                    .expect("read terminal outbox")
                    .iter()
                    .any(|outbox| {
                        *outbox.deployment_identifier.payload() == deployment_identifier
                            && outbox.transition_marker
                                == TransitionMarker::new(marker.payload().clone())
                            && matches!(
                                outbox.outbox_delivery_state,
                                OutboxDeliveryState::Acknowledged
                            )
                    })
            );
            assert!(
                store
                    .deploy_jobs()
                    .expect("read post-ack job rows")
                    .iter()
                    .all(|job| *job.deployment_identifier.payload() != deployment_identifier)
            );
        }
    }

    #[test]
    fn preflight_rejection_intent_recovers_an_acknowledged_terminal_event() {
        for crash_after in ["intent", "bind", "dispatch", "journal"] {
            let directory = tempfile::TempDir::new().expect("temporary store directory");
            let path = directory.path().join("lojix.sema");
            let deployment_identifier = {
                let store = Store::open(&path).expect("open store");
                let record = store
                    .begin_rejected_deployment_request(
                        admission_identity(),
                        ordinary::DeploymentTerminal::Rejected(
                            ordinary::DeploymentTerminalReason::ProposalSourceUnreachable,
                        ),
                    )
                    .expect("atomically persist rejection record and intent");
                let deployment_identifier = *record.deployment_identifier.payload();
                match crash_after {
                    "intent" => {}
                    "bind" => {
                        store
                            .bind_transition_intent(deployment_identifier, 0)
                            .expect("bind rejection receipt and outbox");
                    }
                    "dispatch" => {
                        let intent = store
                            .bind_transition_intent(deployment_identifier, 0)
                            .expect("bind rejection receipt and outbox");
                        store
                            .dispatch_transition_intent(&intent)
                            .expect("persist rejection dispatched state");
                    }
                    "journal" => {
                        let intent = store
                            .bind_transition_intent(deployment_identifier, 0)
                            .expect("bind rejection receipt and outbox");
                        let intent = store
                            .dispatch_transition_intent(&intent)
                            .expect("persist rejection dispatched state");
                        store
                            .append_transition_intent_journal(&intent)
                            .expect("append rejection journal before acknowledgement");
                    }
                    _ => unreachable!(),
                }
                deployment_identifier
            };
            let store = Store::open(&path).expect("reopen rejection recovery");
            let intent = store
                .pending_transition_intents()
                .expect("read rejection intent")
                .into_iter()
                .find(|intent| *intent.deployment_identifier.payload() == deployment_identifier)
                .expect("rejection intent");
            assert!(matches!(
                intent.transition_intent_state,
                TransitionIntentState::Acknowledged
            ));
            assert!(
                store
                    .deployment_outbox()
                    .expect("read rejection outbox")
                    .iter()
                    .any(|outbox| {
                        *outbox.deployment_identifier.payload() == deployment_identifier
                            && outbox.transition_marker
                                == intent.optional_transition_marker.clone().expect("marker")
                            && matches!(
                                outbox.outbox_delivery_state,
                                OutboxDeliveryState::Acknowledged
                            )
                    })
            );
        }
    }
}
