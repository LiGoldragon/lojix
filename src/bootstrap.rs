//! Daemon-free, explicitly-authorized bootstrap pipeline.
//!
//! This is deliberately a separate ingress from the daemon wire contracts.
//! A bootstrap invocation owns every route, input, builder, output path and
//! activation backend; no socket, service configuration, old store, or
//! hostname-derived default is read. The request is the generated
//! `ingress::BootstrapRun`, used as it arrived: admission checks every field
//! before any effect, and the run reads the same request afterwards.
//!
//! A private journal is created below the request's journal parent, named by
//! the request's fingerprint. It holds one `BootstrapJournal` record
//! (`ethos/bootstrap_journal.ethos`): write-ahead intent, receipt and outcome
//! events, and what each stage produced. Every process a run starts is an
//! emission (`ethos/bootstrap_emission.ethos`) carried by the translator in
//! `bootstrap_translator.rs`. The run's one durable artefact is its terminal
//! evidence, atomically committed and directory-synced at the
//! caller-selected path; the finalized journal is retained beside it.

use crate::bootstrap_emission::{
    BootEntriesDirectory, BootOnceDispatch, BootOnceRoute, BootOnceScript, BootOnceUnitOutcome,
    BootOnceUnitQuery, BootOnceUnitState, BootstrapBuild, BootstrapCheckBuild, BootstrapCopy,
    BootstrapEvaluation, BootstrapRooting, ClosurePresenceOutcome, DerivationPath, Emittable as _,
    GeneratedInputDirectory, GeneratedInputHashing, PrintedPathOutcome, RemoteClosureQuery,
    RootStagingPath, SshConfigPath, SshHost, SshIdentity, SshPort, SshRoute, SshUser,
    SystemProfilePath,
};
use crate::bootstrap_journal::{
    BootstrapEffectEvidence, BootstrapEvidenceMode, BootstrapEvidenceStatus, BootstrapJournal,
    BootstrapStage, BootstrapTerminalEvidence, EffectSucceeded, EventSucceeded, FileDevice,
    FileIdentity, FileInode, GcRootReceipt, JournalDirectory, JournalEvent, JournalEventKind,
    JournalSchemaVersion, MaterializedInputs, RequestFingerprint, ResolvedFlakeReference,
};
pub use crate::bootstrap_translator::{
    BootstrapCommand, BootstrapExecutor, BootstrapTranslator, ProcessBootstrapExecutor,
};
use crate::bootstrap_translator::{BootstrapText as _, BootstrapTranslating as _};
use crate::emission::{CallOutcome, HashOutcome, SriHash, UnitName};
pub use crate::ingress::BootstrapRun;
use crate::ingress::{
    BootstrapActivationBackend, BootstrapBuilder, BootstrapBuilderSpec, BootstrapFlakeReference,
    BootstrapGcRootPath, BootstrapInput, BootstrapJournalParent, BootstrapLocalBootstrapV1,
    BootstrapMaterializationShape, BootstrapMode, BootstrapNixSystem, BootstrapOutputSelector,
    BootstrapRemoteNixosSystemdBootV1, BootstrapRequestId, BootstrapSecretsInput,
    BootstrapSshPolicy, BootstrapTerminalEvidencePath, BootstrapTestPlan,
};
use crate::inspected_text::{
    NixStorePath, OfferedPath, PathAdmission as _, PathFault, StoreItemShape as _,
};
use crate::memory::{
    FlakeInputName, FlakeInputUrl, NixArchiveHash, PersistedFlakeInputOverride,
    PersistedFlakeInputReference,
};
use crate::{HorizonArchitecture as _, InlineDatomArguments as _};
use std::collections::BTreeMap;
use std::ffi::OsString;
use std::fs;
use std::io::Write;
use std::os::unix::fs::{MetadataExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use datom_codec::{Actualizing, Potential};
use horizon_lib::{DatomDecoding, HorizonDefinition, Projecting};
use sha2::{Digest, Sha256};
use signal_lojix::ClosurePath;
use thiserror::Error;

/// The journal record's layout; a journal of another layout binds no request.
const JOURNAL_SCHEMA_VERSION: i64 = 6;
const JOURNAL_PREFIX: &str = ".lojix-bootstrap-v6-";
const TEMPORARY_EVIDENCE_PREFIX: &str = ".lojix-bootstrap-evidence-";
const JOURNAL_STATE_FILE: &str = "bootstrap-v6.rkyv";
const PRIVATE_DIRECTORY_MODE: u32 = 0o700;
const PRIVATE_EVIDENCE_MODE: u32 = 0o600;
/// How many times a scheduled boot-once unit's state is read, and how long a
/// run waits between two readings, before it leaves the unit to a resumed
/// run.
const BOOT_ONCE_READINGS: usize = 30;
const BOOT_ONCE_READING_INTERVAL: std::time::Duration = std::time::Duration::from_millis(100);

static JOURNAL_SEQUENCE: AtomicU64 = AtomicU64::new(0);

impl crate::Named for BootstrapEvidenceStatus {
    fn as_str(&self) -> &str {
        match self {
            Self::Succeeded => "Succeeded",
            Self::Failed => "Failed",
        }
    }
}

#[derive(Debug, Error)]
pub enum BootstrapError {
    #[error("bootstrap invocation requires one inline Datom object: {0}")]
    Argument(#[from] crate::Error),
    #[error("bootstrap Datom decode failed: {0}")]
    Decode(String),
    #[error("unsafe or incomplete bootstrap request: {0}")]
    Validation(&'static str),
    #[error("bootstrap journal creation failed: {0}")]
    Journal(std::io::Error),
    #[error("bootstrap materialization failed")]
    Materialization,
    #[error("bootstrap effect {0:?} failed")]
    Effect(BootstrapStage),
    #[error("bootstrap terminal evidence could not be persisted: {0}")]
    Evidence(std::io::Error),
    #[error("bootstrap crash witness interrupted execution")]
    InjectedCrash,
    #[error("bootstrap receipt is pending reconciliation")]
    RecoveryPending,
}

/// Saying what went wrong without saying anything the caller owns.
pub trait Redacting {
    /// Never echo request-owned routes, inputs, command bodies, or process
    /// output to the terminal.  The durable typed evidence is the witness.
    fn redacted(&self) -> &'static str;
}

impl Redacting for BootstrapError {
    fn redacted(&self) -> &'static str {
        match self {
            Self::Argument(_) | Self::Decode(_) | Self::Validation(_) => "InvalidRequest",
            Self::Journal(_) => "JournalFailure",
            Self::Materialization => "MaterializationFailure",
            Self::Effect(stage) => match stage {
                BootstrapStage::Tested => "TestFailure",
                BootstrapStage::Built => "BuildFailure",
                BootstrapStage::GcRooted => "GcRootFailure",
                BootstrapStage::Copied => "CopyFailure",
                BootstrapStage::BootOnceScheduled => "BootOnceFailure",
                _ => "BootstrapFailure",
            },
            Self::Evidence(_) => "EvidenceFailure",
            Self::InjectedCrash => "Interrupted",
            Self::RecoveryPending => "PendingRecovery",
        }
    }
}

/// Crash injection is deliberately a test seam: it returns before terminal
/// evidence or cleanup, exactly like an abrupt process loss at that boundary.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BootstrapCrashPoint {
    /// The root command returned, before the staging receipt is handed off.
    AfterGcRootCommand,
    AfterGcRoot,
    AfterCopy,
    AfterDispatch,
    AfterActivation,
    AfterEvidence,
}

pub trait BootstrapCrashInjector {
    fn after(&mut self, point: BootstrapCrashPoint) -> std::result::Result<(), BootstrapError>;
}

struct NeverCrash;

impl BootstrapCrashInjector for NeverCrash {
    fn after(&mut self, _: BootstrapCrashPoint) -> std::result::Result<(), BootstrapError> {
        Ok(())
    }
}

/// A request fingerprint, read as the names derived from it.
trait FingerprintNames {
    /// The transient systemd unit a boot-once activation of this request runs
    /// as. Deriving it from the fingerprint is what makes a resumed run
    /// recognise the unit its predecessor scheduled.
    fn boot_once_unit_name(&self) -> UnitName;
}

impl FingerprintNames for RequestFingerprint {
    fn boot_once_unit_name(&self) -> UnitName {
        UnitName(format!("lojix-bootstrap-boot-once-{}", &self.0[..24]))
    }
}

/// A bootstrap request admitted to run: the request as it arrived, the
/// fingerprint that binds everything derived from it, and what admission
/// found at the run's output paths.
struct AdmittedBootstrap {
    request: BootstrapRun,
    fingerprint: RequestFingerprint,
    journal_parent: PathBuf,
    gc_root_path: PathBuf,
    gc_root_exists: bool,
    terminal_evidence_path: PathBuf,
    terminal_evidence_exists: bool,
}

/// What a bootstrap request holds, whichever mode it is in.
trait BootstrapModeReading {
    fn input(&self) -> &BootstrapInput;

    fn builder(&self) -> &BootstrapBuilder;

    fn journal_parent(&self) -> &BootstrapJournalParent;

    fn gc_root_path(&self) -> &BootstrapGcRootPath;

    fn terminal_evidence_path(&self) -> &BootstrapTerminalEvidencePath;

    fn evidence_mode(&self) -> BootstrapEvidenceMode;
}

impl BootstrapModeReading for BootstrapMode {
    fn input(&self) -> &BootstrapInput {
        match self {
            Self::BuildOnly(build_only) => &build_only.bootstrap_input,
            Self::BootOnce(boot_once) => &boot_once.bootstrap_input,
        }
    }

    fn builder(&self) -> &BootstrapBuilder {
        match self {
            Self::BuildOnly(build_only) => &build_only.bootstrap_builder,
            Self::BootOnce(boot_once) => &boot_once.bootstrap_builder,
        }
    }

    fn journal_parent(&self) -> &BootstrapJournalParent {
        match self {
            Self::BuildOnly(build_only) => &build_only.bootstrap_journal_parent,
            Self::BootOnce(boot_once) => &boot_once.bootstrap_journal_parent,
        }
    }

    fn gc_root_path(&self) -> &BootstrapGcRootPath {
        match self {
            Self::BuildOnly(build_only) => &build_only.bootstrap_gc_root_path,
            Self::BootOnce(boot_once) => &boot_once.bootstrap_gc_root_path,
        }
    }

    fn terminal_evidence_path(&self) -> &BootstrapTerminalEvidencePath {
        match self {
            Self::BuildOnly(build_only) => &build_only.bootstrap_terminal_evidence_path,
            Self::BootOnce(boot_once) => &boot_once.bootstrap_terminal_evidence_path,
        }
    }

    fn evidence_mode(&self) -> BootstrapEvidenceMode {
        match self {
            Self::BuildOnly(_) => BootstrapEvidenceMode::BuildOnly,
            Self::BootOnce(_) => BootstrapEvidenceMode::BootOnce,
        }
    }
}

/// The three words that name what a bootstrap builds, however the request
/// arrived at them.
trait BootstrapBuildTarget {
    fn flake_reference(&self) -> &BootstrapFlakeReference;

    fn nix_system(&self) -> &BootstrapNixSystem;

    fn output_selector(&self) -> &BootstrapOutputSelector;
}

impl BootstrapBuildTarget for BootstrapInput {
    fn flake_reference(&self) -> &BootstrapFlakeReference {
        match self {
            Self::Direct(input) => &input.bootstrap_flake_reference,
            Self::Horizon(input) => &input.bootstrap_flake_reference,
        }
    }

    fn nix_system(&self) -> &BootstrapNixSystem {
        match self {
            Self::Direct(input) => &input.bootstrap_nix_system,
            Self::Horizon(input) => &input.bootstrap_nix_system,
        }
    }

    fn output_selector(&self) -> &BootstrapOutputSelector {
        match self {
            Self::Direct(input) => &input.bootstrap_output_selector,
            Self::Horizon(input) => &input.bootstrap_output_selector,
        }
    }
}

/// What bootstrap asks of each piece of caller-supplied text before it will
/// put that text in a command line. Every one of these admits the text
/// unchanged or refuses it; nothing is sanitized into acceptability.
trait BootstrapWord {
    /// A pinned GitHub flake: `github:owner/repo/<40-hex revision>`. Nothing
    /// mutable is admitted, so a bootstrap is reproducible by construction.
    fn admit_flake_reference(&self) -> std::result::Result<(), BootstrapError>;

    /// A Nix platform double, alphanumeric with `-` and `_`.
    fn admit_nix_system(&self) -> std::result::Result<(), BootstrapError>;

    /// A flake output attribute path.
    fn admit_output_selector(&self) -> std::result::Result<(), BootstrapError>;

    /// A Horizon cluster or node name: alphanumeric with `-`.
    fn admit_horizon_name(&self) -> std::result::Result<(), BootstrapError>;

    /// A request id: at most eighty alphanumerics, `-` and `_`, a name that
    /// cannot escape the journal parent or collide with a temporary.
    fn admit_request_id(&self) -> std::result::Result<(), BootstrapError>;

    /// A builder specification: non-empty, with no control character.
    fn admit_builder_specification(&self) -> std::result::Result<(), BootstrapError>;
}

impl BootstrapWord for str {
    fn admit_flake_reference(&self) -> std::result::Result<(), BootstrapError> {
        let Some(rest) = self.strip_prefix("github:") else {
            return Err(BootstrapError::Validation("flake reference is unsafe"));
        };
        let parts = rest.split('/').collect::<Vec<_>>();
        if parts.len() != 3
            || parts.iter().take(2).any(|part| {
                part.is_empty()
                    || !part
                        .bytes()
                        .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
            })
            || parts[2].len() != 40
            || !parts[2]
                .bytes()
                .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
        {
            return Err(BootstrapError::Validation(
                "flake reference must be github owner/repo/40-hex-revision",
            ));
        }
        Ok(())
    }

    fn admit_nix_system(&self) -> std::result::Result<(), BootstrapError> {
        if self.is_empty()
            || !self
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
        {
            return Err(BootstrapError::Validation("nix system is unsafe"));
        }
        Ok(())
    }

    fn admit_output_selector(&self) -> std::result::Result<(), BootstrapError> {
        if self.is_empty()
            || !self.bytes().all(|byte| {
                byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'-' | b'_' | b'\'')
            })
        {
            return Err(BootstrapError::Validation("output selector is unsafe"));
        }
        Ok(())
    }

    fn admit_horizon_name(&self) -> std::result::Result<(), BootstrapError> {
        if self.is_empty()
            || !self
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
        {
            return Err(BootstrapError::Validation("horizon name is unsafe"));
        }
        Ok(())
    }

    fn admit_request_id(&self) -> std::result::Result<(), BootstrapError> {
        if self.is_empty()
            || self.len() > 80
            || !self
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
        {
            return Err(BootstrapError::Validation("request id is unsafe"));
        }
        Ok(())
    }

    fn admit_builder_specification(&self) -> std::result::Result<(), BootstrapError> {
        if self.is_empty() || self.chars().any(char::is_control) {
            return Err(BootstrapError::Validation(
                "builder specification is unsafe",
            ));
        }
        Ok(())
    }
}

/// Reading an ssh identity out of the two forms a caller can name one in.
trait SshIdentityText {
    /// An `ssh-ng://user@host[:port]` Nix store URI, so the caller can prove
    /// the store and the ssh destination are the same machine.
    fn nix_store_uri_identity(&self) -> std::result::Result<SshIdentity, BootstrapError>;

    /// A bare `user@host[:port]` ssh destination.
    fn ssh_destination_identity(&self) -> std::result::Result<SshIdentity, BootstrapError>;
}

impl SshIdentityText for str {
    fn nix_store_uri_identity(&self) -> std::result::Result<SshIdentity, BootstrapError> {
        let Some(authority) = self.strip_prefix("ssh-ng://") else {
            return Err(BootstrapError::Validation("nix store URI must use ssh-ng"));
        };
        if authority.contains(['/', '?', '#']) || authority.matches('@').count() != 1 {
            return Err(BootstrapError::Validation(
                "nix store URI is not a canonical ssh-ng identity",
            ));
        }
        let (user, host_port) = authority.split_once('@').ok_or(BootstrapError::Validation(
            "nix store URI lacks user identity",
        ))?;
        SshIdentity::parse(user, host_port)
    }

    fn ssh_destination_identity(&self) -> std::result::Result<SshIdentity, BootstrapError> {
        if self.starts_with('-')
            || self.matches('@').count() != 1
            || self.contains(['/', '?', '#', '[', ']'])
        {
            return Err(BootstrapError::Validation(
                "ssh destination is not a canonical identity",
            ));
        }
        let (user, host_port) = self.split_once('@').ok_or(BootstrapError::Validation(
            "ssh destination lacks user identity",
        ))?;
        SshIdentity::parse(user, host_port)
    }
}

/// Constructing an ssh identity from its two halves.
trait SshIdentityParsing {
    /// Both halves must be canonical: a lowercase unflagged user, and a host
    /// that is a plain DNS name or address with an optional non-zero port.
    fn parse(user: &str, host_port: &str) -> std::result::Result<Self, BootstrapError>
    where
        Self: Sized;
}

impl SshIdentityParsing for SshIdentity {
    fn parse(user: &str, host_port: &str) -> std::result::Result<Self, BootstrapError> {
        if user.is_empty()
            || !user.bytes().all(|byte| {
                byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'_' | b'-')
            })
            || user.starts_with('-')
            || host_port.is_empty()
        {
            return Err(BootstrapError::Validation("ssh user identity is unsafe"));
        }
        let (host, port) = match host_port.split_once(':') {
            Some((host, port)) if !port.contains(':') => {
                let port = port
                    .parse::<u16>()
                    .map_err(|_| BootstrapError::Validation("ssh port is unsafe"))?;
                if port == 0 {
                    return Err(BootstrapError::Validation("ssh port is unsafe"));
                }
                (host, Some(SshPort(i64::from(port))))
            }
            Some(_) => return Err(BootstrapError::Validation("ssh host is unsafe")),
            None => (host_port, None),
        };
        if host.is_empty()
            || host.starts_with('-')
            || host.ends_with('-')
            || host.starts_with('.')
            || host.ends_with('.')
            || host.contains("..")
            || !host.bytes().all(|byte| {
                byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'.' | b'-')
            })
        {
            return Err(BootstrapError::Validation("ssh host identity is unsafe"));
        }
        Ok(Self {
            ssh_user: SshUser(user.to_string()),
            ssh_host: SshHost(host.to_string()),
            ssh_port_option: port,
        })
    }
}

/// Admitting one part of a request: every check it must pass before any
/// effect is issued.
trait BootstrapAdmission {
    fn admit(&self) -> std::result::Result<(), BootstrapError>;
}

impl BootstrapAdmission for BootstrapRequestId {
    fn admit(&self) -> std::result::Result<(), BootstrapError> {
        self.0.admit_request_id()
    }
}

impl BootstrapAdmission for BootstrapInput {
    fn admit(&self) -> std::result::Result<(), BootstrapError> {
        if let Self::Horizon(input) = self {
            input.bootstrap_cluster_name.0.admit_horizon_name()?;
            OfferedPath::from(input.bootstrap_proposal_source.0.as_str())
                .existing_regular_file("horizon-definition.datom")?;
            input.bootstrap_node_name.0.admit_horizon_name()?;
            if let BootstrapSecretsInput::SecretsDirectory(directory) =
                &input.bootstrap_secrets_input
            {
                OfferedPath::from(directory.0.as_str()).existing_directory()?;
            }
        }
        self.flake_reference().0.admit_flake_reference()?;
        self.nix_system().0.admit_nix_system()?;
        self.output_selector().0.admit_output_selector()
    }
}

impl BootstrapAdmission for BootstrapBuilder {
    fn admit(&self) -> std::result::Result<(), BootstrapError> {
        match self {
            Self::NoBuilder => Ok(()),
            Self::NixBuilder(specification) => specification.0.admit_builder_specification(),
        }
    }
}

impl BootstrapAdmission for BootstrapTestPlan {
    fn admit(&self) -> std::result::Result<(), BootstrapError> {
        match self {
            Self::NoTest => Ok(()),
            Self::RunHermeticTest(test) => {
                test.bootstrap_flake_reference.0.admit_flake_reference()?;
                test.bootstrap_nix_system.0.admit_nix_system()?;
                test.bootstrap_output_selector.0.admit_output_selector()
            }
        }
    }
}

impl BootstrapAdmission for BootstrapActivationBackend {
    fn admit(&self) -> std::result::Result<(), BootstrapError> {
        match self {
            Self::RemoteNixosSystemdBootV1(remote) => {
                remote.ssh_identity()?;
                remote.bootstrap_ssh_policy.admit()?;
                OfferedPath::from(remote.bootstrap_system_profile_path.0.as_str())
                    .absolute_normal()?;
                OfferedPath::from(remote.bootstrap_boot_entries_directory.0.as_str())
                    .absolute_normal()?;
                Ok(())
            }
            Self::LocalBootstrapV1(local) => {
                OfferedPath::from(local.bootstrap_system_profile_path.0.as_str())
                    .existing_parent()?;
                OfferedPath::from(local.bootstrap_boot_entries_directory.0.as_str())
                    .existing_directory()?;
                Ok(())
            }
        }
    }
}

impl BootstrapAdmission for BootstrapSshPolicy {
    fn admit(&self) -> std::result::Result<(), BootstrapError> {
        OfferedPath::from(self.bootstrap_ssh_identity_file.0.as_str()).private_regular_file()?;
        OfferedPath::from(self.bootstrap_ssh_known_hosts_file.0.as_str()).private_regular_file()?;
        Ok(())
    }
}

/// The node a remote backend reaches, as one ssh identity proved to be the
/// identity its Nix store URI names too.
trait RemoteBackend {
    fn ssh_identity(&self) -> std::result::Result<SshIdentity, BootstrapError>;

    /// The route to the node through the run's private ssh configuration.
    fn ssh_route(&self, ssh_config: &Path) -> std::result::Result<SshRoute, BootstrapError>;
}

impl RemoteBackend for BootstrapRemoteNixosSystemdBootV1 {
    fn ssh_identity(&self) -> std::result::Result<SshIdentity, BootstrapError> {
        let store_identity = self.bootstrap_nix_store_uri.0.nix_store_uri_identity()?;
        let ssh_identity = self
            .bootstrap_ssh_destination
            .0
            .ssh_destination_identity()?;
        if store_identity != ssh_identity {
            return Err(BootstrapError::Validation(
                "remote store and ssh identities differ",
            ));
        }
        Ok(ssh_identity)
    }

    fn ssh_route(&self, ssh_config: &Path) -> std::result::Result<SshRoute, BootstrapError> {
        Ok(SshRoute {
            ssh_config_path: SshConfigPath(ssh_config.display().to_string()),
            ssh_identity: self.ssh_identity()?,
        })
    }
}

/// Admitting a whole request, and the fingerprint that binds it.
trait BootstrapRequestAdmission {
    /// Every field checked, the output paths observed, nothing done.
    fn admitted(self) -> std::result::Result<AdmittedBootstrap, BootstrapError>;

    /// The private binding between a request and everything derived from it:
    /// its journal directory, its terminal evidence, and its boot-once unit.
    /// The lowercase hexadecimal SHA-256 of the request's archive.
    fn fingerprint(&self) -> std::result::Result<RequestFingerprint, BootstrapError>;
}

impl BootstrapRequestAdmission for BootstrapRun {
    fn admitted(self) -> std::result::Result<AdmittedBootstrap, BootstrapError> {
        self.bootstrap_request_id.admit()?;
        let mode = &self.bootstrap_mode;
        mode.input().admit()?;
        mode.builder().admit()?;
        if let BootstrapMode::BootOnce(boot_once) = mode {
            boot_once.bootstrap_test_plan.admit()?;
            boot_once.bootstrap_activation_backend.admit()?;
        }
        let journal_parent =
            OfferedPath::from(mode.journal_parent().0.as_str()).private_existing_directory()?;
        let (gc_root_path, gc_root_exists) =
            OfferedPath::from(mode.gc_root_path().0.as_str()).private_output_path()?;
        let (terminal_evidence_path, terminal_evidence_exists) =
            OfferedPath::from(mode.terminal_evidence_path().0.as_str()).private_output_path()?;
        if gc_root_path == terminal_evidence_path {
            return Err(BootstrapError::Validation(
                "gc root and evidence paths collide",
            ));
        }
        Ok(AdmittedBootstrap {
            fingerprint: self.fingerprint()?,
            request: self,
            journal_parent,
            gc_root_path,
            gc_root_exists,
            terminal_evidence_path,
            terminal_evidence_exists,
        })
    }

    fn fingerprint(&self) -> std::result::Result<RequestFingerprint, BootstrapError> {
        let archive = rkyv::to_bytes::<rkyv::rancor::Error>(self)
            .map_err(|error| BootstrapError::Journal(std::io::Error::other(error.to_string())))?;
        Ok(RequestFingerprint(
            Sha256::digest(&archive)
                .iter()
                .map(|byte| format!("{byte:02x}"))
                .collect(),
        ))
    }
}

/// A file's identity as the file system reports it, in the journal's
/// integers.
trait ObservedIdentity {
    fn file_identity(&self) -> std::result::Result<FileIdentity, BootstrapError>;
}

impl ObservedIdentity for fs::Metadata {
    fn file_identity(&self) -> std::result::Result<FileIdentity, BootstrapError> {
        let integer = |value: u64| {
            i64::try_from(value).map_err(|_| {
                BootstrapError::Validation("file identity exceeds the journal's integers")
            })
        };
        Ok(FileIdentity {
            file_device: FileDevice(integer(self.dev())?),
            file_inode: FileInode(integer(self.ino())?),
        })
    }
}

/// The private journal one bootstrap run keeps beside its output: the
/// directory named by its request's fingerprint, and the parent it lives in.
#[derive(Debug)]
struct EphemeralJournal {
    directory: PathBuf,
    parent: PathBuf,
}

/// The private write-ahead journal one bootstrap run keeps beside its output:
/// what it intends to do, what it has done, and the terminal evidence it
/// leaves. The run is finished with it once that evidence is written, and
/// nothing outside the run may read it.
trait BootstrapJournalling: Sized {
    fn open_or_create(request: &AdmittedBootstrap) -> std::result::Result<Self, BootstrapError>;

    fn read_state(&self) -> std::result::Result<BootstrapJournal, BootstrapError>;

    fn write_state(&self, state: &BootstrapJournal) -> std::result::Result<(), BootstrapError>;

    fn verify_identity(&self, state: &BootstrapJournal) -> std::result::Result<(), BootstrapError>;

    fn mutate(
        &self,
        mutate: impl FnOnce(&mut BootstrapJournal),
    ) -> std::result::Result<(), BootstrapError>;

    fn succeeded(&self, stage: BootstrapStage) -> std::result::Result<bool, BootstrapError>;

    fn intent(&self, stage: BootstrapStage) -> std::result::Result<(), BootstrapError>;

    fn receipt(&self, stage: BootstrapStage) -> std::result::Result<(), BootstrapError>;

    fn outcome(
        &self,
        stage: BootstrapStage,
        succeeded: bool,
    ) -> std::result::Result<(), BootstrapError>;

    fn event(
        &self,
        stage: BootstrapStage,
        kind: JournalEventKind,
        succeeded: bool,
    ) -> std::result::Result<(), BootstrapError>;

    fn terminal_status(
        &self,
    ) -> std::result::Result<Option<BootstrapEvidenceStatus>, BootstrapError>;

    fn set_terminal_status(
        &self,
        status: BootstrapEvidenceStatus,
    ) -> std::result::Result<(), BootstrapError>;

    fn closure_path(&self) -> std::result::Result<Option<ClosurePath>, BootstrapError>;

    fn set_closure_path(&self, closure: &ClosurePath) -> std::result::Result<(), BootstrapError>;

    fn materialized_inputs(
        &self,
    ) -> std::result::Result<Option<Vec<PersistedFlakeInputOverride>>, BootstrapError>;

    fn set_materialized_inputs(
        &self,
        inputs: &[PersistedFlakeInputOverride],
    ) -> std::result::Result<(), BootstrapError>;

    fn set_root_receipt(
        &self,
        root: &Path,
        closure: &ClosurePath,
    ) -> std::result::Result<(), BootstrapError>;

    fn verify_root_receipt(
        &self,
        root: &Path,
        closure: &ClosurePath,
    ) -> std::result::Result<bool, BootstrapError>;

    fn evidence(
        &self,
        status: BootstrapEvidenceStatus,
    ) -> std::result::Result<BootstrapTerminalEvidence, BootstrapError>;

    fn cleanup(self) -> std::result::Result<(), BootstrapError>;
}

impl BootstrapJournalling for EphemeralJournal {
    fn open_or_create(request: &AdmittedBootstrap) -> std::result::Result<Self, BootstrapError> {
        let directory = request
            .journal_parent
            .join(format!("{JOURNAL_PREFIX}{}", request.fingerprint.0));
        let mode = &request.request.bootstrap_mode;
        match fs::create_dir(&directory) {
            Ok(()) => {
                if request.gc_root_exists || request.terminal_evidence_exists {
                    let _ = fs::remove_dir(&directory);
                    return Err(BootstrapError::Validation(
                        "fresh request output already exists",
                    ));
                }
                fs::set_permissions(
                    &directory,
                    fs::Permissions::from_mode(PRIVATE_DIRECTORY_MODE),
                )
                .map_err(BootstrapError::Journal)?;
                request.journal_parent.sync_directory()?;
                let state = BootstrapJournal {
                    journal_schema_version: JournalSchemaVersion(JOURNAL_SCHEMA_VERSION),
                    request_fingerprint: request.fingerprint.clone(),
                    bootstrap_evidence_mode: mode.evidence_mode(),
                    journal_directory: JournalDirectory(
                        directory.private_directory_metadata()?.file_identity()?,
                    ),
                    resolved_flake_reference: ResolvedFlakeReference(
                        mode.input().flake_reference().0.clone(),
                    ),
                    closure_path_option: None,
                    gc_root_receipt_option: None,
                    materialized_inputs_option: None,
                    bootstrap_evidence_status_option: None,
                    journal_event_vector: vec![JournalEvent {
                        bootstrap_stage: BootstrapStage::JournalCreated,
                        journal_event_kind: JournalEventKind::Outcome,
                        event_succeeded: EventSucceeded(true),
                    }],
                };
                let journal = Self {
                    directory,
                    parent: request.journal_parent.clone(),
                };
                journal.write_state(&state)?;
                request.journal_parent.sync_directory()?;
                Ok(journal)
            }
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                let journal = Self {
                    directory,
                    parent: request.journal_parent.clone(),
                };
                let state = journal.read_state()?;
                if state.journal_schema_version.0 != JOURNAL_SCHEMA_VERSION
                    || state.request_fingerprint != request.fingerprint
                    || state.bootstrap_evidence_mode != mode.evidence_mode()
                {
                    return Err(BootstrapError::Validation(
                        "journal does not bind this request",
                    ));
                }
                journal.verify_identity(&state)?;
                Ok(journal)
            }
            Err(error) => Err(BootstrapError::Journal(error)),
        }
    }

    fn read_state(&self) -> std::result::Result<BootstrapJournal, BootstrapError> {
        let bytes =
            fs::read(self.directory.join(JOURNAL_STATE_FILE)).map_err(BootstrapError::Journal)?;
        rkyv::from_bytes::<BootstrapJournal, rkyv::rancor::Error>(&bytes)
            .map_err(|error| BootstrapError::Journal(std::io::Error::other(error.to_string())))
    }

    fn write_state(&self, state: &BootstrapJournal) -> std::result::Result<(), BootstrapError> {
        let bytes = rkyv::to_bytes::<rkyv::rancor::Error>(state)
            .map_err(|error| BootstrapError::Journal(std::io::Error::other(error.to_string())))?;
        self.verify_identity(state)?;
        let temporary = self.directory.join(format!(
            ".{JOURNAL_STATE_FILE}.tmp-{}",
            JOURNAL_SEQUENCE.fetch_add(1, Ordering::Relaxed)
        ));
        let mut file = fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temporary)
            .map_err(BootstrapError::Journal)?;
        file.write_all(&bytes).map_err(BootstrapError::Journal)?;
        file.sync_all().map_err(BootstrapError::Journal)?;
        fs::rename(&temporary, self.directory.join(JOURNAL_STATE_FILE))
            .map_err(BootstrapError::Journal)?;
        self.directory.sync_directory()?;
        Ok(())
    }

    fn verify_identity(&self, state: &BootstrapJournal) -> std::result::Result<(), BootstrapError> {
        self.parent.private_directory_metadata()?;
        let identity = self
            .directory
            .private_directory_metadata()?
            .file_identity()?;
        if identity != state.journal_directory.0 {
            return Err(BootstrapError::Validation("journal child identity changed"));
        }
        Ok(())
    }

    fn mutate(
        &self,
        mutate: impl FnOnce(&mut BootstrapJournal),
    ) -> std::result::Result<(), BootstrapError> {
        let mut state = self.read_state()?;
        self.verify_identity(&state)?;
        mutate(&mut state);
        self.write_state(&state)
    }

    fn succeeded(&self, stage: BootstrapStage) -> std::result::Result<bool, BootstrapError> {
        Ok(self.read_state()?.journal_event_vector.iter().any(|event| {
            event.bootstrap_stage == stage
                && event.journal_event_kind == JournalEventKind::Outcome
                && event.event_succeeded.0
        }))
    }

    fn intent(&self, stage: BootstrapStage) -> std::result::Result<(), BootstrapError> {
        self.event(stage, JournalEventKind::Intent, true)
    }

    fn receipt(&self, stage: BootstrapStage) -> std::result::Result<(), BootstrapError> {
        self.event(stage, JournalEventKind::Receipt, true)
    }

    fn outcome(
        &self,
        stage: BootstrapStage,
        succeeded: bool,
    ) -> std::result::Result<(), BootstrapError> {
        self.event(stage, JournalEventKind::Outcome, succeeded)
    }

    fn event(
        &self,
        stage: BootstrapStage,
        kind: JournalEventKind,
        succeeded: bool,
    ) -> std::result::Result<(), BootstrapError> {
        self.mutate(|state| {
            state.journal_event_vector.push(JournalEvent {
                bootstrap_stage: stage,
                journal_event_kind: kind,
                event_succeeded: EventSucceeded(succeeded),
            })
        })
    }

    fn terminal_status(
        &self,
    ) -> std::result::Result<Option<BootstrapEvidenceStatus>, BootstrapError> {
        Ok(self.read_state()?.bootstrap_evidence_status_option)
    }

    fn set_terminal_status(
        &self,
        status: BootstrapEvidenceStatus,
    ) -> std::result::Result<(), BootstrapError> {
        self.mutate(|state| state.bootstrap_evidence_status_option = Some(status))
    }

    fn closure_path(&self) -> std::result::Result<Option<ClosurePath>, BootstrapError> {
        Ok(self.read_state()?.closure_path_option)
    }

    fn set_closure_path(&self, closure: &ClosurePath) -> std::result::Result<(), BootstrapError> {
        let closure = closure.clone();
        self.mutate(|state| state.closure_path_option = Some(closure))
    }

    fn materialized_inputs(
        &self,
    ) -> std::result::Result<Option<Vec<PersistedFlakeInputOverride>>, BootstrapError> {
        Ok(self
            .read_state()?
            .materialized_inputs_option
            .map(|inputs| inputs.0))
    }

    fn set_materialized_inputs(
        &self,
        inputs: &[PersistedFlakeInputOverride],
    ) -> std::result::Result<(), BootstrapError> {
        let inputs = MaterializedInputs(inputs.to_vec());
        self.mutate(|state| state.materialized_inputs_option = Some(inputs))
    }

    fn set_root_receipt(
        &self,
        root: &Path,
        closure: &ClosurePath,
    ) -> std::result::Result<(), BootstrapError> {
        let receipt = GcRootReceipt {
            closure_path: closure.clone(),
            file_identity: fs::symlink_metadata(root)
                .map_err(BootstrapError::Journal)?
                .file_identity()?,
        };
        self.mutate(|state| state.gc_root_receipt_option = Some(receipt))
    }

    fn verify_root_receipt(
        &self,
        root: &Path,
        closure: &ClosurePath,
    ) -> std::result::Result<bool, BootstrapError> {
        let state = self.read_state()?;
        let identity = match fs::symlink_metadata(root) {
            Ok(metadata) => metadata.file_identity()?,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(false),
            Err(error) => return Err(BootstrapError::Journal(error)),
        };
        Ok(root.binds_closure(closure)
            && state.gc_root_receipt_option
                == Some(GcRootReceipt {
                    closure_path: closure.clone(),
                    file_identity: identity,
                }))
    }

    fn evidence(
        &self,
        status: BootstrapEvidenceStatus,
    ) -> std::result::Result<BootstrapTerminalEvidence, BootstrapError> {
        let state = self.read_state()?;
        Ok(BootstrapTerminalEvidence {
            journal_schema_version: JournalSchemaVersion(JOURNAL_SCHEMA_VERSION),
            request_fingerprint: state.request_fingerprint,
            bootstrap_evidence_mode: state.bootstrap_evidence_mode,
            bootstrap_evidence_status: status,
            bootstrap_effect_evidence_vector: state
                .journal_event_vector
                .into_iter()
                .filter(|event| event.journal_event_kind == JournalEventKind::Outcome)
                .map(|event| BootstrapEffectEvidence {
                    bootstrap_stage: event.bootstrap_stage,
                    effect_succeeded: EffectSucceeded(event.event_succeeded.0),
                })
                .collect(),
        })
    }

    fn cleanup(self) -> std::result::Result<(), BootstrapError> {
        let state = self.read_state()?;
        self.verify_identity(&state)?;
        if !self.succeeded(BootstrapStage::TerminalEvidenceWritten)? {
            return Err(BootstrapError::Validation(
                "terminal evidence receipt is absent",
            ));
        }
        // Path-based tree deletion after an identity check has a same-UID
        // rename race.  Retain finalized private journals until every cleanup
        // operation can be expressed through inode-bound directory handles.
        // The evidence is terminal and the retained journal is the durable
        // audit record, so safety takes precedence over ephemerality.
        self.directory.sync_directory()?;
        self.parent.sync_directory()?;
        Ok(())
    }
}

/// What bootstrap does with a path it already trusts. Every one of these
/// decides through `symlink_metadata` and creates final names with `hard_link`
/// rather than `rename`, so neither a swapped link nor a racing writer can be
/// admitted after the check that admitted it.
trait BootstrapPath {
    /// The directory's metadata, proving it is a real directory owned by this
    /// process and readable by nobody else.
    fn private_directory_metadata(&self) -> std::result::Result<fs::Metadata, BootstrapError>;

    /// `fsync` the directory so a name created in it survives a crash.
    fn sync_directory(&self) -> std::result::Result<(), BootstrapError>;

    /// Whether any entry exists at this path, symlinks included.
    fn entry_exists(&self) -> std::result::Result<bool, BootstrapError>;

    /// Whether this path is a symbolic link pointing at exactly `closure` —
    /// the receipt that says a gc root binds the closure that was built.
    fn binds_closure(&self, closure: &ClosurePath) -> bool;

    /// Hand a staging gc root over to its final name without ever replacing an
    /// existing one, then drop the staging name.
    fn link_root_no_replace(
        &self,
        root: &Path,
        closure: &ClosurePath,
    ) -> std::result::Result<(), BootstrapError>;

    /// Create a private generated-input flake directory holding `flake_text`
    /// and, optionally, one additional named file.
    fn write_generated_flake(
        &self,
        additional: Option<(&str, String)>,
        flake_text: &str,
    ) -> std::result::Result<(), BootstrapError>;

    /// Create the private generated `secrets` flake, copying every
    /// `<attribute>.sops` file out of the admitted secrets input.
    fn write_secrets_flake(
        &self,
        secrets_input: &BootstrapSecretsInput,
    ) -> std::result::Result<(), BootstrapError>;
}

impl BootstrapPath for Path {
    fn private_directory_metadata(&self) -> std::result::Result<fs::Metadata, BootstrapError> {
        let metadata = fs::symlink_metadata(self).map_err(BootstrapError::Journal)?;
        if !metadata.file_type().is_dir()
            || metadata.file_type().is_symlink()
            || metadata.uid() != rustix::process::getuid().as_raw()
            || metadata.mode() & 0o777 != PRIVATE_DIRECTORY_MODE
        {
            return Err(BootstrapError::Validation(
                "directory is not private and caller-owned",
            ));
        }
        Ok(metadata)
    }

    fn sync_directory(&self) -> std::result::Result<(), BootstrapError> {
        fs::File::open(self)
            .and_then(|directory| directory.sync_all())
            .map_err(BootstrapError::Journal)
    }

    fn entry_exists(&self) -> std::result::Result<bool, BootstrapError> {
        match fs::symlink_metadata(self) {
            Ok(_) => Ok(true),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
            Err(error) => Err(BootstrapError::Journal(error)),
        }
    }

    fn binds_closure(&self, closure: &ClosurePath) -> bool {
        let Ok(metadata) = fs::symlink_metadata(self) else {
            return false;
        };
        metadata.file_type().is_symlink()
            && fs::read_link(self)
                .ok()
                .is_some_and(|target| target == Path::new(&closure.0))
    }

    fn link_root_no_replace(
        &self,
        root: &Path,
        closure: &ClosurePath,
    ) -> std::result::Result<(), BootstrapError> {
        if !self.binds_closure(closure) {
            return Err(BootstrapError::Validation(
                "gc root staging did not bind the built closure",
            ));
        }
        let parent = root
            .parent()
            .ok_or(BootstrapError::Validation("gc root has no parent"))?;
        parent.private_directory_metadata()?;
        fs::hard_link(self, root).map_err(BootstrapError::Journal)?;
        if !root.binds_closure(closure) {
            return Err(BootstrapError::Validation(
                "gc root receipt did not bind the built closure",
            ));
        }
        parent.sync_directory()?;
        fs::remove_file(self).map_err(BootstrapError::Journal)?;
        self.parent()
            .ok_or(BootstrapError::Validation("gc root staging has no parent"))?
            .sync_directory()?;
        Ok(())
    }

    fn write_generated_flake(
        &self,
        additional: Option<(&str, String)>,
        flake_text: &str,
    ) -> std::result::Result<(), BootstrapError> {
        fs::create_dir(self).map_err(BootstrapError::Journal)?;
        fs::set_permissions(self, fs::Permissions::from_mode(0o700))
            .map_err(BootstrapError::Journal)?;
        if let Some((name, contents)) = additional {
            fs::write(self.join(name), contents).map_err(BootstrapError::Journal)?;
        }
        fs::write(self.join("flake.nix"), flake_text).map_err(BootstrapError::Journal)
    }

    fn write_secrets_flake(
        &self,
        secrets_input: &BootstrapSecretsInput,
    ) -> std::result::Result<(), BootstrapError> {
        fs::create_dir(self).map_err(BootstrapError::Journal)?;
        fs::set_permissions(self, fs::Permissions::from_mode(0o700))
            .map_err(BootstrapError::Journal)?;
        let mut entries = String::new();
        if let BootstrapSecretsInput::SecretsDirectory(source) = secrets_input {
            let mut files = Vec::new();
            for entry in fs::read_dir(&source.0).map_err(|_| BootstrapError::Materialization)? {
                let entry = entry.map_err(|_| BootstrapError::Materialization)?;
                let path = entry.path();
                let metadata =
                    fs::symlink_metadata(&path).map_err(|_| BootstrapError::Materialization)?;
                let name = entry
                    .file_name()
                    .into_string()
                    .map_err(|_| BootstrapError::Materialization)?;
                if !metadata.file_type().is_file()
                    || metadata.file_type().is_symlink()
                    || !name.ends_with(".sops")
                    || name.contains(' ')
                {
                    return Err(BootstrapError::Materialization);
                }
                let attribute = name.trim_end_matches(".sops");
                if attribute.is_empty()
                    || !attribute
                        .chars()
                        .all(|character| character.is_ascii_alphanumeric() || character == '_')
                {
                    return Err(BootstrapError::Materialization);
                }
                files.push((attribute.to_string(), name, path));
            }
            files.sort_by(|left, right| left.1.cmp(&right.1));
            let mut seen_attributes = BTreeMap::new();
            for (attribute, name, path) in files {
                if seen_attributes
                    .insert(attribute.clone(), name.clone())
                    .is_some()
                {
                    return Err(BootstrapError::Materialization);
                }
                fs::copy(path, self.join(&name)).map_err(BootstrapError::Journal)?;
                entries.push_str(&format!("    {attribute} = ./{name};\n"));
            }
        }
        fs::write(
            self.join("flake.nix"),
            format!("{{ outputs = _: {{ sopsFiles = {{\n{entries}  }}; }}; }}\n"),
        )
        .map_err(BootstrapError::Journal)
    }
}

/// Reading and writing the one durable artefact a bootstrap run leaves behind.
trait TerminalEvidenceFile {
    /// Read the evidence already at `path`, refusing anything that is not the
    /// private, caller-owned regular file this process would have written.
    fn read_from(path: &Path) -> std::result::Result<Self, BootstrapError>
    where
        Self: Sized;

    /// Write the evidence at `path`, which must not yet exist. The content is
    /// written to a fresh temporary in the same prevalidated directory and
    /// linked into place, because `rename` would overwrite a racing writer.
    fn write_new(&self, path: &Path) -> std::result::Result<(), BootstrapError>;
}

impl TerminalEvidenceFile for BootstrapTerminalEvidence {
    fn read_from(path: &Path) -> std::result::Result<Self, BootstrapError> {
        let metadata = fs::symlink_metadata(path).map_err(BootstrapError::Evidence)?;
        if !metadata.file_type().is_file()
            || metadata.file_type().is_symlink()
            || metadata.uid() != rustix::process::getuid().as_raw()
            || metadata.mode() & 0o777 != PRIVATE_EVIDENCE_MODE
        {
            return Err(BootstrapError::Validation(
                "terminal evidence identity changed",
            ));
        }
        let bytes = fs::read(path).map_err(BootstrapError::Evidence)?;
        rkyv::from_bytes::<Self, rkyv::rancor::Error>(&bytes)
            .map_err(|error| BootstrapError::Evidence(std::io::Error::other(error.to_string())))
    }

    fn write_new(&self, path: &Path) -> std::result::Result<(), BootstrapError> {
        let bytes = rkyv::to_bytes::<rkyv::rancor::Error>(self)
            .map_err(|error| BootstrapError::Evidence(std::io::Error::other(error.to_string())))?;
        let parent = path
            .parent()
            .ok_or(BootstrapError::Validation("evidence path has no parent"))?;
        parent.private_directory_metadata()?;
        for attempt in 0..32u64 {
            let temporary = parent.join(format!(
                "{TEMPORARY_EVIDENCE_PREFIX}{}-{}-{attempt}",
                std::process::id(),
                JOURNAL_SEQUENCE.fetch_add(1, Ordering::Relaxed)
            ));
            match fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&temporary)
            {
                Ok(mut file) => {
                    file.set_permissions(fs::Permissions::from_mode(PRIVATE_EVIDENCE_MODE))
                        .map_err(BootstrapError::Evidence)?;
                    file.write_all(&bytes).map_err(BootstrapError::Evidence)?;
                    file.sync_all().map_err(BootstrapError::Evidence)?;
                    // `rename` would overwrite a racing destination.  A hard
                    // link creates the final name only if it remains absent;
                    // the parent was prevalidated and both files are
                    // necessarily on it.
                    fs::hard_link(&temporary, path).map_err(BootstrapError::Evidence)?;
                    fs::File::open(path)
                        .and_then(|file| file.sync_all())
                        .map_err(BootstrapError::Evidence)?;
                    parent.sync_directory()?;
                    fs::remove_file(&temporary).map_err(BootstrapError::Evidence)?;
                    parent.sync_directory()?;
                    return Ok(());
                }
                Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
                Err(error) => return Err(BootstrapError::Evidence(error)),
            }
        }
        Err(BootstrapError::Validation(
            "could not allocate evidence temporary file",
        ))
    }
}

/// The ssh configuration a remote run writes where only it can read it: the
/// caller's identity and known hosts, and nothing ambient.
trait SshPolicyMaterial {
    fn write_private_config(
        &self,
        journal: &EphemeralJournal,
    ) -> std::result::Result<PathBuf, BootstrapError>;
}

impl SshPolicyMaterial for BootstrapSshPolicy {
    fn write_private_config(
        &self,
        journal: &EphemeralJournal,
    ) -> std::result::Result<PathBuf, BootstrapError> {
        let path = journal.directory.join("ssh-config");
        if path.exists() {
            let metadata = fs::symlink_metadata(&path).map_err(BootstrapError::Journal)?;
            if !metadata.file_type().is_file()
                || metadata.file_type().is_symlink()
                || metadata.uid() != rustix::process::getuid().as_raw()
                || metadata.mode() & 0o777 != PRIVATE_EVIDENCE_MODE
            {
                return Err(BootstrapError::Validation(
                    "private SSH config identity changed",
                ));
            }
            return Ok(path);
        }
        let contents = format!(
            "Host *\n  IdentityFile {}\n  UserKnownHostsFile {}\n  GlobalKnownHostsFile /dev/null\n  StrictHostKeyChecking yes\n  IdentitiesOnly yes\n  IdentityAgent none\n  ProxyCommand none\n  ProxyJump none\n  ControlMaster no\n  ControlPath none\n",
            self.bootstrap_ssh_identity_file.0.ssh_config_quoted(),
            self.bootstrap_ssh_known_hosts_file.0.ssh_config_quoted(),
        );
        let mut file = fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)
            .map_err(BootstrapError::Journal)?;
        file.set_permissions(fs::Permissions::from_mode(PRIVATE_EVIDENCE_MODE))
            .map_err(BootstrapError::Journal)?;
        file.write_all(contents.as_bytes())
            .map_err(BootstrapError::Journal)?;
        file.sync_all().map_err(BootstrapError::Journal)?;
        journal.directory.sync_directory()?;
        Ok(path)
    }
}

impl From<PathFault> for BootstrapError {
    fn from(fault: PathFault) -> Self {
        match fault {
            PathFault::Malformed => Self::Validation("path is not absolute and normal"),
            PathFault::Symlinked => Self::Validation("path contains a symlink"),
            PathFault::WrongKind => Self::Validation("path is not the required entry"),
            PathFault::Unreadable(error) => Self::Journal(error),
        }
    }
}

/// The admissions bootstrap adds to the crate-wide ones: an output path must
/// live in a directory that is private and caller-owned, because everything
/// bootstrap writes — the journal, the gc root, the terminal evidence, the ssh
/// config — is material no other user may read or race.
trait PrivatePathAdmission {
    /// An existing directory that is private and caller-owned.
    fn private_existing_directory(&self) -> std::result::Result<PathBuf, BootstrapError>;

    /// An existing regular file, caller-owned at the private evidence mode,
    /// inside a private caller-owned directory. Used for the SSH identity and
    /// known-hosts files, whose contents are authority.
    fn private_regular_file(&self) -> std::result::Result<PathBuf, BootstrapError>;

    /// A path bootstrap will write to: absolute and normal, inside a private
    /// caller-owned directory, paired with whether anything is there already.
    fn private_output_path(&self) -> std::result::Result<(PathBuf, bool), BootstrapError>;

    /// A path whose parent must already exist, symlink-free, but which need
    /// not exist itself — the local system profile that `nix-env --set`
    /// creates.
    fn existing_parent(&self) -> std::result::Result<PathBuf, BootstrapError>;
}

impl PrivatePathAdmission for OfferedPath<'_> {
    fn private_existing_directory(&self) -> std::result::Result<PathBuf, BootstrapError> {
        let path = self.existing_directory()?;
        path.private_directory_metadata()?;
        Ok(path)
    }

    fn private_regular_file(&self) -> std::result::Result<PathBuf, BootstrapError> {
        let path = self.absolute_normal()?;
        let parent = path
            .parent()
            .ok_or(BootstrapError::Validation("private file has no parent"))?;
        OfferedPath::from(
            parent
                .to_str()
                .ok_or(BootstrapError::Validation("path is not utf8"))?,
        )
        .private_existing_directory()?;
        let metadata = fs::symlink_metadata(&path).map_err(BootstrapError::Journal)?;
        if !metadata.file_type().is_file()
            || metadata.file_type().is_symlink()
            || metadata.uid() != rustix::process::getuid().as_raw()
            || metadata.mode() & 0o777 != PRIVATE_EVIDENCE_MODE
        {
            return Err(BootstrapError::Validation("private SSH file is unsafe"));
        }
        Ok(path)
    }

    fn private_output_path(&self) -> std::result::Result<(PathBuf, bool), BootstrapError> {
        let path = self.absolute_normal()?;
        let parent = path
            .parent()
            .ok_or(BootstrapError::Validation("output path has no parent"))?;
        OfferedPath::from(
            parent
                .to_str()
                .ok_or(BootstrapError::Validation("path is not utf8"))?,
        )
        .private_existing_directory()?;
        Ok((path.clone(), path.entry_exists()?))
    }

    fn existing_parent(&self) -> std::result::Result<PathBuf, BootstrapError> {
        let path = self.absolute_normal()?;
        let parent = path
            .parent()
            .ok_or(BootstrapError::Validation("path has no parent"))?;
        OfferedPath::from(
            parent
                .to_str()
                .ok_or(BootstrapError::Validation("path is not utf8"))?,
        )
        .existing_directory()?;
        Ok(path)
    }
}

/// Starting one bootstrap run: decoding it, and running it to its terminal.
pub trait BootstrapInvocation {
    /// Decode and run exactly one inline Datom object. This never consults
    /// daemon environment variables or socket paths.
    fn run_from_environment() -> std::result::Result<BootstrapEvidenceStatus, BootstrapError>
    where
        Self: Sized,
    {
        let mut executor = ProcessBootstrapExecutor;
        Self::decode_single_inline(std::env::args_os().skip(1))?.run_with_executor(&mut executor)
    }

    /// Decode the one inline Datom operand into a typed run.
    fn decode_single_inline(
        arguments: impl IntoIterator<Item = OsString>,
    ) -> std::result::Result<Self, BootstrapError>
    where
        Self: Sized;

    /// Run the pipeline against an injected executor. The production entry
    /// point uses `ProcessBootstrapExecutor`; all tests use body-suppressed
    /// executors.
    fn run_with_executor<E: BootstrapExecutor>(
        self,
        executor: &mut E,
    ) -> std::result::Result<BootstrapEvidenceStatus, BootstrapError>
    where
        Self: Sized,
    {
        let mut never_crash = NeverCrash;
        self.run_with_executor_and_crash(executor, &mut never_crash)
    }

    /// Hermetic tests use this to prove that each persisted receipt resumes at
    /// the next exact stage without issuing its preceding effect again.
    fn run_with_executor_and_crash<E: BootstrapExecutor, C: BootstrapCrashInjector>(
        self,
        executor: &mut E,
        crash: &mut C,
    ) -> std::result::Result<BootstrapEvidenceStatus, BootstrapError>
    where
        Self: Sized;
}

impl BootstrapInvocation for BootstrapRun {
    fn decode_single_inline(
        arguments: impl IntoIterator<Item = OsString>,
    ) -> std::result::Result<Self, BootstrapError> {
        let text = arguments.single_inline_datom()?;
        Potential::<BootstrapRun>::from(text)
            .actualize(&mut <crate::Ingress as crate::Budgeted>::budget())
            .map_err(|fault| BootstrapError::Decode(format!("{fault:?}")))
    }

    fn run_with_executor_and_crash<E: BootstrapExecutor, C: BootstrapCrashInjector>(
        self,
        executor: &mut E,
        crash: &mut C,
    ) -> std::result::Result<BootstrapEvidenceStatus, BootstrapError> {
        let admitted = self.admitted()?;
        let journal = EphemeralJournal::open_or_create(&admitted)?;
        let translator = BootstrapTranslator::over(executor);
        let status = if let Some(status) = journal.terminal_status()? {
            status
        } else {
            match admitted.execute(&journal, &translator, crash) {
                Ok(()) => BootstrapEvidenceStatus::Succeeded,
                Err(BootstrapError::InjectedCrash) => return Err(BootstrapError::InjectedCrash),
                Err(BootstrapError::RecoveryPending) => {
                    return Err(BootstrapError::RecoveryPending);
                }
                Err(BootstrapError::Effect(stage)) => {
                    journal.outcome(stage, false)?;
                    journal.set_terminal_status(BootstrapEvidenceStatus::Failed)?;
                    BootstrapEvidenceStatus::Failed
                }
                Err(error) => return Err(error),
            }
        };

        if journal.terminal_status()?.is_none() {
            journal.set_terminal_status(status.clone())?;
        }
        admitted.write_terminal_evidence(&journal, status.clone(), crash)?;
        journal.cleanup()?;
        Ok(status)
    }
}

/// Carrying one admitted run through its effect stages. Every stage is
/// journalled before it is issued and again after it returns, so a resumed run
/// reads the journal rather than repeating the effect.
trait BootstrapExecution {
    /// Run every stage this mode calls for, from the far side of whatever the
    /// journal already records.
    fn execute<E: BootstrapExecutor, C: BootstrapCrashInjector>(
        &self,
        journal: &EphemeralJournal,
        translator: &BootstrapTranslator<'_, E>,
        crash: &mut C,
    ) -> std::result::Result<(), BootstrapError>;

    /// Evaluate the derivation and build it, answering with the closure path.
    fn build<E: BootstrapExecutor>(
        &self,
        inputs: &[PersistedFlakeInputOverride],
        translator: &BootstrapTranslator<'_, E>,
    ) -> std::result::Result<ClosurePath, BootstrapError>;

    /// Turn a Horizon input into the generated flake inputs that pin it. A
    /// direct input materializes nothing.
    fn materialize<E: BootstrapExecutor>(
        &self,
        journal: &EphemeralJournal,
        translator: &BootstrapTranslator<'_, E>,
    ) -> std::result::Result<Vec<PersistedFlakeInputOverride>, BootstrapError>;

    /// Root the built closure at the caller's GC-root path, through a staging
    /// root inside the journal, or prove the root already there is this
    /// run's.
    fn root<E: BootstrapExecutor, C: BootstrapCrashInjector>(
        &self,
        journal: &EphemeralJournal,
        closure: &ClosurePath,
        translator: &BootstrapTranslator<'_, E>,
        crash: &mut C,
    ) -> std::result::Result<(), BootstrapError>;

    /// Schedule the boot-once unit on the node `route` reaches, unless it is
    /// scheduled already, and read its state until it is ready.
    fn dispatch_and_reconcile<E: BootstrapExecutor, C: BootstrapCrashInjector>(
        &self,
        journal: &EphemeralJournal,
        route: BootOnceRoute,
        script: BootOnceScript,
        translator: &BootstrapTranslator<'_, E>,
        crash: &mut C,
    ) -> std::result::Result<(), BootstrapError>;

    /// Write the one durable artefact the run leaves behind, or prove that the
    /// artefact already there is this run's own.
    fn write_terminal_evidence<C: BootstrapCrashInjector>(
        &self,
        journal: &EphemeralJournal,
        status: BootstrapEvidenceStatus,
        crash: &mut C,
    ) -> std::result::Result<(), BootstrapError>;
}

impl BootstrapExecution for AdmittedBootstrap {
    fn execute<E: BootstrapExecutor, C: BootstrapCrashInjector>(
        &self,
        journal: &EphemeralJournal,
        translator: &BootstrapTranslator<'_, E>,
        crash: &mut C,
    ) -> std::result::Result<(), BootstrapError> {
        let inputs = if let Some(inputs) = journal.materialized_inputs()? {
            inputs
        } else {
            journal.intent(BootstrapStage::Materialized)?;
            let inputs = self
                .materialize(journal, translator)
                .map_err(|_| BootstrapError::Effect(BootstrapStage::Materialized))?;
            journal.set_materialized_inputs(&inputs)?;
            journal.receipt(BootstrapStage::Materialized)?;
            journal.outcome(BootstrapStage::Materialized, true)?;
            inputs
        };

        if let BootstrapMode::BootOnce(boot_once) = &self.request.bootstrap_mode
            && let BootstrapTestPlan::RunHermeticTest(test) = &boot_once.bootstrap_test_plan
            && !journal.succeeded(BootstrapStage::Tested)?
        {
            journal.intent(BootstrapStage::Tested)?;
            let check = BootstrapCheckBuild {
                bootstrap_nix_system: test.bootstrap_nix_system.clone(),
                bootstrap_flake_reference: test.bootstrap_flake_reference.clone(),
                bootstrap_output_selector: test.bootstrap_output_selector.clone(),
            };
            if let CallOutcome::Failed(_) = check.emit(translator) {
                return Err(BootstrapError::Effect(BootstrapStage::Tested));
            }
            journal.receipt(BootstrapStage::Tested)?;
            journal.outcome(BootstrapStage::Tested, true)?;
        }

        let closure = if let Some(closure) = journal.closure_path()? {
            closure
        } else {
            journal.intent(BootstrapStage::Built)?;
            let closure = self.build(&inputs, translator)?;
            journal.set_closure_path(&closure)?;
            journal.receipt(BootstrapStage::Built)?;
            journal.outcome(BootstrapStage::Built, true)?;
            closure
        };

        self.root(journal, &closure, translator, crash)?;

        // The exact BuildOnly variant has no activation representation, rather
        // than a boolean that a future refactor could accidentally ignore.
        let BootstrapMode::BootOnce(boot_once) = &self.request.bootstrap_mode else {
            return Ok(());
        };
        let unit_name = self.fingerprint.boot_once_unit_name();
        match &boot_once.bootstrap_activation_backend {
            BootstrapActivationBackend::RemoteNixosSystemdBootV1(remote) => {
                let ssh_config = remote.bootstrap_ssh_policy.write_private_config(journal)?;
                let route = remote.ssh_route(&ssh_config)?;
                if !journal.succeeded(BootstrapStage::Copied)? {
                    journal.intent(BootstrapStage::Copied)?;
                    let presence = RemoteClosureQuery {
                        ssh_route: route.clone(),
                        closure_path: closure.clone(),
                    };
                    match presence.emit(translator) {
                        ClosurePresenceOutcome::Present => {}
                        ClosurePresenceOutcome::Absent => {
                            let copy = BootstrapCopy {
                                bootstrap_nix_store_uri: remote.bootstrap_nix_store_uri.clone(),
                                ssh_config_path: route.ssh_config_path.clone(),
                                closure_path: closure.clone(),
                            };
                            if let CallOutcome::Failed(_) = copy.emit(translator) {
                                return Err(BootstrapError::Effect(BootstrapStage::Copied));
                            }
                        }
                        ClosurePresenceOutcome::Failed(_) => {
                            return Err(BootstrapError::RecoveryPending);
                        }
                    }
                    journal.receipt(BootstrapStage::Copied)?;
                    journal.outcome(BootstrapStage::Copied, true)?;
                    crash.after(BootstrapCrashPoint::AfterCopy)?;
                }
                let script = BootOnceScript {
                    unit_name,
                    closure_path: closure,
                    system_profile_path: SystemProfilePath(
                        remote.bootstrap_system_profile_path.0.clone(),
                    ),
                    boot_entries_directory: BootEntriesDirectory(
                        remote.bootstrap_boot_entries_directory.0.clone(),
                    ),
                };
                self.dispatch_and_reconcile(
                    journal,
                    BootOnceRoute::Remote(route),
                    script,
                    translator,
                    crash,
                )
            }
            BootstrapActivationBackend::LocalBootstrapV1(BootstrapLocalBootstrapV1 {
                bootstrap_system_profile_path,
                bootstrap_boot_entries_directory,
            }) => {
                let script = BootOnceScript {
                    unit_name,
                    closure_path: closure,
                    system_profile_path: SystemProfilePath(bootstrap_system_profile_path.0.clone()),
                    boot_entries_directory: BootEntriesDirectory(
                        bootstrap_boot_entries_directory.0.clone(),
                    ),
                };
                self.dispatch_and_reconcile(
                    journal,
                    BootOnceRoute::Local,
                    script,
                    translator,
                    crash,
                )
            }
        }
    }

    fn build<E: BootstrapExecutor>(
        &self,
        inputs: &[PersistedFlakeInputOverride],
        translator: &BootstrapTranslator<'_, E>,
    ) -> std::result::Result<ClosurePath, BootstrapError> {
        let mode = &self.request.bootstrap_mode;
        let input = mode.input();
        let evaluation = BootstrapEvaluation {
            bootstrap_nix_system: input.nix_system().clone(),
            persisted_flake_input_override_vector: inputs.to_vec(),
            bootstrap_flake_reference: input.flake_reference().clone(),
            bootstrap_output_selector: input.output_selector().clone(),
        };
        let PrintedPathOutcome::Printed(derivation) = evaluation.emit(translator) else {
            return Err(BootstrapError::Effect(BootstrapStage::Built));
        };
        if !NixStorePath::from(derivation.0.as_str()).is_canonical_item()
            || !derivation.0.ends_with(".drv")
        {
            return Err(BootstrapError::Effect(BootstrapStage::Built));
        }
        let build = BootstrapBuild {
            bootstrap_nix_system: input.nix_system().clone(),
            bootstrap_builder_spec_option: match mode.builder() {
                BootstrapBuilder::NoBuilder => None,
                BootstrapBuilder::NixBuilder(specification) => {
                    Some(BootstrapBuilderSpec(specification.0.clone()))
                }
            },
            derivation_path: DerivationPath(derivation.0),
        };
        let PrintedPathOutcome::Printed(closure) = build.emit(translator) else {
            return Err(BootstrapError::Effect(BootstrapStage::Built));
        };
        if !NixStorePath::from(closure.0.as_str()).is_canonical_item()
            || closure.0.ends_with(".drv")
        {
            return Err(BootstrapError::Effect(BootstrapStage::Built));
        }
        Ok(ClosurePath(closure.0))
    }

    fn materialize<E: BootstrapExecutor>(
        &self,
        journal: &EphemeralJournal,
        translator: &BootstrapTranslator<'_, E>,
    ) -> std::result::Result<Vec<PersistedFlakeInputOverride>, BootstrapError> {
        let BootstrapInput::Horizon(input) = self.request.bootstrap_mode.input() else {
            return Ok(Vec::new());
        };
        let proposal_text = fs::read_to_string(&input.bootstrap_proposal_source.0)
            .map_err(|_| BootstrapError::Materialization)?;
        let definition: HorizonDefinition = HorizonDefinition::decode(&proposal_text)
            .map_err(|_| BootstrapError::Materialization)?;
        let horizon = definition
            .project(&input.bootstrap_node_name.0)
            .map_err(|_| BootstrapError::Materialization)?;
        let Some(projected_system) = horizon.node.machine.architecture.nix_system() else {
            return Err(BootstrapError::Materialization);
        };
        if projected_system != input.bootstrap_nix_system.0 {
            return Err(BootstrapError::Materialization);
        }

        let generated = journal.directory.join("generated-inputs");
        fs::create_dir(&generated).map_err(BootstrapError::Journal)?;
        fs::set_permissions(
            &generated,
            fs::Permissions::from_mode(PRIVATE_DIRECTORY_MODE),
        )
        .map_err(BootstrapError::Journal)?;
        let mut directories = BTreeMap::new();

        let horizon_directory = generated.join("horizon");
        horizon_directory.write_generated_flake(
            Some((
                "horizon.json",
                serde_json::to_string_pretty(&horizon)
                    .map_err(|_| BootstrapError::Materialization)?,
            )),
            "{ outputs = _: { horizon = builtins.fromJSON (builtins.readFile ./horizon.json); }; }\n",
        )?;
        directories.insert("horizon", horizon_directory);

        let system_directory = generated.join("system");
        system_directory.write_generated_flake(
            None,
            &format!(
                "{{ outputs = _: {{ system = \"{}\"; }}; }}\n",
                input.bootstrap_nix_system.0
            ),
        )?;
        directories.insert("system", system_directory);

        let deployment_directory = generated.join("deployment");
        let (include_home, include_all_firmware) = match input.bootstrap_materialization_shape {
            BootstrapMaterializationShape::CompleteHost => (true, true),
            BootstrapMaterializationShape::BaseHost => (false, false),
        };
        deployment_directory.write_generated_flake(
            None,
            &format!(
                "{{ outputs = _: {{ deployment = {{ includeHome = {include_home}; includeAllFirmware = {include_all_firmware}; }}; }}; }}\n"
            ),
        )?;
        directories.insert("deployment", deployment_directory);

        let secrets_directory = generated.join("secrets");
        secrets_directory.write_secrets_flake(&input.bootstrap_secrets_input)?;
        directories.insert("secrets", secrets_directory);

        let mut inputs = Vec::with_capacity(directories.len());
        for (name, directory) in directories {
            let hashing = GeneratedInputHashing {
                generated_input_directory: GeneratedInputDirectory(directory.display().to_string()),
            };
            let HashOutcome::Hashed(SriHash(hash)) = hashing.emit(translator) else {
                return Err(BootstrapError::Materialization);
            };
            if !hash.starts_with("sha256-") || hash.chars().any(char::is_control) {
                return Err(BootstrapError::Materialization);
            }
            inputs.push(PersistedFlakeInputOverride {
                flake_input_name: FlakeInputName(name.to_string()),
                persisted_flake_input_reference: PersistedFlakeInputReference {
                    flake_input_url: FlakeInputUrl(format!("path:{}", directory.display())),
                    nix_archive_hash: NixArchiveHash(hash.percent_encoded_nar_hash()),
                },
            });
        }
        Ok(inputs)
    }

    fn root<E: BootstrapExecutor, C: BootstrapCrashInjector>(
        &self,
        journal: &EphemeralJournal,
        closure: &ClosurePath,
        translator: &BootstrapTranslator<'_, E>,
        crash: &mut C,
    ) -> std::result::Result<(), BootstrapError> {
        if journal.succeeded(BootstrapStage::GcRooted)? {
            if !journal.verify_root_receipt(&self.gc_root_path, closure)? {
                return Err(BootstrapError::Validation(
                    "gc root receipt identity changed",
                ));
            }
            return Ok(());
        }
        journal.intent(BootstrapStage::GcRooted)?;
        let staging = journal.directory.join("gc-root-staging");
        let staging_exists = staging.entry_exists()?;
        let root_exists = self.gc_root_path.entry_exists()?;
        if staging_exists && root_exists {
            return Err(BootstrapError::Validation("gc root handoff collision"));
        }
        if staging_exists {
            if !staging.binds_closure(closure) {
                return Err(BootstrapError::Validation(
                    "gc root staging receipt changed",
                ));
            }
            staging.link_root_no_replace(&self.gc_root_path, closure)?;
        } else if root_exists {
            if !self.gc_root_path.binds_closure(closure) {
                return Err(BootstrapError::Validation("gc root receipt changed"));
            }
        } else {
            let rooting = BootstrapRooting {
                root_staging_path: RootStagingPath(staging.display().to_string()),
                closure_path: closure.clone(),
            };
            if let CallOutcome::Failed(_) = rooting.emit(translator) {
                return Err(BootstrapError::Effect(BootstrapStage::GcRooted));
            }
            crash.after(BootstrapCrashPoint::AfterGcRootCommand)?;
            staging.link_root_no_replace(&self.gc_root_path, closure)?;
        }
        journal.set_root_receipt(&self.gc_root_path, closure)?;
        journal.receipt(BootstrapStage::GcRooted)?;
        journal.outcome(BootstrapStage::GcRooted, true)?;
        crash.after(BootstrapCrashPoint::AfterGcRoot)
    }

    fn dispatch_and_reconcile<E: BootstrapExecutor, C: BootstrapCrashInjector>(
        &self,
        journal: &EphemeralJournal,
        route: BootOnceRoute,
        script: BootOnceScript,
        translator: &BootstrapTranslator<'_, E>,
        crash: &mut C,
    ) -> std::result::Result<(), BootstrapError> {
        let query = BootOnceUnitQuery {
            boot_once_route: route.clone(),
            unit_name: script.unit_name.clone(),
        };
        if !journal.succeeded(BootstrapStage::BootOnceScheduled)? {
            journal.intent(BootstrapStage::BootOnceScheduled)?;
            match query.emit(translator) {
                BootOnceUnitOutcome::Read(BootOnceUnitState::NotFound) => {
                    let dispatch = BootOnceDispatch {
                        boot_once_route: route,
                        boot_once_script: script,
                    };
                    if let CallOutcome::Failed(_) = dispatch.emit(translator) {
                        return Err(BootstrapError::RecoveryPending);
                    }
                }
                BootOnceUnitOutcome::Read(_) => {}
                BootOnceUnitOutcome::Failed(_) => return Err(BootstrapError::RecoveryPending),
            }
            journal.receipt(BootstrapStage::BootOnceScheduled)?;
            journal.outcome(BootstrapStage::BootOnceScheduled, true)?;
            crash.after(BootstrapCrashPoint::AfterDispatch)?;
        }
        if journal.succeeded(BootstrapStage::BootOnceActivated)? {
            return Ok(());
        }
        journal.intent(BootstrapStage::BootOnceActivated)?;
        for _ in 0..BOOT_ONCE_READINGS {
            match query.emit(translator) {
                BootOnceUnitOutcome::Read(BootOnceUnitState::Ready) => {
                    journal.receipt(BootstrapStage::BootOnceActivated)?;
                    journal.outcome(BootstrapStage::BootOnceActivated, true)?;
                    crash.after(BootstrapCrashPoint::AfterActivation)?;
                    return Ok(());
                }
                BootOnceUnitOutcome::Read(BootOnceUnitState::Failed) => {
                    return Err(BootstrapError::Effect(BootstrapStage::BootOnceActivated));
                }
                BootOnceUnitOutcome::Read(BootOnceUnitState::NotFound)
                | BootOnceUnitOutcome::Failed(_) => return Err(BootstrapError::RecoveryPending),
                BootOnceUnitOutcome::Read(BootOnceUnitState::Waiting) => {
                    std::thread::sleep(BOOT_ONCE_READING_INTERVAL)
                }
            }
        }
        Err(BootstrapError::RecoveryPending)
    }

    fn write_terminal_evidence<C: BootstrapCrashInjector>(
        &self,
        journal: &EphemeralJournal,
        status: BootstrapEvidenceStatus,
        crash: &mut C,
    ) -> std::result::Result<(), BootstrapError> {
        if journal.succeeded(BootstrapStage::TerminalEvidenceWritten)? {
            return Ok(());
        }
        journal.intent(BootstrapStage::TerminalEvidenceWritten)?;
        if self.terminal_evidence_exists {
            let existing = BootstrapTerminalEvidence::read_from(&self.terminal_evidence_path)?;
            if existing.journal_schema_version.0 != JOURNAL_SCHEMA_VERSION
                || existing.request_fingerprint != self.fingerprint
                || existing.bootstrap_evidence_status != status
            {
                return Err(BootstrapError::Validation(
                    "existing evidence does not bind this self",
                ));
            }
        } else {
            journal
                .evidence(status)?
                .write_new(&self.terminal_evidence_path)?;
        }
        crash.after(BootstrapCrashPoint::AfterEvidence)?;
        journal.receipt(BootstrapStage::TerminalEvidenceWritten)?;
        journal.outcome(BootstrapStage::TerminalEvidenceWritten, true)
    }
}

/// A nar hash as it must appear inside a `path:...?narHash=` query.
trait NarHashEncoding {
    fn percent_encoded_nar_hash(&self) -> String;
}

impl NarHashEncoding for str {
    fn percent_encoded_nar_hash(&self) -> String {
        self.chars()
            .flat_map(|character| match character {
                '%' => "%25".chars().collect::<Vec<_>>(),
                '+' => "%2B".chars().collect::<Vec<_>>(),
                '/' => "%2F".chars().collect::<Vec<_>>(),
                '=' => "%3D".chars().collect::<Vec<_>>(),
                _ => vec![character],
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use crate::HorizonArchitecture as _;
    #[test]
    fn horizon_architecture_is_compared_as_a_nix_system() {
        assert_eq!(("x86_64").nix_system(), Some("x86_64-linux"));
        assert_eq!(("aarch64").nix_system(), Some("aarch64-linux"));
        assert_eq!(("riscv64").nix_system(), None);
    }
}
