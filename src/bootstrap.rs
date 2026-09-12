//! Daemon-free, explicitly-authorized bootstrap pipeline.
//!
//! This is deliberately a separate ingress from the daemon wire contracts.
//! A bootstrap invocation owns every route, input, builder, output path and
//! activation backend; no socket, service configuration, old store, or
//! hostname-derived default is read.  A fresh private v5 journal store is
//! created below the request's journal parent. It records write-ahead intent,
//! receipt, and outcome records and is deleted only after terminal evidence
//! has been atomically committed and directory-synced at the caller-selected
//! path.

use crate::inspected_text::{
    NixStorePath, OfferedPath, PathAdmission as _, PathFault, StoreItemShape,
};
use crate::{HorizonArchitecture as _, InlineDatomArguments as _};
use std::collections::BTreeMap;
use std::ffi::OsString;
use std::fs;
use std::io::Write;
use std::os::unix::fs::{MetadataExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::atomic::{AtomicU64, Ordering};

use datom_codec::{Actualizing, Potential};
use horizon_lib::{DatomDecoding, HorizonDefinition, Projecting};
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use sha2::{Digest, Sha256};
use thiserror::Error;

use crate::{Store, ingress};

const JOURNAL_SCHEMA_VERSION: u32 = 5;
const JOURNAL_PREFIX: &str = ".lojix-bootstrap-v5-";
const TEMPORARY_EVIDENCE_PREFIX: &str = ".lojix-bootstrap-evidence-";
const JOURNAL_STATE_FILE: &str = "bootstrap-v5.rkyv";
const JOURNAL_STORE_FILE: &str = "lojix-v5.sema";
const PRIVATE_DIRECTORY_MODE: u32 = 0o700;
const PRIVATE_EVIDENCE_MODE: u32 = 0o600;

static JOURNAL_SEQUENCE: AtomicU64 = AtomicU64::new(0);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BootstrapRun {
    pub request_id: BootstrapRequestId,
    pub mode: BootstrapMode,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BootstrapRequestId(pub String);

#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(clippy::large_enum_variant)]
pub enum BootstrapMode {
    BuildOnly(BootstrapBuildOnly),
    BootOnce(BootstrapBootOnce),
}

/// The exact dry-run/build-only variant.  Its type has no transport or
/// activation field, so a decoded BuildOnly request cannot activate by design.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BootstrapBuildOnly {
    pub input: BootstrapInput,
    pub builder: BootstrapBuilder,
    pub journal_parent: BootstrapJournalParent,
    pub gc_root_path: BootstrapGcRootPath,
    pub terminal_evidence_path: BootstrapTerminalEvidencePath,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BootstrapBootOnce {
    pub input: BootstrapInput,
    pub builder: BootstrapBuilder,
    pub test_plan: BootstrapTestPlan,
    pub activation_backend: BootstrapActivationBackend,
    pub journal_parent: BootstrapJournalParent,
    pub gc_root_path: BootstrapGcRootPath,
    pub terminal_evidence_path: BootstrapTerminalEvidencePath,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BootstrapInput {
    Direct(BootstrapDirectInput),
    Horizon(BootstrapHorizonInput),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BootstrapDirectInput {
    pub flake_reference: BootstrapFlakeReference,
    pub nix_system: BootstrapNixSystem,
    pub output_selector: BootstrapOutputSelector,
}

/// Horizon materialization carries its complete authority surface.  In
/// particular, `secrets_input` is explicit; no sibling `secrets/` directory is
/// inferred from the proposal path.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BootstrapHorizonInput {
    pub proposal_source: BootstrapProposalSource,
    pub cluster_name: BootstrapClusterName,
    pub node_name: BootstrapNodeName,
    pub materialization_shape: BootstrapMaterializationShape,
    pub secrets_input: BootstrapSecretsInput,
    pub flake_reference: BootstrapFlakeReference,
    pub nix_system: BootstrapNixSystem,
    pub output_selector: BootstrapOutputSelector,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BootstrapMaterializationShape {
    CompleteHost,
    BaseHost,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BootstrapSecretsInput {
    NoSecrets,
    SecretsDirectory(BootstrapSecretsDirectory),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BootstrapBuilder {
    NoBuilder,
    NixBuilder(BootstrapBuilderSpec),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BootstrapTestPlan {
    NoTest,
    RunHermeticTest(BootstrapHermeticTest),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BootstrapHermeticTest {
    pub flake_reference: BootstrapFlakeReference,
    pub nix_system: BootstrapNixSystem,
    pub output_selector: BootstrapOutputSelector,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BootstrapActivationBackend {
    RemoteNixosSystemdBootV1(BootstrapRemoteNixosSystemdBootV1),
    LocalBootstrapV1(BootstrapLocalBootstrapV1),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BootstrapRemoteNixosSystemdBootV1 {
    pub nix_store_uri: BootstrapNixStoreUri,
    pub ssh_destination: BootstrapSshDestination,
    pub ssh_policy: BootstrapSshPolicy,
    pub system_profile_path: BootstrapSystemProfilePath,
    pub boot_entries_directory: BootstrapBootEntriesDirectory,
}

/// Request-owned SSH authority. The bootstrapper accepts no ambient agent,
/// config, user/host, proxy, multiplexing, or trust-store default.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BootstrapSshPolicy {
    pub identity_file: BootstrapSshIdentityFile,
    pub known_hosts_file: BootstrapSshKnownHostsFile,
    pub strict_host_key_mode: BootstrapStrictHostKeyMode,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BootstrapStrictHostKeyMode {
    RequireKnownHost,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BootstrapLocalBootstrapV1 {
    pub system_profile_path: BootstrapSystemProfilePath,
    pub boot_entries_directory: BootstrapBootEntriesDirectory,
}

macro_rules! text_field {
    ($name:ident) => {
        #[derive(Debug, Clone, PartialEq, Eq)]
        pub struct $name(pub String);
    };
}

text_field!(BootstrapFlakeReference);
text_field!(BootstrapNixSystem);
text_field!(BootstrapOutputSelector);
text_field!(BootstrapProposalSource);
text_field!(BootstrapClusterName);
text_field!(BootstrapNodeName);
text_field!(BootstrapSecretsDirectory);
text_field!(BootstrapBuilderSpec);
text_field!(BootstrapJournalParent);
text_field!(BootstrapGcRootPath);
text_field!(BootstrapTerminalEvidencePath);
text_field!(BootstrapNixStoreUri);
text_field!(BootstrapSshDestination);
text_field!(BootstrapSshIdentityFile);
text_field!(BootstrapSshKnownHostsFile);
text_field!(BootstrapSystemProfilePath);
text_field!(BootstrapBootEntriesDirectory);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Archive, RkyvSerialize, RkyvDeserialize)]
pub enum BootstrapEvidenceStatus {
    Succeeded,
    Failed,
}

impl BootstrapEvidenceStatus {
    fn as_str(self) -> &'static str {
        match self {
            Self::Succeeded => "Succeeded",
            Self::Failed => "Failed",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Archive, RkyvSerialize, RkyvDeserialize)]
pub enum BootstrapEvidenceMode {
    BuildOnly,
    BootOnce,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Archive, RkyvSerialize, RkyvDeserialize)]
pub enum BootstrapEffectStage {
    JournalCreated,
    Materialized,
    Tested,
    Built,
    GcRooted,
    Copied,
    BootOnceScheduled,
    BootOnceActivated,
    TerminalEvidenceWritten,
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, RkyvSerialize, RkyvDeserialize)]
pub struct BootstrapEffectEvidence {
    pub stage: BootstrapEffectStage,
    pub succeeded: bool,
}

/// The typed durable terminal artifact.  It intentionally contains stage
/// evidence rather than raw command text, routes, proposal paths, or child
/// process output, which keeps the caller's private routing data out of the
/// standard terminal and durable evidence surfaces.
#[derive(Debug, Clone, PartialEq, Eq, Archive, RkyvSerialize, RkyvDeserialize)]
pub struct BootstrapTerminalEvidence {
    pub journal_schema_version: u32,
    /// A one-way binding to the request.  The raw request id, paths, flake
    /// reference, transport identity, and child output stay in the private
    /// journal rather than this caller-retained artifact.
    pub request_hash: Vec<u8>,
    pub mode: BootstrapEvidenceMode,
    pub status: BootstrapEvidenceStatus,
    pub effects: Vec<BootstrapEffectEvidence>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BootstrapTerminal {
    pub status: &'static str,
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
    #[error("bootstrap journal store failed: {0}")]
    JournalStore(crate::Error),
    #[error("bootstrap materialization failed")]
    Materialization,
    #[error("bootstrap effect {0:?} failed")]
    Effect(BootstrapEffectStage),
    #[error("bootstrap terminal evidence could not be persisted: {0}")]
    Evidence(std::io::Error),
    #[error("bootstrap crash witness interrupted execution")]
    InjectedCrash,
    #[error("bootstrap receipt is pending reconciliation")]
    RecoveryPending,
}

impl BootstrapError {
    /// Never echo request-owned routes, inputs, command bodies, or process
    /// output to the terminal.  The durable typed evidence is the witness.
    pub fn redacted(&self) -> &'static str {
        match self {
            Self::Argument(_) | Self::Decode(_) | Self::Validation(_) => "InvalidRequest",
            Self::Journal(_) | Self::JournalStore(_) => "JournalFailure",
            Self::Materialization => "MaterializationFailure",
            Self::Effect(stage) => match stage {
                BootstrapEffectStage::Tested => "TestFailure",
                BootstrapEffectStage::Built => "BuildFailure",
                BootstrapEffectStage::GcRooted => "GcRootFailure",
                BootstrapEffectStage::Copied => "CopyFailure",
                BootstrapEffectStage::BootOnceScheduled => "BootOnceFailure",
                _ => "BootstrapFailure",
            },
            Self::Evidence(_) => "EvidenceFailure",
            Self::InjectedCrash => "Interrupted",
            Self::RecoveryPending => "PendingRecovery",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BootstrapCommand {
    pub program: String,
    pub arguments: Vec<String>,
    pub environment: Vec<(String, String)>,
}

/// The body boundary for every bootstrap effect.  Tests supply an executor
/// that records these values and returns fixtures; production is the only
/// implementation that starts a process.
pub trait BootstrapExecutor {
    fn run(&mut self, command: BootstrapCommand) -> std::result::Result<String, BootstrapError>;
}

#[derive(Debug, Default)]
pub struct ProcessBootstrapExecutor;

impl BootstrapExecutor for ProcessBootstrapExecutor {
    fn run(&mut self, command: BootstrapCommand) -> std::result::Result<String, BootstrapError> {
        let output = Command::new(&command.program)
            .args(&command.arguments)
            .envs(command.environment.iter().map(|(key, value)| (key, value)))
            .output()
            .map_err(|_| BootstrapError::Effect(BootstrapEffectStage::Built))?;
        if !output.status.success() {
            return Err(BootstrapError::Effect(BootstrapEffectStage::Built));
        }
        Ok(String::from_utf8_lossy(&output.stdout).into_owned())
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

#[derive(Debug, Clone)]
struct FlakeOverride {
    name: String,
    reference: String,
}

/// What bootstrap asks of a piece of text, in the one place it asks it.
trait BootstrapText {
    /// The first non-blank line, trimmed. A command that printed nothing
    /// usable did not build what it was asked for.
    fn first_line(&self) -> std::result::Result<String, BootstrapError>;

    /// The text as one shell word, safe inside single quotes.
    fn shell_quoted(&self) -> String;

    /// The text as one `ssh_config` value, safe inside double quotes.
    fn ssh_config_quoted(&self) -> String;

    /// A nar hash as it must appear inside a `path:...?narHash=` query.
    fn percent_encoded_nar_hash(&self) -> String;
}

impl BootstrapText for str {
    fn first_line(&self) -> std::result::Result<String, BootstrapError> {
        let line = self
            .lines()
            .find(|line| !line.trim().is_empty())
            .ok_or(BootstrapError::Effect(BootstrapEffectStage::Built))?;
        Ok(line.trim().to_string())
    }

    fn shell_quoted(&self) -> String {
        format!("'{}'", self.replace('\'', "'\\''"))
    }

    fn ssh_config_quoted(&self) -> String {
        format!("\"{}\"", self.replace('\\', "\\\\").replace('"', "\\\""))
    }

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

/// A request fingerprint, read as the names derived from it.
trait RequestFingerprint {
    fn hex_lower(&self) -> String;

    /// The transient systemd unit a boot-once activation of this request runs
    /// as. Deriving it from the fingerprint is what makes a resumed run
    /// recognise the unit its predecessor scheduled.
    fn boot_once_unit_name(&self) -> String;
}

impl RequestFingerprint for [u8] {
    fn hex_lower(&self) -> String {
        self.iter().map(|byte| format!("{byte:02x}")).collect()
    }

    fn boot_once_unit_name(&self) -> String {
        format!("lojix-bootstrap-boot-once-{}", &self.hex_lower()[..24])
    }
}

/// The one-shot activation a bootstrap run schedules on its target: the
/// request it belongs to, the closure it installs, and the two boot paths it
/// rewrites. The unit name, the script, and the dispatch command are three
/// readings of this one thing, which is why they are not three loose verbs.
struct BootOnceUnit<'run> {
    request_hash: &'run [u8],
    closure: &'run str,
    system_profile_path: &'run Path,
    boot_entries_directory: &'run Path,
}

/// Naming and scripting a one-shot activation.
trait BootOnceActivation {
    fn unit_name(&self) -> String;

    /// The `/bin/sh` script the unit runs: set the profile, switch to the
    /// closure for boot, then arm exactly one boot into the new entry while
    /// leaving the old entry as the default to fall back to.
    fn script(&self) -> String;

    /// The single remote command that schedules the unit over ssh.
    fn remote_dispatch_command(&self) -> String;
}

impl BootOnceActivation for BootOnceUnit<'_> {
    fn unit_name(&self) -> String {
        self.request_hash.boot_once_unit_name()
    }

    fn script(&self) -> String {
        let profile = self
            .system_profile_path
            .display()
            .to_string()
            .shell_quoted();
        let entries = self
            .boot_entries_directory
            .display()
            .to_string()
            .shell_quoted();
        let closure = self.closure.shell_quoted();
        let unit = self.unit_name().shell_quoted();
        format!(
            "set -eu\n\
             PATH=/nix/var/nix/profiles/default/bin:/run/current-system/sw/bin:/usr/bin:/bin\n\
             export PATH\n\
             CLOSURE={closure}\n\
             PROFILE={profile}\n\
             ENTRIES={entries}\n\
             UNIT={unit}\n\
             OLD=$(bootctl status | awk -F': *' '/Current Entry:/ {{print $2}}')\n\
             [ -n \"$OLD\" ]\n\
             nix-env -p \"$PROFILE\" --set \"$CLOSURE\"\n\
             \"$CLOSURE/bin/switch-to-configuration\" boot\n\
             LOADER_CONF=$(dirname \"$ENTRIES\")/loader.conf\n\
             NEW=$(awk '$1 == \"default\" {{print $2; exit}}' \"$LOADER_CONF\")\n\
             [ -n \"$NEW\" ]\n\
             [ -f \"$ENTRIES/$NEW\" ]\n\
             [ \"$NEW\" != \"$OLD\" ]\n\
             bootctl set-default \"$OLD\"\n\
             bootctl set-oneshot \"$NEW\"\n\
             printf '%s\\n' \"$UNIT boot-once prepared\"\n"
        )
    }

    fn remote_dispatch_command(&self) -> String {
        format!(
            "exec /run/current-system/sw/bin/systemd-run --unit={} --no-block --service-type=oneshot --property=RemainAfterExit=yes --setenv=PATH=/nix/var/nix/profiles/default/bin:/run/current-system/sw/bin:/usr/bin:/bin /bin/sh -eu -c {}",
            self.unit_name().shell_quoted(),
            self.script().shell_quoted()
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum UnitState {
    NotFound,
    Ready,
    Waiting,
    Failed,
}

impl From<&str> for UnitState {
    /// Read one `systemctl show` output. A unit that is loaded but neither
    /// finished nor failed is still Waiting, so a resumed run keeps polling
    /// rather than re-dispatching.
    fn from(output: &str) -> Self {
        let load_not_found = output
            .lines()
            .any(|line| line.trim() == "LoadState=not-found");
        let failed = output
            .lines()
            .any(|line| line.trim() == "ActiveState=failed" || line.trim() == "Result=failed");
        let ready = output
            .lines()
            .any(|line| line.trim() == "ActiveState=active")
            && output.lines().any(|line| line.trim() == "Result=success");
        if load_not_found {
            Self::NotFound
        } else if failed {
            Self::Failed
        } else if ready {
            Self::Ready
        } else {
            Self::Waiting
        }
    }
}

/// The systemctl query both activation backends run, differing only in how it
/// is carried to the machine that answers it.
const UNIT_STATE_QUERY: &str = "PATH=/nix/var/nix/profiles/default/bin:/run/current-system/sw/bin:/usr/bin:/bin; export PATH; systemctl show --property=LoadState --property=ActiveState --property=Result {} 2>/dev/null || printf 'LoadState=not-found\\n'";

/// Reaching the remote machine a run activates on.
trait RemoteActivation {
    /// The ssh binary to run. The flake wrapper sets `LOJIX_BOOTSTRAP_OPENSSH`
    /// to its openssh closure path; an unwrapped developer invocation gets a
    /// deterministic non-existent absolute path instead of silently consulting
    /// an ambient `PATH` executable.
    fn ssh_program(&self) -> String {
        std::env::var("LOJIX_BOOTSTRAP_OPENSSH")
            .unwrap_or_else(|_| "/__lojix-bootstrap-wrapper-required__/bin/ssh".to_string())
    }

    /// Whether the remote store already holds the built closure, so the copy
    /// stage can be skipped rather than repeated.
    fn has_closure<E: BootstrapExecutor>(
        &self,
        ssh_config: &Path,
        closure: &str,
        executor: &mut E,
    ) -> std::result::Result<bool, BootstrapError>;

    fn unit_state<E: BootstrapExecutor>(
        &self,
        ssh_config: &Path,
        unit: &str,
        executor: &mut E,
    ) -> std::result::Result<UnitState, BootstrapError>;

    /// Wait for the scheduled unit to finish, treating a unit that has gone
    /// missing as a run that must be recovered rather than one that succeeded.
    fn reconcile_activation<E: BootstrapExecutor, C: BootstrapCrashInjector>(
        &self,
        journal: &EphemeralJournal,
        ssh_config: &Path,
        unit: &str,
        executor: &mut E,
        crash: &mut C,
    ) -> std::result::Result<(), BootstrapError>;
}

impl RemoteActivation for BootstrapRemoteBackendValidated {
    fn has_closure<E: BootstrapExecutor>(
        &self,
        ssh_config: &Path,
        closure: &str,
        executor: &mut E,
    ) -> std::result::Result<bool, BootstrapError> {
        let command = format!(
            "PATH=/nix/var/nix/profiles/default/bin:/run/current-system/sw/bin:/usr/bin:/bin; export PATH; if nix-store --query --references {} >/dev/null 2>&1; then printf Present; else printf Absent; fi",
            closure.shell_quoted()
        );
        let output = executor
            .run(BootstrapCommand {
                program: self.ssh_program(),
                arguments: self.ssh_identity.ssh_arguments(ssh_config, command),
                environment: Vec::new(),
            })
            .map_err(|_| BootstrapError::RecoveryPending)?;
        Ok(output.trim() == "Present")
    }

    fn unit_state<E: BootstrapExecutor>(
        &self,
        ssh_config: &Path,
        unit: &str,
        executor: &mut E,
    ) -> std::result::Result<UnitState, BootstrapError> {
        let command = UNIT_STATE_QUERY.replace("{}", &unit.shell_quoted());
        let output = executor
            .run(BootstrapCommand {
                program: self.ssh_program(),
                arguments: self.ssh_identity.ssh_arguments(ssh_config, command),
                environment: Vec::new(),
            })
            .map_err(|_| BootstrapError::RecoveryPending)?;
        Ok(UnitState::from(output.as_str()))
    }

    fn reconcile_activation<E: BootstrapExecutor, C: BootstrapCrashInjector>(
        &self,
        journal: &EphemeralJournal,
        ssh_config: &Path,
        unit: &str,
        executor: &mut E,
        crash: &mut C,
    ) -> std::result::Result<(), BootstrapError> {
        if journal.succeeded(JournalStage::BootOnceActivated)? {
            return Ok(());
        }
        journal.intent(JournalStage::BootOnceActivated)?;
        for _ in 0..30 {
            match self.unit_state(ssh_config, unit, executor)? {
                UnitState::Ready => {
                    journal.receipt(JournalStage::BootOnceActivated)?;
                    journal.outcome(JournalStage::BootOnceActivated, true)?;
                    crash.after(BootstrapCrashPoint::AfterActivation)?;
                    return Ok(());
                }
                UnitState::Failed => {
                    return Err(BootstrapError::Effect(
                        BootstrapEffectStage::BootOnceActivated,
                    ));
                }
                UnitState::NotFound => return Err(BootstrapError::RecoveryPending),
                UnitState::Waiting => std::thread::sleep(std::time::Duration::from_millis(100)),
            }
        }
        Err(BootstrapError::RecoveryPending)
    }
}

/// Reaching the local machine a run activates on.
trait LocalActivation {
    fn unit_state<E: BootstrapExecutor>(
        &self,
        unit: &str,
        executor: &mut E,
    ) -> std::result::Result<UnitState, BootstrapError>;
}

impl LocalActivation for BootstrapLocalBackendValidated {
    fn unit_state<E: BootstrapExecutor>(
        &self,
        unit: &str,
        executor: &mut E,
    ) -> std::result::Result<UnitState, BootstrapError> {
        let command = UNIT_STATE_QUERY.replace("{}", &unit.shell_quoted());
        let output = executor
            .run(BootstrapCommand {
                program: "/bin/sh".to_string(),
                arguments: vec!["-eu".to_string(), "-c".to_string(), command],
                environment: Vec::new(),
            })
            .map_err(|_| BootstrapError::RecoveryPending)?;
        Ok(UnitState::from(output.as_str()))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Archive, RkyvSerialize, RkyvDeserialize)]
enum JournalStage {
    JournalCreated,
    Materialized,
    Tested,
    Built,
    GcRooted,
    Copied,
    BootOnceScheduled,
    BootOnceActivated,
    TerminalEvidenceWritten,
}

impl JournalStage {
    fn from_effect(stage: BootstrapEffectStage) -> Self {
        match stage {
            BootstrapEffectStage::JournalCreated => Self::JournalCreated,
            BootstrapEffectStage::Materialized => Self::Materialized,
            BootstrapEffectStage::Tested => Self::Tested,
            BootstrapEffectStage::Built => Self::Built,
            BootstrapEffectStage::GcRooted => Self::GcRooted,
            BootstrapEffectStage::Copied => Self::Copied,
            BootstrapEffectStage::BootOnceScheduled => Self::BootOnceScheduled,
            BootstrapEffectStage::BootOnceActivated => Self::BootOnceActivated,
            BootstrapEffectStage::TerminalEvidenceWritten => Self::TerminalEvidenceWritten,
        }
    }

    fn effect(self) -> BootstrapEffectStage {
        match self {
            Self::JournalCreated => BootstrapEffectStage::JournalCreated,
            Self::Materialized => BootstrapEffectStage::Materialized,
            Self::Tested => BootstrapEffectStage::Tested,
            Self::Built => BootstrapEffectStage::Built,
            Self::GcRooted => BootstrapEffectStage::GcRooted,
            Self::Copied => BootstrapEffectStage::Copied,
            Self::BootOnceScheduled => BootstrapEffectStage::BootOnceScheduled,
            Self::BootOnceActivated => BootstrapEffectStage::BootOnceActivated,
            Self::TerminalEvidenceWritten => BootstrapEffectStage::TerminalEvidenceWritten,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Archive, RkyvSerialize, RkyvDeserialize)]
enum JournalEventKind {
    Intent,
    Receipt,
    Outcome,
}

#[derive(Debug, Clone, Archive, RkyvSerialize, RkyvDeserialize)]
struct JournalEvent {
    stage: JournalStage,
    kind: JournalEventKind,
    succeeded: bool,
}

#[derive(Debug, Clone, Archive, RkyvSerialize, RkyvDeserialize)]
struct JournalFlakeOverride {
    name: String,
    reference: String,
}

#[derive(Debug, Archive, RkyvSerialize, RkyvDeserialize)]
struct BootstrapJournalConfiguration {
    schema_version: u32,
    request_hash: Vec<u8>,
    mode: BootstrapEvidenceMode,
    journal_device: u64,
    journal_inode: u64,
    resolved_flake_reference: String,
    closure_path: Option<String>,
    gc_root_target: Option<String>,
    gc_root_device: Option<u64>,
    gc_root_inode: Option<u64>,
    materialized_overrides: Option<Vec<JournalFlakeOverride>>,
    terminal_status: Option<BootstrapEvidenceStatus>,
    events: Vec<JournalEvent>,
}

#[derive(Debug)]
struct EphemeralJournal {
    directory: PathBuf,
    parent: PathBuf,
}

impl EphemeralJournal {
    fn open_or_create(
        request: &ValidatedBootstrapRun,
    ) -> std::result::Result<Self, BootstrapError> {
        let directory = request.journal_parent.join(format!(
            "{JOURNAL_PREFIX}{}",
            request.request_hash.hex_lower()
        ));
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
                let metadata = directory.private_directory_metadata()?;
                let state = BootstrapJournalConfiguration {
                    schema_version: JOURNAL_SCHEMA_VERSION,
                    request_hash: request.request_hash.clone(),
                    mode: request.mode.evidence_mode(),
                    journal_device: metadata.dev(),
                    journal_inode: metadata.ino(),
                    resolved_flake_reference: request.mode.input().flake_reference().to_string(),
                    closure_path: None,
                    gc_root_target: None,
                    gc_root_device: None,
                    gc_root_inode: None,
                    materialized_overrides: None,
                    terminal_status: None,
                    events: vec![JournalEvent {
                        stage: JournalStage::JournalCreated,
                        kind: JournalEventKind::Outcome,
                        succeeded: true,
                    }],
                };
                let journal = Self {
                    directory,
                    parent: request.journal_parent.clone(),
                };
                journal.write_state(&state)?;
                request.journal_parent.sync_directory()?;
                // This new isolated v5 journal always opens a separate Lojix
                // store.  It has no daemon configuration, socket, or legacy
                // store route.
                Store::open(journal.directory.join(JOURNAL_STORE_FILE))
                    .map_err(BootstrapError::JournalStore)?;
                Ok(journal)
            }
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                let journal = Self {
                    directory,
                    parent: request.journal_parent.clone(),
                };
                let state = journal.read_state()?;
                if state.schema_version != JOURNAL_SCHEMA_VERSION
                    || state.request_hash != request.request_hash
                    || state.mode != request.mode.evidence_mode()
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

    fn read_state(&self) -> std::result::Result<BootstrapJournalConfiguration, BootstrapError> {
        let bytes =
            fs::read(self.directory.join(JOURNAL_STATE_FILE)).map_err(BootstrapError::Journal)?;
        rkyv::from_bytes::<BootstrapJournalConfiguration, rkyv::rancor::Error>(&bytes)
            .map_err(|error| BootstrapError::Journal(std::io::Error::other(error.to_string())))
    }

    fn write_state(
        &self,
        state: &BootstrapJournalConfiguration,
    ) -> std::result::Result<(), BootstrapError> {
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

    fn verify_identity(
        &self,
        state: &BootstrapJournalConfiguration,
    ) -> std::result::Result<(), BootstrapError> {
        self.parent.private_directory_metadata()?;
        let metadata = self.directory.private_directory_metadata()?;
        if metadata.dev() != state.journal_device || metadata.ino() != state.journal_inode {
            return Err(BootstrapError::Validation("journal child identity changed"));
        }
        Ok(())
    }

    fn mutate(
        &self,
        mutate: impl FnOnce(&mut BootstrapJournalConfiguration),
    ) -> std::result::Result<(), BootstrapError> {
        let mut state = self.read_state()?;
        self.verify_identity(&state)?;
        mutate(&mut state);
        self.write_state(&state)
    }

    fn events(&self) -> std::result::Result<Vec<JournalEvent>, BootstrapError> {
        Ok(self.read_state()?.events)
    }

    fn succeeded(&self, stage: JournalStage) -> std::result::Result<bool, BootstrapError> {
        Ok(self.events()?.iter().any(|event| {
            event.stage == stage && event.kind == JournalEventKind::Outcome && event.succeeded
        }))
    }

    fn intent(&self, stage: JournalStage) -> std::result::Result<(), BootstrapError> {
        self.event(stage, JournalEventKind::Intent, true)
    }

    fn receipt(&self, stage: JournalStage) -> std::result::Result<(), BootstrapError> {
        self.event(stage, JournalEventKind::Receipt, true)
    }

    fn outcome(
        &self,
        stage: JournalStage,
        succeeded: bool,
    ) -> std::result::Result<(), BootstrapError> {
        self.event(stage, JournalEventKind::Outcome, succeeded)
    }

    fn event(
        &self,
        stage: JournalStage,
        kind: JournalEventKind,
        succeeded: bool,
    ) -> std::result::Result<(), BootstrapError> {
        self.mutate(|state| {
            state.events.push(JournalEvent {
                stage,
                kind,
                succeeded,
            })
        })
    }

    fn terminal_status(
        &self,
    ) -> std::result::Result<Option<BootstrapEvidenceStatus>, BootstrapError> {
        Ok(self.read_state()?.terminal_status)
    }

    fn set_terminal_status(
        &self,
        status: BootstrapEvidenceStatus,
    ) -> std::result::Result<(), BootstrapError> {
        self.mutate(|state| state.terminal_status = Some(status))
    }

    fn closure_path(&self) -> std::result::Result<Option<String>, BootstrapError> {
        Ok(self.read_state()?.closure_path)
    }

    fn set_closure_path(&self, closure: &str) -> std::result::Result<(), BootstrapError> {
        let closure = closure.to_string();
        self.mutate(|state| state.closure_path = Some(closure))
    }

    fn materialized_overrides(
        &self,
    ) -> std::result::Result<Option<Vec<FlakeOverride>>, BootstrapError> {
        Ok(self.read_state()?.materialized_overrides.map(|overrides| {
            overrides
                .into_iter()
                .map(|entry| FlakeOverride {
                    name: entry.name,
                    reference: entry.reference,
                })
                .collect()
        }))
    }

    fn set_materialized_overrides(
        &self,
        overrides: &[FlakeOverride],
    ) -> std::result::Result<(), BootstrapError> {
        let overrides = overrides
            .iter()
            .map(|entry| JournalFlakeOverride {
                name: entry.name.to_string(),
                reference: entry.reference.clone(),
            })
            .collect();
        self.mutate(|state| state.materialized_overrides = Some(overrides))
    }

    fn set_root_receipt(
        &self,
        root: &Path,
        closure: &str,
    ) -> std::result::Result<(), BootstrapError> {
        let metadata = fs::symlink_metadata(root).map_err(BootstrapError::Journal)?;
        let closure = closure.to_string();
        self.mutate(|state| {
            state.gc_root_target = Some(closure);
            state.gc_root_device = Some(metadata.dev());
            state.gc_root_inode = Some(metadata.ino());
        })
    }

    fn verify_root_receipt(
        &self,
        root: &Path,
        closure: &str,
    ) -> std::result::Result<bool, BootstrapError> {
        let state = self.read_state()?;
        let metadata = match fs::symlink_metadata(root) {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(false),
            Err(error) => return Err(BootstrapError::Journal(error)),
        };
        Ok(root.binds_closure(closure)
            && state.gc_root_target.as_deref() == Some(closure)
            && state.gc_root_device == Some(metadata.dev())
            && state.gc_root_inode == Some(metadata.ino()))
    }

    fn evidence(
        &self,
        status: BootstrapEvidenceStatus,
    ) -> std::result::Result<BootstrapTerminalEvidence, BootstrapError> {
        let state = self.read_state()?;
        let mut effects = Vec::new();
        for event in state.events {
            if event.kind == JournalEventKind::Outcome {
                effects.push(BootstrapEffectEvidence {
                    stage: event.stage.effect(),
                    succeeded: event.succeeded,
                });
            }
        }
        Ok(BootstrapTerminalEvidence {
            journal_schema_version: JOURNAL_SCHEMA_VERSION,
            request_hash: state.request_hash,
            mode: state.mode,
            status,
            effects,
        })
    }

    fn cleanup(self) -> std::result::Result<(), BootstrapError> {
        let state = self.read_state()?;
        self.verify_identity(&state)?;
        if !self.succeeded(JournalStage::TerminalEvidenceWritten)? {
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
    fn binds_closure(&self, closure: &str) -> bool;

    /// Hand a staging gc root over to its final name without ever replacing an
    /// existing one, then drop the staging name.
    fn link_root_no_replace(
        &self,
        root: &Path,
        closure: &str,
    ) -> std::result::Result<(), BootstrapError>;

    /// Create a private generated-input flake directory holding `flake_text`
    /// and, optionally, one additional named file.
    fn write_generated_flake(
        &self,
        additional: Option<(&str, String)>,
        flake_text: &str,
    ) -> std::result::Result<(), BootstrapError>;

    /// Create the private generated `secrets` flake, copying every
    /// `<attribute>.sops` file out of the validated secrets input.
    fn write_secrets_flake(
        &self,
        secrets_input: &BootstrapSecretsInputValidated,
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

    fn binds_closure(&self, closure: &str) -> bool {
        let Ok(metadata) = fs::symlink_metadata(self) else {
            return false;
        };
        metadata.file_type().is_symlink()
            && fs::read_link(self)
                .ok()
                .is_some_and(|target| target == Path::new(closure))
    }

    fn link_root_no_replace(
        &self,
        root: &Path,
        closure: &str,
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
        secrets_input: &BootstrapSecretsInputValidated,
    ) -> std::result::Result<(), BootstrapError> {
        fs::create_dir(self).map_err(BootstrapError::Journal)?;
        fs::set_permissions(self, fs::Permissions::from_mode(0o700))
            .map_err(BootstrapError::Journal)?;
        let mut entries = String::new();
        if let BootstrapSecretsInputValidated::Directory(source) = secrets_input {
            let mut files = Vec::new();
            for entry in fs::read_dir(source).map_err(|_| BootstrapError::Materialization)? {
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

#[derive(Debug)]
struct ValidatedBootstrapRun {
    request_hash: Vec<u8>,
    mode: BootstrapModeValidated,
    journal_parent: PathBuf,
    journal_directory: PathBuf,
    gc_root_path: PathBuf,
    gc_root_exists: bool,
    terminal_evidence_path: PathBuf,
    terminal_evidence_exists: bool,
}

#[derive(Debug)]
#[allow(clippy::large_enum_variant)]
enum BootstrapModeValidated {
    BuildOnly {
        input: BootstrapInputValidated,
        builder: Option<String>,
    },
    BootOnce(BootstrapBootOnceValidated),
}

#[derive(Debug)]
struct BootstrapBootOnceValidated {
    input: BootstrapInputValidated,
    builder: Option<String>,
    test_plan: BootstrapTestPlanValidated,
    activation_backend: BootstrapActivationBackendValidated,
}

#[derive(Debug)]
enum BootstrapInputValidated {
    Direct(BootstrapDirectInputValidated),
    Horizon(BootstrapHorizonInputValidated),
}

#[derive(Debug)]
struct BootstrapDirectInputValidated {
    flake_reference: String,
    nix_system: String,
    output_selector: String,
}

#[derive(Debug)]
struct BootstrapHorizonInputValidated {
    proposal_source: PathBuf,
    node_name: String,
    materialization_shape: BootstrapMaterializationShape,
    secrets_input: BootstrapSecretsInputValidated,
    flake_reference: String,
    nix_system: String,
    output_selector: String,
}

#[derive(Debug)]
enum BootstrapSecretsInputValidated {
    None,
    Directory(PathBuf),
}

#[derive(Debug)]
enum BootstrapTestPlanValidated {
    NoTest,
    RunHermeticTest(BootstrapHermeticTestValidated),
}

#[derive(Debug)]
struct BootstrapHermeticTestValidated {
    flake_reference: String,
    nix_system: String,
    output_selector: String,
}

#[derive(Debug)]
enum BootstrapActivationBackendValidated {
    Remote(BootstrapRemoteBackendValidated),
    Local(BootstrapLocalBackendValidated),
}

#[derive(Debug)]
struct BootstrapRemoteBackendValidated {
    nix_store_uri: String,
    ssh_identity: SshIdentity,
    ssh_policy: BootstrapSshPolicyValidated,
    system_profile_path: PathBuf,
    boot_entries_directory: PathBuf,
}

#[derive(Debug)]
struct BootstrapSshPolicyValidated {
    identity_file: PathBuf,
    known_hosts_file: PathBuf,
    strict_host_key_mode: BootstrapStrictHostKeyMode,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct SshIdentity {
    user: String,
    host: String,
    port: Option<u16>,
}

impl SshIdentity {
    fn destination(&self) -> String {
        format!("{}@{}", self.user, self.host)
    }

    fn ssh_arguments(&self, config: &Path, command: String) -> Vec<String> {
        let mut arguments = vec![
            "-F".to_string(),
            config.display().to_string(),
            "-o".to_string(),
            "ProxyCommand=none".to_string(),
            "-o".to_string(),
            "ProxyJump=none".to_string(),
            "-o".to_string(),
            "ControlMaster=no".to_string(),
            "-o".to_string(),
            "ControlPath=none".to_string(),
        ];
        if let Some(port) = self.port {
            arguments.extend(["-p".to_string(), port.to_string()]);
        }
        arguments.extend(["--".to_string(), self.destination(), command]);
        arguments
    }
}

impl BootstrapSshPolicyValidated {
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
        let strict = match self.strict_host_key_mode {
            BootstrapStrictHostKeyMode::RequireKnownHost => "yes",
        };
        let contents = format!(
            "Host *\n  IdentityFile {}\n  UserKnownHostsFile {}\n  GlobalKnownHostsFile /dev/null\n  StrictHostKeyChecking {strict}\n  IdentitiesOnly yes\n  IdentityAgent none\n  ProxyCommand none\n  ProxyJump none\n  ControlMaster no\n  ControlPath none\n",
            self.identity_file.display().to_string().ssh_config_quoted(),
            self.known_hosts_file
                .display()
                .to_string()
                .ssh_config_quoted(),
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

    fn nix_ssh_options(&self, config: &Path) -> String {
        format!(
            "-F {} -o BatchMode=yes -o IdentitiesOnly=yes -o IdentityAgent=none -o ProxyCommand=none -o ProxyJump=none -o ControlMaster=no -o ControlPath=none",
            config.display().to_string().shell_quoted()
        )
    }
}

#[derive(Debug)]
struct BootstrapLocalBackendValidated {
    system_profile_path: PathBuf,
    boot_entries_directory: PathBuf,
}

impl TryFrom<BootstrapRun> for ValidatedBootstrapRun {
    type Error = BootstrapError;

    fn try_from(request: BootstrapRun) -> std::result::Result<Self, Self::Error> {
        request.request_id.validated()?;
        let request_hash = request.fingerprint();
        let (
            mode,
            journal_parent,
            (gc_root_path, gc_root_exists),
            (terminal_evidence_path, terminal_evidence_exists),
        ) = match request.mode {
            BootstrapMode::BuildOnly(build_only) => (
                BootstrapModeValidated::BuildOnly {
                    input: BootstrapInputValidated::try_from(build_only.input)?,
                    builder: build_only.builder.validated()?,
                },
                OfferedPath::from(build_only.journal_parent.0.as_str())
                    .private_existing_directory()?,
                OfferedPath::from(build_only.gc_root_path.0.as_str()).private_output_path()?,
                OfferedPath::from(build_only.terminal_evidence_path.0.as_str())
                    .private_output_path()?,
            ),
            BootstrapMode::BootOnce(boot_once) => (
                BootstrapModeValidated::BootOnce(BootstrapBootOnceValidated {
                    input: BootstrapInputValidated::try_from(boot_once.input)?,
                    builder: boot_once.builder.validated()?,
                    test_plan: BootstrapTestPlanValidated::try_from(boot_once.test_plan)?,
                    activation_backend: BootstrapActivationBackendValidated::try_from(
                        boot_once.activation_backend,
                    )?,
                }),
                OfferedPath::from(boot_once.journal_parent.0.as_str())
                    .private_existing_directory()?,
                OfferedPath::from(boot_once.gc_root_path.0.as_str()).private_output_path()?,
                OfferedPath::from(boot_once.terminal_evidence_path.0.as_str())
                    .private_output_path()?,
            ),
        };
        if gc_root_path == terminal_evidence_path {
            return Err(BootstrapError::Validation(
                "gc root and evidence paths collide",
            ));
        }
        Ok(Self {
            request_hash,
            mode,
            journal_parent,
            // Filled after journal creation: keeping this value a child of the
            // validated parent prevents output authority from escaping it.
            journal_directory: PathBuf::new(),
            gc_root_path,
            gc_root_exists,
            terminal_evidence_path,
            terminal_evidence_exists,
        })
    }
}

impl BootstrapModeValidated {
    fn input(&self) -> &BootstrapInputValidated {
        match self {
            Self::BuildOnly { input, .. } => input,
            Self::BootOnce(boot_once) => &boot_once.input,
        }
    }

    fn builder(&self) -> Option<&str> {
        match self {
            Self::BuildOnly { builder, .. } => builder.as_deref(),
            Self::BootOnce(boot_once) => boot_once.builder.as_deref(),
        }
    }

    fn evidence_mode(&self) -> BootstrapEvidenceMode {
        match self {
            Self::BuildOnly { .. } => BootstrapEvidenceMode::BuildOnly,
            Self::BootOnce(_) => BootstrapEvidenceMode::BootOnce,
        }
    }
}

impl BootstrapInputValidated {
    fn flake_reference(&self) -> &str {
        match self {
            Self::Direct(input) => &input.flake_reference,
            Self::Horizon(input) => &input.flake_reference,
        }
    }

    fn nix_system(&self) -> &str {
        match self {
            Self::Direct(input) => &input.nix_system,
            Self::Horizon(input) => &input.nix_system,
        }
    }

    fn output_selector(&self) -> &str {
        match self {
            Self::Direct(input) => &input.output_selector,
            Self::Horizon(input) => &input.output_selector,
        }
    }
}

/// What bootstrap asks of each piece of caller-supplied text before it will
/// put that text in a command line. Every one of these answers the text back
/// unchanged or refuses it; nothing is sanitized into acceptability.
trait BootstrapWord {
    /// A pinned GitHub flake: `github:owner/repo/<40-hex revision>`. Nothing
    /// mutable is admitted, so a bootstrap is reproducible by construction.
    fn validated_flake_reference(&self) -> std::result::Result<String, BootstrapError>;

    /// A Nix platform double, alphanumeric with `-` and `_`.
    fn validated_nix_system(&self) -> std::result::Result<String, BootstrapError>;

    /// A flake output attribute path.
    fn validated_output_selector(&self) -> std::result::Result<String, BootstrapError>;

    /// A Horizon cluster or node name: alphanumeric with `-`.
    fn validated_horizon_name(&self) -> std::result::Result<String, BootstrapError>;
}

impl BootstrapWord for str {
    fn validated_flake_reference(&self) -> std::result::Result<String, BootstrapError> {
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
        Ok(self.to_string())
    }

    fn validated_nix_system(&self) -> std::result::Result<String, BootstrapError> {
        if self.is_empty()
            || !self
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
        {
            return Err(BootstrapError::Validation("nix system is unsafe"));
        }
        Ok(self.to_string())
    }

    fn validated_output_selector(&self) -> std::result::Result<String, BootstrapError> {
        if self.is_empty()
            || !self.bytes().all(|byte| {
                byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'-' | b'_' | b'\'')
            })
        {
            return Err(BootstrapError::Validation("output selector is unsafe"));
        }
        Ok(self.to_string())
    }

    fn validated_horizon_name(&self) -> std::result::Result<String, BootstrapError> {
        if self.is_empty()
            || !self
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
        {
            return Err(BootstrapError::Validation("horizon name is unsafe"));
        }
        Ok(self.to_string())
    }
}

/// Reading an ssh identity out of the two forms a caller can name one in.
trait SshIdentityText {
    /// An `ssh-ng://user@host[:port]` Nix store URI, answered with the URI and
    /// the identity it names, so the caller can prove the store and the ssh
    /// destination are the same machine.
    fn nix_store_uri_identity(&self) -> std::result::Result<(String, SshIdentity), BootstrapError>;

    /// A bare `user@host[:port]` ssh destination.
    fn ssh_destination_identity(&self) -> std::result::Result<SshIdentity, BootstrapError>;
}

impl SshIdentityText for str {
    fn nix_store_uri_identity(&self) -> std::result::Result<(String, SshIdentity), BootstrapError> {
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
        Ok((self.to_string(), SshIdentity::parse(user, host_port)?))
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
                (host, Some(port))
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
            user: user.to_string(),
            host: host.to_string(),
            port,
        })
    }
}

/// What a request id must be before it can name a private journal directory.
trait RequestIdentity {
    /// At most eighty alphanumerics, `-` and `_`: a name that cannot escape
    /// the journal parent or collide with a temporary.
    fn validated(&self) -> std::result::Result<String, BootstrapError>;
}

impl RequestIdentity for BootstrapRequestId {
    fn validated(&self) -> std::result::Result<String, BootstrapError> {
        let value = &self.0;
        if value.is_empty()
            || value.len() > 80
            || !value
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
        {
            return Err(BootstrapError::Validation("request id is unsafe"));
        }
        Ok(value.to_string())
    }
}

/// Raising a decoded builder choice to the validated optional specification.
/// The validated form is `Option<String>`, so this cannot be a `TryFrom`.
trait BuilderValidation {
    fn validated(self) -> std::result::Result<Option<String>, BootstrapError>;
}

impl BuilderValidation for BootstrapBuilder {
    fn validated(self) -> std::result::Result<Option<String>, BootstrapError> {
        match self {
            Self::NoBuilder => Ok(None),
            Self::NixBuilder(specification) => {
                let value = specification.0;
                if value.is_empty() || value.chars().any(char::is_control) || value.contains('\n') {
                    return Err(BootstrapError::Validation(
                        "builder specification is unsafe",
                    ));
                }
                Ok(Some(value))
            }
        }
    }
}

impl TryFrom<BootstrapInput> for BootstrapInputValidated {
    type Error = BootstrapError;

    fn try_from(input: BootstrapInput) -> std::result::Result<Self, Self::Error> {
        match input {
            BootstrapInput::Direct(input) => Ok(Self::Direct(BootstrapDirectInputValidated {
                flake_reference: input.flake_reference.0.validated_flake_reference()?,
                nix_system: input.nix_system.0.validated_nix_system()?,
                output_selector: input.output_selector.0.validated_output_selector()?,
            })),
            BootstrapInput::Horizon(input) => {
                input.cluster_name.0.validated_horizon_name()?;
                Ok(Self::Horizon(BootstrapHorizonInputValidated {
                    proposal_source: OfferedPath::from(input.proposal_source.0.as_str())
                        .existing_regular_file("horizon-definition.datom")?,
                    node_name: input.node_name.0.validated_horizon_name()?,
                    materialization_shape: input.materialization_shape,
                    secrets_input: match input.secrets_input {
                        BootstrapSecretsInput::NoSecrets => BootstrapSecretsInputValidated::None,
                        BootstrapSecretsInput::SecretsDirectory(directory) => {
                            BootstrapSecretsInputValidated::Directory(
                                OfferedPath::from(directory.0.as_str()).existing_directory()?,
                            )
                        }
                    },
                    flake_reference: input.flake_reference.0.validated_flake_reference()?,
                    nix_system: input.nix_system.0.validated_nix_system()?,
                    output_selector: input.output_selector.0.validated_output_selector()?,
                }))
            }
        }
    }
}

impl TryFrom<BootstrapTestPlan> for BootstrapTestPlanValidated {
    type Error = BootstrapError;

    fn try_from(plan: BootstrapTestPlan) -> std::result::Result<Self, Self::Error> {
        match plan {
            BootstrapTestPlan::NoTest => Ok(Self::NoTest),
            BootstrapTestPlan::RunHermeticTest(test) => {
                Ok(Self::RunHermeticTest(BootstrapHermeticTestValidated {
                    flake_reference: test.flake_reference.0.validated_flake_reference()?,
                    nix_system: test.nix_system.0.validated_nix_system()?,
                    output_selector: test.output_selector.0.validated_output_selector()?,
                }))
            }
        }
    }
}

impl TryFrom<BootstrapActivationBackend> for BootstrapActivationBackendValidated {
    type Error = BootstrapError;

    fn try_from(backend: BootstrapActivationBackend) -> std::result::Result<Self, Self::Error> {
        match backend {
            BootstrapActivationBackend::RemoteNixosSystemdBootV1(remote) => {
                let (nix_store_uri, store_identity) =
                    remote.nix_store_uri.0.nix_store_uri_identity()?;
                let ssh_identity = remote.ssh_destination.0.ssh_destination_identity()?;
                if store_identity != ssh_identity {
                    return Err(BootstrapError::Validation(
                        "remote store and ssh identities differ",
                    ));
                }
                Ok(Self::Remote(BootstrapRemoteBackendValidated {
                    nix_store_uri,
                    ssh_identity,
                    ssh_policy: BootstrapSshPolicyValidated::try_from(remote.ssh_policy)?,
                    system_profile_path: OfferedPath::from(remote.system_profile_path.0.as_str())
                        .absolute_normal()?,
                    boot_entries_directory: OfferedPath::from(
                        remote.boot_entries_directory.0.as_str(),
                    )
                    .absolute_normal()?,
                }))
            }
            BootstrapActivationBackend::LocalBootstrapV1(local) => {
                Ok(Self::Local(BootstrapLocalBackendValidated {
                    system_profile_path: OfferedPath::from(local.system_profile_path.0.as_str())
                        .existing_parent()?,
                    boot_entries_directory: OfferedPath::from(
                        local.boot_entries_directory.0.as_str(),
                    )
                    .existing_directory()?,
                }))
            }
        }
    }
}

impl TryFrom<BootstrapSshPolicy> for BootstrapSshPolicyValidated {
    type Error = BootstrapError;

    fn try_from(policy: BootstrapSshPolicy) -> std::result::Result<Self, Self::Error> {
        Ok(Self {
            identity_file: OfferedPath::from(policy.identity_file.0.as_str())
                .private_regular_file()?,
            known_hosts_file: OfferedPath::from(policy.known_hosts_file.0.as_str())
                .private_regular_file()?,
            strict_host_key_mode: policy.strict_host_key_mode,
        })
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

impl From<ingress::BootstrapRequest> for BootstrapRun {
    fn from(request: ingress::BootstrapRequest) -> Self {
        let ingress::BootstrapRequest::BootstrapRun(ingress::BootstrapRun {
            bootstrap_request_id: request_id,
            bootstrap_mode: mode,
        }) = request;
        Self {
            request_id: BootstrapRequestId(request_id),
            mode: BootstrapMode::from(mode),
        }
    }
}

impl From<ingress::BootstrapMode> for BootstrapMode {
    fn from(value: ingress::BootstrapMode) -> Self {
        match value {
            ingress::BootstrapMode::BuildOnly(ingress::BootstrapBuildOnly {
                bootstrap_input: input,
                bootstrap_builder: builder,
                bootstrap_journal_parent: journal_parent,
                bootstrap_gc_root_path: gc_root_path,
                bootstrap_terminal_evidence_path: terminal_evidence_path,
            }) => BootstrapMode::BuildOnly(BootstrapBuildOnly {
                input: BootstrapInput::from(input),
                builder: BootstrapBuilder::from(builder),
                journal_parent: BootstrapJournalParent(journal_parent),
                gc_root_path: BootstrapGcRootPath(gc_root_path),
                terminal_evidence_path: BootstrapTerminalEvidencePath(terminal_evidence_path),
            }),
            ingress::BootstrapMode::BootOnce(ingress::BootstrapBootOnce {
                bootstrap_input: input,
                bootstrap_builder: builder,
                bootstrap_test_plan: test_plan,
                bootstrap_activation_backend: activation_backend,
                bootstrap_journal_parent: journal_parent,
                bootstrap_gc_root_path: gc_root_path,
                bootstrap_terminal_evidence_path: terminal_evidence_path,
            }) => BootstrapMode::BootOnce(BootstrapBootOnce {
                input: BootstrapInput::from(input),
                builder: BootstrapBuilder::from(builder),
                test_plan: BootstrapTestPlan::from(test_plan),
                activation_backend: BootstrapActivationBackend::from(activation_backend),
                journal_parent: BootstrapJournalParent(journal_parent),
                gc_root_path: BootstrapGcRootPath(gc_root_path),
                terminal_evidence_path: BootstrapTerminalEvidencePath(terminal_evidence_path),
            }),
        }
    }
}

impl From<ingress::BootstrapInput> for BootstrapInput {
    fn from(value: ingress::BootstrapInput) -> Self {
        match value {
            ingress::BootstrapInput::Direct(ingress::BootstrapDirectInput {
                bootstrap_flake_reference: flake_reference,
                bootstrap_nix_system: nix_system,
                bootstrap_output_selector: output_selector,
            }) => BootstrapInput::Direct(BootstrapDirectInput {
                flake_reference: BootstrapFlakeReference(flake_reference),
                nix_system: BootstrapNixSystem(nix_system),
                output_selector: BootstrapOutputSelector(output_selector),
            }),
            ingress::BootstrapInput::Horizon(ingress::BootstrapHorizonInput {
                bootstrap_proposal_source: proposal_source,
                bootstrap_cluster_name: cluster_name,
                bootstrap_node_name: node_name,
                bootstrap_materialization_shape: materialization_shape,
                bootstrap_secrets_input: secrets_input,
                bootstrap_flake_reference: flake_reference,
                bootstrap_nix_system: nix_system,
                bootstrap_output_selector: output_selector,
            }) => BootstrapInput::Horizon(BootstrapHorizonInput {
                proposal_source: BootstrapProposalSource(proposal_source),
                cluster_name: BootstrapClusterName(cluster_name),
                node_name: BootstrapNodeName(node_name),
                materialization_shape: match materialization_shape {
                    ingress::BootstrapMaterializationShape::CompleteHost => {
                        BootstrapMaterializationShape::CompleteHost
                    }
                    ingress::BootstrapMaterializationShape::BaseHost => {
                        BootstrapMaterializationShape::BaseHost
                    }
                },
                secrets_input: match secrets_input {
                    ingress::BootstrapSecretsInput::NoSecrets => BootstrapSecretsInput::NoSecrets,
                    ingress::BootstrapSecretsInput::SecretsDirectory(path) => {
                        BootstrapSecretsInput::SecretsDirectory(BootstrapSecretsDirectory(path))
                    }
                },
                flake_reference: BootstrapFlakeReference(flake_reference),
                nix_system: BootstrapNixSystem(nix_system),
                output_selector: BootstrapOutputSelector(output_selector),
            }),
        }
    }
}

impl From<ingress::BootstrapBuilder> for BootstrapBuilder {
    fn from(value: ingress::BootstrapBuilder) -> Self {
        match value {
            ingress::BootstrapBuilder::NoBuilder => BootstrapBuilder::NoBuilder,
            ingress::BootstrapBuilder::NixBuilder(value) => {
                BootstrapBuilder::NixBuilder(BootstrapBuilderSpec(value))
            }
        }
    }
}

impl From<ingress::BootstrapTestPlan> for BootstrapTestPlan {
    fn from(value: ingress::BootstrapTestPlan) -> Self {
        match value {
            ingress::BootstrapTestPlan::NoTest => BootstrapTestPlan::NoTest,
            ingress::BootstrapTestPlan::RunHermeticTest(ingress::BootstrapHermeticTest {
                bootstrap_flake_reference: flake_reference,
                bootstrap_nix_system: nix_system,
                bootstrap_output_selector: output_selector,
            }) => BootstrapTestPlan::RunHermeticTest(BootstrapHermeticTest {
                flake_reference: BootstrapFlakeReference(flake_reference),
                nix_system: BootstrapNixSystem(nix_system),
                output_selector: BootstrapOutputSelector(output_selector),
            }),
        }
    }
}

impl From<ingress::BootstrapActivationBackend> for BootstrapActivationBackend {
    fn from(value: ingress::BootstrapActivationBackend) -> Self {
        match value {
            ingress::BootstrapActivationBackend::RemoteNixosSystemdBootV1(
                ingress::BootstrapRemoteNixosSystemdBootV1 {
                    bootstrap_nix_store_uri: nix_store_uri,
                    bootstrap_ssh_destination: ssh_destination,
                    bootstrap_ssh_policy:
                        ingress::BootstrapSshPolicy {
                            bootstrap_ssh_identity_file: identity_file,
                            bootstrap_ssh_known_hosts_file: known_hosts_file,
                            bootstrap_strict_host_key_mode: strict_host_key_mode,
                        },
                    bootstrap_system_profile_path: system_profile_path,
                    bootstrap_boot_entries_directory: boot_entries_directory,
                },
            ) => BootstrapActivationBackend::RemoteNixosSystemdBootV1(
                BootstrapRemoteNixosSystemdBootV1 {
                    nix_store_uri: BootstrapNixStoreUri(nix_store_uri),
                    ssh_destination: BootstrapSshDestination(ssh_destination),
                    ssh_policy: BootstrapSshPolicy {
                        identity_file: BootstrapSshIdentityFile(identity_file),
                        known_hosts_file: BootstrapSshKnownHostsFile(known_hosts_file),
                        strict_host_key_mode: match strict_host_key_mode {
                            ingress::BootstrapStrictHostKeyMode::RequireKnownHost => {
                                BootstrapStrictHostKeyMode::RequireKnownHost
                            }
                        },
                    },
                    system_profile_path: BootstrapSystemProfilePath(system_profile_path),
                    boot_entries_directory: BootstrapBootEntriesDirectory(boot_entries_directory),
                },
            ),
            ingress::BootstrapActivationBackend::LocalBootstrapV1(
                ingress::BootstrapLocalBootstrapV1 {
                    bootstrap_system_profile_path: system_profile_path,
                    bootstrap_boot_entries_directory: boot_entries_directory,
                },
            ) => BootstrapActivationBackend::LocalBootstrapV1(BootstrapLocalBootstrapV1 {
                system_profile_path: BootstrapSystemProfilePath(system_profile_path),
                boot_entries_directory: BootstrapBootEntriesDirectory(boot_entries_directory),
            }),
        }
    }
}

/// Starting one bootstrap run: decoding it, and running it to its terminal.
pub trait BootstrapInvocation {
    /// Decode and run exactly one inline Datom object. This never consults
    /// daemon environment variables or socket paths.
    fn run_from_environment() -> std::result::Result<BootstrapTerminal, BootstrapError>
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

    /// The private binding between a request and everything derived from it:
    /// its journal directory, its terminal evidence, and its boot-once unit.
    fn fingerprint(&self) -> Vec<u8>;

    /// Run the pipeline against an injected executor. The production entry
    /// point uses `ProcessBootstrapExecutor`; all tests use body-suppressed
    /// executors.
    fn run_with_executor<E: BootstrapExecutor>(
        self,
        executor: &mut E,
    ) -> std::result::Result<BootstrapTerminal, BootstrapError>
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
    ) -> std::result::Result<BootstrapTerminal, BootstrapError>
    where
        Self: Sized;
}

impl BootstrapInvocation for BootstrapRun {
    fn decode_single_inline(
        arguments: impl IntoIterator<Item = OsString>,
    ) -> std::result::Result<Self, BootstrapError> {
        let text = arguments.single_inline_datom()?;
        let request = Potential::<ingress::BootstrapRequest>::from(text)
            .actualize(&mut <crate::Ingress as crate::Budgeted>::budget())
            .map_err(|fault| BootstrapError::Decode(format!("{fault:?}")))?;
        Ok(Self::from(request))
    }

    fn fingerprint(&self) -> Vec<u8> {
        // `BootstrapRun` is entirely structured and its derived Debug form has
        // a stable field/variant order within this versioned ingress. The
        // digest is used only as the private journal/evidence binding and unit
        // derivation; raw request material never leaves the private journal
        // directory.
        Sha256::digest(format!("{self:?}").as_bytes()).to_vec()
    }

    fn run_with_executor_and_crash<E: BootstrapExecutor, C: BootstrapCrashInjector>(
        self,
        executor: &mut E,
        crash: &mut C,
    ) -> std::result::Result<BootstrapTerminal, BootstrapError> {
        let mut validated = ValidatedBootstrapRun::try_from(self)?;
        let journal = EphemeralJournal::open_or_create(&validated)?;
        validated.journal_directory = journal.directory.clone();
        let status = if let Some(status) = journal.terminal_status()? {
            status
        } else {
            match validated.execute(&journal, executor, crash) {
                Ok(()) => BootstrapEvidenceStatus::Succeeded,
                Err(BootstrapError::InjectedCrash) => return Err(BootstrapError::InjectedCrash),
                Err(BootstrapError::RecoveryPending) => {
                    return Err(BootstrapError::RecoveryPending);
                }
                Err(BootstrapError::Effect(stage)) => {
                    journal.outcome(JournalStage::from_effect(stage), false)?;
                    journal.set_terminal_status(BootstrapEvidenceStatus::Failed)?;
                    BootstrapEvidenceStatus::Failed
                }
                Err(error) => return Err(error),
            }
        };

        if journal.terminal_status()?.is_none() {
            journal.set_terminal_status(status)?;
        }
        validated.write_terminal_evidence(&journal, status, crash)?;
        journal.cleanup()?;

        Ok(BootstrapTerminal {
            status: status.as_str(),
        })
    }
}

/// Carrying one validated run through its effect stages. Every stage is
/// journalled before it is issued and again after it returns, so a resumed run
/// reads the journal rather than repeating the effect.
trait BootstrapExecution {
    /// Run every stage this mode calls for, from the far side of whatever the
    /// journal already records.
    fn execute<E: BootstrapExecutor, C: BootstrapCrashInjector>(
        &self,
        journal: &EphemeralJournal,
        executor: &mut E,
        crash: &mut C,
    ) -> std::result::Result<(), BootstrapError>;

    /// Evaluate the derivation and build it, answering with the closure path.
    fn build<E: BootstrapExecutor>(
        &self,
        overrides: &[FlakeOverride],
        executor: &mut E,
    ) -> std::result::Result<String, BootstrapError>;

    /// Turn a Horizon input into the generated flake inputs that pin it. A
    /// direct input materializes nothing.
    fn materialize<E: BootstrapExecutor>(
        &self,
        executor: &mut E,
    ) -> std::result::Result<Vec<FlakeOverride>, BootstrapError>;

    fn dispatch_and_reconcile_remote<E: BootstrapExecutor, C: BootstrapCrashInjector>(
        &self,
        journal: &EphemeralJournal,
        remote: &BootstrapRemoteBackendValidated,
        ssh_config: &Path,
        closure: &str,
        executor: &mut E,
        crash: &mut C,
    ) -> std::result::Result<(), BootstrapError>;

    fn dispatch_and_reconcile_local<E: BootstrapExecutor, C: BootstrapCrashInjector>(
        &self,
        journal: &EphemeralJournal,
        local: &BootstrapLocalBackendValidated,
        closure: &str,
        executor: &mut E,
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

impl BootstrapExecution for ValidatedBootstrapRun {
    fn execute<E: BootstrapExecutor, C: BootstrapCrashInjector>(
        &self,
        journal: &EphemeralJournal,
        executor: &mut E,
        crash: &mut C,
    ) -> std::result::Result<(), BootstrapError> {
        let overrides = if let Some(overrides) = journal.materialized_overrides()? {
            overrides
        } else {
            journal.intent(JournalStage::Materialized)?;
            let overrides = self
                .materialize(executor)
                .map_err(|_| BootstrapError::Effect(BootstrapEffectStage::Materialized))?;
            journal.set_materialized_overrides(&overrides)?;
            journal.receipt(JournalStage::Materialized)?;
            journal.outcome(JournalStage::Materialized, true)?;
            overrides
        };

        if let BootstrapModeValidated::BootOnce(boot_once) = &self.mode
            && let BootstrapTestPlanValidated::RunHermeticTest(test) = &boot_once.test_plan
            && !journal.succeeded(JournalStage::Tested)?
        {
            journal.intent(JournalStage::Tested)?;
            test.run(executor)
                .map_err(|_| BootstrapError::Effect(BootstrapEffectStage::Tested))?;
            journal.receipt(JournalStage::Tested)?;
            journal.outcome(JournalStage::Tested, true)?;
        }

        let closure = if let Some(closure) = journal.closure_path()? {
            closure
        } else {
            journal.intent(JournalStage::Built)?;
            let closure = self.build(&overrides, executor)?;
            journal.set_closure_path(&closure)?;
            journal.receipt(JournalStage::Built)?;
            journal.outcome(JournalStage::Built, true)?;
            closure
        };

        if !journal.succeeded(JournalStage::GcRooted)? {
            journal.intent(JournalStage::GcRooted)?;
            let staging = journal.directory.join("gc-root-staging");
            let staging_exists = staging.entry_exists()?;
            let root_exists = self.gc_root_path.entry_exists()?;
            if staging_exists && root_exists {
                return Err(BootstrapError::Validation("gc root handoff collision"));
            }
            if staging_exists {
                if !staging.binds_closure(&closure) {
                    return Err(BootstrapError::Validation(
                        "gc root staging receipt changed",
                    ));
                }
                staging.link_root_no_replace(&self.gc_root_path, &closure)?;
            } else if root_exists {
                if !self.gc_root_path.binds_closure(&closure) {
                    return Err(BootstrapError::Validation("gc root receipt changed"));
                }
            } else {
                executor
                    .run(BootstrapCommand {
                        program: "nix-store".to_string(),
                        arguments: vec![
                            "--add-root".to_string(),
                            staging.display().to_string(),
                            "--realise".to_string(),
                            closure.clone(),
                        ],
                        environment: Vec::new(),
                    })
                    .map_err(|_| BootstrapError::Effect(BootstrapEffectStage::GcRooted))?;
                crash.after(BootstrapCrashPoint::AfterGcRootCommand)?;
                staging.link_root_no_replace(&self.gc_root_path, &closure)?;
            }
            journal.set_root_receipt(&self.gc_root_path, &closure)?;
            journal.receipt(JournalStage::GcRooted)?;
            journal.outcome(JournalStage::GcRooted, true)?;
            crash.after(BootstrapCrashPoint::AfterGcRoot)?;
        } else if !journal.verify_root_receipt(&self.gc_root_path, &closure)? {
            return Err(BootstrapError::Validation(
                "gc root receipt identity changed",
            ));
        }

        // The exact BuildOnly variant has no activation representation, rather
        // than a boolean that a future refactor could accidentally ignore.
        let BootstrapModeValidated::BootOnce(boot_once) = &self.mode else {
            return Ok(());
        };

        match &boot_once.activation_backend {
            BootstrapActivationBackendValidated::Remote(remote) => {
                let ssh_config = remote.ssh_policy.write_private_config(journal)?;
                if !journal.succeeded(JournalStage::Copied)? {
                    journal.intent(JournalStage::Copied)?;
                    if !remote.has_closure(&ssh_config, &closure, executor)? {
                        executor
                            .run(BootstrapCommand {
                                program: "nix".to_string(),
                                arguments: vec![
                                    "copy".to_string(),
                                    "--substitute-on-destination".to_string(),
                                    "--to".to_string(),
                                    remote.nix_store_uri.clone(),
                                    closure.clone(),
                                ],
                                environment: vec![(
                                    "NIX_SSHOPTS".to_string(),
                                    remote.ssh_policy.nix_ssh_options(&ssh_config),
                                )],
                            })
                            .map_err(|_| BootstrapError::Effect(BootstrapEffectStage::Copied))?;
                    }
                    journal.receipt(JournalStage::Copied)?;
                    journal.outcome(JournalStage::Copied, true)?;
                    crash.after(BootstrapCrashPoint::AfterCopy)?;
                }
                self.dispatch_and_reconcile_remote(
                    journal,
                    remote,
                    &ssh_config,
                    &closure,
                    executor,
                    crash,
                )?;
            }
            BootstrapActivationBackendValidated::Local(local) => {
                self.dispatch_and_reconcile_local(journal, local, &closure, executor, crash)?;
            }
        }
        Ok(())
    }

    fn build<E: BootstrapExecutor>(
        &self,
        overrides: &[FlakeOverride],
        executor: &mut E,
    ) -> std::result::Result<String, BootstrapError> {
        let input = self.mode.input();
        let mut evaluation_arguments = vec![
            "eval".to_string(),
            "--raw".to_string(),
            "--system".to_string(),
            input.nix_system().to_string(),
        ];
        evaluation_arguments.extend(overrides.flattened());
        evaluation_arguments.push(format!(
            "{}#{}.drvPath",
            input.flake_reference(),
            input.output_selector()
        ));
        let derivation = (executor.run(BootstrapCommand {
            program: "nix".to_string(),
            arguments: evaluation_arguments,
            environment: Vec::new(),
        })?)
        .first_line()?;
        if !NixStorePath::from(derivation.as_str()).is_canonical_item()
            || !derivation.ends_with(".drv")
        {
            return Err(BootstrapError::Effect(BootstrapEffectStage::Built));
        }

        let mut build_arguments = vec![
            "build".to_string(),
            "--no-link".to_string(),
            "--print-out-paths".to_string(),
            "--system".to_string(),
            input.nix_system().to_string(),
        ];
        if let Some(builder) = self.mode.builder() {
            build_arguments.extend([
                "--option".to_string(),
                "max-jobs".to_string(),
                "0".to_string(),
                "--builders".to_string(),
                builder.to_string(),
            ]);
        }
        build_arguments.push(format!("{derivation}^*"));
        let closure = (executor.run(BootstrapCommand {
            program: "nix".to_string(),
            arguments: build_arguments,
            environment: Vec::new(),
        })?)
        .first_line()?;
        if !NixStorePath::from(closure.as_str()).is_canonical_item() || closure.ends_with(".drv") {
            return Err(BootstrapError::Effect(BootstrapEffectStage::Built));
        }
        Ok(closure)
    }

    fn materialize<E: BootstrapExecutor>(
        &self,
        executor: &mut E,
    ) -> std::result::Result<Vec<FlakeOverride>, BootstrapError> {
        let BootstrapInputValidated::Horizon(input) = self.mode.input() else {
            return Ok(Vec::new());
        };
        let proposal_text = fs::read_to_string(&input.proposal_source)
            .map_err(|_| BootstrapError::Materialization)?;
        let definition: HorizonDefinition = HorizonDefinition::decode(&proposal_text)
            .map_err(|_| BootstrapError::Materialization)?;
        let horizon = definition
            .project(&input.node_name)
            .map_err(|_| BootstrapError::Materialization)?;
        let Some(projected_system) = horizon.node.machine.architecture.nix_system() else {
            return Err(BootstrapError::Materialization);
        };
        if projected_system != input.nix_system {
            return Err(BootstrapError::Materialization);
        }

        let generated = self.journal_directory.join("generated-inputs");
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
            serde_json::to_string_pretty(&horizon).map_err(|_| BootstrapError::Materialization)?,
        )),
        "{ outputs = _: { horizon = builtins.fromJSON (builtins.readFile ./horizon.json); }; }\n",
    )?;
        directories.insert("horizon", horizon_directory);

        let system_directory = generated.join("system");
        system_directory.write_generated_flake(
            None,
            &format!(
                "{{ outputs = _: {{ system = \"{}\"; }}; }}\n",
                input.nix_system
            ),
        )?;
        directories.insert("system", system_directory);

        let deployment_directory = generated.join("deployment");
        let (include_home, include_all_firmware) = match input.materialization_shape {
            BootstrapMaterializationShape::CompleteHost => (true, true),
            BootstrapMaterializationShape::BaseHost => (false, false),
        };
        deployment_directory.write_generated_flake(
        None,
        &format!(
            "{{ outputs = _: {{ deployment = {{ includeHome = {}; includeAllFirmware = {}; }}; }}; }}\n",
            include_home, include_all_firmware
        ),
    )?;
        directories.insert("deployment", deployment_directory);

        let secrets_directory = generated.join("secrets");
        secrets_directory.write_secrets_flake(&input.secrets_input)?;
        directories.insert("secrets", secrets_directory);

        let mut overrides = Vec::with_capacity(directories.len());
        for (name, directory) in directories {
            let hash = (executor.run(BootstrapCommand {
                program: "nix".to_string(),
                arguments: vec![
                    "hash".to_string(),
                    "path".to_string(),
                    "--type".to_string(),
                    "sha256".to_string(),
                    "--sri".to_string(),
                    directory.display().to_string(),
                ],
                environment: Vec::new(),
            })?)
            .first_line()?;
            if !hash.starts_with("sha256-") || hash.chars().any(char::is_control) {
                return Err(BootstrapError::Materialization);
            }
            overrides.push(FlakeOverride {
                name: name.to_string(),
                reference: format!(
                    "path:{}?narHash={}",
                    directory.display(),
                    hash.percent_encoded_nar_hash()
                ),
            });
        }
        Ok(overrides)
    }

    fn dispatch_and_reconcile_remote<E: BootstrapExecutor, C: BootstrapCrashInjector>(
        &self,
        journal: &EphemeralJournal,
        remote: &BootstrapRemoteBackendValidated,
        ssh_config: &Path,
        closure: &str,
        executor: &mut E,
        crash: &mut C,
    ) -> std::result::Result<(), BootstrapError> {
        let boot_once = BootOnceUnit {
            request_hash: &self.request_hash,
            closure,
            system_profile_path: &remote.system_profile_path,
            boot_entries_directory: &remote.boot_entries_directory,
        };
        let unit = boot_once.unit_name();
        if !journal.succeeded(JournalStage::BootOnceScheduled)? {
            journal.intent(JournalStage::BootOnceScheduled)?;
            if remote.unit_state(ssh_config, &unit, executor)? == UnitState::NotFound {
                executor
                    .run(BootstrapCommand {
                        program: remote.ssh_program(),
                        arguments: remote
                            .ssh_identity
                            .ssh_arguments(ssh_config, boot_once.remote_dispatch_command()),
                        environment: Vec::new(),
                    })
                    .map_err(|_| BootstrapError::RecoveryPending)?;
            }
            journal.receipt(JournalStage::BootOnceScheduled)?;
            journal.outcome(JournalStage::BootOnceScheduled, true)?;
            crash.after(BootstrapCrashPoint::AfterDispatch)?;
        }
        remote.reconcile_activation(journal, ssh_config, &unit, executor, crash)
    }

    fn dispatch_and_reconcile_local<E: BootstrapExecutor, C: BootstrapCrashInjector>(
        &self,
        journal: &EphemeralJournal,
        local: &BootstrapLocalBackendValidated,
        closure: &str,
        executor: &mut E,
        crash: &mut C,
    ) -> std::result::Result<(), BootstrapError> {
        let boot_once = BootOnceUnit {
            request_hash: &self.request_hash,
            closure,
            system_profile_path: &local.system_profile_path,
            boot_entries_directory: &local.boot_entries_directory,
        };
        let unit = boot_once.unit_name();
        if !journal.succeeded(JournalStage::BootOnceScheduled)? {
            journal.intent(JournalStage::BootOnceScheduled)?;
            if local.unit_state(&unit, executor)? == UnitState::NotFound {
                executor
                .run(BootstrapCommand {
                    program: "/run/current-system/sw/bin/systemd-run".to_string(),
                    arguments: vec![
                        format!("--unit={unit}"),
                        "--no-block".to_string(),
                        "--service-type=oneshot".to_string(),
                        "--property=RemainAfterExit=yes".to_string(),
                        "--setenv=PATH=/nix/var/nix/profiles/default/bin:/run/current-system/sw/bin:/usr/bin:/bin".to_string(),
                        "/bin/sh".to_string(),
                        "-eu".to_string(),
                        "-c".to_string(),
                        boot_once.script(),
                    ],
                    environment: Vec::new(),
                })
                .map_err(|_| BootstrapError::RecoveryPending)?;
            }
            journal.receipt(JournalStage::BootOnceScheduled)?;
            journal.outcome(JournalStage::BootOnceScheduled, true)?;
            crash.after(BootstrapCrashPoint::AfterDispatch)?;
        }
        if journal.succeeded(JournalStage::BootOnceActivated)? {
            return Ok(());
        }
        journal.intent(JournalStage::BootOnceActivated)?;
        for _ in 0..30 {
            match local.unit_state(&unit, executor)? {
                UnitState::Ready => {
                    journal.receipt(JournalStage::BootOnceActivated)?;
                    journal.outcome(JournalStage::BootOnceActivated, true)?;
                    crash.after(BootstrapCrashPoint::AfterActivation)?;
                    return Ok(());
                }
                UnitState::Failed => {
                    return Err(BootstrapError::Effect(
                        BootstrapEffectStage::BootOnceActivated,
                    ));
                }
                UnitState::NotFound => return Err(BootstrapError::RecoveryPending),
                UnitState::Waiting => std::thread::sleep(std::time::Duration::from_millis(100)),
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
        if journal.succeeded(JournalStage::TerminalEvidenceWritten)? {
            return Ok(());
        }
        journal.intent(JournalStage::TerminalEvidenceWritten)?;
        if self.terminal_evidence_exists {
            let existing = BootstrapTerminalEvidence::read_from(&self.terminal_evidence_path)?;
            if existing.journal_schema_version != JOURNAL_SCHEMA_VERSION
                || existing.request_hash != self.request_hash
                || existing.status != status
            {
                return Err(BootstrapError::Validation(
                    "existing evidence does not bind this self",
                ));
            }
        } else {
            let evidence = journal.evidence(status)?;
            evidence.write_new(&self.terminal_evidence_path)?;
        }
        crash.after(BootstrapCrashPoint::AfterEvidence)?;
        journal.receipt(JournalStage::TerminalEvidenceWritten)?;
        journal.outcome(JournalStage::TerminalEvidenceWritten, true)
    }
}

/// Running the hermetic test a boot-once plan may carry.
trait HermeticTestRunning {
    fn run<E: BootstrapExecutor>(
        &self,
        executor: &mut E,
    ) -> std::result::Result<(), BootstrapError>;
}

impl HermeticTestRunning for BootstrapHermeticTestValidated {
    fn run<E: BootstrapExecutor>(
        &self,
        executor: &mut E,
    ) -> std::result::Result<(), BootstrapError> {
        executor.run(BootstrapCommand {
            program: "nix".to_string(),
            arguments: vec![
                "build".to_string(),
                "--no-link".to_string(),
                "--print-out-paths".to_string(),
                "--system".to_string(),
                self.nix_system.clone(),
                format!("{}#{}", self.flake_reference, self.output_selector),
            ],
            environment: Vec::new(),
        })?;
        Ok(())
    }
}

/// The generated flake inputs a materialized run pins, read as the `nix`
/// arguments that pin them.
trait FlakeOverrides {
    fn flattened(&self) -> Vec<String>;
}

impl FlakeOverrides for [FlakeOverride] {
    fn flattened(&self) -> Vec<String> {
        let mut arguments = Vec::with_capacity(self.len() * 3);
        for override_input in self {
            arguments.extend([
                "--override-input".to_string(),
                override_input.name.clone(),
                override_input.reference.clone(),
            ]);
        }
        arguments
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
