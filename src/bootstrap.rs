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

use std::collections::BTreeMap;
use std::ffi::OsString;
use std::fs;
use std::io::Write;
use std::os::unix::fs::{MetadataExt, PermissionsExt};
use std::path::{Component, Path, PathBuf};
use std::process::Command;
use std::sync::atomic::{AtomicU64, Ordering};

use dotos::{DotosDecode, DotosDecodeError, DotosSource};
use horizon_lib::name::{ClusterName as HorizonClusterName, NodeName as HorizonNodeName};
use horizon_lib::{ClusterProposal, Viewpoint};
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use sha2::{Digest, Sha256};
use thiserror::Error;

use crate::Store;

const JOURNAL_SCHEMA_VERSION: u32 = 5;
const JOURNAL_PREFIX: &str = ".lojix-bootstrap-v5-";
const TEMPORARY_EVIDENCE_PREFIX: &str = ".lojix-bootstrap-evidence-";
const BOOT_ENTRY_PREFIX: &str = "nixos-generation-";
const JOURNAL_STATE_FILE: &str = "bootstrap-v5.rkyv";
const JOURNAL_STORE_FILE: &str = "lojix-v5.sema";
const PRIVATE_DIRECTORY_MODE: u32 = 0o700;
const PRIVATE_EVIDENCE_MODE: u32 = 0o600;

static JOURNAL_SEQUENCE: AtomicU64 = AtomicU64::new(0);

/// The one accepted inline object.
#[derive(Debug, Clone, PartialEq, Eq, DotosDecode)]
pub enum BootstrapCliInput {
    BootstrapRun(BootstrapRun),
}

#[derive(Debug, Clone, PartialEq, Eq, DotosDecode)]
pub struct BootstrapRun {
    pub request_id: BootstrapRequestId,
    pub mode: BootstrapMode,
}

#[derive(Debug, Clone, PartialEq, Eq, DotosDecode)]
pub struct BootstrapRequestId(pub String);

#[derive(Debug, Clone, PartialEq, Eq, DotosDecode)]
pub enum BootstrapMode {
    BuildOnly(BootstrapBuildOnly),
    BootOnce(BootstrapBootOnce),
}

/// The exact dry-run/build-only variant.  Its type has no transport or
/// activation field, so a decoded BuildOnly request cannot activate by design.
#[derive(Debug, Clone, PartialEq, Eq, DotosDecode)]
pub struct BootstrapBuildOnly {
    pub input: BootstrapInput,
    pub builder: BootstrapBuilder,
    pub journal_parent: BootstrapJournalParent,
    pub gc_root_path: BootstrapGcRootPath,
    pub terminal_evidence_path: BootstrapTerminalEvidencePath,
}

#[derive(Debug, Clone, PartialEq, Eq, DotosDecode)]
pub struct BootstrapBootOnce {
    pub input: BootstrapInput,
    pub builder: BootstrapBuilder,
    pub test_plan: BootstrapTestPlan,
    pub activation_backend: BootstrapActivationBackend,
    pub journal_parent: BootstrapJournalParent,
    pub gc_root_path: BootstrapGcRootPath,
    pub terminal_evidence_path: BootstrapTerminalEvidencePath,
}

#[derive(Debug, Clone, PartialEq, Eq, DotosDecode)]
pub enum BootstrapInput {
    Direct(BootstrapDirectInput),
    Horizon(BootstrapHorizonInput),
}

#[derive(Debug, Clone, PartialEq, Eq, DotosDecode)]
pub struct BootstrapDirectInput {
    pub flake_reference: BootstrapFlakeReference,
    pub nix_system: BootstrapNixSystem,
    pub output_selector: BootstrapOutputSelector,
}

/// Horizon materialization carries its complete authority surface.  In
/// particular, `secrets_input` is explicit; no sibling `secrets/` directory is
/// inferred from the proposal path.
#[derive(Debug, Clone, PartialEq, Eq, DotosDecode)]
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, DotosDecode)]
pub enum BootstrapMaterializationShape {
    CompleteHost,
    BaseHost,
}

#[derive(Debug, Clone, PartialEq, Eq, DotosDecode)]
pub enum BootstrapSecretsInput {
    NoSecrets,
    SecretsDirectory(BootstrapSecretsDirectory),
}

#[derive(Debug, Clone, PartialEq, Eq, DotosDecode)]
pub enum BootstrapBuilder {
    NoBuilder,
    NixBuilder(BootstrapBuilderSpec),
}

#[derive(Debug, Clone, PartialEq, Eq, DotosDecode)]
pub enum BootstrapTestPlan {
    NoTest,
    RunHermeticTest(BootstrapHermeticTest),
}

#[derive(Debug, Clone, PartialEq, Eq, DotosDecode)]
pub struct BootstrapHermeticTest {
    pub flake_reference: BootstrapFlakeReference,
    pub nix_system: BootstrapNixSystem,
    pub output_selector: BootstrapOutputSelector,
}

#[derive(Debug, Clone, PartialEq, Eq, DotosDecode)]
pub enum BootstrapActivationBackend {
    RemoteNixosSystemdBootV1(BootstrapRemoteNixosSystemdBootV1),
    LocalBootstrapV1(BootstrapLocalBootstrapV1),
}

#[derive(Debug, Clone, PartialEq, Eq, DotosDecode)]
pub struct BootstrapRemoteNixosSystemdBootV1 {
    pub nix_store_uri: BootstrapNixStoreUri,
    pub ssh_destination: BootstrapSshDestination,
    pub system_profile_path: BootstrapSystemProfilePath,
    pub boot_entries_directory: BootstrapBootEntriesDirectory,
}

#[derive(Debug, Clone, PartialEq, Eq, DotosDecode)]
pub struct BootstrapLocalBootstrapV1 {
    pub system_profile_path: BootstrapSystemProfilePath,
    pub boot_entries_directory: BootstrapBootEntriesDirectory,
}

macro_rules! text_field {
    ($name:ident) => {
        #[derive(Debug, Clone, PartialEq, Eq, DotosDecode)]
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
    #[error("bootstrap invocation requires one inline DOTOS object: {0}")]
    Argument(#[from] crate::Error),
    #[error("bootstrap DOTOS decode failed: {0}")]
    Decode(#[from] DotosDecodeError),
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
            .output()
            .map_err(|_| BootstrapError::Effect(BootstrapEffectStage::Built))?;
        if !output.status.success() {
            return Err(BootstrapError::Effect(BootstrapEffectStage::Built));
        }
        Ok(String::from_utf8_lossy(&output.stdout).into_owned())
    }
}

/// Decode and run exactly one inline DOTOS object.  This function never
/// consults daemon environment variables or socket paths.
pub fn run_from_environment() -> std::result::Result<BootstrapTerminal, BootstrapError> {
    let request = decode_single_inline(std::env::args_os().skip(1))?;
    let mut executor = ProcessBootstrapExecutor;
    run_with_executor(request, &mut executor)
}

pub fn decode_single_inline(
    arguments: impl IntoIterator<Item = OsString>,
) -> std::result::Result<BootstrapRun, BootstrapError> {
    let text = crate::single_inline_dotos_argument(arguments)?;
    let BootstrapCliInput::BootstrapRun(request) = DotosSource::new(&text).parse()?;
    Ok(request)
}

/// Run the pipeline against an injected executor.  The production entry point
/// uses `ProcessBootstrapExecutor`; all tests use body-suppressed executors.
pub fn run_with_executor<E: BootstrapExecutor>(
    request: BootstrapRun,
    executor: &mut E,
) -> std::result::Result<BootstrapTerminal, BootstrapError> {
    let mut never_crash = NeverCrash;
    run_with_executor_and_crash(request, executor, &mut never_crash)
}

/// Crash injection is deliberately a test seam: it returns before terminal
/// evidence or cleanup, exactly like an abrupt process loss at that boundary.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BootstrapCrashPoint {
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

/// Hermetic tests use this to prove that each persisted receipt resumes at the
/// next exact stage without issuing its preceding effect again.
pub fn run_with_executor_and_crash<E: BootstrapExecutor, C: BootstrapCrashInjector>(
    request: BootstrapRun,
    executor: &mut E,
    crash: &mut C,
) -> std::result::Result<BootstrapTerminal, BootstrapError> {
    let mut validated = ValidatedBootstrapRun::try_from(request)?;
    let journal = EphemeralJournal::open_or_create(&validated)?;
    validated.journal_directory = journal.directory.clone();
    let status = if let Some(status) = journal.terminal_status()? {
        status
    } else {
        match execute(&validated, &journal, executor, crash) {
            Ok(()) => BootstrapEvidenceStatus::Succeeded,
            Err(BootstrapError::InjectedCrash) => return Err(BootstrapError::InjectedCrash),
            Err(BootstrapError::RecoveryPending) => return Err(BootstrapError::RecoveryPending),
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
    write_terminal_evidence(&validated, &journal, status, crash)?;
    journal.cleanup()?;

    Ok(BootstrapTerminal {
        status: status.as_str(),
    })
}

fn execute<E: BootstrapExecutor, C: BootstrapCrashInjector>(
    request: &ValidatedBootstrapRun,
    journal: &EphemeralJournal,
    executor: &mut E,
    crash: &mut C,
) -> std::result::Result<(), BootstrapError> {
    let overrides = if let Some(overrides) = journal.materialized_overrides()? {
        overrides
    } else {
        journal.intent(JournalStage::Materialized)?;
        let overrides = materialize(request, executor)
            .map_err(|_| BootstrapError::Effect(BootstrapEffectStage::Materialized))?;
        journal.set_materialized_overrides(&overrides)?;
        journal.receipt(JournalStage::Materialized)?;
        journal.outcome(JournalStage::Materialized, true)?;
        overrides
    };

    if let BootstrapModeValidated::BootOnce(boot_once) = &request.mode
        && let BootstrapTestPlanValidated::RunHermeticTest(test) = &boot_once.test_plan
        && !journal.succeeded(JournalStage::Tested)?
    {
        journal.intent(JournalStage::Tested)?;
        run_hermetic_test(test, executor)
            .map_err(|_| BootstrapError::Effect(BootstrapEffectStage::Tested))?;
        journal.receipt(JournalStage::Tested)?;
        journal.outcome(JournalStage::Tested, true)?;
    }

    let closure = if let Some(closure) = journal.closure_path()? {
        closure
    } else {
        journal.intent(JournalStage::Built)?;
        let closure = build(request, &overrides, executor)?;
        journal.set_closure_path(&closure)?;
        journal.receipt(JournalStage::Built)?;
        journal.outcome(JournalStage::Built, true)?;
        closure
    };

    if !journal.succeeded(JournalStage::GcRooted)? {
        journal.intent(JournalStage::GcRooted)?;
        if !verify_root_receipt(&request.gc_root_path, &closure) {
            let staging = journal.directory.join("gc-root-staging");
            if staging.exists() {
                return Err(BootstrapError::Validation("gc root staging already exists"));
            }
            executor
                .run(BootstrapCommand {
                    program: "nix-store".to_string(),
                    arguments: vec![
                        "--add-root".to_string(),
                        staging.display().to_string(),
                        "--realise".to_string(),
                        closure.clone(),
                    ],
                })
                .map_err(|_| BootstrapError::Effect(BootstrapEffectStage::GcRooted))?;
            link_root_no_replace(&staging, &request.gc_root_path, &closure)?;
        }
        journal.set_root_receipt(&request.gc_root_path, &closure)?;
        journal.receipt(JournalStage::GcRooted)?;
        journal.outcome(JournalStage::GcRooted, true)?;
        crash.after(BootstrapCrashPoint::AfterGcRoot)?;
    } else if !journal.verify_root_receipt(&request.gc_root_path, &closure)? {
        return Err(BootstrapError::Validation(
            "gc root receipt identity changed",
        ));
    }

    // The exact BuildOnly variant has no activation representation, rather
    // than a boolean that a future refactor could accidentally ignore.
    let BootstrapModeValidated::BootOnce(boot_once) = &request.mode else {
        return Ok(());
    };

    match &boot_once.activation_backend {
        BootstrapActivationBackendValidated::Remote(remote) => {
            if !journal.succeeded(JournalStage::Copied)? {
                journal.intent(JournalStage::Copied)?;
                if !remote_has_closure(remote, &closure, executor)? {
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
                        })
                        .map_err(|_| BootstrapError::Effect(BootstrapEffectStage::Copied))?;
                }
                journal.receipt(JournalStage::Copied)?;
                journal.outcome(JournalStage::Copied, true)?;
                crash.after(BootstrapCrashPoint::AfterCopy)?;
            }
            dispatch_and_reconcile_remote(request, journal, remote, &closure, executor, crash)?;
        }
        BootstrapActivationBackendValidated::Local(local) => {
            dispatch_and_reconcile_local(request, journal, local, &closure, executor, crash)?;
        }
    }
    Ok(())
}

fn run_hermetic_test<E: BootstrapExecutor>(
    test: &BootstrapHermeticTestValidated,
    executor: &mut E,
) -> std::result::Result<(), BootstrapError> {
    executor.run(BootstrapCommand {
        program: "nix".to_string(),
        arguments: vec![
            "build".to_string(),
            "--no-link".to_string(),
            "--print-out-paths".to_string(),
            "--system".to_string(),
            test.nix_system.clone(),
            format!("{}#{}", test.flake_reference, test.output_selector),
        ],
    })?;
    Ok(())
}

fn build<E: BootstrapExecutor>(
    request: &ValidatedBootstrapRun,
    overrides: &[FlakeOverride],
    executor: &mut E,
) -> std::result::Result<String, BootstrapError> {
    let input = request.mode.input();
    let mut evaluation_arguments = vec![
        "eval".to_string(),
        "--raw".to_string(),
        "--system".to_string(),
        input.nix_system().to_string(),
    ];
    evaluation_arguments.extend(flatten_overrides(overrides));
    evaluation_arguments.push(format!(
        "{}#{}.drvPath",
        input.flake_reference(),
        input.output_selector()
    ));
    let derivation = first_line(executor.run(BootstrapCommand {
        program: "nix".to_string(),
        arguments: evaluation_arguments,
    })?)?;
    if !is_canonical_nix_store_item(&derivation) || !derivation.ends_with(".drv") {
        return Err(BootstrapError::Effect(BootstrapEffectStage::Built));
    }

    let mut build_arguments = vec![
        "build".to_string(),
        "--no-link".to_string(),
        "--print-out-paths".to_string(),
        "--system".to_string(),
        input.nix_system().to_string(),
    ];
    if let Some(builder) = request.mode.builder() {
        build_arguments.extend([
            "--option".to_string(),
            "max-jobs".to_string(),
            "0".to_string(),
            "--builders".to_string(),
            builder.to_string(),
        ]);
    }
    build_arguments.push(format!("{derivation}^*"));
    let closure = first_line(executor.run(BootstrapCommand {
        program: "nix".to_string(),
        arguments: build_arguments,
    })?)?;
    if !is_canonical_nix_store_item(&closure) || closure.ends_with(".drv") {
        return Err(BootstrapError::Effect(BootstrapEffectStage::Built));
    }
    Ok(closure)
}

#[derive(Debug, Clone)]
struct FlakeOverride {
    name: String,
    reference: String,
}

fn flatten_overrides(overrides: &[FlakeOverride]) -> Vec<String> {
    let mut arguments = Vec::with_capacity(overrides.len() * 3);
    for override_input in overrides {
        arguments.extend([
            "--override-input".to_string(),
            override_input.name.clone(),
            override_input.reference.clone(),
        ]);
    }
    arguments
}

fn materialize<E: BootstrapExecutor>(
    request: &ValidatedBootstrapRun,
    executor: &mut E,
) -> std::result::Result<Vec<FlakeOverride>, BootstrapError> {
    let BootstrapInputValidated::Horizon(input) = request.mode.input() else {
        return Ok(Vec::new());
    };
    let proposal_text =
        fs::read_to_string(&input.proposal_source).map_err(|_| BootstrapError::Materialization)?;
    let proposal: ClusterProposal = DotosSource::new(&proposal_text)
        .parse()
        .map_err(|_| BootstrapError::Materialization)?;
    let cluster = HorizonClusterName::try_new(input.cluster_name.clone())
        .map_err(|_| BootstrapError::Materialization)?;
    let node = HorizonNodeName::try_new(input.node_name.clone())
        .map_err(|_| BootstrapError::Materialization)?;
    let horizon = proposal
        .project(&Viewpoint { cluster, node })
        .map_err(|_| BootstrapError::Materialization)?;
    let projected_system = match &horizon.node.system {
        horizon_lib::species::System::X86_64Linux => "x86_64-linux",
        horizon_lib::species::System::Aarch64Linux => "aarch64-linux",
    };
    if projected_system != input.nix_system {
        return Err(BootstrapError::Materialization);
    }

    let generated = request.journal_directory.join("generated-inputs");
    fs::create_dir(&generated).map_err(BootstrapError::Journal)?;
    fs::set_permissions(
        &generated,
        fs::Permissions::from_mode(PRIVATE_DIRECTORY_MODE),
    )
    .map_err(BootstrapError::Journal)?;
    let mut directories = BTreeMap::new();

    let horizon_directory = generated.join("horizon");
    write_generated_flake(
        &horizon_directory,
        Some((
            "horizon.json",
            serde_json::to_string_pretty(&horizon).map_err(|_| BootstrapError::Materialization)?,
        )),
        "{ outputs = _: { horizon = builtins.fromJSON (builtins.readFile ./horizon.json); }; }\n",
    )?;
    directories.insert("horizon", horizon_directory);

    let system_directory = generated.join("system");
    write_generated_flake(
        &system_directory,
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
    write_generated_flake(
        &deployment_directory,
        None,
        &format!(
            "{{ outputs = _: {{ deployment = {{ includeHome = {}; includeAllFirmware = {}; }}; }}; }}\n",
            include_home, include_all_firmware
        ),
    )?;
    directories.insert("deployment", deployment_directory);

    let secrets_directory = generated.join("secrets");
    write_secrets_flake(&secrets_directory, &input.secrets_input)?;
    directories.insert("secrets", secrets_directory);

    let mut overrides = Vec::with_capacity(directories.len());
    for (name, directory) in directories {
        let hash = first_line(executor.run(BootstrapCommand {
            program: "nix".to_string(),
            arguments: vec![
                "hash".to_string(),
                "path".to_string(),
                "--type".to_string(),
                "sha256".to_string(),
                "--sri".to_string(),
                directory.display().to_string(),
            ],
        })?)?;
        if !hash.starts_with("sha256-") || hash.chars().any(char::is_control) {
            return Err(BootstrapError::Materialization);
        }
        overrides.push(FlakeOverride {
            name: name.to_string(),
            reference: format!(
                "path:{}?narHash={}",
                directory.display(),
                percent_encode_nar_hash(&hash)
            ),
        });
    }
    Ok(overrides)
}

fn write_generated_flake(
    directory: &Path,
    additional: Option<(&str, String)>,
    flake_text: &str,
) -> std::result::Result<(), BootstrapError> {
    fs::create_dir(directory).map_err(BootstrapError::Journal)?;
    fs::set_permissions(directory, fs::Permissions::from_mode(0o700))
        .map_err(BootstrapError::Journal)?;
    if let Some((name, contents)) = additional {
        fs::write(directory.join(name), contents).map_err(BootstrapError::Journal)?;
    }
    fs::write(directory.join("flake.nix"), flake_text).map_err(BootstrapError::Journal)
}

fn write_secrets_flake(
    directory: &Path,
    secrets_input: &BootstrapSecretsInputValidated,
) -> std::result::Result<(), BootstrapError> {
    fs::create_dir(directory).map_err(BootstrapError::Journal)?;
    fs::set_permissions(directory, fs::Permissions::from_mode(0o700))
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
            fs::copy(path, directory.join(&name)).map_err(BootstrapError::Journal)?;
            entries.push_str(&format!("    {attribute} = ./{name};\n"));
        }
    }
    fs::write(
        directory.join("flake.nix"),
        format!("{{ outputs = _: {{ sopsFiles = {{\n{entries}  }}; }}; }}\n"),
    )
    .map_err(BootstrapError::Journal)
}

fn first_line(output: String) -> std::result::Result<String, BootstrapError> {
    let line = output
        .lines()
        .find(|line| !line.trim().is_empty())
        .ok_or(BootstrapError::Effect(BootstrapEffectStage::Built))?;
    Ok(line.trim().to_string())
}

fn percent_encode_nar_hash(value: &str) -> String {
    value
        .chars()
        .flat_map(|character| match character {
            '%' => "%25".chars().collect::<Vec<_>>(),
            '+' => "%2B".chars().collect::<Vec<_>>(),
            '/' => "%2F".chars().collect::<Vec<_>>(),
            '=' => "%3D".chars().collect::<Vec<_>>(),
            _ => vec![character],
        })
        .collect()
}

fn boot_once_unit_name(request_hash: &[u8]) -> String {
    let encoded = hex_lower(request_hash);
    format!("lojix-bootstrap-boot-once-{}", &encoded[..24])
}

fn shell_quote(value: &str) -> String {
    format!("'{}'", value.replace('\'', "'\\''"))
}

fn boot_once_script(
    request_hash: &[u8],
    closure: &str,
    system_profile_path: &Path,
    boot_entries_directory: &Path,
) -> String {
    let profile = shell_quote(&system_profile_path.display().to_string());
    let entries = shell_quote(&boot_entries_directory.display().to_string());
    let closure = shell_quote(closure);
    let unit = shell_quote(&boot_once_unit_name(request_hash));
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
         SYSTEM_LINK=$(readlink \"$PROFILE\")\n\
         GENERATION=$(echo \"$SYSTEM_LINK\" | sed -E 's/^system-([0-9]+)-link$/\\1/')\n\
         NEW=\"{BOOT_ENTRY_PREFIX}$GENERATION.conf\"\n\
         [ -f \"$ENTRIES/$NEW\" ]\n\
         [ \"$NEW\" != \"$OLD\" ]\n\
         bootctl set-default \"$OLD\"\n\
         bootctl set-oneshot \"$NEW\"\n\
         printf '%s\\n' \"$UNIT boot-once prepared\"\n"
    )
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum UnitState {
    NotFound,
    Ready,
    Waiting,
    Failed,
}

fn unit_state(output: &str) -> UnitState {
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
        UnitState::NotFound
    } else if failed {
        UnitState::Failed
    } else if ready {
        UnitState::Ready
    } else {
        UnitState::Waiting
    }
}

fn remote_has_closure<E: BootstrapExecutor>(
    remote: &BootstrapRemoteBackendValidated,
    closure: &str,
    executor: &mut E,
) -> std::result::Result<bool, BootstrapError> {
    let command = format!(
        "PATH=/nix/var/nix/profiles/default/bin:/run/current-system/sw/bin:/usr/bin:/bin; export PATH; if nix-store --query --references {} >/dev/null 2>&1; then printf Present; else printf Absent; fi",
        shell_quote(closure)
    );
    let output = executor
        .run(BootstrapCommand {
            program: "ssh".to_string(),
            arguments: remote.ssh_identity.ssh_arguments(command),
        })
        .map_err(|_| BootstrapError::RecoveryPending)?;
    Ok(output.trim() == "Present")
}

fn remote_unit_state<E: BootstrapExecutor>(
    remote: &BootstrapRemoteBackendValidated,
    unit: &str,
    executor: &mut E,
) -> std::result::Result<UnitState, BootstrapError> {
    let command = format!(
        "PATH=/nix/var/nix/profiles/default/bin:/run/current-system/sw/bin:/usr/bin:/bin; export PATH; systemctl show --property=LoadState --property=ActiveState --property=Result {} 2>/dev/null || printf 'LoadState=not-found\\n'",
        shell_quote(unit)
    );
    let output = executor
        .run(BootstrapCommand {
            program: "ssh".to_string(),
            arguments: remote.ssh_identity.ssh_arguments(command),
        })
        .map_err(|_| BootstrapError::RecoveryPending)?;
    Ok(unit_state(&output))
}

fn local_unit_state<E: BootstrapExecutor>(
    unit: &str,
    executor: &mut E,
) -> std::result::Result<UnitState, BootstrapError> {
    let command = format!(
        "PATH=/nix/var/nix/profiles/default/bin:/run/current-system/sw/bin:/usr/bin:/bin; export PATH; systemctl show --property=LoadState --property=ActiveState --property=Result {} 2>/dev/null || printf 'LoadState=not-found\\n'",
        shell_quote(unit)
    );
    let output = executor
        .run(BootstrapCommand {
            program: "/bin/sh".to_string(),
            arguments: vec!["-eu".to_string(), "-c".to_string(), command],
        })
        .map_err(|_| BootstrapError::RecoveryPending)?;
    Ok(unit_state(&output))
}

fn remote_dispatch_command(
    unit: &str,
    request_hash: &[u8],
    closure: &str,
    profile: &Path,
    entries: &Path,
) -> String {
    let script = boot_once_script(request_hash, closure, profile, entries);
    format!(
        "exec /run/current-system/sw/bin/systemd-run --unit={} --no-block --service-type=oneshot --property=RemainAfterExit=yes --setenv=PATH=/nix/var/nix/profiles/default/bin:/run/current-system/sw/bin:/usr/bin:/bin /bin/sh -eu -c {}",
        shell_quote(unit),
        shell_quote(&script)
    )
}

fn dispatch_and_reconcile_remote<E: BootstrapExecutor, C: BootstrapCrashInjector>(
    request: &ValidatedBootstrapRun,
    journal: &EphemeralJournal,
    remote: &BootstrapRemoteBackendValidated,
    closure: &str,
    executor: &mut E,
    crash: &mut C,
) -> std::result::Result<(), BootstrapError> {
    let unit = boot_once_unit_name(&request.request_hash);
    if !journal.succeeded(JournalStage::BootOnceScheduled)? {
        journal.intent(JournalStage::BootOnceScheduled)?;
        if remote_unit_state(remote, &unit, executor)? == UnitState::NotFound {
            executor
                .run(BootstrapCommand {
                    program: "ssh".to_string(),
                    arguments: remote.ssh_identity.ssh_arguments(remote_dispatch_command(
                        &unit,
                        &request.request_hash,
                        closure,
                        &remote.system_profile_path,
                        &remote.boot_entries_directory,
                    )),
                })
                .map_err(|_| BootstrapError::RecoveryPending)?;
        }
        journal.receipt(JournalStage::BootOnceScheduled)?;
        journal.outcome(JournalStage::BootOnceScheduled, true)?;
        crash.after(BootstrapCrashPoint::AfterDispatch)?;
    }
    reconcile_remote_activation(journal, remote, &unit, executor, crash)
}

fn reconcile_remote_activation<E: BootstrapExecutor, C: BootstrapCrashInjector>(
    journal: &EphemeralJournal,
    remote: &BootstrapRemoteBackendValidated,
    unit: &str,
    executor: &mut E,
    crash: &mut C,
) -> std::result::Result<(), BootstrapError> {
    if journal.succeeded(JournalStage::BootOnceActivated)? {
        return Ok(());
    }
    journal.intent(JournalStage::BootOnceActivated)?;
    for _ in 0..30 {
        match remote_unit_state(remote, unit, executor)? {
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

fn dispatch_and_reconcile_local<E: BootstrapExecutor, C: BootstrapCrashInjector>(
    request: &ValidatedBootstrapRun,
    journal: &EphemeralJournal,
    local: &BootstrapLocalBackendValidated,
    closure: &str,
    executor: &mut E,
    crash: &mut C,
) -> std::result::Result<(), BootstrapError> {
    let unit = boot_once_unit_name(&request.request_hash);
    if !journal.succeeded(JournalStage::BootOnceScheduled)? {
        journal.intent(JournalStage::BootOnceScheduled)?;
        if local_unit_state(&unit, executor)? == UnitState::NotFound {
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
                        boot_once_script(&request.request_hash, closure, &local.system_profile_path, &local.boot_entries_directory),
                    ],
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
        match local_unit_state(&unit, executor)? {
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
            hex_lower(&request.request_hash)
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
                let metadata = private_directory_metadata(&directory)?;
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
        sync_directory(&self.directory)?;
        Ok(())
    }

    fn verify_identity(
        &self,
        state: &BootstrapJournalConfiguration,
    ) -> std::result::Result<(), BootstrapError> {
        private_directory_metadata(&self.parent)?;
        let metadata = private_directory_metadata(&self.directory)?;
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
        Ok(verify_root_receipt(root, closure)
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
        if self.directory.join("generated-inputs").exists() {
            remove_known_generated_inputs(&self.directory.join("generated-inputs"))?;
        }
        remove_known_file(&self.directory, JOURNAL_STORE_FILE)?;
        remove_known_file(&self.directory, JOURNAL_STATE_FILE)?;
        let mut entries = fs::read_dir(&self.directory).map_err(BootstrapError::Journal)?;
        if entries.next().is_some() {
            return Err(BootstrapError::Validation(
                "journal contains an unowned entry",
            ));
        }
        fs::remove_dir(&self.directory).map_err(BootstrapError::Journal)?;
        sync_directory(&self.parent)?;
        Ok(())
    }
}

fn write_terminal_evidence<C: BootstrapCrashInjector>(
    request: &ValidatedBootstrapRun,
    journal: &EphemeralJournal,
    status: BootstrapEvidenceStatus,
    crash: &mut C,
) -> std::result::Result<(), BootstrapError> {
    if journal.succeeded(JournalStage::TerminalEvidenceWritten)? {
        return Ok(());
    }
    journal.intent(JournalStage::TerminalEvidenceWritten)?;
    if request.terminal_evidence_exists {
        let existing = read_evidence(&request.terminal_evidence_path)?;
        if existing.journal_schema_version != JOURNAL_SCHEMA_VERSION
            || existing.request_hash != request.request_hash
            || existing.status != status
        {
            return Err(BootstrapError::Validation(
                "existing evidence does not bind this request",
            ));
        }
    } else {
        let evidence = journal.evidence(status)?;
        write_evidence_new(&request.terminal_evidence_path, &evidence)?;
    }
    crash.after(BootstrapCrashPoint::AfterEvidence)?;
    journal.receipt(JournalStage::TerminalEvidenceWritten)?;
    journal.outcome(JournalStage::TerminalEvidenceWritten, true)
}

fn read_evidence(path: &Path) -> std::result::Result<BootstrapTerminalEvidence, BootstrapError> {
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
    rkyv::from_bytes::<BootstrapTerminalEvidence, rkyv::rancor::Error>(&bytes)
        .map_err(|error| BootstrapError::Evidence(std::io::Error::other(error.to_string())))
}

fn private_directory_metadata(path: &Path) -> std::result::Result<fs::Metadata, BootstrapError> {
    let metadata = fs::symlink_metadata(path).map_err(BootstrapError::Journal)?;
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

fn sync_directory(path: &Path) -> std::result::Result<(), BootstrapError> {
    fs::File::open(path)
        .and_then(|directory| directory.sync_all())
        .map_err(BootstrapError::Journal)
}

fn verify_root_receipt(root: &Path, closure: &str) -> bool {
    let Ok(metadata) = fs::symlink_metadata(root) else {
        return false;
    };
    metadata.file_type().is_symlink()
        && fs::read_link(root)
            .ok()
            .is_some_and(|target| target == Path::new(closure))
}

fn link_root_no_replace(
    staging: &Path,
    root: &Path,
    closure: &str,
) -> std::result::Result<(), BootstrapError> {
    if !verify_root_receipt(staging, closure) {
        return Err(BootstrapError::Validation(
            "gc root staging did not bind the built closure",
        ));
    }
    let parent = root
        .parent()
        .ok_or(BootstrapError::Validation("gc root has no parent"))?;
    private_directory_metadata(parent)?;
    fs::hard_link(staging, root).map_err(BootstrapError::Journal)?;
    if !verify_root_receipt(root, closure) {
        return Err(BootstrapError::Validation(
            "gc root receipt did not bind the built closure",
        ));
    }
    sync_directory(parent)?;
    fs::remove_file(staging).map_err(BootstrapError::Journal)?;
    sync_directory(
        staging
            .parent()
            .ok_or(BootstrapError::Validation("gc root staging has no parent"))?,
    )?;
    Ok(())
}

fn remove_known_file(parent: &Path, name: &str) -> std::result::Result<(), BootstrapError> {
    let path = parent.join(name);
    let metadata = fs::symlink_metadata(&path).map_err(BootstrapError::Journal)?;
    if !metadata.file_type().is_file() || metadata.file_type().is_symlink() {
        return Err(BootstrapError::Validation(
            "journal known file identity changed",
        ));
    }
    fs::remove_file(path).map_err(BootstrapError::Journal)
}

fn remove_known_generated_inputs(generated: &Path) -> std::result::Result<(), BootstrapError> {
    private_directory_metadata(generated)?;
    for name in ["horizon", "system", "deployment", "secrets"] {
        let directory = generated.join(name);
        private_directory_metadata(&directory)?;
        for entry in fs::read_dir(&directory).map_err(BootstrapError::Journal)? {
            let entry = entry.map_err(BootstrapError::Journal)?;
            let file_name = entry.file_name();
            let name = file_name.to_string_lossy();
            let metadata = fs::symlink_metadata(entry.path()).map_err(BootstrapError::Journal)?;
            let permitted = name == "flake.nix"
                || (directory.ends_with("horizon") && name == "horizon.json")
                || (directory.ends_with("secrets")
                    && name.ends_with(".sops")
                    && name
                        .trim_end_matches(".sops")
                        .bytes()
                        .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_'));
            if !permitted || !metadata.file_type().is_file() || metadata.file_type().is_symlink() {
                return Err(BootstrapError::Validation(
                    "journal generated entry changed",
                ));
            }
            fs::remove_file(entry.path()).map_err(BootstrapError::Journal)?;
        }
        fs::remove_dir(&directory).map_err(BootstrapError::Journal)?;
    }
    fs::remove_dir(generated).map_err(BootstrapError::Journal)
}

fn write_evidence_new(
    path: &Path,
    evidence: &BootstrapTerminalEvidence,
) -> std::result::Result<(), BootstrapError> {
    let bytes = rkyv::to_bytes::<rkyv::rancor::Error>(evidence)
        .map_err(|error| BootstrapError::Evidence(std::io::Error::other(error.to_string())))?;
    let parent = path
        .parent()
        .ok_or(BootstrapError::Validation("evidence path has no parent"))?;
    private_directory_metadata(parent)?;
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
                // `rename` would overwrite a racing destination.  A hard link
                // creates the final name only if it remains absent; the parent
                // was prevalidated and both files are necessarily on it.
                fs::hard_link(&temporary, path).map_err(BootstrapError::Evidence)?;
                fs::File::open(path)
                    .and_then(|file| file.sync_all())
                    .map_err(BootstrapError::Evidence)?;
                sync_directory(parent)?;
                fs::remove_file(&temporary).map_err(BootstrapError::Evidence)?;
                sync_directory(parent)?;
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
    cluster_name: String,
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
    system_profile_path: PathBuf,
    boot_entries_directory: PathBuf,
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

    fn ssh_arguments(&self, command: String) -> Vec<String> {
        // Disable user-configured host aliases, ProxyCommand/ProxyJump, and
        // multiplexed control paths: this backend's route is only the exact
        // request-owned identity that was validated above.
        let mut arguments = vec![
            "-F".to_string(),
            "/dev/null".to_string(),
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

#[derive(Debug)]
struct BootstrapLocalBackendValidated {
    system_profile_path: PathBuf,
    boot_entries_directory: PathBuf,
}

impl TryFrom<BootstrapRun> for ValidatedBootstrapRun {
    type Error = BootstrapError;

    fn try_from(request: BootstrapRun) -> std::result::Result<Self, Self::Error> {
        validate_request_id(&request.request_id.0)?;
        let request_hash = request_fingerprint(&request);
        let (
            mode,
            journal_parent,
            (gc_root_path, gc_root_exists),
            (terminal_evidence_path, terminal_evidence_exists),
        ) = match request.mode {
            BootstrapMode::BuildOnly(build_only) => (
                BootstrapModeValidated::BuildOnly {
                    input: validate_input(build_only.input)?,
                    builder: validate_builder(build_only.builder)?,
                },
                safe_private_existing_directory(&build_only.journal_parent.0)?,
                private_output_path(&build_only.gc_root_path.0)?,
                private_output_path(&build_only.terminal_evidence_path.0)?,
            ),
            BootstrapMode::BootOnce(boot_once) => (
                BootstrapModeValidated::BootOnce(BootstrapBootOnceValidated {
                    input: validate_input(boot_once.input)?,
                    builder: validate_builder(boot_once.builder)?,
                    test_plan: validate_test_plan(boot_once.test_plan)?,
                    activation_backend: validate_activation_backend(boot_once.activation_backend)?,
                }),
                safe_private_existing_directory(&boot_once.journal_parent.0)?,
                private_output_path(&boot_once.gc_root_path.0)?,
                private_output_path(&boot_once.terminal_evidence_path.0)?,
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

fn validate_request_id(value: &str) -> std::result::Result<String, BootstrapError> {
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

fn request_fingerprint(request: &BootstrapRun) -> Vec<u8> {
    // `BootstrapRun` is entirely structured and its derived Debug form has a
    // stable field/variant order within this versioned ingress.  The digest is
    // used only as the private journal/evidence binding and unit derivation;
    // raw request material never leaves the private journal directory.
    Sha256::digest(format!("{request:?}").as_bytes()).to_vec()
}

fn hex_lower(value: &[u8]) -> String {
    value.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn validate_input(
    input: BootstrapInput,
) -> std::result::Result<BootstrapInputValidated, BootstrapError> {
    match input {
        BootstrapInput::Direct(input) => Ok(BootstrapInputValidated::Direct(
            BootstrapDirectInputValidated {
                flake_reference: validate_flake_reference(&input.flake_reference.0)?,
                nix_system: validate_nix_system(&input.nix_system.0)?,
                output_selector: validate_output_selector(&input.output_selector.0)?,
            },
        )),
        BootstrapInput::Horizon(input) => Ok(BootstrapInputValidated::Horizon(
            BootstrapHorizonInputValidated {
                proposal_source: safe_existing_regular_file(
                    &input.proposal_source.0,
                    Some("dotos"),
                )?,
                cluster_name: validate_name(&input.cluster_name.0)?,
                node_name: validate_name(&input.node_name.0)?,
                materialization_shape: input.materialization_shape,
                secrets_input: match input.secrets_input {
                    BootstrapSecretsInput::NoSecrets => BootstrapSecretsInputValidated::None,
                    BootstrapSecretsInput::SecretsDirectory(directory) => {
                        BootstrapSecretsInputValidated::Directory(safe_existing_directory(
                            &directory.0,
                        )?)
                    }
                },
                flake_reference: validate_flake_reference(&input.flake_reference.0)?,
                nix_system: validate_nix_system(&input.nix_system.0)?,
                output_selector: validate_output_selector(&input.output_selector.0)?,
            },
        )),
    }
}

fn validate_builder(
    builder: BootstrapBuilder,
) -> std::result::Result<Option<String>, BootstrapError> {
    match builder {
        BootstrapBuilder::NoBuilder => Ok(None),
        BootstrapBuilder::NixBuilder(specification) => {
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

fn validate_test_plan(
    plan: BootstrapTestPlan,
) -> std::result::Result<BootstrapTestPlanValidated, BootstrapError> {
    match plan {
        BootstrapTestPlan::NoTest => Ok(BootstrapTestPlanValidated::NoTest),
        BootstrapTestPlan::RunHermeticTest(test) => Ok(
            BootstrapTestPlanValidated::RunHermeticTest(BootstrapHermeticTestValidated {
                flake_reference: validate_flake_reference(&test.flake_reference.0)?,
                nix_system: validate_nix_system(&test.nix_system.0)?,
                output_selector: validate_output_selector(&test.output_selector.0)?,
            }),
        ),
    }
}

fn validate_activation_backend(
    backend: BootstrapActivationBackend,
) -> std::result::Result<BootstrapActivationBackendValidated, BootstrapError> {
    match backend {
        BootstrapActivationBackend::RemoteNixosSystemdBootV1(remote) => Ok({
            let (nix_store_uri, store_identity) = validate_nix_store_uri(&remote.nix_store_uri.0)?;
            let ssh_identity = validate_ssh_destination(&remote.ssh_destination.0)?;
            if store_identity != ssh_identity {
                return Err(BootstrapError::Validation(
                    "remote store and ssh identities differ",
                ));
            }
            BootstrapActivationBackendValidated::Remote(BootstrapRemoteBackendValidated {
                nix_store_uri,
                ssh_identity,
                system_profile_path: absolute_normal_path(&remote.system_profile_path.0)?,
                boot_entries_directory: absolute_normal_path(&remote.boot_entries_directory.0)?,
            })
        }),
        BootstrapActivationBackend::LocalBootstrapV1(local) => Ok(
            BootstrapActivationBackendValidated::Local(BootstrapLocalBackendValidated {
                system_profile_path: safe_existing_path_parent(&local.system_profile_path.0)?,
                boot_entries_directory: safe_existing_directory(&local.boot_entries_directory.0)?,
            }),
        ),
    }
}

fn validate_flake_reference(value: &str) -> std::result::Result<String, BootstrapError> {
    let Some(rest) = value.strip_prefix("github:") else {
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
    Ok(value.to_string())
}

fn validate_nix_system(value: &str) -> std::result::Result<String, BootstrapError> {
    if value.is_empty()
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
    {
        return Err(BootstrapError::Validation("nix system is unsafe"));
    }
    Ok(value.to_string())
}

fn validate_output_selector(value: &str) -> std::result::Result<String, BootstrapError> {
    if value.is_empty()
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'-' | b'_' | b'\''))
    {
        return Err(BootstrapError::Validation("output selector is unsafe"));
    }
    Ok(value.to_string())
}

fn validate_name(value: &str) -> std::result::Result<String, BootstrapError> {
    if value.is_empty()
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
    {
        return Err(BootstrapError::Validation("horizon name is unsafe"));
    }
    Ok(value.to_string())
}

fn validate_nix_store_uri(
    value: &str,
) -> std::result::Result<(String, SshIdentity), BootstrapError> {
    let Some(authority) = value.strip_prefix("ssh-ng://") else {
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
    let identity = parse_ssh_identity(user, host_port)?;
    Ok((value.to_string(), identity))
}

fn validate_ssh_destination(value: &str) -> std::result::Result<SshIdentity, BootstrapError> {
    if value.starts_with('-')
        || value.matches('@').count() != 1
        || value.contains(['/', '?', '#', '[', ']'])
    {
        return Err(BootstrapError::Validation(
            "ssh destination is not a canonical identity",
        ));
    }
    let (user, host_port) = value.split_once('@').ok_or(BootstrapError::Validation(
        "ssh destination lacks user identity",
    ))?;
    parse_ssh_identity(user, host_port)
}

fn parse_ssh_identity(
    user: &str,
    host_port: &str,
) -> std::result::Result<SshIdentity, BootstrapError> {
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
    Ok(SshIdentity {
        user: user.to_string(),
        host: host.to_string(),
        port,
    })
}

fn safe_existing_regular_file(
    value: &str,
    extension: Option<&str>,
) -> std::result::Result<PathBuf, BootstrapError> {
    let path = safe_existing_path(value)?;
    let metadata = fs::symlink_metadata(&path).map_err(BootstrapError::Journal)?;
    if !metadata.file_type().is_file()
        || extension.is_some_and(|extension| {
            path.extension().and_then(|value| value.to_str()) != Some(extension)
        })
    {
        return Err(BootstrapError::Validation(
            "path is not the required regular file",
        ));
    }
    Ok(path)
}

fn safe_existing_directory(value: &str) -> std::result::Result<PathBuf, BootstrapError> {
    let path = safe_existing_path(value)?;
    let metadata = fs::symlink_metadata(&path).map_err(BootstrapError::Journal)?;
    if !metadata.file_type().is_dir() {
        return Err(BootstrapError::Validation(
            "path is not an existing directory",
        ));
    }
    Ok(path)
}

fn safe_private_existing_directory(value: &str) -> std::result::Result<PathBuf, BootstrapError> {
    let path = safe_existing_directory(value)?;
    private_directory_metadata(&path)?;
    Ok(path)
}

fn private_output_path(value: &str) -> std::result::Result<(PathBuf, bool), BootstrapError> {
    let path = absolute_normal_path(value)?;
    let parent = path
        .parent()
        .ok_or(BootstrapError::Validation("output path has no parent"))?;
    safe_private_existing_directory(
        parent
            .to_str()
            .ok_or(BootstrapError::Validation("path is not utf8"))?,
    )?;
    match fs::symlink_metadata(&path) {
        Ok(_) => Ok((path, true)),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok((path, false)),
        Err(error) => Err(BootstrapError::Journal(error)),
    }
}

fn safe_existing_path_parent(value: &str) -> std::result::Result<PathBuf, BootstrapError> {
    let path = absolute_normal_path(value)?;
    let parent = path
        .parent()
        .ok_or(BootstrapError::Validation("path has no parent"))?;
    safe_existing_directory(
        parent
            .to_str()
            .ok_or(BootstrapError::Validation("path is not utf8"))?,
    )?;
    Ok(path)
}

fn safe_existing_path(value: &str) -> std::result::Result<PathBuf, BootstrapError> {
    let path = absolute_normal_path(value)?;
    let mut prefix = PathBuf::from("/");
    for component in path.components() {
        let Component::Normal(component) = component else {
            continue;
        };
        prefix.push(component);
        let metadata = fs::symlink_metadata(&prefix).map_err(BootstrapError::Journal)?;
        if metadata.file_type().is_symlink() {
            return Err(BootstrapError::Validation("path contains a symlink"));
        }
    }
    Ok(path)
}

fn absolute_normal_path(value: &str) -> std::result::Result<PathBuf, BootstrapError> {
    if value.is_empty() || value.chars().any(char::is_control) {
        return Err(BootstrapError::Validation(
            "path is empty or contains a control",
        ));
    }
    let path = PathBuf::from(value);
    if !path.is_absolute()
        || path
            .components()
            .any(|component| !matches!(component, Component::RootDir | Component::Normal(_)))
    {
        return Err(BootstrapError::Validation(
            "path is not absolute and normal",
        ));
    }
    Ok(path)
}

fn is_canonical_nix_store_item(value: &str) -> bool {
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
}
