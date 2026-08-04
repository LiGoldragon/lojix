//! Daemon-free, explicitly-authorized bootstrap pipeline.
//!
//! This is deliberately a separate ingress from the daemon wire contracts.
//! A bootstrap invocation owns every route, input, builder, output path and
//! activation backend; no socket, service configuration, old store, or
//! hostname-derived default is read.  A fresh Lojix v4 store is created below
//! the request's journal parent only to preserve the pipeline's durable-journal
//! boundary.  It is deleted only after terminal evidence has been atomically
//! committed to the caller-selected path.

use std::collections::BTreeMap;
use std::ffi::OsString;
use std::fs;
use std::io::Write;
use std::os::unix::fs::PermissionsExt;
use std::path::{Component, Path, PathBuf};
use std::process::Command;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use dotos::{DotosDecode, DotosDecodeError, DotosSource};
use horizon_lib::name::{ClusterName as HorizonClusterName, NodeName as HorizonNodeName};
use horizon_lib::{ClusterProposal, Viewpoint};
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use thiserror::Error;

use crate::Store;

const JOURNAL_SCHEMA_VERSION: u32 = 4;
const JOURNAL_PREFIX: &str = ".lojix-bootstrap-v4-";
const TEMPORARY_EVIDENCE_PREFIX: &str = ".lojix-bootstrap-evidence-";
const BOOT_ENTRY_PREFIX: &str = "nixos-generation-";

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
    pub request_id: String,
    pub mode: BootstrapEvidenceMode,
    pub status: BootstrapEvidenceStatus,
    pub closure_path: Option<String>,
    pub gc_root_path: String,
    pub effects: Vec<BootstrapEffectEvidence>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BootstrapTerminal {
    pub request_id: String,
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
    let mut validated = ValidatedBootstrapRun::try_from(request)?;
    let journal = EphemeralJournal::create(&validated)?;
    validated.journal_directory = journal.directory.clone();
    let mut effects = vec![BootstrapEffectEvidence {
        stage: BootstrapEffectStage::JournalCreated,
        succeeded: true,
    }];

    let execution = execute(&validated, executor, &mut effects);
    let (status, closure_path) = match execution {
        Ok(closure_path) => (BootstrapEvidenceStatus::Succeeded, Some(closure_path)),
        Err(stage) => {
            effects.push(BootstrapEffectEvidence {
                stage,
                succeeded: false,
            });
            (BootstrapEvidenceStatus::Failed, None)
        }
    };

    let mut evidence = BootstrapTerminalEvidence {
        journal_schema_version: JOURNAL_SCHEMA_VERSION,
        request_id: validated.request_id.clone(),
        mode: validated.mode.evidence_mode(),
        status,
        closure_path,
        gc_root_path: validated.gc_root_path.display().to_string(),
        effects,
    };
    // This stage is appended before serialization so the artifact itself
    // proves the terminal write committed; the file is created atomically.
    evidence.effects.push(BootstrapEffectEvidence {
        stage: BootstrapEffectStage::TerminalEvidenceWritten,
        succeeded: true,
    });
    write_evidence_new(&validated.terminal_evidence_path, &evidence)?;

    // Retain a failed journal until durable terminal evidence exists.  After
    // that point the guard may remove only the verified child it created.
    journal.cleanup()?;

    Ok(BootstrapTerminal {
        request_id: validated.request_id,
        status: status.as_str(),
    })
}

fn execute<E: BootstrapExecutor>(
    request: &ValidatedBootstrapRun,
    executor: &mut E,
    evidence: &mut Vec<BootstrapEffectEvidence>,
) -> std::result::Result<String, BootstrapEffectStage> {
    let overrides =
        materialize(request, executor).map_err(|_| BootstrapEffectStage::Materialized)?;
    evidence.push(BootstrapEffectEvidence {
        stage: BootstrapEffectStage::Materialized,
        succeeded: true,
    });

    if let BootstrapModeValidated::BootOnce(boot_once) = &request.mode
        && let BootstrapTestPlanValidated::RunHermeticTest(test) = &boot_once.test_plan
    {
        run_hermetic_test(test, executor).map_err(|_| BootstrapEffectStage::Tested)?;
        evidence.push(BootstrapEffectEvidence {
            stage: BootstrapEffectStage::Tested,
            succeeded: true,
        });
    }

    let closure = build(request, &overrides, executor).map_err(|_| BootstrapEffectStage::Built)?;
    evidence.push(BootstrapEffectEvidence {
        stage: BootstrapEffectStage::Built,
        succeeded: true,
    });

    executor
        .run(BootstrapCommand {
            program: "nix-store".to_string(),
            arguments: vec![
                "--add-root".to_string(),
                request.gc_root_path.display().to_string(),
                "--realise".to_string(),
                closure.clone(),
            ],
        })
        .map_err(|_| BootstrapEffectStage::GcRooted)?;
    evidence.push(BootstrapEffectEvidence {
        stage: BootstrapEffectStage::GcRooted,
        succeeded: true,
    });

    // The exact BuildOnly variant has no activation representation, rather
    // than a boolean that a future refactor could accidentally ignore.
    let BootstrapModeValidated::BootOnce(boot_once) = &request.mode else {
        return Ok(closure);
    };

    match &boot_once.activation_backend {
        BootstrapActivationBackendValidated::Remote(remote) => {
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
                .map_err(|_| BootstrapEffectStage::Copied)?;
            evidence.push(BootstrapEffectEvidence {
                stage: BootstrapEffectStage::Copied,
                succeeded: true,
            });
            executor
                .run(BootstrapCommand {
                    program: "ssh".to_string(),
                    arguments: vec![
                        remote.ssh_destination.clone(),
                        boot_once_script(
                            &request.request_id,
                            &closure,
                            &remote.system_profile_path,
                            &remote.boot_entries_directory,
                        ),
                    ],
                })
                .map_err(|_| BootstrapEffectStage::BootOnceScheduled)?;
        }
        BootstrapActivationBackendValidated::Local(local) => {
            executor
                .run(BootstrapCommand {
                    program: "systemd-run".to_string(),
                    arguments: vec![
                        format!("--unit={}", boot_once_unit_name(&request.request_id)),
                        "--collect".to_string(),
                        "--wait".to_string(),
                        "--service-type=oneshot".to_string(),
                        "/bin/sh".to_string(),
                        "-c".to_string(),
                        boot_once_script(
                            &request.request_id,
                            &closure,
                            &local.system_profile_path,
                            &local.boot_entries_directory,
                        ),
                    ],
                })
                .map_err(|_| BootstrapEffectStage::BootOnceScheduled)?;
        }
    }
    evidence.push(BootstrapEffectEvidence {
        stage: BootstrapEffectStage::BootOnceScheduled,
        succeeded: true,
    });
    Ok(closure)
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
    name: &'static str,
    reference: String,
}

fn flatten_overrides(overrides: &[FlakeOverride]) -> Vec<String> {
    let mut arguments = Vec::with_capacity(overrides.len() * 3);
    for override_input in overrides {
        arguments.extend([
            "--override-input".to_string(),
            override_input.name.to_string(),
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
            name,
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

fn boot_once_unit_name(request_id: &str) -> String {
    format!("lojix-bootstrap-boot-once-{request_id}")
}

fn shell_quote(value: &str) -> String {
    format!("'{}'", value.replace('\'', "'\\''"))
}

fn boot_once_script(
    request_id: &str,
    closure: &str,
    system_profile_path: &Path,
    boot_entries_directory: &Path,
) -> String {
    let profile = shell_quote(&system_profile_path.display().to_string());
    let entries = shell_quote(&boot_entries_directory.display().to_string());
    let closure = shell_quote(closure);
    let unit = shell_quote(&boot_once_unit_name(request_id));
    format!(
        "set -eu\n\
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

#[derive(Debug)]
struct EphemeralJournal {
    directory: PathBuf,
}

#[derive(Debug, Archive, RkyvSerialize, RkyvDeserialize)]
struct BootstrapJournalConfiguration {
    schema_version: u32,
    request_id: String,
    mode: BootstrapEvidenceMode,
}

impl EphemeralJournal {
    fn create(request: &ValidatedBootstrapRun) -> std::result::Result<Self, BootstrapError> {
        let directory = create_ephemeral_child(&request.journal_parent)?;
        let configuration = BootstrapJournalConfiguration {
            schema_version: JOURNAL_SCHEMA_VERSION,
            request_id: request.request_id.clone(),
            mode: request.mode.evidence_mode(),
        };
        let bytes = rkyv::to_bytes::<rkyv::rancor::Error>(&configuration)
            .map_err(|error| BootstrapError::Journal(std::io::Error::other(error.to_string())))?;
        fs::write(directory.join("bootstrap-v4.rkyv"), bytes).map_err(BootstrapError::Journal)?;
        // This is a new, isolated Lojix v4 store.  It has no daemon sockets or
        // route configuration and cannot read an existing daemon journal.
        Store::open(directory.join("lojix-v4.sema")).map_err(BootstrapError::JournalStore)?;
        Ok(Self { directory })
    }

    fn cleanup(self) -> std::result::Result<(), BootstrapError> {
        let metadata = fs::symlink_metadata(&self.directory).map_err(BootstrapError::Journal)?;
        if !metadata.file_type().is_dir()
            || metadata.file_type().is_symlink()
            || !self
                .directory
                .file_name()
                .is_some_and(|name| name.to_string_lossy().starts_with(JOURNAL_PREFIX))
        {
            return Err(BootstrapError::Validation(
                "journal child changed before cleanup",
            ));
        }
        fs::remove_dir_all(self.directory).map_err(BootstrapError::Journal)
    }
}

fn create_ephemeral_child(parent: &Path) -> std::result::Result<PathBuf, BootstrapError> {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| BootstrapError::Validation("clock before unix epoch"))?
        .as_nanos();
    for _ in 0..32 {
        let sequence = JOURNAL_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let name = format!("{JOURNAL_PREFIX}{}-{nanos}-{sequence}", std::process::id());
        let directory = parent.join(name);
        match fs::create_dir(&directory) {
            Ok(()) => {
                fs::set_permissions(&directory, fs::Permissions::from_mode(0o700))
                    .map_err(BootstrapError::Journal)?;
                return Ok(directory);
            }
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(error) => return Err(BootstrapError::Journal(error)),
        }
    }
    Err(BootstrapError::Validation(
        "could not allocate fresh journal child",
    ))
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
                file.write_all(&bytes).map_err(BootstrapError::Evidence)?;
                file.sync_all().map_err(BootstrapError::Evidence)?;
                // `rename` would overwrite a racing destination.  A hard link
                // creates the final name only if it remains absent; the parent
                // was prevalidated and both files are necessarily on it.
                fs::hard_link(&temporary, path).map_err(BootstrapError::Evidence)?;
                fs::remove_file(&temporary).map_err(BootstrapError::Evidence)?;
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
    request_id: String,
    mode: BootstrapModeValidated,
    journal_parent: PathBuf,
    journal_directory: PathBuf,
    gc_root_path: PathBuf,
    terminal_evidence_path: PathBuf,
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
    ssh_destination: String,
    system_profile_path: PathBuf,
    boot_entries_directory: PathBuf,
}

#[derive(Debug)]
struct BootstrapLocalBackendValidated {
    system_profile_path: PathBuf,
    boot_entries_directory: PathBuf,
}

impl TryFrom<BootstrapRun> for ValidatedBootstrapRun {
    type Error = BootstrapError;

    fn try_from(request: BootstrapRun) -> std::result::Result<Self, Self::Error> {
        let request_id = validate_request_id(&request.request_id.0)?;
        let (mode, journal_parent, gc_root_path, terminal_evidence_path) = match request.mode {
            BootstrapMode::BuildOnly(build_only) => (
                BootstrapModeValidated::BuildOnly {
                    input: validate_input(build_only.input)?,
                    builder: validate_builder(build_only.builder)?,
                },
                safe_existing_directory(&build_only.journal_parent.0)?,
                safe_new_path(&build_only.gc_root_path.0)?,
                safe_new_path(&build_only.terminal_evidence_path.0)?,
            ),
            BootstrapMode::BootOnce(boot_once) => (
                BootstrapModeValidated::BootOnce(BootstrapBootOnceValidated {
                    input: validate_input(boot_once.input)?,
                    builder: validate_builder(boot_once.builder)?,
                    test_plan: validate_test_plan(boot_once.test_plan)?,
                    activation_backend: validate_activation_backend(boot_once.activation_backend)?,
                }),
                safe_existing_directory(&boot_once.journal_parent.0)?,
                safe_new_path(&boot_once.gc_root_path.0)?,
                safe_new_path(&boot_once.terminal_evidence_path.0)?,
            ),
        };
        if gc_root_path == terminal_evidence_path {
            return Err(BootstrapError::Validation(
                "gc root and evidence paths collide",
            ));
        }
        Ok(Self {
            request_id,
            mode,
            journal_parent,
            // Filled after journal creation: keeping this value a child of the
            // validated parent prevents output authority from escaping it.
            journal_directory: PathBuf::new(),
            gc_root_path,
            terminal_evidence_path,
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
        BootstrapActivationBackend::RemoteNixosSystemdBootV1(remote) => Ok(
            BootstrapActivationBackendValidated::Remote(BootstrapRemoteBackendValidated {
                nix_store_uri: validate_nix_store_uri(&remote.nix_store_uri.0)?,
                ssh_destination: validate_ssh_destination(&remote.ssh_destination.0)?,
                system_profile_path: absolute_normal_path(&remote.system_profile_path.0)?,
                boot_entries_directory: absolute_normal_path(&remote.boot_entries_directory.0)?,
            }),
        ),
        BootstrapActivationBackend::LocalBootstrapV1(local) => Ok(
            BootstrapActivationBackendValidated::Local(BootstrapLocalBackendValidated {
                system_profile_path: safe_existing_path_parent(&local.system_profile_path.0)?,
                boot_entries_directory: safe_existing_directory(&local.boot_entries_directory.0)?,
            }),
        ),
    }
}

fn validate_flake_reference(value: &str) -> std::result::Result<String, BootstrapError> {
    if value.is_empty()
        || value.chars().any(char::is_control)
        || value.contains(char::is_whitespace)
        || !value.starts_with("github:")
        || value.contains("token")
        || value.contains("password")
        || value.contains("@")
    {
        return Err(BootstrapError::Validation("flake reference is unsafe"));
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

fn validate_nix_store_uri(value: &str) -> std::result::Result<String, BootstrapError> {
    if value.is_empty()
        || value.chars().any(char::is_control)
        || value.contains(char::is_whitespace)
        || !value.contains("://")
        || value.contains('@')
        || value.contains("token")
        || value.contains("password")
    {
        return Err(BootstrapError::Validation("nix store uri is unsafe"));
    }
    Ok(value.to_string())
}

fn validate_ssh_destination(value: &str) -> std::result::Result<String, BootstrapError> {
    if value.is_empty()
        || value.chars().any(char::is_control)
        || value.contains(char::is_whitespace)
        || value.contains('/')
        || value.contains("token")
        || value.contains("password")
    {
        return Err(BootstrapError::Validation("ssh destination is unsafe"));
    }
    Ok(value.to_string())
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

fn safe_new_path(value: &str) -> std::result::Result<PathBuf, BootstrapError> {
    let path = absolute_normal_path(value)?;
    match fs::symlink_metadata(&path) {
        Ok(_) => return Err(BootstrapError::Validation("output target already exists")),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(BootstrapError::Journal(error)),
    }
    let parent = path
        .parent()
        .ok_or(BootstrapError::Validation("output path has no parent"))?;
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
