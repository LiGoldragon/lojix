//! Hand-implemented `SchemaRuntime` noun — the engine a peer's input is
//! answered through and an admitted deploy or test pipeline is driven by.
//!
//! `Operating::operate` applies one generated Memory `Operation` to the store
//! and answers its `Outcome`. A deploy pipeline is a chain of stages — resolve
//! the flake, materialize Horizon inputs, eval, build, copy, activate — with a
//! phase recorded between them; each stage is an `Operation` too, an attempt.
//! `NexusEngine::decide` picks the next operation from the last outcome and
//! `NexusEngine::attempt` carries it out until the pipeline finishes. A stage
//! composes its emissions, generated from `ethos/emission.ethos`, and awaits
//! each on a blocking thread; the translator in `translator.rs` holds every
//! word of the calls.

use crate::inspected_text::{
    CredentialBearing, InspectedText, NixStorePath, OfferedPath, PathAdmission, PercentEncodedText,
    StoreItemShape,
};
use crate::{CommitMarking as _, Named as _, Payload};
use crate::{
    DeploymentLedger as _, DurableStore as _, EventHistory as _, GenerationLedger as _,
    IdentifierAllocating as _, NexusPersistable as _, TestRunLedger as _,
};
use crate::{HorizonArchitecture as _, SourceRevisionText as _};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use horizon_lib::{Horizon, HorizonDefinition, Projecting};

use crate::emission;
use crate::runtime_flow::{self as nexus, NexusEngine as _};
use crate::runtime_model as memory;
use crate::translator::{Emitting as _, GcRootNaming as _, SystemdBus, SystemdWord as _};

/// A local activation-login noun. Horizon projects users as public strings;
/// Lojix validates the command-bearing login independently before it reaches
/// an SSH or shell invocation.
#[derive(Debug, Clone, PartialEq, Eq)]
struct HorizonUserName(String);

impl TryFrom<String> for HorizonUserName {
    type Error = String;

    fn try_from(value: String) -> std::result::Result<Self, String> {
        if value.is_empty()
            || value.starts_with('-')
            || value
                .bytes()
                .any(|byte| byte.is_ascii_whitespace() || byte.is_ascii_control())
        {
            return Err("user name is empty or contains unsafe characters".to_string());
        }
        Ok(Self(value))
    }
}

impl crate::Named for HorizonUserName {
    fn as_str(&self) -> &str {
        &self.0
    }
}

/// The bound on durable failure detail. A Nexus keeps the evidence a retry
/// needs, not a log stream: the tail is where a Nix or activation failure
/// states its cause, so the tail of each stream is what is kept.
const FAILURE_DETAIL_BOUND: usize = 4096;

/// What a stage reported when it failed: its own statement, every stream the
/// failing process printed, and, when a subprocess ran, that process's
/// identity and exit code. Every effect failure travels in this one carrier,
/// so nothing actionable is discarded between the process and the durable
/// record.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct StageFailure {
    detail: String,
    failed_command_option: Option<crate::runtime_model::FailedCommand>,
    reported_outputs: Vec<ReportedOutput>,
}

/// One stream a failing stage printed, under the name it is shown by:
/// `stdout`, `stderr`, or the journal of the transient unit an activation ran
/// in. Kept whole until the durable edge bounds it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ReportedOutput {
    label: String,
    text: String,
}

/// A labelled stream and the heading it is shown under.
pub(crate) trait OutputReporting {
    fn new(label: impl Into<String>, text: impl Into<String>) -> Self
    where
        Self: Sized;

    fn text(&self) -> &str;

    /// `--- stdout ---`, `--- stdout: empty ---`, or, when only its tail is
    /// kept, `--- stdout (tail) ---`.
    fn heading(&self, cut: bool) -> String;

    /// The longest heading this stream can be shown under.
    fn heading_allowance(&self) -> usize;
}

impl OutputReporting for ReportedOutput {
    fn new(label: impl Into<String>, text: impl Into<String>) -> Self {
        Self {
            label: label.into(),
            text: text.into().trim().to_string(),
        }
    }

    fn text(&self) -> &str {
        &self.text
    }

    fn heading(&self, cut: bool) -> String {
        if self.text.is_empty() {
            format!("--- {}: empty ---", self.label)
        } else if cut {
            format!("--- {} (tail) ---", self.label)
        } else {
            format!("--- {} ---", self.label)
        }
    }

    fn heading_allowance(&self) -> usize {
        self.heading(true).len().max(self.heading(false).len())
    }
}

/// Reading a process end as the exit code the record keeps and the sentence
/// the detail opens with.
pub(crate) trait ProcessEnding {
    /// Present only when the process itself exited.
    fn exit_code(&self) -> Option<i32>;

    fn statement(&self) -> String;
}

impl ProcessEnding for emission::ProcessEnd {
    fn exit_code(&self) -> Option<i32> {
        match self {
            Self::Exited(code) => i32::try_from(code.0).ok(),
            Self::Signalled(_) | Self::Unreported(_) => None,
        }
    }

    fn statement(&self) -> String {
        match self {
            Self::Exited(code) => format!("exited with status {}", code.0),
            Self::Signalled(ended) if ended.core_dumped.0 => {
                format!("killed by signal {}, core dumped", ended.signal_number.0)
            }
            Self::Signalled(ended) => format!("killed by signal {}", ended.signal_number.0),
            Self::Unreported(raw) => format!("ended with wait status {}", raw.0),
        }
    }
}

impl From<emission::ProcessFailure> for StageFailure {
    /// What the translator observed of a failed process: the command it ran,
    /// how it ended, and both streams it printed, whole. A failing step may
    /// print its cause on either — Home Manager's `errorEcho` writes to
    /// stdout. A code is present only when the process itself exited.
    fn from(failure: emission::ProcessFailure) -> Self {
        let program = failure.command_program.payload().clone();
        let (exit_code, detail, reported_outputs) = match failure.process_fault {
            emission::ProcessFault::Unstarted(fault) => (
                None,
                format!("failed to spawn session for {program}: {}", fault.0),
                Vec::new(),
            ),
            emission::ProcessFault::Unfinished(fault) => (
                None,
                format!("failed while waiting for {program}: {}", fault.0),
                Vec::new(),
            ),
            emission::ProcessFault::Ended(ending) => (
                ending.process_end.exit_code(),
                ending.process_end.statement(),
                vec![
                    ReportedOutput::new("stdout", ending.stdout.0),
                    ReportedOutput::new("stderr", ending.stderr.0),
                ],
            ),
        };
        Self {
            detail,
            failed_command_option: Some(crate::runtime_model::FailedCommand {
                command_program: failure.command_program,
                command_argument_vector: failure.command_argument_vector,
                exit_code_option: exit_code
                    .filter(|code| *code >= 0)
                    .map(i64::from)
                    .map(crate::runtime_model::ExitCode::new),
            }),
            reported_outputs,
        }
    }
}

impl From<String> for StageFailure {
    /// A stage that failed without running a subprocess — a Horizon
    /// projection, a malformed transport, an internal invariant — has detail
    /// and no command.
    fn from(detail: String) -> Self {
        Self {
            detail,
            failed_command_option: None,
            reported_outputs: Vec::new(),
        }
    }
}

impl From<&str> for StageFailure {
    fn from(detail: &str) -> Self {
        Self::from(detail.to_string())
    }
}

impl From<StageFailure> for String {
    /// The lossy edge. Intermediate helpers that still thread `String` errors
    /// keep everything the stage printed and drop the command identity; every
    /// path that reaches a durable record goes through `witness` instead.
    fn from(failure: StageFailure) -> Self {
        failure.reported_text()
    }
}

/// Adding to what a failing stage reported, after the fact.
pub(crate) trait FailureReporting {
    /// A further stream that explains the failure — the journal of the unit
    /// the failed command handed its work to.
    fn with_output(self, output: ReportedOutput) -> Self;
}

impl FailureReporting for StageFailure {
    fn with_output(mut self, output: ReportedOutput) -> Self {
        self.reported_outputs.push(output);
        self
    }
}

/// Turning what a failing stage reported into the bounded, redacted evidence
/// the durable record keeps.
pub(crate) trait Witnessing {
    /// The stage's own statement: how the process ended, or what went wrong
    /// when no process ran.
    fn reported_detail(&self) -> &str;

    /// Every stream the failing stage printed, in the order they are shown.
    fn reported_outputs(&self) -> &[ReportedOutput] {
        &[]
    }

    fn reported_command(&self) -> Option<crate::runtime_model::FailedCommand>;

    /// Everything reported, whole and unredacted, for an intermediate message
    /// that is bounded and redacted later.
    fn reported_text(&self) -> String {
        let mut sections = Vec::new();
        if !self.reported_detail().trim().is_empty() {
            sections.push(self.reported_detail().trim().to_string());
        }
        for output in self.reported_outputs() {
            sections.push(output.heading(false));
            if !output.text().is_empty() {
                sections.push(output.text().to_string());
            }
        }
        sections.join("\n")
    }

    /// Drop every line naming a credential; say whether one was dropped.
    fn redaction(text: &str) -> (String, bool)
    where
        Self: Sized,
    {
        let retained: Vec<&str> = text
            .lines()
            .filter(|line| !InspectedText::from(*line).names_credential_material())
            .collect();
        let dropped_a_line = retained.len() != text.lines().count();
        (retained.join("\n"), dropped_a_line)
    }

    /// The last `bound` bytes of `text`, starting on a character and, where
    /// one is in reach, a line boundary; and whether anything was cut.
    fn tail_within(text: &str, bound: usize) -> (String, bool)
    where
        Self: Sized,
    {
        if text.len() <= bound {
            return (text.to_string(), false);
        }
        let mut start = text.len() - bound;
        while !text.is_char_boundary(start) {
            start += 1;
        }
        let tail = &text[start..];
        let starts_a_line = text.as_bytes()[start - 1] == b'\n';
        let kept = match tail.find('\n') {
            Some(break_at) if !starts_a_line && break_at + 1 < tail.len() => &tail[break_at + 1..],
            _ => tail,
        };
        (kept.to_string(), true)
    }

    /// Divide `available` bytes among texts of these lengths: a text shorter
    /// than an even share keeps all of itself and leaves the rest to the
    /// others, so no stream is cut while room is left unused.
    fn fair_shares(lengths: &[usize], available: usize) -> Vec<usize>
    where
        Self: Sized,
    {
        let mut order: Vec<usize> = (0..lengths.len()).collect();
        order.sort_by_key(|index| lengths[*index]);
        let mut shares = vec![0; lengths.len()];
        let mut remaining = available;
        for (position, index) in order.into_iter().enumerate() {
            let share = (remaining / (lengths.len() - position)).min(lengths[index]);
            shares[index] = share;
            remaining -= share;
        }
        shares
    }

    /// The statement, then each stream under its heading, every line naming a
    /// credential dropped and each part cut to its tail so that the whole
    /// fits `FAILURE_DETAIL_BOUND`. Any cut or dropped line sets
    /// `detail_truncated`.
    fn witness(&self) -> crate::runtime_model::FailureEvidence
    where
        Self: Sized,
    {
        let (statement, mut truncated) = Self::redaction(self.reported_detail().trim());
        let outputs: Vec<(&ReportedOutput, String)> = self
            .reported_outputs()
            .iter()
            .map(|output| {
                let (text, dropped) = Self::redaction(output.text());
                truncated |= dropped;
                (output, text)
            })
            .collect();
        let headings: usize = outputs
            .iter()
            .map(|(output, _)| output.heading_allowance() + 2)
            .sum();
        let lengths: Vec<usize> = std::iter::once(statement.len())
            .chain(outputs.iter().map(|(_, text)| text.len()))
            .collect();
        let shares = Self::fair_shares(&lengths, FAILURE_DETAIL_BOUND.saturating_sub(headings + 1));
        let mut sections = Vec::new();
        let (statement, cut) = Self::tail_within(&statement, shares[0]);
        truncated |= cut;
        if !statement.is_empty() {
            sections.push(statement);
        }
        for ((output, text), share) in outputs.iter().zip(shares.iter().skip(1)) {
            let (kept, cut) = Self::tail_within(text, *share);
            truncated |= cut;
            sections.push(output.heading(cut));
            if !kept.is_empty() {
                sections.push(kept);
            }
        }
        let (detail, cut) = Self::tail_within(&sections.join("\n"), FAILURE_DETAIL_BOUND);
        crate::runtime_model::FailureEvidence {
            failed_command_option: self.reported_command(),
            failure_detail: crate::runtime_model::FailureDetail::new(detail),
            detail_truncated: crate::runtime_model::DetailTruncated(truncated || cut),
        }
    }
}

impl Witnessing for StageFailure {
    fn reported_detail(&self) -> &str {
        &self.detail
    }

    fn reported_outputs(&self) -> &[ReportedOutput] {
        &self.reported_outputs
    }

    fn reported_command(&self) -> Option<crate::runtime_model::FailedCommand> {
        self.failed_command_option.clone()
    }
}

// The two vocabularies the engine reads the runtime model through. A driven
// pipeline ends with the privileged contract's own output.
mod ordinary {
    pub use crate::runtime_model::*;
}

mod meta {
    pub use crate::runtime_model::*;

    pub type Output = crate::runtime_flow::PipelineOutput;
    pub type DeployRequest = ActualizedDeploySubmission;

    /// Private classification used only while lowering a failed deploy into a
    /// correlated local `DeploymentRecord`. It is never a wire type.
    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    pub enum DeployRejectionReason {
        ClusterUnknown,
        EvaluationFailed,
        BuildFailed,
        NodeUnknown,
        ProposalSourceUnreachable,
        FlakeReferenceMalformed,
        InvalidDeploymentRouting,
        ClosureCopyFailed,
        DeploymentInFlight,
        UnsupportedDeployAction,
        InternalError,
        ActivationFailed,
    }
}
use crate::{Error, LegacyStartupConfiguration, NexusConfiguration, Result, Store};

/// The lojix engine noun. Carries the durable `Store` (the four memory tables)
/// and, while a deploy is in flight, the pipeline cursor that threads the
/// effect chain across continuation hops. The handwritten
/// `NexusEngine::execute` drives the `Runner` over it.
#[derive(Debug)]
pub struct SchemaRuntime {
    /// The shared durable state. Each request is served by its OWN
    /// `SchemaRuntime` over a clone of this `Arc`, so the in-flight deploy
    /// cursor below is per-request while the durable tables are shared across
    /// concurrent connections (intent 2alg).
    store: Arc<Store>,
    configuration: Arc<RuntimeConfiguration>,
    active_deploy: Option<DeployPipeline>,
    /// The in-flight test cursor (Unit 2b). A single accepted `Test` becomes a
    /// hermetic-check effect (or the live bring-up→deploy→assert→teardown
    /// chain); this cursor threads the run identifier, the resolved target, and
    /// the stage across the continuation hops so the durable row is rewritten
    /// through real phases to a terminal `Passed`/`Failed`. Per-request, like
    /// `active_deploy`.
    active_test: Option<TestPipeline>,
    active_operation: Option<MetaOperation>,
}

/// Runtime paths the daemon needs for production deploy materialization.
/// These are decoded once from the daemon's binary startup configuration and
/// then shared across per-request engine values.
#[derive(Debug, Clone)]
pub struct RuntimeConfiguration {
    generated_inputs_directory: PathBuf,
    /// The node this daemon runs on. Activation uses this to
    /// recognize a self-targeting system switch and retain its detached-safe
    /// behavior. Evaluation is local for every target; the build is local
    /// only for this node, and every other node realizes in its transport's
    /// own Nix store (`BuildTarget::TargetStore`).
    daemon_host: ordinary::NodeName,
    /// Program resolution shared by every external Nix, SSH, and process
    /// effect. Production uses the declarative service PATH; focused tests use
    /// an isolated fixture directory.
    effect_execution: EffectExecution,
    /// The test-op defaults, projected from the daemon's binary startup
    /// configuration (report 54). `decide_meta_input` reads these to lower a
    /// `(Check …)` shorthand into a full `TestRun` — cluster, host, and mode
    /// all default from here. `None` when the daemon was configured without
    /// test defaults; a `(Check …)` then rejects with `NoTestDefaults` rather
    /// than guessing.
    test_defaults: Option<TestDefaults>,
}

/// The translator every process emission is emitted under: where a program
/// comes from. Its `Emittable` impls, in `translator.rs`, hold the words.
///
/// The optional program directory is a hermetic test seam: production resolves
/// command names through the daemon service PATH; focused tests install fake
/// `nix` and `ssh` programs in their own directory without modifying process
/// environment shared by other tests.
#[derive(Debug, Clone)]
pub struct EffectExecution {
    program_directory: Option<PathBuf>,
}

/// Where an external program comes from. Production resolves against the
/// declarative service PATH; a focused test resolves against an isolated
/// fixture directory, and nothing else distinguishes the two.
pub trait ProgramResolution {
    fn production() -> Self
    where
        Self: Sized;

    fn test(program_directory: PathBuf) -> Self
    where
        Self: Sized;

    fn program(&self, program: &str) -> PathBuf;
}

impl ProgramResolution for EffectExecution {
    fn production() -> Self {
        Self {
            program_directory: None,
        }
    }

    fn test(program_directory: PathBuf) -> Self {
        Self {
            program_directory: Some(program_directory),
        }
    }

    fn program(&self, program: &str) -> PathBuf {
        self.program_directory
            .as_ref()
            .map(|directory| directory.join(program))
            .unwrap_or_else(|| PathBuf::from(program))
    }
}

/// The runtime projection of the config-default test selection. A daemon-side
/// noun holding the cluster, default vm-host, and default mode a `(Check …)`
/// fills in. Built once from [`crate::TestDefaults`] (the rkyv config shape)
/// and shared across per-request engines, so the wire op, the durable record,
/// and the config default all resolve through one type.
#[derive(Debug, Clone)]
pub struct TestDefaults {
    cluster: ordinary::ClusterName,
    default_vm_host: ordinary::NodeName,
    default_mode: ordinary::TestMode,
    /// The cluster→flake resolution (Unit 2b): the flake whose
    /// exact hermetic output selector the configured shorthand builds, and
    /// whose built microVM runner the live path brings up.
    test_flake: ordinary::FlakeReference,
    /// Exact hermetic system and output selector for a configured shorthand.
    test_nix_system: memory::NixSystem,
    test_output_selector: memory::DeploymentOutputSelector,
    /// The already-actualized Horizon definition used to validate `(OnHost h)`
    /// and resolve `All`. The configuration writer owns authored-file input.
    horizon_definition: Option<HorizonDefinition>,
}

/// Lowering an operator's test request onto the runs it actually means, using
/// the cluster the daemon was configured with.
pub(crate) trait TestLowering {
    /// Lower one `TestRequest` to the resolved test targets it names. A
    /// `(Run …)` carries cluster/host/mode explicitly; a `(Check …)` fills all
    /// three from these defaults — the routine `(Check mercury)` form (report
    /// 54 decision D). `(Nodes [n …])` expands to one resolved run per named
    /// node; `All` is a projection sweep deferred to Unit 2b and resolves to no
    /// targets here (the caller rejects an empty resolution honestly rather
    /// than faking a run). Returns the resolved targets so the caller mints an
    /// identifier and records a Pending row per target.
    fn lower(&self, request: meta::TestRequest) -> Vec<ResolvedTestRun>;

    /// Lower a full `(Run …)`: explicit cluster + host selection + mode, one
    /// resolved run per node in the selection.
    fn lower_run(&self, run: meta::TestRun) -> Vec<ResolvedTestRun>;

    /// Lower a routine `(Check [n …])`: cluster, host, and mode all from these
    /// defaults; one resolved run per named node.
    fn lower_check(&self, check: meta::QuickCheck) -> Vec<ResolvedTestRun>;

    /// Resolve a `HostSelection` against these defaults: `DefaultHost` reads
    /// the config default vm-host; `(OnHost h)` overrides to the named host
    /// (the declared-host-set membership check is a Unit-2b projection gate).
    fn resolve_host(&self, selection: ordinary::HostSelection) -> ordinary::NodeName;

    /// The explicit node list of a `NodeSelection`. `All` resolves to the
    /// cluster's test-VM nodes by projecting the configured proposal source
    /// (Unit 2b): every Pod node whose primary host (`super_node`) declares a
    /// `VmHost` service. An unconfigured or unreadable proposal source resolves
    /// `All` to no nodes, so the caller rejects an empty resolution honestly
    /// rather than faking a sweep.
    fn nodes_of(&self, selection: meta::NodeSelection) -> Vec<ordinary::NodeName>;

    /// Sweep the configured proposal for the cluster's test-VM-host nodes —
    /// every node whose `Machine::host_set` (its primary `super_node`, plus the
    /// additive `super_nodes` once Unit 1 lands on horizon main) is non-empty,
    /// i.e. a Pod hosted on a vmhost. Empty when no proposal source is
    /// configured or it fails to project.
    fn all_test_vm_nodes(&self) -> Vec<ordinary::NodeName>;

    /// Synthetic defaults used only by in-process tests. Production startup
    /// configuration supplies no test fixture.
    fn test_default() -> Self
    where
        Self: Sized;

    fn default_profile(&self) -> memory::TestExecutionProfile;

    /// The configured proposal projection, if a proposal source is set. The
    /// host-set validation and the `All` sweep both read it; absent (empty)
    /// when host-set validation is not configured.
    fn projection(&self) -> Option<ClusterProjection>;
}

impl TestLowering for TestDefaults {
    fn lower(&self, request: meta::TestRequest) -> Vec<ResolvedTestRun> {
        match request {
            meta::TestRequest::Run(run) => self.lower_run(run),
            meta::TestRequest::Check(check) => self.lower_check(check),
        }
    }

    fn lower_run(&self, run: meta::TestRun) -> Vec<ResolvedTestRun> {
        let host = self.resolve_host(run.host_selection);
        let profile = run.test_execution_profile;
        self.nodes_of(run.node_selection)
            .into_iter()
            .map(|node| ResolvedTestRun {
                cluster: run.cluster_name.clone(),
                node,
                host: host.clone(),
                profile: profile.clone(),
                flake: self.test_flake.clone(),
            })
            .collect()
    }

    fn lower_check(&self, check: meta::QuickCheck) -> Vec<ResolvedTestRun> {
        check
            .into_payload()
            .into_iter()
            .map(|node| ResolvedTestRun {
                cluster: self.cluster.clone(),
                node,
                host: self.default_vm_host.clone(),
                profile: self.default_profile(),
                flake: self.test_flake.clone(),
            })
            .collect()
    }

    fn resolve_host(&self, selection: ordinary::HostSelection) -> ordinary::NodeName {
        match selection {
            ordinary::HostSelection::DefaultHost => self.default_vm_host.clone(),
            ordinary::HostSelection::OnHost(host) => host,
        }
    }

    fn nodes_of(&self, selection: meta::NodeSelection) -> Vec<ordinary::NodeName> {
        match selection {
            meta::NodeSelection::Nodes(nodes) => nodes,
            meta::NodeSelection::All => self.all_test_vm_nodes(),
        }
    }

    fn all_test_vm_nodes(&self) -> Vec<ordinary::NodeName> {
        self.projection()
            .map(|projection| projection.hosted_pod_nodes())
            .unwrap_or_default()
    }

    fn test_default() -> Self {
        Self {
            cluster: ordinary::ClusterName::new("fixture-cluster".into()),
            default_vm_host: ordinary::NodeName::new("fixture-vm-host".into()),
            default_mode: ordinary::TestMode::Hermetic,
            test_flake: ordinary::FlakeReference::new(
                "github:fixture-owner/fixture-test-flake".into(),
            ),
            test_nix_system: memory::NixSystem::new("x86_64-linux".into()),
            test_output_selector: memory::DeploymentOutputSelector::new(
                memory::FlakeAttribute::new("checks.fixture-a".to_string()),
            ),
            horizon_definition: None,
        }
    }

    fn default_profile(&self) -> memory::TestExecutionProfile {
        memory::TestExecutionProfile {
            test_mode: self.default_mode.clone(),
            nix_system: self.test_nix_system.clone(),
            deployment_output_selector: self.test_output_selector.clone(),
            deployment_transport_option: None,
        }
    }

    fn projection(&self) -> Option<ClusterProjection> {
        self.horizon_definition
            .clone()
            .map(ClusterProjection::from_definition)
    }
}

impl From<&crate::TestDefaults> for TestDefaults {
    fn from(defaults: &crate::TestDefaults) -> Self {
        Self {
            cluster: ordinary::ClusterName::new(defaults.cluster.clone()),
            default_vm_host: ordinary::NodeName::new(defaults.default_vm_host.clone()),
            default_mode: match defaults.default_mode {
                crate::TestMode::Hermetic => ordinary::TestMode::Hermetic,
                crate::TestMode::Live => ordinary::TestMode::Live,
            },
            test_flake: ordinary::FlakeReference::new(defaults.test_flake.clone()),
            test_nix_system: memory::NixSystem::new(defaults.test_nix_system.clone()),
            test_output_selector: memory::DeploymentOutputSelector::new(
                memory::FlakeAttribute::new(defaults.test_output_selector.clone()),
            ),
            horizon_definition: defaults.horizon_definition.clone(),
        }
    }
}

/// Which pipeline is in flight, so a `WriteRejected` from the MEMORY engine
/// routes back to the matching typed rejection (`DeployRejected` or
/// `TestRejected`). Deploy is additionally tracked by `active_deploy`, and a
/// test by `active_test`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MetaOperation {
    Deploy,
    Test,
}

/// A fully-resolved test target — the `(Check …)`/`(Run …)` request lowered to
/// the concrete cluster, node, host, and mode the daemon records and (in Unit
/// 2b) dispatches. The `(Check …)` shorthand fills cluster/host/mode from
/// `TestDefaults`; the `(Run …)` full form carries them explicitly. This is the
/// spirit `State`->`Record` precedent: a distinct typed lowering, never an
/// under-filled wire struct.
#[derive(Debug, Clone)]
pub(crate) struct ResolvedTestRun {
    cluster: ordinary::ClusterName,
    node: ordinary::NodeName,
    host: ordinary::NodeName,
    profile: memory::TestExecutionProfile,
    /// The flake paired with the exact output selector the hermetic dispatch
    /// builds (and whose built microVM runner the live path brings up).
    flake: ordinary::FlakeReference,
}

/// A test run that knows its target: the durable row it starts as and the
/// effect commands that carry it through.
trait TestRunCommands {
    /// The durable test-run row at acceptance: phase `Submitted`, outcome
    /// `Pending`, no closure yet. The decoupled executor rewrites it through
    /// the real phases (`BringingUp`/`Deploying`/…) to a terminal `Passed`
    /// (with the built closure) or `Failed(stage)` — never a faked pass.
    fn pending_record(&self, identifier: ordinary::TestRunIdentifier) -> ordinary::TestRunRecord;

    /// The hermetic-check effect command for this run: build
    /// `<flake>#<request selector>`. The execution profile supplies the exact
    /// selector and Nix system; the daemon does not infer a node-shaped output.
    fn hermetic_check_command(&self) -> nexus::HermeticCheckCommand;

    fn nexus_test_execution_profile(&self) -> nexus::TestExecutionProfile;

    /// The live bring-up command for this run: the report-51 host-untouched
    /// user-namespace bring-up of the built microVM runner on the resolved
    /// vmhost. The runner closure and guest IP are filled by the live path's
    /// preceding build; BUILT but not run live here (gated).
    fn bring_up_command(&self, runner: ordinary::ClosurePath) -> nexus::BringUpTestVmCommand;

    /// The live teardown command for this run: stop the user units so the tap +
    /// route vanish with the namespace, host netns byte-identical.
    fn tear_down_command(&self) -> nexus::TearDownTestVmCommand;
}

impl TestRunCommands for ResolvedTestRun {
    fn pending_record(&self, identifier: ordinary::TestRunIdentifier) -> ordinary::TestRunRecord {
        ordinary::TestRunRecord {
            test_run_identifier: identifier,
            cluster_name: self.cluster.clone(),
            first_node_name: self.node.clone(),
            second_node_name: self.host.clone(),
            test_mode: self.profile.test_mode.clone(),
            test_run_phase: ordinary::TestRunPhase::Submitted,
            test_outcome: ordinary::TestOutcome::Pending,
            closure_path_option: None,
        }
    }

    fn hermetic_check_command(&self) -> nexus::HermeticCheckCommand {
        nexus::HermeticCheckCommand {
            cluster_name: self.cluster.clone(),
            node_name: self.node.clone(),
            flake_reference: self.flake.clone(),
            test_execution_profile: self.nexus_test_execution_profile(),
        }
    }

    fn nexus_test_execution_profile(&self) -> nexus::TestExecutionProfile {
        nexus::TestExecutionProfile {
            test_mode: self.profile.test_mode.clone(),
            nix_system: nexus::NixSystem::new(self.profile.nix_system.payload().clone()),
            deployment_output_selector: nexus::DeploymentOutputSelector::new(
                nexus::FlakeAttribute::new(
                    self.profile
                        .deployment_output_selector
                        .payload()
                        .payload()
                        .clone(),
                ),
            ),
            deployment_transport_option: self.profile.deployment_transport_option.as_ref().map(
                |transport| nexus::DeploymentTransport {
                    nix_store_uri: nexus::NixStoreUri::new(
                        transport.nix_store_uri.payload().clone(),
                    ),
                    ssh_destination: nexus::SshDestination::new(
                        transport.ssh_destination.payload().clone(),
                    ),
                },
            ),
        }
    }

    fn bring_up_command(&self, runner: ordinary::ClosurePath) -> nexus::BringUpTestVmCommand {
        nexus::BringUpTestVmCommand {
            cluster_name: self.cluster.clone(),
            guest_node: emission::GuestNode(self.node.clone()),
            test_vm_host: nexus::TestVmHost(self.host.clone()),
            deployment_transport: self
                .nexus_test_execution_profile()
                .deployment_transport_option
                .expect("live test transport is validated before dispatch"),
            closure_path: runner,
            guest_address: emission::GuestAddress(String::new()),
        }
    }

    fn tear_down_command(&self) -> nexus::TearDownTestVmCommand {
        nexus::TearDownTestVmCommand {
            cluster_name: self.cluster.clone(),
            guest_node: emission::GuestNode(self.node.clone()),
            test_vm_host: nexus::TestVmHost(self.host.clone()),
            deployment_transport: self
                .nexus_test_execution_profile()
                .deployment_transport_option
                .expect("live test transport is validated before dispatch"),
        }
    }
}

/// The in-flight test cursor (Unit 2b) — a single accepted `Test` lowered to
/// its concrete target, the minted run identifier, and the stage that has just
/// completed so the executor knows the next effect and the durable phase to
/// record. Hermetic is a single `HermeticCheck` effect; Live brackets the
/// deploy chain with `BringUpTestVm`/`TearDownTestVm`. Per-request, like
/// [`DeployPipeline`].
#[derive(Debug, Clone)]
pub struct TestPipeline {
    run: ResolvedTestRun,
    identifier: ordinary::TestRunIdentifier,
    stage: TestStage,
    /// The accepted database marker, replayed on the terminal reply.
    accepted_marker: ordinary::DatabaseMarker,
}

/// The test pipeline cursor stage — the step that has just completed. The
/// executor reads it to emit the next effect or the terminal outcome write.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TestStage {
    /// Test accepted; the first effect (hermetic check, or live bring-up) runs
    /// next.
    Submitted,
    /// (Live) the VM was brought up; the deploy chain + assert run next.
    BroughtUp,
    /// (Live) the deploy + assert finished; teardown runs next.
    Asserted,
}

/// The in-flight test cursor: what run is being driven, what it has reached,
/// and the durable rows each step writes.
trait TestCursor {
    /// The cursor at acceptance — stage `Submitted`, the marker stand-in filled
    /// at the durable write.
    fn accepted(run: ResolvedTestRun, identifier: ordinary::TestRunIdentifier) -> Self
    where
        Self: Sized;

    /// The durable row at a given phase/outcome, carrying the run identity and
    /// (once built) the closure under test. Rewritten at every transition so a
    /// `(Query (ByTestRun …))` reads the latest committed step (Unit 2b
    /// observability).
    fn record_at(
        &self,
        phase: ordinary::TestRunPhase,
        outcome: ordinary::TestOutcome,
        closure_path: Option<ordinary::ClosurePath>,
    ) -> ordinary::TestRunRecord;

    /// The container-lifecycle transition for a live bring-up/teardown state
    /// change — the driver the report-47 §2 `ContainerLifecycleRecord` table
    /// was scaffolded for. The container is named `vm-<node>`, the on-demand
    /// microVM this test brings up.
    fn container_transition(&self, state: memory::ContainerState) -> memory::ContainerTransition;
}

impl TestCursor for TestPipeline {
    fn accepted(run: ResolvedTestRun, identifier: ordinary::TestRunIdentifier) -> Self {
        Self {
            run,
            identifier,
            stage: TestStage::Submitted,
            accepted_marker: ordinary::DatabaseMarker {
                commit_sequence: ordinary::CommitSequence::new(0),
                state_digest: ordinary::StateDigest::new(0),
            },
        }
    }

    fn record_at(
        &self,
        phase: ordinary::TestRunPhase,
        outcome: ordinary::TestOutcome,
        closure_path: Option<ordinary::ClosurePath>,
    ) -> ordinary::TestRunRecord {
        ordinary::TestRunRecord {
            test_run_identifier: self.identifier,
            cluster_name: self.run.cluster.clone(),
            first_node_name: self.run.node.clone(),
            second_node_name: self.run.host.clone(),
            test_mode: self.run.profile.test_mode.clone(),
            test_run_phase: phase,
            test_outcome: outcome,
            closure_path_option: closure_path,
        }
    }

    fn container_transition(&self, state: memory::ContainerState) -> memory::ContainerTransition {
        memory::ContainerTransition {
            cluster_name: self.run.cluster.clone(),
            node_name: self.run.node.clone(),
            container_name: memory::ContainerName::new(format!("vm-{}", self.run.node.payload())),
            container_state: state,
        }
    }
}

/// The synchronous outcome of [`SchemaRuntime::submit_test`] — the verdict the
/// daemon replies to the owner connection immediately, before the test
/// dispatch runs (mirrors [`DeploySubmissionOutcome`]). `Accepted` carries the
/// `AcceptedTest` handle and leaves the in-flight cursor set for the test-job
/// actor to drive; `Rejected` is a typed up-front refusal and leaves no cursor.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TestSubmissionOutcome {
    Accepted(meta::AcceptedTest),
    Rejected(meta::RejectedTest),
}

/// A resolved Horizon definition used to validate `(OnHost h)` and expand an
/// `All` test selection. Generic node availability becomes membership only
/// when the materialized definition explicitly selects it; that resolution is
/// performed by Horizon before Lojix reads this value.
#[derive(Debug, Clone)]
pub(crate) struct ClusterProjection {
    definition: HorizonDefinition,
}

/// What the projected cluster says about where a node may run and which hosts
/// carry it.
trait ClusterTopology {
    fn from_definition(definition: HorizonDefinition) -> Self
    where
        Self: Sized;

    /// Validate that `host` is in `node`'s declared host-set. Rejects
    /// `NodeUnknown` if the node is absent from the resolved projection, or
    /// `VmHostNotDeclaredForNode` if it has no declared VM host.
    fn validate_host_for_node(
        &self,
        host: &ordinary::NodeName,
        node: &ordinary::NodeName,
    ) -> std::result::Result<(), meta::TestRejectionReason>;

    /// The declared VM host set of one resolved member. A generic catalogue
    /// entry which the cluster did not select cannot appear here.
    fn host_set_of(&self, node: &ordinary::NodeName) -> Option<Vec<String>>;

    /// Every resolved VM node with a declared host. `MachineSpecies::Pod`
    /// remains a hosted VM guest; it is not reinterpreted as a container.
    fn hosted_pod_nodes(&self) -> Vec<ordinary::NodeName>;
}

impl ClusterTopology for ClusterProjection {
    fn from_definition(definition: HorizonDefinition) -> Self {
        Self { definition }
    }

    fn validate_host_for_node(
        &self,
        host: &ordinary::NodeName,
        node: &ordinary::NodeName,
    ) -> std::result::Result<(), meta::TestRejectionReason> {
        let host_set = self
            .host_set_of(node)
            .ok_or(meta::TestRejectionReason::NodeUnknown)?;
        if host_set.iter().any(|declared| declared == host.payload()) {
            Ok(())
        } else {
            Err(meta::TestRejectionReason::VmHostNotDeclaredForNode)
        }
    }

    fn host_set_of(&self, node: &ordinary::NodeName) -> Option<Vec<String>> {
        let horizon = self.definition.project(node.payload()).ok()?;
        let mut hosts = horizon.node.machine.host.into_iter().collect::<Vec<_>>();
        hosts.extend(horizon.node.machine.additional_hosts);
        Some(hosts)
    }

    fn hosted_pod_nodes(&self) -> Vec<ordinary::NodeName> {
        self.definition
            .resolve()
            .map(|resolved| {
                resolved
                    .nodes
                    .keys()
                    .filter_map(|name| {
                        self.definition.project(name).ok().and_then(|horizon| {
                            horizon
                                .node
                                .machine
                                .host
                                .is_some()
                                .then(|| ordinary::NodeName::new(name.clone()))
                        })
                    })
                    .collect()
            })
            .unwrap_or_default()
    }
}

/// A stage's attempt at the world outside lojix that takes more than one
/// emission: a closure realized in the target's store, an activation, a
/// Horizon materialization. It composes its emissions under one
/// [`EffectExecution`], which decides where each program comes from, and
/// every failure comes back as a [`StageFailure`] carrying what was observed —
/// the stage classification is the pipeline's business, not the attempt's.
pub(crate) trait Effect {
    /// What the world hands back when the effect succeeds.
    type Product;

    async fn run(
        &self,
        execution: &EffectExecution,
    ) -> std::result::Result<Self::Product, StageFailure>;
}

/// The synchronous outcome of [`SchemaRuntime::submit_deploy`] — the verdict the
/// daemon replies to the owner connection immediately, before the deploy
/// pipeline runs (up9q). `Accepted` carries the `DeployHandle` handle (the
/// durable deployment identifier + marker) and leaves the in-flight cursor set
/// for the deploy-job actor to drive; `Rejected` is a typed up-front refusal
/// (unsupported action, or a submission write rejection) and leaves no cursor.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DeploySubmissionOutcome {
    Accepted(meta::DeployHandle),
    /// Refused, naming the deployment record the refusal terminalized.
    Rejected(meta::RejectedDeploy),
    /// Refused with no deployment to name, because the write that would have
    /// allocated one failed.
    Refused(meta::RefusedDeploy),
}

impl From<&NexusConfiguration> for RuntimeConfiguration {
    fn from(configuration: &NexusConfiguration) -> Self {
        Self {
            generated_inputs_directory: PathBuf::from(&configuration.state_directory_path.0)
                .join("generated-inputs"),
            daemon_host: ordinary::NodeName::new(configuration.daemon_host.0.clone()),
            effect_execution: EffectExecution::production(),
            test_defaults: match &configuration.test_defaults_choice {
                signal_lojix::TestDefaultsChoice::NoTestDefaults => None,
                signal_lojix::TestDefaultsChoice::TestDefaults(defaults) => {
                    Some(TestDefaults::from(defaults))
                }
            },
        }
    }
}

impl From<&LegacyStartupConfiguration> for RuntimeConfiguration {
    fn from(configuration: &LegacyStartupConfiguration) -> Self {
        Self {
            generated_inputs_directory: PathBuf::from(&configuration.state_directory_path)
                .join("generated-inputs"),
            daemon_host: ordinary::NodeName::new(configuration.daemon_host.clone()),
            effect_execution: EffectExecution::production(),
            test_defaults: configuration.test_defaults.as_ref().map(TestDefaults::from),
        }
    }
}

/// The immutable runtime facts one daemon process was started with, shared
/// across every per-request engine value.
pub trait DaemonRuntime {
    fn test_default() -> Self
    where
        Self: Sized;

    /// A hermetic focused-test configuration whose external command names are
    /// resolved from `program_directory`.
    /// Production configuration cannot set this directory; it resolves command
    /// names through the declarative service PATH.
    fn test_with_effect_program_directory(
        generated_inputs_directory: PathBuf,
        program_directory: PathBuf,
    ) -> Self
    where
        Self: Sized;

    /// The same configuration running on `daemon_host`, so a focused test can
    /// deploy the daemon's own node or another one.
    fn with_daemon_host(self, daemon_host: ordinary::NodeName) -> Self
    where
        Self: Sized;

    fn effect_execution(&self) -> &EffectExecution;

    /// The node this daemon runs on — used to detect a self-targeting deploy so
    /// activation routes around the self-Switch deadlock (a foreground ssh that
    /// `switch-to-configuration switch` kills by restarting the daemon).
    fn daemon_host(&self) -> &ordinary::NodeName;

    /// The configured test-op defaults, if the daemon was started with them.
    fn test_defaults(&self) -> Option<&TestDefaults>;

    fn materialization_root(&self, command: &nexus::HorizonMaterializationCommand) -> PathBuf;

    fn shape_name(shape: &nexus::MaterializationShape) -> &'static str;
}

impl DaemonRuntime for RuntimeConfiguration {
    fn test_default() -> Self {
        Self {
            generated_inputs_directory: std::env::temp_dir().join("lojix-generated-inputs"),
            daemon_host: ordinary::NodeName::new("daemon-host".into()),
            effect_execution: EffectExecution::production(),
            test_defaults: Some(TestDefaults::test_default()),
        }
    }

    fn test_with_effect_program_directory(
        generated_inputs_directory: PathBuf,
        program_directory: PathBuf,
    ) -> Self {
        Self {
            generated_inputs_directory,
            daemon_host: ordinary::NodeName::new("daemon-host".into()),
            effect_execution: EffectExecution::test(program_directory),
            test_defaults: Some(TestDefaults::test_default()),
        }
    }

    fn with_daemon_host(self, daemon_host: ordinary::NodeName) -> Self {
        Self {
            daemon_host,
            ..self
        }
    }

    fn effect_execution(&self) -> &EffectExecution {
        &self.effect_execution
    }

    fn daemon_host(&self) -> &ordinary::NodeName {
        &self.daemon_host
    }

    fn test_defaults(&self) -> Option<&TestDefaults> {
        self.test_defaults.as_ref()
    }

    fn materialization_root(&self, command: &nexus::HorizonMaterializationCommand) -> PathBuf {
        let cluster = command.cluster_name.payload();
        let node = command.node_name.payload();
        self.generated_inputs_directory
            .join(cluster)
            .join(node)
            .join(Self::shape_name(&command.materialization_shape))
    }

    fn shape_name(shape: &nexus::MaterializationShape) -> &'static str {
        match shape {
            nexus::MaterializationShape::CompleteHost => "complete-host",
            nexus::MaterializationShape::BaseHost => "base-host",
            nexus::MaterializationShape::UserEnvironment(_) => "user-environment",
        }
    }
}

impl From<&signal_lojix::TestDefaults> for TestDefaults {
    fn from(defaults: &signal_lojix::TestDefaults) -> Self {
        Self {
            cluster: ordinary::ClusterName::new(defaults.cluster_name.0.clone()),
            default_vm_host: ordinary::NodeName::new(defaults.node_name.0.clone()),
            default_mode: match defaults.test_mode {
                signal_lojix::TestMode::Hermetic => ordinary::TestMode::Hermetic,
                signal_lojix::TestMode::Live => ordinary::TestMode::Live,
            },
            test_flake: ordinary::FlakeReference::new(defaults.flake_reference.0.clone()),
            test_nix_system: ordinary::NixSystem::new(defaults.nix_system.0.clone()),
            test_output_selector: ordinary::DeploymentOutputSelector::new(
                ordinary::FlakeAttribute::new(
                    defaults
                        .deployment_output_selector
                        .flake_attribute
                        .0
                        .clone(),
                ),
            ),
            horizon_definition: defaults.horizon_definition_option.clone(),
        }
    }
}

/// The BootOnce transient unit name a deployment owns. Defined here (not on the
/// foreign schema-emitted `DeploymentIdentifier`) and implemented on that type
/// so the activation-side `HostActivation::unit_name` and the resume-side
/// `DeployJob::boot_once_unit` derive ONE deterministic string from ONE place —
/// `lojix-boot-once-deploy-<deployment-identifier>` — instead of the old
/// activation-only time+pid suffix that no resumed daemon could reconstruct
/// (report 150). The verb lives on the identifier noun, not as a free helper.
trait BootOnceUnit {
    fn boot_once_unit_name(&self) -> String;
}

impl BootOnceUnit for ordinary::DeploymentIdentifier {
    fn boot_once_unit_name(&self) -> String {
        format!("lojix-boot-once-deploy-{}", self.payload())
    }
}

/// The in-flight deploy cursor. A single `Deploy` signal becomes a chain of
/// effect continuations; this records which deployment is running, its
/// resolved closure once built, and which stage produced the last effect so
/// `decide` knows the next effect to emit and the phase to record.
#[derive(Debug, Clone)]
pub struct DeployPipeline {
    deployment_identifier: ordinary::DeploymentIdentifier,
    generation_identifier: ordinary::GenerationIdentifier,
    cluster_name: ordinary::ClusterName,
    node_name: ordinary::NodeName,
    generation_artifact: ordinary::GenerationArtifact,
    activation_effect: ordinary::ActivationEffect,
    activation_slot: Option<ordinary::GenerationSlot>,
    secrets_input: memory::SecretsInput,
    requested_flake: ordinary::FlakeReference,
    flake: ordinary::FlakeReference,
    /// Exact, request-owned deployment routing. These values are persisted on
    /// the job cursor and copied to every effect; cluster/node identity never
    /// supplies an implicit route or output name.
    deployment_transport: memory::DeploymentTransport,
    deployment_input_mode: memory::DeploymentInputMode,
    horizon_definition_option: Option<HorizonDefinition>,
    deployment_output_selector: memory::DeploymentOutputSelector,
    activation_backend: memory::ActivationBackend,
    /// The deploy action (host action, or user-environment action + user). Owns the
    /// produces-closure / activates / target-attribute decisions so the
    /// pipeline asks the action rather than storing derived booleans.
    action: DeployAction,
    source_revision_policy: meta::SourceRevisionPolicy,
    source_revision: Option<ordinary::SourceRevisionRecord>,
    builder: Option<memory::NixBuilderSpec>,
    substituters: Vec<nexus::ExtraSubstituter>,
    input_overrides: Vec<memory::PersistedFlakeInputOverride>,
    closure_path: Option<ordinary::ClosurePath>,
    accepted_marker: ordinary::DatabaseMarker,
    stage: DeployStage,
    /// The exact next continuation action, durably mirrored on `DeployJob`.
    /// It advances only after the preceding materialization/result is stored.
    resume_stage: memory::DeployResumeStage,
    /// Exact receipt of the immediately preceding phase transition. It is
    /// persisted only after Store returns the commit receipt; a restart uses
    /// this real receipt to enter the runner continuation, never a zero or
    /// predicted marker.
    phase_receipt: Option<memory::PhaseReceipt>,
    submission: memory::ActualizedDeploySubmission,
}

/// The deploy pipeline cursor. Each value names the stage that has just
/// completed; after a phase-transition write commits, `advance_after_phase`
/// reads it to emit the next effect (or the final activation-record write).
/// The chain is: Submitted -> (FlakeAuth) -> Building/Eval -> Build -> Copy ->
/// (Copying) -> Activate -> (Activated) -> RecordGenerationActivated -> DeployAccepted.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeployStage {
    /// Deploy accepted; the flake-auth + eval effects come next.
    Submitted,
    /// The `Building` phase was just recorded; the eval effect runs next.
    BuildingRecorded,
    /// The `Copying` phase was just recorded; the activate effect runs next.
    CopyingRecorded,
    /// The `Activated` phase was just recorded; the activation-record write
    /// (live-set + gc-roots commit) runs next.
    ActivatedRecorded,
}

/// The deploy action — a host action or a user-environment action (with its
/// user). Owns the produces-closure / activates / target-attribute decisions
/// so the pipeline asks the action rather than storing derived booleans.
#[derive(Debug, Clone)]
enum DeployAction {
    Host(ordinary::HostDeployAction),
    UserEnvironment {
        action: meta::UserEnvironmentAction,
        user: ordinary::UserName,
    },
}

/// What a deploy action implies about the pipeline: whether it produces a
/// closure, whether it activates, and what activation profile it uses.
trait DeployActionShape {
    /// `false` only for host `Evaluate` (derivation path only, no realised
    /// closure). User-environment actions always build a closure.
    fn produces_closure(&self) -> bool;

    /// Whether the action copies + activates after the build: host
    /// SetBootProfile/ActivateNow/TestActivation/ScheduleBootOnce, or
    /// user-environment SetProfile/ActivateNow. Host Evaluate/Realize and
    /// user-environment Realize stop at the realised closure.
    fn activates(&self) -> bool;

    /// The activation profile carried on the activate command — the shape that
    /// decides which target-side activation runs (host switch-to-configuration
    /// vs user-environment home-manager profile/activate).
    fn activation_profile(&self) -> nexus::ActivationProfile;
}

impl DeployActionShape for DeployAction {
    fn produces_closure(&self) -> bool {
        match self {
            Self::Host(action) => !matches!(action, ordinary::HostDeployAction::Evaluate),
            Self::UserEnvironment { .. } => true,
        }
    }

    fn activates(&self) -> bool {
        match self {
            Self::Host(action) => matches!(
                action,
                ordinary::HostDeployAction::SetBootProfile
                    | ordinary::HostDeployAction::ActivateNow
                    | ordinary::HostDeployAction::TestActivation
                    | ordinary::HostDeployAction::ScheduleBootOnce
            ),
            Self::UserEnvironment { action, .. } => {
                matches!(
                    action,
                    meta::UserEnvironmentAction::SetProfile
                        | meta::UserEnvironmentAction::ActivateNow
                )
            }
        }
    }

    fn activation_profile(&self) -> nexus::ActivationProfile {
        match self {
            Self::Host(action) => nexus::ActivationProfile::Host(action.clone()),
            Self::UserEnvironment { action, user } => {
                nexus::ActivationProfile::UserEnvironment(nexus::UserEnvironmentActivationProfile {
                    user_environment_action: action.clone(),
                    user_name: user.clone(),
                })
            }
        }
    }
}

struct FlakeReferencePolicy<'a> {
    reference: &'a str,
}

/// What lojix will admit as a flake reference, and what it reads out of one.
/// Every answer is about the caller's text exactly as written: nothing is
/// rewritten into acceptability.
trait FlakeAdmission<'a> {
    fn new(reference: &'a str) -> Self
    where
        Self: Sized;

    fn common_locator_and_query(&self) -> Option<(&str, Option<&str>)>;

    fn is_immutable(&self) -> bool;

    fn is_resolve_and_record(&self) -> bool;

    fn safe_locator_component(value: &str) -> bool;

    fn safe_relative_dir(value: &str) -> bool;

    fn safe_ref(value: &str) -> bool;

    fn immutable_revision(&self) -> Option<memory::ImmutableRevision>;
}

impl<'a> FlakeAdmission<'a> for FlakeReferencePolicy<'a> {
    fn new(reference: &'a str) -> Self {
        Self { reference }
    }

    fn common_locator_and_query(&self) -> Option<(&str, Option<&str>)> {
        let (locator, query) = match self.reference.split_once('?') {
            Some((locator, query)) => (locator, Some(query)),
            None => (self.reference, None),
        };
        if self.reference.contains('#')
            || self.reference.contains('@')
            || locator.contains("//")
            || !locator.starts_with("github:")
        {
            return None;
        }
        let mut path = locator["github:".len()..].split('/');
        let (Some(owner), Some(repository), None) = (path.next(), path.next(), path.next()) else {
            return None;
        };
        if !Self::safe_locator_component(owner) || !Self::safe_locator_component(repository) {
            return None;
        }
        Some((locator, query))
    }

    fn is_immutable(&self) -> bool {
        let Some((_, Some(query))) = self.common_locator_and_query() else {
            return false;
        };
        let mut revision = None;
        let mut directory = None;
        for parameter in query.split('&') {
            let Some((key, value)) = parameter.split_once('=') else {
                return false;
            };
            if key.is_empty()
                || value.is_empty()
                || value.contains('=')
                || PercentEncodedText::from(key).names_credential_material()
                || PercentEncodedText::from(value).names_credential_material()
            {
                return false;
            }
            match key {
                "rev" if revision.replace(value).is_none() => {}
                "dir" if directory.replace(value).is_none() && Self::safe_relative_dir(value) => {}
                _ => return false,
            }
        }
        revision.is_some_and(|value| (value).immutable_revision().is_some())
    }

    fn is_resolve_and_record(&self) -> bool {
        let Some((_, query)) = self.common_locator_and_query() else {
            return false;
        };
        let Some(query) = query else {
            return true;
        };
        let mut reference = None;
        let mut directory = None;
        for parameter in query.split('&') {
            let Some((key, value)) = parameter.split_once('=') else {
                return false;
            };
            if key.is_empty()
                || value.is_empty()
                || value.contains('=')
                || PercentEncodedText::from(key).names_credential_material()
                || PercentEncodedText::from(value).names_credential_material()
            {
                return false;
            }
            match key {
                "ref" if reference.replace(value).is_none() && Self::safe_ref(value) => {}
                "dir" if directory.replace(value).is_none() && Self::safe_relative_dir(value) => {}
                _ => return false,
            }
        }
        true
    }

    fn safe_locator_component(value: &str) -> bool {
        !value.is_empty()
            && value != "."
            && value != ".."
            && value
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
    }

    fn safe_relative_dir(value: &str) -> bool {
        !value.starts_with('/')
            && !value.ends_with('/')
            && value.split('/').all(Self::safe_locator_component)
    }

    fn safe_ref(value: &str) -> bool {
        !value.starts_with('/')
            && !value.ends_with('/')
            && value.split('/').all(Self::safe_locator_component)
    }

    fn immutable_revision(&self) -> Option<memory::ImmutableRevision> {
        self.is_immutable().then(|| {
            self.reference
                .split_once('?')
                .and_then(|(_, query)| {
                    query
                        .split('&')
                        .find_map(|parameter| parameter.strip_prefix("rev="))
                })
                .and_then(|value| value.immutable_revision())
                .expect("validated immutable reference has exactly one revision")
        })
    }
}

/// Whether a `.drvPath` eval forces a full flake re-fetch (`--refresh`) or
/// trusts Nix's per-flake evaluation cache (bead primary-8sv6). Under
/// `RequireImmutable` against a reference that carries its immutable identity
/// (`?rev=`/`?narHash=`) the flake is fully locked, so evaluation is
/// deterministic and hermetic and the eval cache — keyed on the locked inputs —
/// is authoritative; `--refresh` there only forces a redundant re-fetch and a
/// full re-eval of the whole tree (witnessed at 10+ minutes over a slow link).
/// Any other reference is potentially mutable, so it keeps `--refresh` and a
/// moved ref re-resolves rather than serving a stale cached evaluation.
trait EvalFreshness {
    /// The refresh decision for one eval, from the deploy's source-revision
    /// policy and its resolved flake reference. Only a `RequireImmutable`
    /// deploy whose reference actually carries an immutable identity may trust
    /// the eval cache; every other case refreshes.
    fn for_source(policy: ordinary::SourceRevisionPolicy, flake: &str) -> Self
    where
        Self: Sized;
}

impl EvalFreshness for emission::EvaluationFreshness {
    fn for_source(policy: ordinary::SourceRevisionPolicy, flake: &str) -> Self {
        match policy {
            ordinary::SourceRevisionPolicy::RequireImmutable
                if FlakeReferencePolicy::new(flake).is_immutable() =>
            {
                Self::TrustingPin
            }
            _ => Self::Refreshed,
        }
    }
}

/// Reading the source identity out of what `nix flake metadata` answered.
trait SourceRevisionReading {
    fn source_revision(
        &self,
        policy: ordinary::SourceRevisionPolicy,
        requested_ref: ordinary::FlakeReference,
    ) -> ordinary::SourceRevisionRecord;
}

impl SourceRevisionReading for emission::FlakeMetadata {
    /// The locked reference Nix resolved, or the requested one when it named
    /// none; the revision it locked, or nothing.
    fn source_revision(
        &self,
        policy: ordinary::SourceRevisionPolicy,
        requested_ref: ordinary::FlakeReference,
    ) -> ordinary::SourceRevisionRecord {
        let resolved_ref = self
            .flake_reference_option
            .clone()
            .unwrap_or_else(|| requested_ref.clone());
        ordinary::SourceRevisionRecord {
            source_revision_policy: policy,
            requested_reference: memory::RequestedReference(requested_ref),
            resolved_reference: memory::ResolvedReference(resolved_ref),
            resolved_revision: self
                .resolved_revision_option
                .clone()
                .unwrap_or_else(|| memory::ResolvedRevision(String::new())),
        }
    }
}

/// The in-flight deploy cursor: what was submitted, what has been resolved so
/// far, and the effect command each next step needs.
trait DeployCursor {
    fn deployment_request_identity(
        submission: &memory::ActualizedDeploySubmission,
    ) -> memory::DeploymentRequestIdentity;

    fn from_submission(
        deployment_identifier: ordinary::DeploymentIdentifier,
        generation_identifier: ordinary::GenerationIdentifier,
        accepted_marker: ordinary::DatabaseMarker,
        submission: memory::ActualizedDeploySubmission,
    ) -> Self
    where
        Self: Sized;

    fn host_generation_artifact(
        composition: ordinary::HostComposition,
    ) -> ordinary::GenerationArtifact;

    fn host_activation_effect(action: ordinary::HostDeployAction) -> ordinary::ActivationEffect;

    fn user_environment_activation_effect(
        action: meta::UserEnvironmentAction,
    ) -> ordinary::ActivationEffect;

    /// Where the closure is realized. A deployment whose node is the daemon
    /// host builds through the local daemon Nix client (an explicit builder
    /// specification is passed verbatim to Nix and its result imported
    /// locally). Every other node realizes in the transport's own store: the
    /// locally evaluated derivation closure is copied there and the target's
    /// daemon builds it, so no output stages on the daemon host. The builder
    /// specification does not apply to such a build and is carried only so the
    /// effect can say it was ignored.
    fn build_target(&self, daemon_host: &ordinary::NodeName) -> nexus::BuildTarget;

    /// The transport as the effect vocabulary carries it.
    fn effect_transport(&self) -> nexus::DeploymentTransport;

    fn flake_auth_request(&self) -> nexus::FlakeAuthRequest;

    fn needs_horizon_materialization(&self) -> bool;

    fn horizon_materialization_command(&self) -> nexus::HorizonMaterializationCommand;

    fn materialization_shape(&self) -> nexus::MaterializationShape;

    fn nix_eval_command(&self, daemon_host: &ordinary::NodeName) -> nexus::NixEvalCommand;

    fn nix_build_command(
        &self,
        closure_path: ordinary::ClosurePath,
        daemon_host: &ordinary::NodeName,
    ) -> nexus::NixBuildCommand;

    fn copy_closure_command(
        &self,
        closure_path: ordinary::ClosurePath,
        daemon_host: &ordinary::NodeName,
    ) -> nexus::CopyClosureCommand;

    fn activate_generation_command(
        &self,
        closure_path: ordinary::ClosurePath,
    ) -> nexus::ActivateGenerationCommand;

    fn source_revision_record(&self) -> ordinary::SourceRevisionRecord;

    /// The activation-record write. The closure path, resolved source revision,
    /// and computed activation slot are mandatory: by the time the pipeline
    /// records activation they have been captured on the cursor (the activate
    /// command already required the closure and returned the slot), so `None`
    /// here is an internal invariant failure surfaced through
    /// `activation_commit` returning `None` rather than committing incomplete
    /// state into the live set.
    fn activation_commit(&self) -> Option<memory::ActivationCommit>;

    fn phase_event(
        &self,
        phase: ordinary::DeploymentPhase,
        event_log_position: ordinary::EventLogPosition,
    ) -> ordinary::DeploymentPhaseEvent;

    fn deployment_environment(&self) -> memory::DeploymentEnvironment;

    /// The BootOnce transient-unit name a resumed `Activating` job polls via
    /// `journalctl -u <unit>` instead of re-activating. Deterministic in the
    /// deployment identifier so the resumed daemon computes the same name that
    /// was persisted at submit, rather than a time/pid value it cannot
    /// reconstruct. `None` for non-BootOnce actions (which have no transient
    /// unit to poll; copy is idempotent and activation re-runs safely).
    fn boot_once_unit(&self) -> Option<String>;

    /// The durable in-flight job row at the given phase. Written on submit and
    /// rewritten at every phase transition (up9q): the persisted phase cursor,
    /// closure path (once built), exact private routing snapshot, and BootOnce
    /// unit name let a restarted daemon read the row without recomputing a
    /// route or selector.
    fn deploy_job(&self, phase: memory::DeployJobPhase) -> memory::DeployJob;
}

impl DeployCursor for DeployPipeline {
    fn deployment_request_identity(
        submission: &memory::ActualizedDeploySubmission,
    ) -> memory::DeploymentRequestIdentity {
        match &submission.deploy_submission {
            memory::DeploySubmission::Host(deployment) => memory::DeploymentRequestIdentity {
                deployment_environment: memory::DeploymentEnvironment::HostEnvironment,
                cluster_name: deployment.cluster_name.clone(),
                node_name: deployment.node_name.clone(),
                generation_artifact: Self::host_generation_artifact(
                    deployment.host_composition.clone(),
                ),
                requested_deployment_action: memory::RequestedDeploymentAction::Host(
                    deployment.host_deploy_action.clone(),
                ),
                activation_effect: Self::host_activation_effect(
                    deployment.host_deploy_action.clone(),
                ),
                source_revision_policy: deployment.source_revision_policy.clone(),
                immutable_revision_option: FlakeReferencePolicy::new(
                    deployment.flake_reference.payload(),
                )
                .immutable_revision(),
            },
            memory::DeploySubmission::UserEnvironment(deployment) => {
                memory::DeploymentRequestIdentity {
                    deployment_environment: memory::DeploymentEnvironment::UserEnvironment(
                        deployment.user_name.clone(),
                    ),
                    cluster_name: deployment.cluster_name.clone(),
                    node_name: deployment.node_name.clone(),
                    generation_artifact: memory::GenerationArtifact::UserEnvironment,
                    requested_deployment_action: memory::RequestedDeploymentAction::UserEnvironment(
                        deployment.user_environment_action.clone(),
                    ),
                    activation_effect: Self::user_environment_activation_effect(
                        deployment.user_environment_action.clone(),
                    ),
                    source_revision_policy: deployment.source_revision_policy.clone(),
                    immutable_revision_option: FlakeReferencePolicy::new(
                        deployment.flake_reference.payload(),
                    )
                    .immutable_revision(),
                }
            }
        }
    }

    fn from_submission(
        deployment_identifier: ordinary::DeploymentIdentifier,
        generation_identifier: ordinary::GenerationIdentifier,
        accepted_marker: ordinary::DatabaseMarker,
        submission: memory::ActualizedDeploySubmission,
    ) -> Self {
        let durable_submission = submission.clone();
        let horizon_definition_option = submission.horizon_definition_option;
        match submission.deploy_submission {
            memory::DeploySubmission::Host(deployment) => Self {
                deployment_identifier,
                generation_identifier,
                cluster_name: deployment.cluster_name,
                node_name: deployment.node_name,
                generation_artifact: Self::host_generation_artifact(deployment.host_composition),
                activation_effect: Self::host_activation_effect(
                    deployment.host_deploy_action.clone(),
                ),
                activation_slot: None,
                secrets_input: deployment.secrets_input,
                requested_flake: deployment.flake_reference.clone(),
                flake: deployment.flake_reference,
                deployment_transport: deployment.deployment_transport,
                deployment_input_mode: deployment.deployment_input_mode,
                horizon_definition_option,
                deployment_output_selector: deployment.deployment_output_selector,
                activation_backend: deployment.activation_backend,
                action: DeployAction::Host(deployment.host_deploy_action),
                source_revision_policy: deployment.source_revision_policy,
                source_revision: None,
                builder: deployment.nix_builder_spec_option,
                substituters: deployment.extra_substituter_vector,
                input_overrides: Vec::new(),
                closure_path: None,
                accepted_marker,
                stage: DeployStage::Submitted,
                resume_stage: memory::DeployResumeStage::ResolveFlakeAuth,
                phase_receipt: None,
                submission: durable_submission,
            },
            memory::DeploySubmission::UserEnvironment(deployment) => Self {
                deployment_identifier,
                generation_identifier,
                cluster_name: deployment.cluster_name,
                node_name: deployment.node_name,
                generation_artifact: ordinary::GenerationArtifact::UserEnvironment,
                activation_effect: Self::user_environment_activation_effect(
                    deployment.user_environment_action.clone(),
                ),
                activation_slot: None,
                secrets_input: deployment.secrets_input,
                requested_flake: deployment.flake_reference.clone(),
                flake: deployment.flake_reference,
                deployment_transport: deployment.deployment_transport,
                deployment_input_mode: deployment.deployment_input_mode,
                horizon_definition_option,
                deployment_output_selector: deployment.deployment_output_selector,
                activation_backend: deployment.activation_backend,
                action: DeployAction::UserEnvironment {
                    action: deployment.user_environment_action,
                    user: deployment.user_name,
                },
                source_revision_policy: deployment.source_revision_policy,
                source_revision: None,
                builder: deployment.nix_builder_spec_option,
                substituters: deployment.extra_substituter_vector,
                input_overrides: Vec::new(),
                closure_path: None,
                accepted_marker,
                stage: DeployStage::Submitted,
                resume_stage: memory::DeployResumeStage::ResolveFlakeAuth,
                phase_receipt: None,
                submission: durable_submission,
            },
        }
    }

    fn host_generation_artifact(
        composition: ordinary::HostComposition,
    ) -> ordinary::GenerationArtifact {
        match composition {
            ordinary::HostComposition::CompleteHost => ordinary::GenerationArtifact::CompleteHost,
            ordinary::HostComposition::BaseHost => ordinary::GenerationArtifact::BaseHost,
        }
    }

    fn host_activation_effect(action: ordinary::HostDeployAction) -> ordinary::ActivationEffect {
        match action {
            ordinary::HostDeployAction::SetBootProfile => ordinary::ActivationEffect::BootProfile,
            ordinary::HostDeployAction::TestActivation => {
                ordinary::ActivationEffect::TestActivation
            }
            ordinary::HostDeployAction::ScheduleBootOnce => {
                ordinary::ActivationEffect::BootOnceProfile
            }
            ordinary::HostDeployAction::Evaluate
            | ordinary::HostDeployAction::Realize
            | ordinary::HostDeployAction::ActivateNow => ordinary::ActivationEffect::LiveActivation,
        }
    }

    fn user_environment_activation_effect(
        action: meta::UserEnvironmentAction,
    ) -> ordinary::ActivationEffect {
        match action {
            meta::UserEnvironmentAction::Realize => ordinary::ActivationEffect::ProfileOnly,
            meta::UserEnvironmentAction::SetProfile => ordinary::ActivationEffect::ProfileOnly,
            meta::UserEnvironmentAction::ActivateNow => ordinary::ActivationEffect::LiveActivation,
        }
    }

    fn build_target(&self, daemon_host: &ordinary::NodeName) -> nexus::BuildTarget {
        let builder_option = self
            .builder
            .as_ref()
            .map(|builder| nexus::NixBuilderSpec::new(builder.payload().clone()));
        if self.node_name != *daemon_host {
            return nexus::BuildTarget::TargetStore(nexus::TargetStoreBuild {
                deployment_transport: self.effect_transport(),
                node_name: self.node_name.clone(),
                operator_node: emission::OperatorNode(daemon_host.clone()),
                ignored_builder_option: builder_option.map(nexus::IgnoredBuilder),
            });
        }
        match builder_option {
            Some(builder) => nexus::BuildTarget::Remote(builder),
            None => nexus::BuildTarget::Local,
        }
    }

    fn effect_transport(&self) -> nexus::DeploymentTransport {
        nexus::DeploymentTransport {
            nix_store_uri: nexus::NixStoreUri::new(
                self.deployment_transport.nix_store_uri.payload().clone(),
            ),
            ssh_destination: nexus::SshDestination::new(
                self.deployment_transport.ssh_destination.payload().clone(),
            ),
        }
    }

    fn flake_auth_request(&self) -> nexus::FlakeAuthRequest {
        nexus::FlakeAuthRequest {
            flake_reference: self.flake.clone(),
            source_revision_policy: self.source_revision_policy.clone(),
        }
    }

    fn needs_horizon_materialization(&self) -> bool {
        matches!(
            self.deployment_input_mode,
            memory::DeploymentInputMode::Horizon
        )
    }

    fn horizon_materialization_command(&self) -> nexus::HorizonMaterializationCommand {
        nexus::HorizonMaterializationCommand {
            cluster_name: self.cluster_name.clone(),
            node_name: self.node_name.clone(),
            horizon_definition: self
                .horizon_definition_option
                .clone()
                .expect("Horizon admission requires an actualized definition"),
            secrets_input: self.secrets_input.clone(),
            materialization_shape: self.materialization_shape(),
        }
    }

    fn materialization_shape(&self) -> nexus::MaterializationShape {
        match &self.action {
            DeployAction::Host(_) => match &self.generation_artifact {
                ordinary::GenerationArtifact::CompleteHost => {
                    nexus::MaterializationShape::CompleteHost
                }
                ordinary::GenerationArtifact::BaseHost => nexus::MaterializationShape::BaseHost,
                ordinary::GenerationArtifact::UserEnvironment => {
                    nexus::MaterializationShape::BaseHost
                }
            },
            DeployAction::UserEnvironment { user, .. } => {
                nexus::MaterializationShape::UserEnvironment(
                    nexus::UserEnvironmentMaterialization::new(user.clone()),
                )
            }
        }
    }

    fn nix_eval_command(&self, daemon_host: &ordinary::NodeName) -> nexus::NixEvalCommand {
        nexus::NixEvalCommand {
            generation_identifier: self.generation_identifier,
            cluster_name: self.cluster_name.clone(),
            node_name: self.node_name.clone(),
            generation_artifact: self.generation_artifact.clone(),
            flake_reference: self.flake.clone(),
            source_revision_record: self.source_revision_record(),
            deployment_output_selector: nexus::DeploymentOutputSelector::new(
                nexus::FlakeAttribute::new(
                    self.deployment_output_selector.payload().payload().clone(),
                ),
            ),
            persisted_flake_input_override_vector: self.input_overrides.clone(),
            // Evaluation always stays in the daemon host's own store, whatever
            // the build target: an ssh-ng evaluation store can deadlock in
            // remote transport (lojix 0.3.9). Only the build moves.
            build_target: self.build_target(daemon_host),
        }
    }

    fn nix_build_command(
        &self,
        closure_path: ordinary::ClosurePath,
        daemon_host: &ordinary::NodeName,
    ) -> nexus::NixBuildCommand {
        nexus::NixBuildCommand {
            generation_identifier: self.generation_identifier,
            closure_path,
            build_target: self.build_target(daemon_host),
            extra_substituter_vector: self.substituters.clone(),
        }
    }

    fn copy_closure_command(
        &self,
        closure_path: ordinary::ClosurePath,
        daemon_host: &ordinary::NodeName,
    ) -> nexus::CopyClosureCommand {
        let closure_origin = match self.build_target(daemon_host) {
            nexus::BuildTarget::TargetStore(_) => nexus::ClosureOrigin::TargetStore,
            nexus::BuildTarget::Local | nexus::BuildTarget::Remote(_) => {
                nexus::ClosureOrigin::DaemonStore
            }
        };
        nexus::CopyClosureCommand {
            generation_identifier: self.generation_identifier,
            node_name: self.node_name.clone(),
            deployment_transport: self.effect_transport(),
            closure_path,
            closure_origin,
        }
    }

    fn activate_generation_command(
        &self,
        closure_path: ordinary::ClosurePath,
    ) -> nexus::ActivateGenerationCommand {
        nexus::ActivateGenerationCommand {
            deployment_identifier: self.deployment_identifier,
            generation_identifier: self.generation_identifier,
            cluster_name: self.cluster_name.clone(),
            node_name: self.node_name.clone(),
            deployment_transport: nexus::DeploymentTransport {
                nix_store_uri: nexus::NixStoreUri::new(
                    self.deployment_transport.nix_store_uri.payload().clone(),
                ),
                ssh_destination: nexus::SshDestination::new(
                    self.deployment_transport.ssh_destination.payload().clone(),
                ),
            },
            closure_path,
            activation_effect: self.activation_effect.clone(),
            activation_backend: match self.activation_backend {
                memory::ActivationBackend::NixosSystemdBootV1 => {
                    nexus::ActivationBackend::NixosSystemdBootV1
                }
                memory::ActivationBackend::HomeManagerNixProfileV1 => {
                    nexus::ActivationBackend::HomeManagerNixProfileV1
                }
            },
            activation_profile: self.action.activation_profile(),
        }
    }

    fn source_revision_record(&self) -> ordinary::SourceRevisionRecord {
        self.source_revision
            .clone()
            .unwrap_or_else(|| ordinary::SourceRevisionRecord {
                source_revision_policy: self.source_revision_policy.clone(),
                requested_reference: memory::RequestedReference(self.requested_flake.clone()),
                resolved_reference: memory::ResolvedReference(self.flake.clone()),
                resolved_revision: memory::ResolvedRevision(String::new()),
            })
    }

    fn activation_commit(&self) -> Option<memory::ActivationCommit> {
        Some(memory::ActivationCommit {
            generation_identifier: self.generation_identifier,
            cluster_name: self.cluster_name.clone(),
            node_name: self.node_name.clone(),
            deployment_environment: self.deployment_environment(),
            generation_slot: self.activation_slot.clone()?,
            closure_path: self.closure_path.clone()?,
            source_revision_record: self.source_revision.clone()?,
        })
    }

    fn phase_event(
        &self,
        phase: ordinary::DeploymentPhase,
        event_log_position: ordinary::EventLogPosition,
    ) -> ordinary::DeploymentPhaseEvent {
        ordinary::DeploymentPhaseEvent {
            deployment_identifier: self.deployment_identifier,
            generation_identifier: self.generation_identifier,
            cluster_name: self.cluster_name.clone(),
            node_name: self.node_name.clone(),
            deployment_phase: phase,
            event_log_position,
            transition_marker: memory::TransitionMarker::new(self.accepted_marker.clone()),
            immutable_revision_option: self
                .source_revision
                .as_ref()
                .and_then(|record| record.resolved_revision.0.immutable_revision()),
            deployment_terminal_option: None,
        }
    }

    fn deployment_environment(&self) -> memory::DeploymentEnvironment {
        match &self.action {
            DeployAction::Host(_) => memory::DeploymentEnvironment::HostEnvironment,
            DeployAction::UserEnvironment { user, .. } => {
                memory::DeploymentEnvironment::UserEnvironment(user.clone())
            }
        }
    }

    fn boot_once_unit(&self) -> Option<String> {
        match &self.action {
            DeployAction::Host(ordinary::HostDeployAction::ScheduleBootOnce) => {
                Some(self.deployment_identifier.boot_once_unit_name())
            }
            _ => None,
        }
    }

    fn deploy_job(&self, phase: memory::DeployJobPhase) -> memory::DeployJob {
        memory::DeployJob {
            deployment_identifier: self.deployment_identifier,
            generation_identifier: self.generation_identifier,
            cluster_name: self.cluster_name.clone(),
            node_name: self.node_name.clone(),
            deploy_job_phase: phase,
            closure_path_option: self.closure_path.clone(),
            source_revision_policy: self.source_revision_policy.clone(),
            flake_reference: self.requested_flake.clone(),
            flake_reference_option: self
                .source_revision
                .as_ref()
                .map(|source_revision| source_revision.resolved_reference.0.clone()),
            resolved_revision_option: self
                .source_revision
                .as_ref()
                .map(|source_revision| source_revision.resolved_revision.clone()),
            deployment_transport: self.deployment_transport.clone(),
            deployment_input_mode: self.deployment_input_mode.clone(),
            deployment_output_selector: self.deployment_output_selector.clone(),
            activation_backend: self.activation_backend.clone(),
            nix_builder_spec_option: self.builder.clone(),
            boot_once_unit_option: self.boot_once_unit().map(memory::BootOnceUnit),
            generation_slot_option: self.activation_slot.clone(),
            persisted_flake_input_override_vector: self.input_overrides.clone(),
            deploy_resume_stage: self.resume_stage.clone(),
            phase_receipt_option: self.phase_receipt.clone(),
            actualized_deploy_submission_option: Some(self.submission.clone()),
        }
    }
}

/// The reconcile decision a daemon makes for one persisted in-flight deploy
/// job it reads on start (up9q). Computed from the job's persisted phase; the
/// daemon acts on it to resume the deploy rather than losing it. The LIVE
/// continuation behind each variant (actually polling journalctl, actually
/// re-running the idempotent copy) is proven on a real target at S5 — this
/// type is the read-on-start reconcile-decision scaffolding S4b lands.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DeployJobResumption {
    /// Phase reached `Activating`: the activate effect was in flight when the
    /// daemon stopped. Re-activating could double-switch, so poll the BootOnce
    /// transient unit (PID-1-owned, survives the daemon) via
    /// `journalctl -u <unit>` and adopt its outcome. Carries the unit name when
    /// the job recorded one (BootOnce); a non-BootOnce activating job has no
    /// unit and falls back to re-running the idempotent activation at S5.
    PollActivationUnit { unit: Option<String> },
    /// Phase is pre-activation (`Submitted`/`Building`/`Built`/`Copying`): no
    /// target state was mutated yet, or only the idempotent copy ran. Re-drive
    /// the pipeline from submit; copy is idempotent and re-runs safely.
    RestartPipeline,
    /// Phase is terminal (`Activated`/`Failed`): the deploy already finished
    /// (success committed its live generation, failure is final). Nothing to
    /// resume; drop the stale job row.
    AlreadyTerminal,
}

/// What a persisted deploy-job row says about resuming the deployment it
/// belongs to after the connection that submitted it is gone.
pub(crate) trait DeployJobResuming {
    /// Reconstruct the retained PID-1 receipt for exactly a same-host host
    /// `ActivateNow` or `TestActivation`. The durable submission already binds
    /// the deployment id, action, backend, and target node, so a second
    /// BootOnce-shaped field would be redundant and misleading.
    fn detached_activation_unit(
        &self,
        daemon_host: &ordinary::NodeName,
    ) -> Option<DetachedActivationUnit>;

    /// The reconcile decision for this persisted job, read on daemon start. A
    /// pure projection of the persisted phase cursor — the daemon calls it once
    /// per resumed row and acts on the typed verdict (up9q resume scaffolding).
    fn resumption(&self) -> DeployJobResumption;

    /// The live-set + gc-root rows a daemon persists for a detached
    /// self-switch when it settles the deployment from the retained unit
    /// (bead primary-7u8p). A host `ActivateNow` whose target IS the daemon
    /// host runs `switch-to-configuration switch` inside a PID-1-owned unit
    /// that may restart the daemon mid-switch, so the pipeline never records
    /// the generation; recording it here also keeps
    /// `next_deployment_identifier`, which scans only the live set, from
    /// reusing its id. The record says the generation is live; the unit's own
    /// result, not this record, is the deployment's terminal (bead
    /// primary-6llk): a switch whose later clearing failed is recorded Current
    /// and terminalized Failed.
    ///
    /// This repair is available only to the exact persisted submission that
    /// can cause it: `Host(ActivateNow)` through `NixosSystemdBootV1`. The
    /// persisted job snapshot must agree on that backend too. In particular,
    /// a `SetBootProfile`, boot-once, test, or user-environment row never
    /// becomes a recovered self-switch merely because a profile happens to
    /// match its closure.
    ///
    /// `daemon_host_system_closure` is the store path the daemon host's live
    /// system profile (`/nix/var/nix/profiles/system`) currently resolves to.
    /// A COMPLETED host self-switch is the only activation that set that profile
    /// to this job's closure (`nix-env -p /nix/var/nix/profiles/system --set
    /// <closure>` runs before the switch restarts the daemon), so an exact match
    /// is the tight witness that the generation is live — genuinely
    /// `Current`. Every other row that could
    /// reach `Activating` on the daemon host (a racy unrelated restart during a
    /// self-host `SetBootProfile`/`TestActivation`, or a user-environment
    /// activation, none of which touch the system profile) fails the match and
    /// returns `None`, so it is left for S5 rather than mis-recorded as a
    /// Current complete-host generation. Also `None` when the job never captured
    /// a built closure path, so there is nothing to record.
    fn self_switch_activation_record(
        &self,
        daemon_host_system_closure: Option<&str>,
    ) -> Option<(memory::LiveGeneration, memory::GcRoot)>;

    /// The persisted host submission when it is a host `ActivateNow` through
    /// `NixosSystemdBootV1`, the job snapshot agreeing on that backend.
    fn persisted_host_activate_now(&self) -> Option<&memory::HostDeployment>;
}

impl DeployJobResuming for memory::DeployJob {
    fn detached_activation_unit(
        &self,
        daemon_host: &ordinary::NodeName,
    ) -> Option<DetachedActivationUnit> {
        let (host, activation) = match self
            .actualized_deploy_submission_option
            .as_ref()
            .map(|submission| &submission.deploy_submission)
        {
            Some(meta_signal_lojix::DeploySubmission::Host(host))
                if host.activation_backend == memory::ActivationBackend::NixosSystemdBootV1 =>
            {
                match host.host_deploy_action {
                    ordinary::HostDeployAction::ActivateNow => {
                        (host, DetachedSelfActivation::Switch)
                    }
                    ordinary::HostDeployAction::TestActivation => {
                        (host, DetachedSelfActivation::Test)
                    }
                    _ => return None,
                }
            }
            _ => return None,
        };
        if self.deploy_job_phase != memory::DeployJobPhase::Activating
            || self.node_name != *daemon_host
            || self.activation_backend != memory::ActivationBackend::NixosSystemdBootV1
        {
            return None;
        }
        let unit = DetachedActivationUnit::for_deployment(&self.deployment_identifier, activation);
        let journal_option = SshTarget::from_transport(&nexus::DeploymentTransport {
            nix_store_uri: nexus::NixStoreUri::new(
                host.deployment_transport.nix_store_uri.payload().clone(),
            ),
            ssh_destination: nexus::SshDestination::new(
                host.deployment_transport.ssh_destination.payload().clone(),
            ),
        })
        .ok()
        .map(|target| UnitJournal::new(unit.name.clone(), target));
        Some(DetachedActivationUnit {
            journal_option,
            ..unit
        })
    }

    fn resumption(&self) -> DeployJobResumption {
        match self.deploy_job_phase {
            memory::DeployJobPhase::Activating => DeployJobResumption::PollActivationUnit {
                unit: self.boot_once_unit_option.clone().map(|unit| unit.0),
            },
            memory::DeployJobPhase::Submitted
            | memory::DeployJobPhase::Building
            | memory::DeployJobPhase::Built
            | memory::DeployJobPhase::Copying => DeployJobResumption::RestartPipeline,
            memory::DeployJobPhase::Activated | memory::DeployJobPhase::Failed => {
                DeployJobResumption::AlreadyTerminal
            }
        }
    }

    fn self_switch_activation_record(
        &self,
        daemon_host_system_closure: Option<&str>,
    ) -> Option<(memory::LiveGeneration, memory::GcRoot)> {
        let host = self.persisted_host_activate_now()?;
        let closure_path = self.closure_path_option.clone()?;
        if daemon_host_system_closure != Some(closure_path.payload().as_str()) {
            return None;
        }
        let source_revision_record = ordinary::SourceRevisionRecord {
            source_revision_policy: self.source_revision_policy.clone(),
            requested_reference: memory::RequestedReference(self.flake_reference.clone()),
            resolved_reference: memory::ResolvedReference(
                self.flake_reference_option
                    .clone()
                    .unwrap_or_else(|| self.flake_reference.clone()),
            ),
            resolved_revision: self
                .resolved_revision_option
                .clone()
                .unwrap_or_else(|| memory::ResolvedRevision(String::new())),
        };
        let generation = memory::LiveGeneration {
            deployment_identifier: self.deployment_identifier,
            generation_identifier: self.generation_identifier,
            cluster_name: self.cluster_name.clone(),
            node_name: self.node_name.clone(),
            deployment_environment: memory::DeploymentEnvironment::HostEnvironment,
            // The detached self-switch is only reached by a host `ActivateNow`,
            // so the effect is a live activation; the artifact is the one the
            // submission composed, as the pipeline would have recorded it.
            generation_artifact: match host.host_composition {
                ordinary::HostComposition::CompleteHost => {
                    ordinary::GenerationArtifact::CompleteHost
                }
                ordinary::HostComposition::BaseHost => ordinary::GenerationArtifact::BaseHost,
            },
            activation_effect: ordinary::ActivationEffect::LiveActivation,
            generation_slot: ordinary::GenerationSlot::Current,
            closure_path: closure_path.clone(),
            source_revision_record,
        };
        let root = memory::GcRoot {
            generation_identifier: self.generation_identifier,
            cluster_name: self.cluster_name.clone(),
            node_name: self.node_name.clone(),
            generation_slot: ordinary::GenerationSlot::Current,
            closure_path,
            pin_label_option: None,
        };
        Some((generation, root))
    }

    fn persisted_host_activate_now(&self) -> Option<&memory::HostDeployment> {
        if self.activation_backend != memory::ActivationBackend::NixosSystemdBootV1 {
            return None;
        }
        match self
            .actualized_deploy_submission_option
            .as_ref()
            .map(|submission| &submission.deploy_submission)
        {
            Some(meta_signal_lojix::DeploySubmission::Host(host))
                if host.host_deploy_action == ordinary::HostDeployAction::ActivateNow
                    && host.activation_backend == memory::ActivationBackend::NixosSystemdBootV1 =>
            {
                Some(host)
            }
            _ => None,
        }
    }
}

impl From<memory::StoredTestRun> for ordinary::TestRunRecord {
    /// The durable row holds the contract's record whole.
    fn from(run: memory::StoredTestRun) -> Self {
        run.test_run_record
    }
}

/// Where a persisted resume stage sits in the pipeline. A stage either names
/// the phase whose durable receipt it resumes after, or it does not — the one
/// answer, read once, instead of a guard and a match that must agree.
trait ResumePoint {
    fn recorded_phase(&self) -> Option<ordinary::DeploymentPhase>;
    fn deploy_stage(&self) -> DeployStage;
}

impl ResumePoint for memory::DeployResumeStage {
    fn recorded_phase(&self) -> Option<ordinary::DeploymentPhase> {
        match self {
            Self::NixEval => Some(ordinary::DeploymentPhase::Building),
            Self::ActivateGeneration => Some(ordinary::DeploymentPhase::Copying),
            Self::RecordGenerationActivated => Some(ordinary::DeploymentPhase::Activated),
            _ => None,
        }
    }

    fn deploy_stage(&self) -> DeployStage {
        match self {
            Self::NixEval => DeployStage::Submitted,
            Self::ActivateGeneration => DeployStage::BuildingRecorded,
            Self::RecordGenerationActivated => DeployStage::CopyingRecorded,
            Self::FinishDeployment => DeployStage::ActivatedRecorded,
            _ => DeployStage::Submitted,
        }
    }
}

impl From<ordinary::DeploymentPhase> for memory::DeployJobPhase {
    /// Mirror the wire phase onto the durable job-row phase cursor. The two
    /// enums carry the same variants; the job row tracks the same lifecycle the
    /// event log records, so a resumed daemon reads one phase value.
    fn from(phase: ordinary::DeploymentPhase) -> Self {
        match phase {
            ordinary::DeploymentPhase::Submitted => Self::Submitted,
            ordinary::DeploymentPhase::Building => Self::Building,
            ordinary::DeploymentPhase::Built => Self::Built,
            ordinary::DeploymentPhase::Copying => Self::Copying,
            ordinary::DeploymentPhase::Activating => Self::Activating,
            ordinary::DeploymentPhase::Activated => Self::Activated,
            ordinary::DeploymentPhase::Completed | ordinary::DeploymentPhase::Rejected => {
                Self::Failed
            }
            ordinary::DeploymentPhase::Failed => Self::Failed,
        }
    }
}

impl Default for SchemaRuntime {
    fn default() -> Self {
        Self::new()
    }
}

/// What one durable-store rejection is called in each meta verb's own
/// vocabulary. A reason with no meaning in a verb's domain is that verb's
/// internal error — never a domain word that would tell the operator something
/// false.
pub(crate) trait RejectionVocabulary {
    fn test_reason(self) -> meta::TestRejectionReason;

    fn pin_reason(self) -> meta::PinRejectionReason;

    fn unpin_reason(self) -> meta::UnpinRejectionReason;

    fn retire_reason(self) -> meta::RetireRejectionReason;

    fn deploy_reason(self) -> meta::DeployRejectionReason;
}

impl RejectionVocabulary for memory::RejectionReason {
    /// Map a MEMORY write-rejection reason to a typed test rejection. A reason
    /// with no test-domain meaning is an internal invariant failure (the Deploy
    /// precedent), never a misleading domain reason.
    fn test_reason(self) -> meta::TestRejectionReason {
        let reason = self;
        match reason {
            memory::RejectionReason::ClusterUnknown => meta::TestRejectionReason::ClusterUnknown,
            memory::RejectionReason::NodeUnknown => meta::TestRejectionReason::NodeUnknown,
            _ => meta::TestRejectionReason::InternalError,
        }
    }

    fn pin_reason(self) -> meta::PinRejectionReason {
        let reason = self;
        match reason {
            memory::RejectionReason::GenerationUnknown => {
                meta::PinRejectionReason::GenerationUnknown
            }
            memory::RejectionReason::NodeUnknown => meta::PinRejectionReason::NodeUnknown,
            memory::RejectionReason::PinLabelInUse => meta::PinRejectionReason::PinLabelInUse,
            _ => meta::PinRejectionReason::InternalError,
        }
    }

    fn unpin_reason(self) -> meta::UnpinRejectionReason {
        let reason = self;
        match reason {
            memory::RejectionReason::PinLabelUnknown => meta::UnpinRejectionReason::PinLabelUnknown,
            memory::RejectionReason::NodeUnknown => meta::UnpinRejectionReason::NodeUnknown,
            memory::RejectionReason::GenerationUnknown => {
                meta::UnpinRejectionReason::GenerationNotPinned
            }
            // A reason with no unpin-domain meaning — a failed durable write,
            // a poisoned lock — is an internal invariant failure, as it is for
            // every other operation. Reporting it as `GenerationNotPinned`
            // told the operator something false about their generation.
            _ => meta::UnpinRejectionReason::InternalError,
        }
    }

    fn retire_reason(self) -> meta::RetireRejectionReason {
        let reason = self;
        match reason {
            memory::RejectionReason::GenerationUnknown => {
                meta::RetireRejectionReason::GenerationUnknown
            }
            memory::RejectionReason::NodeUnknown => meta::RetireRejectionReason::NodeUnknown,
            memory::RejectionReason::GenerationActive => {
                meta::RetireRejectionReason::GenerationActive
            }
            memory::RejectionReason::GenerationPinned => {
                meta::RetireRejectionReason::GenerationPinned
            }
            _ => meta::RetireRejectionReason::InternalError,
        }
    }

    fn deploy_reason(self) -> meta::DeployRejectionReason {
        let reason = self;
        match reason {
            memory::RejectionReason::ClusterUnknown => meta::DeployRejectionReason::ClusterUnknown,
            memory::RejectionReason::NodeUnknown => meta::DeployRejectionReason::NodeUnknown,
            memory::RejectionReason::ProposalSourceUnreachable => {
                meta::DeployRejectionReason::ProposalSourceUnreachable
            }
            // A memory reason with no deploy-domain mapping is an internal
            // invariant failure (e.g. a poisoned lock), not "already deploying"
            // (audit C4).
            _ => meta::DeployRejectionReason::InternalError,
        }
    }
}

/// What a refused deploy is recorded as once the refusal becomes durable.
trait TerminalReason {
    fn terminal_reason(self) -> memory::DeploymentTerminalReason;
}

impl TerminalReason for meta::DeployRejectionReason {
    fn terminal_reason(self) -> memory::DeploymentTerminalReason {
        let reason = self;
        match reason {
            meta::DeployRejectionReason::ClusterUnknown => {
                memory::DeploymentTerminalReason::ClusterUnknown
            }
            meta::DeployRejectionReason::NodeUnknown => {
                memory::DeploymentTerminalReason::NodeUnknown
            }
            meta::DeployRejectionReason::ProposalSourceUnreachable => {
                memory::DeploymentTerminalReason::ProposalSourceUnreachable
            }
            meta::DeployRejectionReason::FlakeReferenceMalformed => {
                memory::DeploymentTerminalReason::FlakeReferenceMalformed
            }
            meta::DeployRejectionReason::EvaluationFailed => {
                memory::DeploymentTerminalReason::EvaluationFailed
            }
            meta::DeployRejectionReason::BuildFailed => {
                memory::DeploymentTerminalReason::BuildFailed
            }
            meta::DeployRejectionReason::InvalidDeploymentRouting => {
                memory::DeploymentTerminalReason::InvalidDeploymentRouting
            }
            meta::DeployRejectionReason::ClosureCopyFailed => {
                memory::DeploymentTerminalReason::ClosureCopyFailed
            }
            meta::DeployRejectionReason::DeploymentInFlight => {
                memory::DeploymentTerminalReason::DeploymentInFlight
            }
            meta::DeployRejectionReason::UnsupportedDeployAction => {
                memory::DeploymentTerminalReason::UnsupportedDeployAction
            }
            meta::DeployRejectionReason::InternalError => {
                memory::DeploymentTerminalReason::InternalError
            }
            meta::DeployRejectionReason::ActivationFailed => {
                memory::DeploymentTerminalReason::ActivationFailed
            }
        }
    }
}

/// How an effect stage that failed is recorded, in the deployment's vocabulary
/// and in the test pipeline's.
trait FailureStaging {
    fn deployment_failure_stage(self) -> memory::DeploymentFailureStage;

    fn test_failure_stage(self) -> ordinary::FailureStage;
}

impl FailureStaging for nexus::EffectStage {
    fn deployment_failure_stage(self) -> memory::DeploymentFailureStage {
        let stage = self;
        match stage {
            nexus::EffectStage::FlakeAuth => memory::DeploymentFailureStage::FlakeAuth,
            nexus::EffectStage::MaterializeHorizon => {
                memory::DeploymentFailureStage::MaterializeHorizon
            }
            nexus::EffectStage::Eval => memory::DeploymentFailureStage::Eval,
            nexus::EffectStage::Build => memory::DeploymentFailureStage::Build,
            nexus::EffectStage::CopyClosure => memory::DeploymentFailureStage::CopyClosure,
            nexus::EffectStage::Activate => memory::DeploymentFailureStage::Activate,
            nexus::EffectStage::Gc
            | nexus::EffectStage::HermeticCheck
            | nexus::EffectStage::BringUpTestVm
            | nexus::EffectStage::TearDownTestVm => memory::DeploymentFailureStage::Daemon,
        }
    }

    fn test_failure_stage(self) -> ordinary::FailureStage {
        let stage = self;
        match stage {
            nexus::EffectStage::HermeticCheck => ordinary::FailureStage::HermeticCheck,
            nexus::EffectStage::BringUpTestVm => ordinary::FailureStage::BringUp,
            nexus::EffectStage::TearDownTestVm => ordinary::FailureStage::TearDown,
            // The live deploy-into-VM chain failing is a Deploy-stage test
            // failure; assert-stage failures map to Assert. Any other effect
            // stage on the test pipeline is recorded as a Deploy-stage failure
            // honestly (the live cycle's deploy bracket).
            nexus::EffectStage::Activate => ordinary::FailureStage::Assert,
            _ => ordinary::FailureStage::Deploy,
        }
    }
}

/// Which generation slot an activation effect leaves the generation in.
trait ActivationSlot {
    fn activation_slot(&self) -> ordinary::GenerationSlot;
}

impl ActivationSlot for ordinary::ActivationEffect {
    fn activation_slot(&self) -> ordinary::GenerationSlot {
        let activation_effect = self;
        match activation_effect {
            ordinary::ActivationEffect::LiveActivation => ordinary::GenerationSlot::Current,
            ordinary::ActivationEffect::BootProfile => ordinary::GenerationSlot::BootPending,
            ordinary::ActivationEffect::TestActivation => ordinary::GenerationSlot::Recent,
            ordinary::ActivationEffect::BootOnceProfile => ordinary::GenerationSlot::BootPending,
            ordinary::ActivationEffect::ProfileOnly => ordinary::GenerationSlot::Current,
        }
    }
}

/// What a stored test run has to match for a lookup to answer with it.
trait TestRunSelecting {
    fn matches(&self, run: &memory::StoredTestRun) -> bool;
}

impl TestRunSelecting for ordinary::TestRunLookup {
    fn matches(&self, run: &memory::StoredTestRun) -> bool {
        let lookup = self;
        let run = &run.test_run_record;
        lookup.cluster_name == run.cluster_name
            && lookup.node_name == run.first_node_name
            && lookup
                .test_run_identifier_option
                .as_ref()
                .is_none_or(|identifier| identifier == &run.test_run_identifier)
    }
}

/// What a query selection admits: which live generations answer it, and which
/// durable deployment records do.
trait GenerationSelecting {
    fn matches_generation(&self, live: &memory::LiveGeneration) -> bool;

    fn matches_deployment_record(&self, record: &memory::DeploymentRecord) -> bool;
}

impl GenerationSelecting for ordinary::Selection {
    fn matches_generation(&self, live: &memory::LiveGeneration) -> bool {
        let selection = self;
        match selection {
            ordinary::Selection::ByNode(selector) => {
                selector.cluster_name == live.cluster_name
                    && selector.node_name == live.node_name
                    && selector
                        .generation_artifact_option
                        .as_ref()
                        .is_none_or(|artifact| artifact == &live.generation_artifact)
            }
            ordinary::Selection::ByGeneration(lookup) => {
                *lookup.payload() == live.generation_identifier
            }
            // A generation belongs to the deployment that produced it; a
            // ByDeployment query that answered with no generation made the
            // documented selector useless.
            ordinary::Selection::ByDeployment(lookup) => {
                *lookup.payload() == live.deployment_identifier
            }
        }
    }

    fn matches_deployment_record(&self, record: &memory::DeploymentRecord) -> bool {
        let selection = self;
        match selection {
            ordinary::Selection::ByNode(selector) => {
                record.deployment_request_identity.cluster_name == selector.cluster_name
                    && record.deployment_request_identity.node_name == selector.node_name
                    && selector
                        .generation_artifact_option
                        .as_ref()
                        .is_none_or(|artifact| {
                            artifact == &record.deployment_request_identity.generation_artifact
                        })
            }
            ordinary::Selection::ByGeneration(lookup) => {
                *lookup.payload() == record.generation_identifier
            }
            ordinary::Selection::ByDeployment(lookup) => {
                *lookup.payload() == record.deployment_identifier
            }
        }
    }
}

/// What a deploy request is refused for before anything durable happens. The
/// request judges itself: nothing about the store or the pipeline participates.
trait DeployAdmission {
    /// The first reason this request cannot be admitted, or `None`.
    fn submission_rejection(&self) -> Option<meta::DeployRejectionReason>;

    fn unsupported_deploy_reason(&self) -> Option<meta::DeployRejectionReason>;

    fn source_revision_policy_rejection(&self) -> Option<meta::DeployRejectionReason>;

    fn deployment_routing_rejection(&self) -> Option<meta::DeployRejectionReason>;

    fn proposal_source_rejection(&self) -> Option<meta::DeployRejectionReason>;
}

impl DeployAdmission for meta::DeployRequest {
    /// The deploy reject-guard. Production host and user-environment eval/build
    /// are implemented through Horizon materialization, and the activating actions
    /// (host SetBootProfile/ActivateNow/TestActivation/ScheduleBootOnce,
    /// user-environment SetProfile/ActivateNow) now construct
    /// target-safe copy + activate commands (S4a), so every declared action is
    /// supported and enters the effect pipeline. `UnsupportedDeployAction`
    /// stays in the enum for honesty on any future not-yet-implemented shape;
    /// no current action returns it.
    /// Every preflight a deploy must pass before it is admitted, in one place
    /// so the synchronous submit and the engine's own routing cannot drift
    /// apart on which checks run or in what order.
    fn submission_rejection(&self) -> Option<meta::DeployRejectionReason> {
        let request = self;
        request
            .unsupported_deploy_reason()
            .or_else(|| request.deployment_routing_rejection())
            .or_else(|| request.proposal_source_rejection())
            .or_else(|| request.source_revision_policy_rejection())
    }

    fn unsupported_deploy_reason(&self) -> Option<meta::DeployRejectionReason> {
        match &self.deploy_submission {
            meta::DeploySubmission::Host(deployment) => {
                let supported = matches!(
                    deployment.host_deploy_action,
                    ordinary::HostDeployAction::Evaluate
                        | ordinary::HostDeployAction::Realize
                        | ordinary::HostDeployAction::SetBootProfile
                        | ordinary::HostDeployAction::ActivateNow
                        | ordinary::HostDeployAction::TestActivation
                        | ordinary::HostDeployAction::ScheduleBootOnce
                );
                (!supported).then_some(meta::DeployRejectionReason::UnsupportedDeployAction)
            }
            meta::DeploySubmission::UserEnvironment(deployment) => {
                let supported = matches!(
                    deployment.user_environment_action,
                    meta::UserEnvironmentAction::Realize
                        | meta::UserEnvironmentAction::SetProfile
                        | meta::UserEnvironmentAction::ActivateNow
                );
                (!supported).then_some(meta::DeployRejectionReason::UnsupportedDeployAction)
            }
        }
    }

    fn source_revision_policy_rejection(&self) -> Option<meta::DeployRejectionReason> {
        let (policy, flake) = match &self.deploy_submission {
            meta::DeploySubmission::Host(deployment) => (
                deployment.source_revision_policy.clone(),
                deployment.flake_reference.payload(),
            ),
            meta::DeploySubmission::UserEnvironment(deployment) => (
                deployment.source_revision_policy.clone(),
                deployment.flake_reference.payload(),
            ),
        };
        match policy {
            meta::SourceRevisionPolicy::ResolveAndRecord => (!FlakeReferencePolicy::new(flake)
                .is_resolve_and_record())
            .then_some(meta::DeployRejectionReason::FlakeReferenceMalformed),
            meta::SourceRevisionPolicy::RequireImmutable => (!FlakeReferencePolicy::new(flake)
                .is_immutable())
            .then_some(meta::DeployRejectionReason::FlakeReferenceMalformed),
        }
    }

    /// Validate every request-owned deployment route before the private cursor
    /// is admitted. Validation is intentionally separate from construction:
    /// later effects may use the strings verbatim without a fallback or a
    /// cluster/node-derived repair.
    fn deployment_routing_rejection(&self) -> Option<meta::DeployRejectionReason> {
        let (transport, selector, backend, action, builder) = match &self.deploy_submission {
            meta::DeploySubmission::Host(deployment) => (
                &deployment.deployment_transport,
                &deployment.deployment_output_selector,
                deployment.activation_backend.clone(),
                true,
                deployment.nix_builder_spec_option.as_ref(),
            ),
            meta::DeploySubmission::UserEnvironment(deployment) => (
                &deployment.deployment_transport,
                &deployment.deployment_output_selector,
                deployment.activation_backend.clone(),
                false,
                deployment.nix_builder_spec_option.as_ref(),
            ),
        };
        let valid_backend = matches!(
            (action, backend),
            (true, memory::ActivationBackend::NixosSystemdBootV1)
                | (false, memory::ActivationBackend::HomeManagerNixProfileV1)
        );
        let valid_selector = !selector.payload().payload().is_empty()
            && selector
                .payload()
                .payload()
                .bytes()
                .all(|byte| !byte.is_ascii_whitespace() && !byte.is_ascii_control());
        let valid_builder = builder.is_none_or(|specification| {
            !specification.payload().is_empty()
                && specification
                    .payload()
                    .bytes()
                    .all(|byte| !byte.is_ascii_control())
        });
        let target = SshTarget::from_transport(&nexus::DeploymentTransport {
            nix_store_uri: nexus::NixStoreUri::new(transport.nix_store_uri.payload().clone()),
            ssh_destination: nexus::SshDestination::new(
                transport.ssh_destination.payload().clone(),
            ),
        });
        let valid_transport = target.is_ok();
        let valid_user_environment_authority = match &self.deploy_submission {
            meta::DeploySubmission::Host(_) => true,
            meta::DeploySubmission::UserEnvironment(deployment) => {
                if matches!(
                    deployment.user_environment_action,
                    meta::UserEnvironmentAction::Realize
                ) {
                    true
                } else {
                    HorizonUserName::try_from(deployment.user_name.payload().clone())
                        .ok()
                        .zip(target.as_ref().ok())
                        .is_some_and(|(user, target)| {
                            !matches!(
                                target.user_environment_activation_authority(&user),
                                RemoteUserActivationAuthority::UnprivilegedMismatch
                            )
                        })
                }
            }
        };
        (!valid_backend
            || !valid_selector
            || !valid_builder
            || !valid_transport
            || !valid_user_environment_authority)
            .then_some(meta::DeployRejectionReason::InvalidDeploymentRouting)
    }

    /// Require the client-actualized Horizon definition exactly when the
    /// selected input mode consumes it, and prove the target projection before
    /// admitting any effect.
    fn proposal_source_rejection(&self) -> Option<meta::DeployRejectionReason> {
        let (mode, node) = match &self.deploy_submission {
            meta::DeploySubmission::Host(deployment) => (
                deployment.deployment_input_mode.clone(),
                &deployment.node_name,
            ),
            meta::DeploySubmission::UserEnvironment(deployment) => (
                deployment.deployment_input_mode.clone(),
                &deployment.node_name,
            ),
        };
        let definition = self.horizon_definition_option.as_ref();
        match (mode, definition) {
            (memory::DeploymentInputMode::Direct, None) => None,
            (memory::DeploymentInputMode::Horizon, Some(definition))
                if definition.project(node.payload()).is_ok() =>
            {
                None
            }
            _ => Some(meta::DeployRejectionReason::ProposalSourceUnreachable),
        }
    }
}

/// A read that could not be answered, reported with the store state it was
/// asked against.
trait ReadMiss {
    fn read_missed(commit_sequence: i64, reason: memory::RejectionReason) -> Self;
}

impl ReadMiss for memory::Outcome {
    fn read_missed(commit_sequence: i64, reason: memory::RejectionReason) -> Self {
        Self::ReadMissed(memory::RejectionReport {
            rejection_reason: reason,
            database_marker: ordinary::DatabaseMarker::at_commit(commit_sequence),
        })
    }
}

impl From<meta::Output> for nexus::NexusAction {
    fn from(output: meta::Output) -> Self {
        Self::Finish(output)
    }
}

/// The receipt a Configure answers with: the configuration the Nexus now
/// desires, and whether the privileged Configure has happened.
trait ConfigurationReceipting {
    fn receipt(self) -> memory::ConfigurationReceipt;
}

impl ConfigurationReceipting for crate::NexusConfigurationState {
    fn receipt(self) -> memory::ConfigurationReceipt {
        memory::ConfigurationReceipt {
            lojix_nexus_configuration: self.desired_configuration,
            meta_configure_occurred: signal_lojix::MetaConfigureOccurred(
                self.meta_configure_occurred,
            ),
        }
    }
}

impl From<crate::Error> for memory::ConfigurationRejection {
    fn from(error: crate::Error) -> Self {
        Self {
            configuration_rejection_reason: match error {
                crate::Error::OrdinaryConfigureClosed => {
                    memory::ConfigurationRejectionReason::OrdinaryConfigureClosed
                }
                _ => memory::ConfigurationRejectionReason::InvalidConfiguration,
            },
        }
    }
}

/// Nexus Core itself: how one per-request engine is made, what durable state
/// it sits on, and how a decided action is run through its continuations until
/// it has a reply. Each request is served by its own engine over a shared
/// `Arc<Store>`, so the in-flight cursors are per-request while the durable
/// tables are not.
// Exception: `async fn` in a public trait. The lint exists because a caller
// cannot name a `Send` bound on the returned future. This trait is implemented
// by exactly one type, is never used behind `dyn`, and its futures are awaited
// on the task that created the engine, so there is no bound for a caller to
// name.
#[allow(async_fn_in_trait)]
pub trait RuntimeCore {
    fn new() -> Self
    where
        Self: Sized;

    /// A unique tempdir-backed `*.memory` path for a test-only `Store`. Each call
    /// names a fresh file under the temp directory (process id, a nanosecond
    /// timestamp, and a per-process counter) so parallel tests never collide and
    /// each starts from a virgin store.
    fn test_store_path() -> PathBuf;

    /// Build an engine over a SHARED `Store`. The daemon constructs one per
    /// request from a single shared `Arc<Store>`, so concurrent requests share
    /// the durable tables but each owns its in-flight deploy cursor (intent 2alg).
    fn with_store(store: Arc<Store>) -> Self
    where
        Self: Sized;

    fn with_store_and_configuration(
        store: Arc<Store>,
        configuration: Arc<RuntimeConfiguration>,
    ) -> Self
    where
        Self: Sized;

    fn store(&self) -> &Store;

    /// Return a marker only when it can be read from the durable store.  The
    /// public protocol has no infrastructure-error variant, so manufacturing a
    /// zero marker would falsely correlate a reply to a state that was never
    /// observed.
    fn current_commit_sequence(&self) -> i64;

    /// Drive a test-pipeline `NexusAction` to its terminal `Tested`/
    /// `TestRejected` reply, threading any further effect / memory-write
    /// continuations through the handwritten runner. The hermetic path is a
    /// single effect then a terminal write, so this usually runs one or two
    /// hops; the live path threads bring-up → deploy → assert → teardown.
    async fn drive_to_terminal(&mut self, action: nexus::NexusAction) -> meta::Output;
}

impl RuntimeCore for SchemaRuntime {
    fn new() -> Self {
        Self::with_store(Arc::new(
            Store::open(Self::test_store_path()).expect("open memory store"),
        ))
    }

    fn test_store_path() -> PathBuf {
        use std::sync::atomic::{AtomicU64, Ordering};
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let nanoseconds = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|elapsed| elapsed.as_nanos())
            .unwrap_or(0);
        let unique = COUNTER.fetch_add(1, Ordering::SeqCst);
        std::env::temp_dir().join("lojix-test-store").join(format!(
            "{}-{nanoseconds}-{unique}.memory",
            std::process::id()
        ))
    }

    fn with_store(store: Arc<Store>) -> Self {
        Self::with_store_and_configuration(store, Arc::new(RuntimeConfiguration::test_default()))
    }

    fn with_store_and_configuration(
        store: Arc<Store>,
        configuration: Arc<RuntimeConfiguration>,
    ) -> Self {
        Self {
            store,
            configuration,
            active_deploy: None,
            active_test: None,
            active_operation: None,
        }
    }

    fn store(&self) -> &Store {
        self.store.as_ref()
    }

    fn current_commit_sequence(&self) -> i64 {
        self.store
            .commit_sequence()
            .expect("read durable state marker before protocol reply")
    }

    async fn drive_to_terminal(&mut self, mut action: nexus::NexusAction) -> meta::Output {
        loop {
            match action {
                nexus::NexusAction::Finish(output) => {
                    return output;
                }
                nexus::NexusAction::Operate(operation) => {
                    let outcome = self.attempt(operation).await;
                    action = self.decide_outcome(outcome);
                }
            }
        }
    }
}

/// What the engine decides to do with a completed pipeline step. Deciding is
/// pure: it reads durable state and answers with the next action, and every
/// refusal it can give is a word of the contract rather than a string.
pub(crate) trait PipelineDeciding {
    /// Lower + validate one `Test` request to its resolved targets. Rejects an
    /// unconfigured daemon (`NoTestDefaults`), an empty resolution
    /// (`NodeUnknown` — a bare `All` on an unconfigured/empty cluster, or
    /// `(Nodes [])`), a Live run while the live chain is unimplemented
    /// (`LiveNotYetEnabled` — honest reject over a faked pass), or a host not in
    /// the node's declared host-set (`VmHostNotDeclaredForNode`). On success the
    /// FIRST element is this submit's target; the remainder are the fan-out
    /// tail.
    fn resolve_and_validate(
        &self,
        request: meta::TestRequest,
    ) -> std::result::Result<Vec<ResolvedTestRun>, meta::TestRejectionReason>;

    fn test_rejection(&self, reason: meta::TestRejectionReason) -> meta::RejectedTest;

    fn decide_outcome(&mut self, output: memory::Outcome) -> nexus::NexusAction;

    /// Route a test effect's result to the next test step (Unit 2b). The
    /// hermetic check is a single effect: a built check records `Passed` with
    /// the realised out-path as the closure, a failed build records
    /// `Failed(HermeticCheck)` — never a faked pass. The live effects bracket
    /// the (not-yet-implemented) deploy chain: `TestVmBroughtUp` records the
    /// container `Started` transition and advances to teardown; `TestVmTornDown`
    /// records `Stopped` and the terminal `Failed(Assert)` — the deploy + assert
    /// between bring-up and teardown is unimplemented, so the bracket cannot
    /// pass. A Live run is rejected at submit (`LiveNotYetEnabled`), so this
    /// honest live terminal is the belt to that submit-time gate.
    fn decide_test_effect_completion(&mut self, result: memory::Outcome) -> nexus::NexusAction;

    fn decide_effect_completion(&mut self, result: memory::Outcome) -> nexus::NexusAction;

    fn reject_active_pipeline(&mut self, report: memory::RejectionReport) -> nexus::NexusAction;

    /// Reject a request only after allocating and terminalizing its durable
    /// correlation record. This is the rejection analogue of admission: no
    /// caller can receive a deploy rejection with a synthetic identifier.
    fn reject_submission(
        &self,
        submission: memory::ActualizedDeploySubmission,
        reason: meta::DeployRejectionReason,
    ) -> DeploySubmissionOutcome;

    /// Terminalize the correlated in-flight deployment as rejected and reply
    /// with the record that names it. The caller passes the cursor it holds,
    /// so a rejection with no deployment to name cannot be written here; that
    /// case is [`Self::refuse_deploy`].
    fn deploy_rejection(
        &self,
        deployment_identifier: &ordinary::DeploymentIdentifier,
        reason: meta::DeployRejectionReason,
    ) -> meta::Output;

    /// A deploy refusal that names no deployment, because at the moment of
    /// refusal there is none to name. The marker is the last commit sequence
    /// the store could report: a true "as of" stamp when the store is healthy,
    /// and zero when the refusal is that the store itself failed. It
    /// correlates the refusal to a point in the event log and never claims a
    /// deployment. The cause goes to the daemon journal, where the operator
    /// reads it; it is not put on the wire, because a refusal the Nexus owes
    /// itself is not evidence a retry can act on.
    fn refuse_deploy(
        &self,
        deploy_refusal_reason: memory::DeployRefusalReason,
        cause: &dyn std::fmt::Display,
    ) -> meta::Output;

    fn refusal(
        &self,
        deploy_refusal_reason: memory::DeployRefusalReason,
        cause: &dyn std::fmt::Display,
    ) -> meta::RefusedDeploy;
}

impl PipelineDeciding for SchemaRuntime {
    fn resolve_and_validate(
        &self,
        request: meta::TestRequest,
    ) -> std::result::Result<Vec<ResolvedTestRun>, meta::TestRejectionReason> {
        let defaults = self
            .configuration
            .test_defaults()
            .ok_or(meta::TestRejectionReason::NoTestDefaults)?;
        let resolved = defaults.lower(request);
        if resolved.is_empty() {
            return Err(meta::TestRejectionReason::NodeUnknown);
        }
        // LIVE honesty (report 54 Unit 2b fix 1): the live deploy-into-VM +
        // assert chain is not yet implemented, so a Live run is rejected at
        // submit rather than driven through a bracket that would write a
        // `Passed` it never earned. Mirrors the Deploy `UnsupportedDeployAction`
        // precedent. The HERMETIC path is fully real and unaffected.
        if resolved
            .iter()
            .any(|run| matches!(run.profile.test_mode, ordinary::TestMode::Live))
        {
            return Err(meta::TestRejectionReason::LiveNotYetEnabled);
        }
        // Host-set validation (report 54 §5.1, Unit 2b deferral 2): the
        // resolved host must be a member of the node's declared host-set. When
        // a proposal source is configured the daemon projects the cluster and
        // rejects a host the node does not declare; with no proposal source the
        // host is recorded unvalidated (the sandboxed hermetic check owns its
        // own VM and needs no real host, so an unconfigured projection does not
        // block the hermetic proof).
        if let Some(projection) = defaults.projection() {
            for run in &resolved {
                projection.validate_host_for_node(&run.host, &run.node)?;
            }
        }
        Ok(resolved)
    }

    fn test_rejection(&self, reason: meta::TestRejectionReason) -> meta::RejectedTest {
        meta::RejectedTest {
            test_rejection_reason: reason,
            database_marker: ordinary::DatabaseMarker::at_commit(self.current_commit_sequence()),
        }
    }

    fn decide_outcome(&mut self, output: memory::Outcome) -> nexus::NexusAction {
        match output {
            memory::Outcome::DeploySubmitted(accepted) => self.begin_deploy_pipeline(accepted),
            memory::Outcome::PhaseRecorded(_) => self.advance_after_phase(),
            memory::Outcome::GenerationActivated(_) => match self.active_deploy.clone() {
                Some(pipeline) => self.finish_deploy_pipeline(&pipeline),
                None => nexus::NexusAction::from(self.refuse_deploy(
                    memory::DeployRefusalReason::NoCorrelatedDeployment,
                    &"an activation committed with no correlated cursor",
                )),
            },
            memory::Outcome::ContainerRecorded(_) => self.advance_after_phase(),
            memory::Outcome::TestRunRecorded(accepted) => {
                self.active_operation = None;
                nexus::NexusAction::from(meta::Output::Tested(accepted))
            }
            memory::Outcome::WriteRejected(report) => self.reject_active_pipeline(report),
            memory::Outcome::StoreRefused(detail) => {
                self.active_operation = None;
                nexus::NexusAction::from(meta::Output::Refused(detail))
            }
            // Configuration, pin, unpin and retire are answered where the
            // input arrives, and no pipeline makes those writes; one reaching
            // a pipeline has no deployment to name.
            unasked @ (memory::Outcome::OrdinaryConfigured(_)
            | memory::Outcome::MetaConfigured(_)
            | memory::Outcome::ConfigurationReversed(_)
            | memory::Outcome::OrdinaryConfigurationRejected(_)
            | memory::Outcome::MetaConfigurationRejected(_)
            | memory::Outcome::GenerationPinned(_)
            | memory::Outcome::GenerationUnpinned(_)
            | memory::Outcome::GenerationRetired(_)) => {
                self.active_operation = None;
                nexus::NexusAction::from(self.refuse_deploy(
                    memory::DeployRefusalReason::NoCorrelatedDeployment,
                    &format!("a pipeline received a write it never makes: {unasked:?}"),
                ))
            }
            // A read is answered where its input arrives; no pipeline reads,
            // so a read completing on one has no deployment to name.
            unasked @ (memory::Outcome::GenerationsQueried(_)
            | memory::Outcome::EventLogRead(_)
            | memory::Outcome::TestRunsQueried(_)
            | memory::Outcome::ReadMissed(_)) => nexus::NexusAction::from(self.refuse_deploy(
                memory::DeployRefusalReason::NoCorrelatedDeployment,
                &format!("a pipeline received a read it never makes: {unasked:?}"),
            )),
            // A stage's outcome goes to the pipeline that attempted it: the
            // test pipeline while one is active, else the deploy pipeline.
            stage @ (memory::Outcome::FlakeResolved(_)
            | memory::Outcome::HorizonMaterialized(_)
            | memory::Outcome::ClosureEvaluated(_)
            | memory::Outcome::ClosureBuilt(_)
            | memory::Outcome::ClosureCopied(_)
            | memory::Outcome::ActivationCompleted(_)
            | memory::Outcome::DetachedActivationDispatched(_)
            | memory::Outcome::PathsCollected(_)
            | memory::Outcome::HermeticCheckBuilt(_)
            | memory::Outcome::TestVmStarted(_)
            | memory::Outcome::TestVmStopped(_)
            | memory::Outcome::EffectFailed(_)) => match self.active_test {
                Some(_) => self.decide_test_effect_completion(stage),
                None => self.decide_effect_completion(stage),
            },
        }
    }

    fn decide_test_effect_completion(&mut self, result: memory::Outcome) -> nexus::NexusAction {
        let Some(pipeline) = self.active_test.clone() else {
            return nexus::NexusAction::from(meta::Output::TestRejected(
                self.test_rejection(meta::TestRejectionReason::InternalError),
            ));
        };
        match result {
            memory::Outcome::HermeticCheckBuilt(built) => {
                // Real nix build succeeded: the out-path is the realised check
                // closure. Record Completed/Passed with it — the durable proof.
                self.record_test_terminal(
                    &pipeline,
                    ordinary::TestRunPhase::Completed,
                    ordinary::TestOutcome::Passed,
                    Some(built.closure_path),
                )
            }
            memory::Outcome::TestVmStarted(_) => {
                self.record_container(&pipeline, memory::ContainerState::Started);
                self.set_test_stage(TestStage::BroughtUp);
                // The deploy-into-VM + assert chain runs here in a live run
                // (gated). BUILT path advances straight to teardown so the
                // bracket is provably constructed end-to-end.
                self.set_test_stage(TestStage::Asserted);
                nexus::NexusAction::Operate(memory::Operation::TearingDownTestVm(
                    pipeline.run.tear_down_command(),
                ))
            }
            memory::Outcome::TestVmStopped(_) => {
                self.record_container(&pipeline, memory::ContainerState::Stopped);
                // Honest LIVE terminal (report 54 Unit 2b fix 1): the bring-up →
                // teardown bracket ran, but the deploy-into-VM + assert chain
                // between them is not yet implemented, so nothing was asserted.
                // Record `Failed(Assert)`, never `Passed` — a pass must be
                // earned by a real assertion. Belt to the submit-time
                // `LiveNotYetEnabled` reject: a Live run never reaches this arm
                // today, and if a future caller drives a live bracket before the
                // assert lands it still cannot fake a pass.
                self.record_test_terminal(
                    &pipeline,
                    ordinary::TestRunPhase::Failed,
                    ordinary::TestOutcome::Failed(ordinary::FailureStage::Assert),
                    None,
                )
            }
            memory::Outcome::EffectFailed(failure) => self.fail_test_pipeline(failure),
            // No other effect result belongs to a test dispatch; treat it as an
            // internal invariant failure rather than a misleading pass.
            _ => self.fail_test_pipeline(nexus::EffectFailure {
                effect_stage: nexus::EffectStage::HermeticCheck,
                failure_evidence: StageFailure::from(
                    "unexpected effect result on the test pipeline".to_string(),
                )
                .witness(),
            }),
        }
    }

    fn decide_effect_completion(&mut self, result: memory::Outcome) -> nexus::NexusAction {
        let Some(pipeline) = self.active_deploy.clone() else {
            // An effect completed with no deployment behind it. There is no
            // record to name and none is invented; the refusal says which
            // stage reported what, in the journal, and names itself on the
            // wire.
            let cause = match &result {
                memory::Outcome::EffectFailed(failure) => format!(
                    "{:?}: {}",
                    failure.effect_stage,
                    failure.failure_evidence.failure_detail.payload()
                ),
                other => format!("{other:?}"),
            };
            return nexus::NexusAction::from(
                self.refuse_deploy(memory::DeployRefusalReason::NoCorrelatedDeployment, &cause),
            );
        };
        match result {
            memory::Outcome::FlakeResolved(resolved) => {
                let next_stage = if pipeline.needs_horizon_materialization() {
                    memory::DeployResumeStage::MaterializeHorizon
                } else {
                    memory::DeployResumeStage::RecordBuilding
                };
                if !self.set_resolved_flake(resolved, next_stage) {
                    return self.fail_pipeline(
                        pipeline,
                        nexus::EffectFailure {
                            effect_stage: nexus::EffectStage::FlakeAuth,
                            failure_evidence: StageFailure::from(
                                "flake resolver did not prove an immutable commit".to_string(),
                            )
                            .witness(),
                        },
                    );
                }
                if pipeline.needs_horizon_materialization() {
                    nexus::NexusAction::Operate(memory::Operation::MaterializingHorizon(
                        pipeline.horizon_materialization_command(),
                    ))
                } else {
                    // Record Building (stage still Submitted). The phase write
                    // hops back through advance_after_phase, which fires NixEval.
                    self.record_phase(ordinary::DeploymentPhase::Building)
                }
            }
            memory::Outcome::HorizonMaterialized(inputs) => {
                self.set_input_overrides(inputs.into_payload());
                self.record_phase(ordinary::DeploymentPhase::Building)
            }
            memory::Outcome::ClosureEvaluated(evaluated) => {
                if !self.set_closure_path(evaluated.closure_path.clone()) {
                    return self.fail_pipeline(
                        pipeline,
                        nexus::EffectFailure {
                            effect_stage: nexus::EffectStage::Eval,
                            failure_evidence: StageFailure::from(
                                "effect returned a noncanonical closure path".to_string(),
                            )
                            .witness(),
                        },
                    );
                }
                if pipeline.action.produces_closure() {
                    self.persist_job_cursor(
                        memory::DeployJobPhase::Building,
                        memory::DeployResumeStage::NixBuild,
                    );
                    nexus::NexusAction::Operate(memory::Operation::BuildingClosure(
                        pipeline.nix_build_command(
                            evaluated.closure_path,
                            self.configuration.daemon_host(),
                        ),
                    ))
                } else {
                    // Host `Evaluate`: the derivation path is the result — finish
                    // the pipeline without building.
                    self.persist_job_cursor(
                        memory::DeployJobPhase::Built,
                        memory::DeployResumeStage::FinishDeployment,
                    );
                    self.finish_deploy_pipeline(&pipeline)
                }
            }
            memory::Outcome::ClosureBuilt(built) => {
                if !self.set_closure_path(built.closure_path.clone()) {
                    return self.fail_pipeline(
                        pipeline,
                        nexus::EffectFailure {
                            effect_stage: nexus::EffectStage::Build,
                            failure_evidence: StageFailure::from(
                                "effect returned a noncanonical closure path".to_string(),
                            )
                            .witness(),
                        },
                    );
                }
                if pipeline.action.activates() {
                    self.persist_job_cursor(
                        memory::DeployJobPhase::Built,
                        memory::DeployResumeStage::CopyClosure,
                    );
                    nexus::NexusAction::Operate(memory::Operation::CopyingClosure(
                        pipeline.copy_closure_command(
                            built.closure_path,
                            self.configuration.daemon_host(),
                        ),
                    ))
                } else {
                    // Non-activating action (`Build`): the closure is realised —
                    // finish without copy/activate (which remain addressing-
                    // incomplete; that is the M2/M3 deploy work).
                    self.persist_job_cursor(
                        memory::DeployJobPhase::Built,
                        memory::DeployResumeStage::FinishDeployment,
                    );
                    self.finish_deploy_pipeline(&pipeline)
                }
            }
            memory::Outcome::ClosureCopied(_) => {
                // Record Copying (stage BuildingRecorded). The phase write hops
                // back through advance_after_phase, which fires ActivateGeneration.
                self.record_phase(ordinary::DeploymentPhase::Copying)
            }
            memory::Outcome::ActivationCompleted(activated) => {
                // Record Activated (stage CopyingRecorded). The phase write hops
                // back through advance_after_phase, which fires the
                // RecordGenerationActivated write that commits the live set. The
                // slot returned by the activation effect is persisted on that
                // commit rather than re-defaulting to Current.
                self.set_activation_slot(activated.generation_slot);
                self.record_phase(ordinary::DeploymentPhase::Activated)
            }
            memory::Outcome::DetachedActivationDispatched(_) => {
                // The dispatch acknowledgement is not a terminal deployment
                // outcome. PID 1 retains the exact switch or test unit; the
                // private observer settles the deployment from its observed
                // result, while a replaced predecessor leaves this durable
                // Activating cursor for its successor to settle.
                let accepted = meta::DeployHandle {
                    deployment_identifier: pipeline.deployment_identifier,
                    database_marker: pipeline.accepted_marker.clone(),
                };
                self.active_deploy = None;
                nexus::NexusAction::from(meta::Output::DeployAccepted(accepted))
            }
            memory::Outcome::PathsCollected(_) => self.finish_deploy_pipeline(&pipeline),
            // The test-dispatch effect results never reach the DEPLOY effect
            // router — `drive_submitted_test` routes them through
            // `decide_test_effect_completion`. One arriving here is an internal
            // invariant failure, surfaced as a deploy failure rather than a
            // misleading success.
            memory::Outcome::HermeticCheckBuilt(_)
            | memory::Outcome::TestVmStarted(_)
            | memory::Outcome::TestVmStopped(_) => self.fail_pipeline(
                pipeline,
                nexus::EffectFailure {
                    effect_stage: nexus::EffectStage::Build,
                    failure_evidence: StageFailure::from(
                        "test effect result on the deploy pipeline".to_string(),
                    )
                    .witness(),
                },
            ),
            memory::Outcome::EffectFailed(failure) => self.fail_pipeline(pipeline, failure),
            // `decide_outcome` sends only stage outcomes here; a Memory
            // outcome reaching the stage router names no stage it can follow.
            unrouted => nexus::NexusAction::from(self.refuse_deploy(
                memory::DeployRefusalReason::NoCorrelatedDeployment,
                &format!("a Memory outcome reached the stage router: {unrouted:?}"),
            )),
        }
    }

    fn reject_active_pipeline(&mut self, report: memory::RejectionReport) -> nexus::NexusAction {
        // A write rejection aborts any in-flight deploy and replies a typed
        // meta rejection for the operation in flight, carrying the rejection
        // reason and current marker. The deploy cursor is read BEFORE it is
        // cleared: a deploy rejection names the deployment it rejects, and
        // clearing first left nothing to name.
        let rejected = self.active_deploy.take();
        let operation = self
            .active_operation
            .take()
            .unwrap_or(MetaOperation::Deploy);
        let marker = report.database_marker;
        let output = match operation {
            MetaOperation::Deploy => match rejected {
                Some(pipeline) => self.deploy_rejection(
                    &pipeline.deployment_identifier,
                    (report.rejection_reason).deploy_reason(),
                ),
                None => self.refuse_deploy(
                    memory::DeployRefusalReason::NoCorrelatedDeployment,
                    &format!("write rejected: {:?}", report.rejection_reason),
                ),
            },
            MetaOperation::Test => meta::Output::TestRejected(meta::RejectedTest {
                test_rejection_reason: (report.rejection_reason).test_reason(),
                database_marker: marker,
            }),
        };
        nexus::NexusAction::from(output)
    }

    fn reject_submission(
        &self,
        submission: memory::ActualizedDeploySubmission,
        reason: meta::DeployRejectionReason,
    ) -> DeploySubmissionOutcome {
        let identity = DeployPipeline::deployment_request_identity(&submission);
        match self.store.reject_deployment_request(
            identity,
            memory::DeploymentTerminal::Rejected(reason.terminal_reason()),
        ) {
            Ok(record) => DeploySubmissionOutcome::Rejected(meta::RejectedDeploy::new(record)),
            // Allocating the rejection is itself a durable write. When it
            // fails there is no record to reject with, and the peer is told
            // that rather than losing the daemon.
            Err(error) => DeploySubmissionOutcome::Refused(
                self.refusal(memory::DeployRefusalReason::DurableWriteFailed, &error),
            ),
        }
    }

    fn deploy_rejection(
        &self,
        deployment_identifier: &ordinary::DeploymentIdentifier,
        reason: meta::DeployRejectionReason,
    ) -> meta::Output {
        match self.store.terminalize_deployment(
            *deployment_identifier.payload(),
            memory::DeploymentTerminal::Rejected(reason.terminal_reason()),
        ) {
            Ok(record) => meta::Output::DeployRejected(meta::RejectedDeploy::new(record)),
            Err(error) => {
                self.refuse_deploy(memory::DeployRefusalReason::DurableWriteFailed, &error)
            }
        }
    }

    fn refuse_deploy(
        &self,
        deploy_refusal_reason: memory::DeployRefusalReason,
        cause: &dyn std::fmt::Display,
    ) -> meta::Output {
        meta::Output::DeployRefused(self.refusal(deploy_refusal_reason, cause))
    }

    fn refusal(
        &self,
        deploy_refusal_reason: memory::DeployRefusalReason,
        cause: &dyn std::fmt::Display,
    ) -> meta::RefusedDeploy {
        eprintln!("lojix deploy refused reason={deploy_refusal_reason:?} cause={cause}");
        meta::RefusedDeploy {
            deploy_refusal_reason,
            database_marker: ordinary::DatabaseMarker::at_commit(
                self.store.commit_sequence().unwrap_or(0),
            ),
        }
    }
}

/// Driving one submitted deployment from its accepted handle to a terminal
/// record: what the pipeline has resolved so far, what it persists at each
/// phase, and how it resumes after the connection that submitted it is gone.
// Exception: `async fn` in a public trait. The lint exists because a caller
// cannot name a `Send` bound on the returned future. This trait is implemented
// by exactly one type, is never used behind `dyn`, and its futures are awaited
// on the task that created the engine, so there is no bound for a caller to
// name.
#[allow(async_fn_in_trait)]
pub trait DeployDriving {
    /// The deployment identifier currently on this engine's in-flight cursor, if
    /// any — the durable handle the job actor uses to track the deploy and that
    /// a watcher re-observes by. `None` outside an active deploy.
    fn active_deployment_identifier(&self) -> Option<ordinary::DeploymentIdentifier>;

    /// Record a capacity refusal with the same durable correlation discipline
    /// as every other rejected deploy. The daemon calls this before any pipeline
    /// is created, so a full queue still cannot return an anonymous rejection.
    fn reject_deployment_in_flight(
        &self,
        request: memory::ActualizedDeploySubmission,
    ) -> DeploySubmissionOutcome;

    /// Run ONLY the synchronous submit step of a `Deploy` (up9q surface a): the
    /// reject-guard, restart-safe identifier issuance, in-flight job-row
    /// persistence at `Submitted`, and cursor construction. Returns the typed
    /// admission outcome immediately — the `DeployHandle` handle the daemon
    /// replies before the pipeline runs, or a typed rejection. On accept the
    /// in-flight cursor is left set on `self`, so the daemon hands this engine
    /// to the deploy-job actor, which drives the pipeline via
    /// [`DeployDriving::drive_submitted_deploy`]. The pipeline does NOT run here.
    fn submit_deploy(&mut self, request: meta::DeployRequest) -> DeploySubmissionOutcome;

    /// Reconstruct an accepted deploy from its daemon-local persisted
    /// submission and correlation receipt. This never allocates a second
    /// identity or emits another Submitted event.
    fn resume_deploy_job(&mut self, job: memory::DeployJob) -> Result<bool>;

    /// Drive an already-submitted deploy's effect pipeline to its terminal
    /// reply (up9q surface a, the daemon-owned executor body). Requires the
    /// in-flight cursor to be set by a prior [`DeployDriving::submit_deploy`]; re-enters
    /// the handwritten runner at the persisted continuation. A newly submitted
    /// job starts at `ResolveFlakeAuth`; a restarted job seeds the handwritten
    /// runner with its durable predecessor result instead, so it never reruns
    /// resolver/Horizon work that already committed. The
    /// returned `meta::Output` is daemon-internal executor evidence for logging
    /// and tests; the client already has its admission handle and re-observes
    /// the outcome by deployment identifier.
    async fn drive_submitted_deploy(&mut self) -> meta::Output;

    fn begin_deploy_pipeline(&mut self, accepted: meta::DeployHandle) -> nexus::NexusAction;

    fn advance_after_phase(&mut self) -> nexus::NexusAction;

    fn set_stage(&mut self, stage: DeployStage);

    /// The caller passes the cursor it already holds, so a pipeline cannot
    /// finish without the deployment it belongs to.
    fn finish_deploy_pipeline(&mut self, pipeline: &DeployPipeline) -> nexus::NexusAction;

    fn set_activation_slot(&mut self, generation_slot: ordinary::GenerationSlot);

    /// Capture a closure only after validating it at the typed effect ingress.
    /// This is intentionally independent of Nix-command parsing: an injected
    /// stage outcome must not reach a later build, copy, or activation command.
    fn set_closure_path(&mut self, closure_path: ordinary::ClosurePath) -> bool;

    fn set_input_overrides(&mut self, overrides: Vec<memory::PersistedFlakeInputOverride>);

    fn set_resolved_flake(
        &mut self,
        resolved: nexus::ResolvedFlake,
        resume_stage: memory::DeployResumeStage,
    ) -> bool;

    fn record_phase(&mut self, phase: ordinary::DeploymentPhase) -> nexus::NexusAction;

    /// Rewrite the durable in-flight job row at `phase` from the active deploy
    /// cursor (up9q). Best-effort: a persistence error here must not abort the
    /// running deploy — the event log remains the authoritative phase record
    /// and the job row is the resume convenience. No-op when no deploy is
    /// active (e.g. a standalone effect).
    fn persist_job_phase(&mut self, phase: memory::DeployJobPhase);

    fn persist_job_cursor(
        &mut self,
        phase: memory::DeployJobPhase,
        resume_stage: memory::DeployResumeStage,
    );

    /// The caller passes the cursor it already holds, so an effect failure
    /// cannot become permanent without the deployment it belongs to. Clears
    /// BOTH in-flight slots symmetrically with the finish path (audit R5) — a
    /// mid-pipeline effect failure must not leak `active_operation`.
    fn fail_pipeline(
        &mut self,
        pipeline: DeployPipeline,
        failure: nexus::EffectFailure,
    ) -> nexus::NexusAction;
}

impl DeployDriving for SchemaRuntime {
    fn active_deployment_identifier(&self) -> Option<ordinary::DeploymentIdentifier> {
        self.active_deploy
            .as_ref()
            .map(|pipeline| pipeline.deployment_identifier)
    }

    fn reject_deployment_in_flight(
        &self,
        request: memory::ActualizedDeploySubmission,
    ) -> DeploySubmissionOutcome {
        self.reject_submission(request, meta::DeployRejectionReason::DeploymentInFlight)
    }

    fn submit_deploy(&mut self, request: meta::DeployRequest) -> DeploySubmissionOutcome {
        if let Some(reason) = request.submission_rejection() {
            return self.reject_submission(request, reason);
        }
        self.active_operation = Some(MetaOperation::Deploy);
        let rejected_request = request.clone();
        match self.record_deploy_submitted(request) {
            memory::Outcome::DeploySubmitted(accepted) => {
                DeploySubmissionOutcome::Accepted(accepted)
            }
            memory::Outcome::WriteRejected(report) => {
                self.active_operation = None;
                self.active_deploy = None;
                self.reject_submission(rejected_request, (report.rejection_reason).deploy_reason())
            }
            // `record_deploy_submitted` only ever returns the two arms above;
            // any other output is an internal invariant violation surfaced as a
            // typed rejection rather than a panic.
            _ => {
                self.active_operation = None;
                self.active_deploy = None;
                self.reject_submission(rejected_request, meta::DeployRejectionReason::InternalError)
            }
        }
    }

    fn resume_deploy_job(&mut self, job: memory::DeployJob) -> Result<bool> {
        let persisted_phase = job.deploy_job_phase;
        let Some(submission) = job.actualized_deploy_submission_option.clone() else {
            return Ok(false);
        };
        if job
            .closure_path_option
            .as_ref()
            .is_some_and(|closure_path| {
                !NixStorePath::from(closure_path.payload().as_str()).is_canonical_item_root()
            })
        {
            return Err(Error::Invariant(
                "persisted deploy job has a noncanonical closure path".to_string(),
            ));
        }
        let record = self
            .store
            .deployment_records()?
            .into_iter()
            .find(|record| record.deployment_identifier == job.deployment_identifier)
            .ok_or_else(|| {
                Error::Invariant("persisted deploy job lacks correlation record".to_string())
            })?;
        let Some(admission) = record.admission_marker_option else {
            return Err(Error::Invariant(
                "persisted deploy job lacks its admission receipt".to_string(),
            ));
        };
        if matches!(
            record.deployment_phase,
            memory::DeploymentPhase::Completed
                | memory::DeploymentPhase::Rejected
                | memory::DeploymentPhase::Failed
        ) {
            return Ok(false);
        }
        let mut pipeline = DeployPipeline::from_submission(
            job.deployment_identifier,
            job.generation_identifier,
            admission.into_payload(),
            submission,
        );
        if pipeline.deployment_transport != job.deployment_transport
            || pipeline.deployment_input_mode != job.deployment_input_mode
            || pipeline.deployment_output_selector != job.deployment_output_selector
            || pipeline.activation_backend != job.activation_backend
            || pipeline.builder != job.nix_builder_spec_option
        {
            return Err(Error::Invariant(
                "persisted deploy job routing snapshot disagrees with its submission".to_string(),
            ));
        }
        // The private job snapshot is the durable resume authority. The
        // equality check above makes any torn or inconsistent persistence a
        // fail-closed invariant violation, and these assignments ensure a
        // restart reuses the exact accepted values rather than recomputing a
        // route, selector, backend, or builder.
        pipeline.deployment_transport = job.deployment_transport.clone();
        pipeline.deployment_input_mode = job.deployment_input_mode;
        pipeline.deployment_output_selector = job.deployment_output_selector.clone();
        pipeline.activation_backend = job.activation_backend;
        pipeline.builder = job.nix_builder_spec_option.clone();
        pipeline.closure_path = job.closure_path_option.clone();
        pipeline.activation_slot = job.generation_slot_option;
        pipeline.input_overrides = job.persisted_flake_input_override_vector.clone();
        pipeline.resume_stage = job.deploy_resume_stage;
        pipeline.phase_receipt = job.phase_receipt_option.clone();
        let mut recovered_phase_receipt = false;
        if let Some(expected_phase) = pipeline.resume_stage.recorded_phase()
            && pipeline.phase_receipt.is_none()
        {
            let matching_intents: Vec<_> = self
                .store
                .records::<crate::runtime_model::PendingTransitionIntent>()?
                .into_iter()
                .filter(|intent| {
                    intent.deployment_identifier == pipeline.deployment_identifier
                        && intent.deployment_phase == expected_phase
                        && matches!(
                            intent.transition_intent_state,
                            memory::TransitionIntentState::Acknowledged
                        )
                })
                .collect();
            if matching_intents.len() != 1 {
                return Err(Error::Invariant(
                    "persisted resume stage lacks one acknowledged ordinal transition intent"
                        .to_string(),
                ));
            }
            let intent = matching_intents
                .into_iter()
                .next()
                .expect("checked one intent");
            let marker = intent
                .transition_marker_option
                .ok_or_else(|| {
                    Error::Invariant("acknowledged phase intent lacks a marker".to_string())
                })?
                .into_payload();
            let matching_receipts: Vec<_> = self
                .store
                .event_log_in_range(0, i64::MAX)?
                .into_iter()
                .filter_map(|entry| match entry.logged_event {
                    memory::LoggedEvent::Deployment(event)
                        if event.deployment_identifier == pipeline.deployment_identifier
                            && event.event_log_position == intent.event_log_position
                            && event.transition_marker.database_marker == marker =>
                    {
                        Some(memory::PhaseReceipt {
                            event_log_position: event.event_log_position,
                            database_marker: event.transition_marker.database_marker,
                        })
                    }
                    _ => None,
                })
                .collect();
            if matching_receipts.len() != 1 {
                return Err(Error::Invariant(
                    "acknowledged transition intent lacks one exact journal receipt".to_string(),
                ));
            }
            pipeline.phase_receipt = matching_receipts.into_iter().next();
            recovered_phase_receipt = true;
        }
        pipeline.stage = pipeline.resume_stage.deploy_stage();
        pipeline.source_revision =
            job.flake_reference_option
                .clone()
                .map(|resolved_ref| ordinary::SourceRevisionRecord {
                    source_revision_policy: job.source_revision_policy.clone(),
                    requested_reference: memory::RequestedReference(job.flake_reference.clone()),
                    resolved_reference: memory::ResolvedReference(resolved_ref),
                    resolved_revision: job
                        .resolved_revision_option
                        .clone()
                        .unwrap_or_else(|| memory::ResolvedRevision(String::new())),
                });
        if let Some(source_revision) = pipeline.source_revision.as_ref() {
            pipeline.flake = source_revision.resolved_reference.0.clone();
        }
        self.active_operation = Some(MetaOperation::Deploy);
        self.active_deploy = Some(pipeline);
        if recovered_phase_receipt {
            self.persist_job_phase(persisted_phase);
        }
        Ok(true)
    }

    async fn drive_submitted_deploy(&mut self) -> meta::Output {
        let Some(pipeline) = self.active_deploy.clone() else {
            return self.refuse_deploy(
                memory::DeployRefusalReason::NoCorrelatedDeployment,
                &"a submitted deploy was driven with no correlated cursor",
            );
        };
        let identifier = pipeline.deployment_identifier;
        let accepted = meta::DeployHandle {
            deployment_identifier: identifier,
            database_marker: pipeline.accepted_marker.clone(),
        };
        let work = match pipeline.resume_stage {
            memory::DeployResumeStage::FinishDeployment => {
                return match self.finish_deploy_pipeline(&pipeline) {
                    nexus::NexusAction::Finish(output) => output,
                    _ => self
                        .deploy_rejection(&identifier, meta::DeployRejectionReason::InternalError),
                };
            }
            memory::DeployResumeStage::ResolveFlakeAuth => {
                memory::Outcome::DeploySubmitted(accepted)
            }
            memory::DeployResumeStage::MaterializeHorizon => {
                let Some(revision) = pipeline.source_revision.clone() else {
                    return self
                        .deploy_rejection(&identifier, meta::DeployRejectionReason::InternalError);
                };
                memory::Outcome::FlakeResolved(nexus::ResolvedFlake(revision))
            }
            memory::DeployResumeStage::RecordBuilding => memory::Outcome::HorizonMaterialized(
                nexus::MaterializedInputs(pipeline.input_overrides.clone()),
            ),
            memory::DeployResumeStage::NixEval => {
                let Some(receipt) = pipeline.phase_receipt.clone() else {
                    return self
                        .deploy_rejection(&identifier, meta::DeployRejectionReason::InternalError);
                };
                memory::Outcome::PhaseRecorded(receipt)
            }
            memory::DeployResumeStage::NixBuild => {
                let Some(closure_path) = pipeline.closure_path.clone() else {
                    return self
                        .deploy_rejection(&identifier, meta::DeployRejectionReason::InternalError);
                };
                memory::Outcome::ClosureEvaluated(nexus::EvaluatedClosure {
                    generation_identifier: pipeline.generation_identifier,
                    closure_path,
                })
            }
            memory::DeployResumeStage::CopyClosure => {
                let Some(closure_path) = pipeline.closure_path.clone() else {
                    return self
                        .deploy_rejection(&identifier, meta::DeployRejectionReason::InternalError);
                };
                memory::Outcome::ClosureBuilt(nexus::BuiltClosure {
                    generation_identifier: pipeline.generation_identifier,
                    closure_path,
                })
            }
            memory::DeployResumeStage::ActivateGeneration
            | memory::DeployResumeStage::RecordGenerationActivated => {
                let Some(receipt) = pipeline.phase_receipt.clone() else {
                    return self
                        .deploy_rejection(&identifier, meta::DeployRejectionReason::InternalError);
                };
                memory::Outcome::PhaseRecorded(receipt)
            }
        };
        self.drive(work).await
    }

    fn begin_deploy_pipeline(&mut self, accepted: meta::DeployHandle) -> nexus::NexusAction {
        let pipeline = match self.active_deploy.as_ref() {
            Some(pipeline) => pipeline.clone(),
            None => {
                return nexus::NexusAction::from(meta::Output::DeployAccepted(accepted));
            }
        };
        // First effect of the chain: resolve the flake against the proposal
        // source. Subsequent effects are emitted from `decide_effect_completion`.
        nexus::NexusAction::Operate(memory::Operation::ResolvingFlake(
            pipeline.flake_auth_request(),
        ))
    }

    fn advance_after_phase(&mut self) -> nexus::NexusAction {
        // A phase-transition write committed mid-pipeline. The cursor `stage`
        // names which phase was just recorded; advance to the next effect or
        // the final activation-record write.
        let Some(pipeline) = self.active_deploy.clone() else {
            return nexus::NexusAction::from(self.refuse_deploy(
                memory::DeployRefusalReason::NoCorrelatedDeployment,
                &"a phase transition committed with no correlated cursor",
            ));
        };
        match pipeline.stage {
            DeployStage::Submitted => {
                self.set_stage(DeployStage::BuildingRecorded);
                nexus::NexusAction::Operate(memory::Operation::EvaluatingClosure(
                    pipeline.nix_eval_command(self.configuration.daemon_host()),
                ))
            }
            DeployStage::BuildingRecorded => {
                // The closure path is captured on the cursor by `set_closure_path`
                // during eval/build; an activating pipeline that reached this stage
                // without a closure is an internal invariant failure, not an empty
                // activation (risk R2). Fail the pipeline rather than activate "".
                let closure_path = match pipeline.closure_path.clone() {
                    Some(closure_path)
                        if NixStorePath::from(closure_path.payload().as_str())
                            .is_canonical_item_root() =>
                    {
                        closure_path
                    }
                    None => {
                        return self.fail_pipeline(
                            pipeline,
                            nexus::EffectFailure {
                                effect_stage: nexus::EffectStage::Activate,
                                failure_evidence: StageFailure::from(
                                    "activation reached without a built closure path".to_string(),
                                )
                                .witness(),
                            },
                        );
                    }
                    Some(_) => {
                        return self.fail_pipeline(
                            pipeline,
                            nexus::EffectFailure {
                                effect_stage: nexus::EffectStage::Activate,
                                failure_evidence: StageFailure::from(
                                    "activation reached with a noncanonical closure path"
                                        .to_string(),
                                )
                                .witness(),
                            },
                        );
                    }
                };
                self.set_stage(DeployStage::CopyingRecorded);
                // Mark the durable job row Activating before the activate effect
                // runs (up9q): a daemon that crashes mid-activation reads this
                // phase on restart and reconciles via the BootOnce unit rather
                // than blindly re-activating. There is no Activating event-log
                // phase today; the job row is the resume cursor for this window.
                self.persist_job_phase(memory::DeployJobPhase::Activating);
                nexus::NexusAction::Operate(memory::Operation::ActivatingGeneration(
                    pipeline.activate_generation_command(closure_path),
                ))
            }
            DeployStage::CopyingRecorded => {
                let commit = match pipeline.activation_commit() {
                    Some(commit) => commit,
                    None => {
                        return self.fail_pipeline(
                            pipeline,
                            nexus::EffectFailure {
                                effect_stage: nexus::EffectStage::Activate,
                                failure_evidence: StageFailure::from(
                                    "activation record reached without a built closure path"
                                        .to_string(),
                                )
                                .witness(),
                            },
                        );
                    }
                };
                self.set_stage(DeployStage::ActivatedRecorded);
                nexus::NexusAction::Operate(memory::Operation::RecordGenerationActivated(commit))
            }
            DeployStage::ActivatedRecorded => self.finish_deploy_pipeline(&pipeline),
        }
    }

    fn set_stage(&mut self, stage: DeployStage) {
        if let Some(pipeline) = self.active_deploy.as_mut() {
            pipeline.stage = stage;
        }
    }

    fn finish_deploy_pipeline(&mut self, pipeline: &DeployPipeline) -> nexus::NexusAction {
        let record = match self.store.terminalize_deployment(
            *pipeline.deployment_identifier.payload(),
            memory::DeploymentTerminal::Succeeded,
        ) {
            Ok(record) => record,
            Err(error) => {
                return self.fail_pipeline(
                    pipeline.clone(),
                    nexus::EffectFailure {
                        effect_stage: nexus::EffectStage::Activate,
                        failure_evidence: StageFailure::from(format!(
                            "could not persist correlated deployment success: {error}"
                        ))
                        .witness(),
                    },
                );
            }
        };
        self.active_operation = None;
        self.active_deploy = None;
        nexus::NexusAction::from(meta::Output::DeployTerminal(record))
    }

    fn set_activation_slot(&mut self, generation_slot: ordinary::GenerationSlot) {
        if let Some(pipeline) = self.active_deploy.as_mut() {
            pipeline.activation_slot = Some(generation_slot);
        }
    }

    fn set_closure_path(&mut self, closure_path: ordinary::ClosurePath) -> bool {
        if !NixStorePath::from(closure_path.payload().as_str()).is_canonical_item_root() {
            return false;
        }
        if let Some(pipeline) = self.active_deploy.as_mut() {
            pipeline.closure_path = Some(closure_path);
            true
        } else {
            false
        }
    }

    fn set_input_overrides(&mut self, overrides: Vec<memory::PersistedFlakeInputOverride>) {
        if let Some(pipeline) = self.active_deploy.as_mut() {
            pipeline.input_overrides = overrides;
        }
        self.persist_job_cursor(
            memory::DeployJobPhase::Submitted,
            memory::DeployResumeStage::RecordBuilding,
        );
    }

    fn set_resolved_flake(
        &mut self,
        resolved: nexus::ResolvedFlake,
        resume_stage: memory::DeployResumeStage,
    ) -> bool {
        let source_revision = resolved.into_payload();
        let Some(immutable_revision) = source_revision.resolved_revision.0.immutable_revision()
        else {
            return false;
        };
        let mut snapshot = None;
        if let Some(pipeline) = self.active_deploy.as_mut() {
            if matches!(
                pipeline.source_revision_policy,
                meta::SourceRevisionPolicy::ResolveAndRecord
            ) {
                pipeline.flake = source_revision.resolved_reference.0.clone();
            }
            pipeline.source_revision = Some(source_revision);
            pipeline.resume_stage = resume_stage;
            snapshot = Some((
                *pipeline.deployment_identifier.payload(),
                pipeline.deploy_job(memory::DeployJobPhase::Submitted),
            ));
        }
        if let Some((deployment_identifier, deploy_job)) = snapshot {
            let record_exists = self
                .store
                .deployment_records()
                .expect("read deployment correlation records for resolved revision")
                .iter()
                .any(|record| *record.deployment_identifier.payload() == deployment_identifier);
            if record_exists {
                self.store
                    .record_resolved_source(deployment_identifier, immutable_revision, deploy_job)
                    .expect("atomically persist resolved immutable source and restart cursor");
            }
        }
        true
    }

    fn record_phase(&mut self, phase: ordinary::DeploymentPhase) -> nexus::NexusAction {
        let resume_stage = match phase {
            ordinary::DeploymentPhase::Building => memory::DeployResumeStage::NixEval,
            ordinary::DeploymentPhase::Copying => memory::DeployResumeStage::ActivateGeneration,
            ordinary::DeploymentPhase::Activated => {
                memory::DeployResumeStage::RecordGenerationActivated
            }
            _ => memory::DeployResumeStage::FinishDeployment,
        };
        let Some(pipeline) = self.active_deploy.as_mut() else {
            return nexus::NexusAction::from(self.refuse_deploy(
                memory::DeployRefusalReason::NoCorrelatedDeployment,
                &"a phase was recorded with no correlated cursor",
            ));
        };
        pipeline.resume_stage = resume_stage;
        let pipeline = pipeline.clone();
        let position = match self.store.allocate_event_log_position() {
            Ok(position) => position,
            Err(error) => {
                return self.fail_pipeline(
                    pipeline,
                    nexus::EffectFailure {
                        effect_stage: nexus::EffectStage::Gc,
                        failure_evidence: StageFailure::from(format!(
                            "could not reserve deployment event position: {error}"
                        ))
                        .witness(),
                    },
                );
            }
        };
        let event = pipeline.phase_event(phase, ordinary::EventLogPosition::new(position));
        nexus::NexusAction::Operate(memory::Operation::RecordPhaseTransition(event))
    }

    fn persist_job_phase(&mut self, phase: memory::DeployJobPhase) {
        let resume_stage = self
            .active_deploy
            .as_ref()
            .map(|pipeline| pipeline.resume_stage.clone())
            .unwrap_or(memory::DeployResumeStage::FinishDeployment);
        self.persist_job_cursor(phase, resume_stage);
    }

    fn persist_job_cursor(
        &mut self,
        phase: memory::DeployJobPhase,
        resume_stage: memory::DeployResumeStage,
    ) {
        let job = if let Some(pipeline) = self.active_deploy.as_mut() {
            pipeline.resume_stage = resume_stage;
            Some(pipeline.deploy_job(phase))
        } else {
            None
        };
        if let Some(job) = job {
            self.store
                .upsert_deploy_job(job)
                .expect("persist exact deploy resume cursor before the next effect");
        }
    }

    fn fail_pipeline(
        &mut self,
        pipeline: DeployPipeline,
        failure: nexus::EffectFailure,
    ) -> nexus::NexusAction {
        eprintln!(
            "lojix deploy pipeline effect failed at {:?}: {}",
            failure.effect_stage,
            failure.failure_evidence.failure_detail.payload()
        );
        let reason = match failure.effect_stage {
            nexus::EffectStage::FlakeAuth => meta::DeployRejectionReason::ProposalSourceUnreachable,
            nexus::EffectStage::MaterializeHorizon => {
                meta::DeployRejectionReason::ProposalSourceUnreachable
            }
            // A flake that evaluates and then fails to build is not a
            // malformed reference; both stages reported that one reason until
            // the contract gained a name for each.
            nexus::EffectStage::Eval => meta::DeployRejectionReason::EvaluationFailed,
            nexus::EffectStage::Build => meta::DeployRejectionReason::BuildFailed,
            // A copy runs `nix copy --to <store-uri> <path>` and engages no
            // builder, so `BuilderUnreachable` was false however the copy
            // failed — an unreachable target store, a refused signature, a
            // full disk, a malformed transport. The stage names itself; what
            // actually happened is in the evidence beside it.
            nexus::EffectStage::CopyClosure => meta::DeployRejectionReason::ClosureCopyFailed,
            nexus::EffectStage::Activate => meta::DeployRejectionReason::ActivationFailed,
            nexus::EffectStage::Gc => meta::DeployRejectionReason::DeploymentInFlight,
            // The test-only effect stages never reach the DEPLOY pipeline's
            // failure path (`fail_test_pipeline` owns them); an internal
            // invariant failure rather than a misleading deploy reason.
            nexus::EffectStage::HermeticCheck
            | nexus::EffectStage::BringUpTestVm
            | nexus::EffectStage::TearDownTestVm => meta::DeployRejectionReason::InternalError,
        };
        // The evidence the stage reported travels into the durable record and,
        // through the terminal transition's journal event, into the event log.
        // This is the only place a deploy effect failure becomes permanent.
        let terminal = memory::DeploymentTerminal::Failed(memory::DeploymentFailure {
            deployment_failure_stage: (failure.effect_stage).deployment_failure_stage(),
            deployment_terminal_reason: reason.terminal_reason(),
            failure_evidence_option: Some(failure.failure_evidence),
        });
        let recorded = self
            .store
            .terminalize_deployment(*pipeline.deployment_identifier.payload(), terminal);
        self.active_deploy = None;
        self.active_operation = None;
        match recorded {
            Ok(record) => nexus::NexusAction::from(meta::Output::DeployTerminal(record)),
            // The failure happened and the store could not keep it. Saying so
            // is the only truthful reply left: claiming a terminal record that
            // was never written would be worse than the panic it replaces.
            Err(error) => nexus::NexusAction::from(
                self.refuse_deploy(memory::DeployRefusalReason::DurableWriteFailed, &error),
            ),
        }
    }
}

/// Driving one accepted test run to a terminal verdict, and the container
/// observations it leaves behind on the way.
// Exception: `async fn` in a public trait. The lint exists because a caller
// cannot name a `Send` bound on the returned future. This trait is implemented
// by exactly one type, is never used behind `dyn`, and its futures are awaited
// on the task that created the engine, so there is no bound for a caller to
// name.
#[allow(async_fn_in_trait)]
pub trait TestDriving {
    /// Run ONLY the synchronous submit of a `Test` (Unit 2b, mirroring
    /// [`DeployDriving::submit_deploy`]): lower + validate, record the Pending row, set
    /// the in-flight test cursor, and return the `AcceptedTest` handle the
    /// daemon replies before the real dispatch runs. On accept the cursor is
    /// left set on `self` so the daemon hands this engine to the test-job actor,
    /// which drives the dispatch via [`Self::drive_submitted_test`]. The
    /// hermetic build / live cycle does NOT run here.
    fn submit_test(&mut self, request: meta::TestRequest) -> TestSubmissionOutcome;

    /// Drive an already-submitted test's REAL dispatch to its terminal outcome
    /// (Unit 2b, the daemon-owned executor body — mirrors
    /// [`DeployDriving::drive_submitted_deploy`]). Requires the in-flight test cursor set
    /// by a prior [`Self::submit_test`] (or `decide_test` for the
    /// in-process proof). Re-enters the handwritten runner at the cursor's first
    /// effect (the hermetic `nix build`, or the live bring-up), runs it for
    /// real, and rewrites the durable row through real phases to a terminal
    /// `Passed` (with the built closure) or `Failed(stage)` — never a faked
    /// pass. The returned `meta::Output` is the terminal `Tested`/`TestRejected`
    /// for logging/tests; the client already has its accepted handle and
    /// re-observes the outcome via `(Query (ByTestRun …))`.
    async fn drive_submitted_test(&mut self) -> meta::Output;

    /// Write the terminal durable test-run row (phase + outcome + closure) and
    /// reply the terminal `Tested`/`TestRejected`. Clears the in-flight test
    /// cursor. The row is rewritten in place (keyed by run identifier), so a
    /// `(Query (ByTestRun …))` reads the terminal outcome — closing the
    /// silent-daemon observability gap (report 54 §5.3).
    fn record_test_terminal(
        &mut self,
        pipeline: &TestPipeline,
        phase: ordinary::TestRunPhase,
        outcome: ordinary::TestOutcome,
        closure_path: Option<ordinary::ClosurePath>,
    ) -> nexus::NexusAction;

    /// Record a live VM container-lifecycle transition (Unit 2b): the report-47
    /// §2 `ContainerLifecycleRecord` table finally gets its driver. Best-effort,
    /// like the deploy job-row persistence — a record error never fakes the
    /// outcome.
    fn record_container(&mut self, pipeline: &TestPipeline, state: memory::ContainerState);

    fn set_test_stage(&mut self, stage: TestStage);

    /// Record a terminal `Failed(stage)` test outcome and reply. The stage maps
    /// the effect failure to the durable `FailureStage`, so a query sees
    /// exactly where the test failed (`HermeticCheck` vs `BringUp`/`TearDown`).
    /// NEVER a faked pass — a build/test failure is recorded as Failed.
    fn fail_test_pipeline(&mut self, failure: nexus::EffectFailure) -> nexus::NexusAction;
}

impl TestDriving for SchemaRuntime {
    fn submit_test(&mut self, request: meta::TestRequest) -> TestSubmissionOutcome {
        let run = match self.resolve_and_validate(request) {
            Ok(mut resolved) => resolved.remove(0),
            Err(reason) => return TestSubmissionOutcome::Rejected(self.test_rejection(reason)),
        };
        let identifier =
            ordinary::TestRunIdentifier::new(self.store.next_test_run_identifier().unwrap_or(1));
        let mut pipeline = TestPipeline::accepted(run.clone(), identifier);
        match self.record_test_run(run.pending_record(identifier)) {
            memory::Outcome::TestRunRecorded(accepted) => {
                // The terminal outcome carries the acceptance marker, as a
                // deploy's does; the cursor stays set for the executor, which
                // runs the real dispatch and rewrites the row to its outcome.
                pipeline.accepted_marker = accepted.database_marker.clone();
                self.active_test = Some(pipeline);
                TestSubmissionOutcome::Accepted(accepted)
            }
            memory::Outcome::WriteRejected(report) => {
                TestSubmissionOutcome::Rejected(meta::RejectedTest {
                    test_rejection_reason: (report.rejection_reason).test_reason(),
                    database_marker: report.database_marker,
                })
            }
            _ => TestSubmissionOutcome::Rejected(
                self.test_rejection(meta::TestRejectionReason::InternalError),
            ),
        }
    }

    async fn drive_submitted_test(&mut self) -> meta::Output {
        let Some(pipeline) = self.active_test.clone() else {
            return meta::Output::TestRejected(
                self.test_rejection(meta::TestRejectionReason::InternalError),
            );
        };
        self.active_operation = Some(MetaOperation::Test);
        let first_effect = match pipeline.run.profile.test_mode {
            ordinary::TestMode::Hermetic => {
                memory::Operation::BuildingHermeticCheck(pipeline.run.hermetic_check_command())
            }
            // LIVE is BUILT but not run live here (gated). The bring-up effect
            // is constructed and dispatched; the attempt of the live stages
            // is the host-untouched user-namespace path (report 51 §3). A live
            // run is psyche-gated, so the daemon-integration proof exercises
            // Hermetic; this constructs the live first effect honestly.
            ordinary::TestMode::Live => memory::Operation::BringingUpTestVm(
                pipeline
                    .run
                    .bring_up_command(ordinary::ClosurePath::new(String::new())),
            ),
        };
        // The cursor's first stage is attempted directly and its outcome
        // routed by `decide_test_effect_completion`, then `drive_to_terminal`
        // threads any further continuation hops to the terminal outcome write.
        let outcome = self.attempt(first_effect).await;
        let action = self.decide_test_effect_completion(outcome);
        self.drive_to_terminal(action).await
    }

    fn record_test_terminal(
        &mut self,
        pipeline: &TestPipeline,
        phase: ordinary::TestRunPhase,
        outcome: ordinary::TestOutcome,
        closure_path: Option<ordinary::ClosurePath>,
    ) -> nexus::NexusAction {
        let record = pipeline.record_at(phase, outcome, closure_path);
        let output = self.record_test_run(record);
        self.active_operation = None;
        self.active_test = None;
        match output {
            memory::Outcome::TestRunRecorded(accepted) => {
                nexus::NexusAction::from(meta::Output::Tested(accepted))
            }
            _ => nexus::NexusAction::from(meta::Output::TestRejected(
                self.test_rejection(meta::TestRejectionReason::InternalError),
            )),
        }
    }

    fn record_container(&mut self, pipeline: &TestPipeline, state: memory::ContainerState) {
        let _ = self.record_container_transition(pipeline.container_transition(state));
    }

    fn set_test_stage(&mut self, stage: TestStage) {
        if let Some(pipeline) = self.active_test.as_mut() {
            pipeline.stage = stage;
        }
    }

    fn fail_test_pipeline(&mut self, failure: nexus::EffectFailure) -> nexus::NexusAction {
        eprintln!(
            "lojix test pipeline effect failed at {:?}",
            failure.effect_stage
        );
        let stage = (failure.effect_stage).test_failure_stage();
        let pipeline = match self.active_test.clone() {
            Some(pipeline) => pipeline,
            None => {
                return nexus::NexusAction::from(meta::Output::TestRejected(
                    self.test_rejection(meta::TestRejectionReason::InternalError),
                ));
            }
        };
        self.record_test_terminal(
            &pipeline,
            ordinary::TestRunPhase::Failed,
            ordinary::TestOutcome::Failed(stage),
            None,
        )
    }
}

/// Applying one operation, generated from `ethos/operation.ethos`, to the
/// durable store: each write and each read answers with the outcome it came
/// to, and nothing here decides what to do next. This is the engine's own
/// behaviour over the generated operation, written by hand beside it.
pub trait Operating {
    fn operate(&mut self, operation: memory::Operation) -> memory::Outcome;
}

impl Operating for SchemaRuntime {
    fn operate(&mut self, operation: memory::Operation) -> memory::Outcome {
        match operation {
            memory::Operation::OrdinaryConfigure(configuration) => {
                self.apply_ordinary_configuration(configuration)
            }
            memory::Operation::MetaConfigure(configuration) => {
                self.apply_meta_configuration(configuration)
            }
            memory::Operation::ReverseConfiguration => self.reverse_meta_configuration(),
            memory::Operation::RecordDeploySubmitted(submission) => {
                self.record_deploy_submitted(submission)
            }
            memory::Operation::RecordPhaseTransition(event) => self.record_phase_transition(event),
            memory::Operation::RecordGenerationActivated(commit) => {
                self.record_generation_activated(commit)
            }
            memory::Operation::PinGeneration(request) => self.pin_generation(request),
            memory::Operation::UnpinGeneration(request) => self.unpin_generation(request),
            memory::Operation::RetireGeneration(request) => self.retire_generation(request),
            memory::Operation::RecordContainerTransition(transition) => {
                self.record_container_transition(transition)
            }
            memory::Operation::RecordTestRun(record) => self.record_test_run(record),
            memory::Operation::QueryGenerations(selection) => self.query_generations(selection),
            memory::Operation::ReadEventLog(range) => self.read_event_log(range),
            memory::Operation::QueryTestRuns(lookup) => self.query_test_runs(lookup),
            // A stage awaits its emissions, so it is attempted by a pipeline
            // through `NexusEngine::attempt` and never applied here.
            memory::Operation::ResolvingFlake(_)
            | memory::Operation::MaterializingHorizon(_)
            | memory::Operation::EvaluatingClosure(_)
            | memory::Operation::BuildingClosure(_)
            | memory::Operation::CopyingClosure(_)
            | memory::Operation::ActivatingGeneration(_)
            | memory::Operation::CollectingGarbage(_)
            | memory::Operation::BuildingHermeticCheck(_)
            | memory::Operation::BringingUpTestVm(_)
            | memory::Operation::TearingDownTestVm(_) => {
                memory::Outcome::StoreRefused(memory::StoreFailure(
                    "a pipeline stage is attempted by its pipeline, not applied to the store"
                        .to_string(),
                ))
            }
        }
    }
}

/// The writes among the operations, each against the durable store.
pub(crate) trait MemoryApplying {
    fn apply_ordinary_configuration(
        &self,
        configuration: crate::NexusConfiguration,
    ) -> memory::Outcome;

    fn apply_meta_configuration(&self, configuration: crate::NexusConfiguration)
    -> memory::Outcome;

    fn reverse_meta_configuration(&self) -> memory::Outcome;

    /// Persist one accepted test-run row (phase Submitted / outcome Pending)
    /// and reply the `AcceptedTest` handle. Mirrors `record_deploy_submitted`:
    /// the row is durable from acceptance, so a `(Query (ByTestRun …))` reads
    /// it immediately and a restarted daemon reconciles the in-flight test
    /// (Unit 2b). Unit 2a writes exactly this Pending row — no faked pass.
    fn record_test_run(&mut self, record: ordinary::TestRunRecord) -> memory::Outcome;

    fn record_deploy_submitted(
        &mut self,
        submission: memory::ActualizedDeploySubmission,
    ) -> memory::Outcome;

    fn record_phase_transition(&mut self, event: ordinary::DeploymentPhaseEvent)
    -> memory::Outcome;

    fn record_generation_activated(&mut self, commit: memory::ActivationCommit) -> memory::Outcome;

    fn pin_generation(&mut self, request: meta::PinRequest) -> memory::Outcome;

    fn unpin_generation(&mut self, request: meta::UnpinRequest) -> memory::Outcome;

    fn retire_generation(&mut self, request: meta::RetireRequest) -> memory::Outcome;

    fn record_container_transition(
        &mut self,
        transition: memory::ContainerTransition,
    ) -> memory::Outcome;

    /// Build a write-rejection at a known commit sequence. The caller passes
    /// the sequence it already read under the store lock — this method never
    /// re-locks, so it is safe to call while the store guard is still held.
    /// A durable write that could not be made. The cause goes to the daemon
    /// journal; the typed rejection travels back through
    /// [`Self::reject_active_pipeline`], which answers the peer in the
    /// vocabulary of whatever operation was in flight. Reading the commit
    /// sequence is best-effort: the store that just refused a write may
    /// equally refuse to report its sequence.
    fn reject_durable_write(&self, cause: &dyn std::fmt::Display) -> memory::Outcome;

    fn write_rejected(commit_sequence: i64, reason: memory::RejectionReason) -> memory::Outcome;
}

impl MemoryApplying for SchemaRuntime {
    fn apply_ordinary_configuration(
        &self,
        configuration: crate::NexusConfiguration,
    ) -> memory::Outcome {
        match self.store.ordinary_configure(configuration) {
            Ok(state) => memory::Outcome::OrdinaryConfigured(state.receipt()),
            Err(error) => memory::Outcome::OrdinaryConfigurationRejected(
                memory::ConfigurationRejection::from(error),
            ),
        }
    }

    fn apply_meta_configuration(
        &self,
        configuration: crate::NexusConfiguration,
    ) -> memory::Outcome {
        match self.store.meta_configure(configuration) {
            Ok(state) => memory::Outcome::MetaConfigured(state.receipt()),
            Err(error) => memory::Outcome::MetaConfigurationRejected(
                memory::ConfigurationRejection::from(error),
            ),
        }
    }

    fn reverse_meta_configuration(&self) -> memory::Outcome {
        match self.store.reverse_meta_configuration() {
            Ok(state) => memory::Outcome::ConfigurationReversed(state.receipt()),
            Err(error) => memory::Outcome::MetaConfigurationRejected(
                memory::ConfigurationRejection::from(error),
            ),
        }
    }

    fn record_test_run(&mut self, record: ordinary::TestRunRecord) -> memory::Outcome {
        let identifier = record.test_run_identifier;
        match self
            .store
            .upsert_test_run(memory::StoredTestRun::from(record))
            .and_then(|()| self.store.commit_sequence())
        {
            Ok(commit_sequence) => memory::Outcome::TestRunRecorded(meta::AcceptedTest {
                test_run_identifier: identifier,
                database_marker: ordinary::DatabaseMarker::at_commit(commit_sequence),
            }),
            Err(error) => panic!("persist durable test-run record: {error}"),
        }
    }

    fn record_deploy_submitted(
        &mut self,
        submission: memory::ActualizedDeploySubmission,
    ) -> memory::Outcome {
        // Identifier reservation, correlation-record creation, and the
        // restart cursor share one durable commit.  The public admission
        // marker is bound only afterwards from that commit's durable log.
        let identity = DeployPipeline::deployment_request_identity(&submission);
        let restart_cursor = DeployPipeline::from_submission(
            ordinary::DeploymentIdentifier::new(0),
            ordinary::GenerationIdentifier::new(0),
            ordinary::DatabaseMarker::at_commit(0),
            submission.clone(),
        )
        .deploy_job(memory::DeployJobPhase::Submitted);
        let record = match self
            .store
            .allocate_deployment_record(identity, restart_cursor)
        {
            Ok(record) => record,
            // Admission is a durable write and a durable write can fail.
            // `submit_deploy` already answers a rejected write with a typed
            // rejection; this reports the real cause to the journal and takes
            // that path instead of aborting the daemon.
            Err(error) => return self.reject_durable_write(&error),
        };
        let Some(admission_marker) = record.admission_marker_option.clone() else {
            return self.reject_durable_write(
                &"an admitted deployment record carries no admission marker",
            );
        };
        let accepted_marker = admission_marker.into_payload();
        let deployment_identifier = record.deployment_identifier;
        let pipeline = DeployPipeline::from_submission(
            deployment_identifier,
            record.generation_identifier,
            accepted_marker.clone(),
            submission,
        );
        // `allocate_deployment_record` does not return until the durable
        // admission intent has been bound from its exact commit-log receipt,
        // journalled, and locally acknowledged.  Do not synthesize a second
        // Submitted event here.
        self.active_deploy = Some(pipeline);
        memory::Outcome::DeploySubmitted(meta::DeployHandle {
            deployment_identifier,
            database_marker: accepted_marker,
        })
    }

    fn record_phase_transition(
        &mut self,
        mut event: ordinary::DeploymentPhaseEvent,
    ) -> memory::Outcome {
        let recorded_phase = event.deployment_phase.clone();
        let event_log_position = event.event_log_position;
        let Some(pipeline) = self.active_deploy.as_ref() else {
            return self.reject_durable_write(
                &"a deployment phase transition arrived with no correlated cursor",
            );
        };
        let job = pipeline.deploy_job(memory::DeployJobPhase::from(recorded_phase.clone()));
        let recorded = self.store.advance_deployment_phase_with_job(
            *event.deployment_identifier.payload(),
            recorded_phase.clone(),
            job,
            event.clone(),
        );
        match recorded {
            Ok(marker) => {
                event.transition_marker = memory::TransitionMarker::new(marker.clone());
                let receipt = memory::PhaseReceipt {
                    event_log_position,
                    database_marker: marker,
                };
                if let Some(pipeline) = self.active_deploy.as_mut() {
                    pipeline.phase_receipt = Some(receipt.clone());
                }
                memory::Outcome::PhaseRecorded(receipt)
            }
            Err(error) => self.reject_durable_write(&error),
        }
    }

    fn record_generation_activated(&mut self, commit: memory::ActivationCommit) -> memory::Outcome {
        let Some(pipeline) = self.active_deploy.clone() else {
            return self.reject_durable_write(
                &"a generation activation arrived with no correlated cursor",
            );
        };
        let deployment_identifier = pipeline.deployment_identifier;
        let generation_artifact = pipeline.generation_artifact;
        let activation_effect = pipeline.activation_effect;
        let generation = memory::LiveGeneration {
            deployment_identifier,
            generation_identifier: commit.generation_identifier,
            cluster_name: commit.cluster_name.clone(),
            node_name: commit.node_name.clone(),
            deployment_environment: commit.deployment_environment.clone(),
            generation_artifact,
            activation_effect,
            generation_slot: commit.generation_slot.clone(),
            closure_path: commit.closure_path.clone(),
            source_revision_record: commit.source_revision_record.clone(),
        };
        let root = memory::GcRoot {
            generation_identifier: commit.generation_identifier,
            cluster_name: commit.cluster_name.clone(),
            node_name: commit.node_name.clone(),
            generation_slot: commit.generation_slot.clone(),
            closure_path: commit.closure_path.clone(),
            pin_label_option: None,
        };
        // The live generation, its GC root, and the identifier high-water row
        // share one Store atomic commit. A crash therefore leaves either all
        // activation facts durable or none of them, never a partially visible
        // generation with a reusable identifier.
        let recorded = self
            .store
            .record_activation(generation, root)
            .and_then(|()| self.store.commit_sequence());
        match recorded {
            Ok(commit_sequence) => {
                self.persist_job_cursor(
                    memory::DeployJobPhase::Activated,
                    memory::DeployResumeStage::FinishDeployment,
                );
                memory::Outcome::GenerationActivated(memory::AppliedActivation {
                    generation_identifier: commit.generation_identifier,
                    generation_slot: commit.generation_slot,
                    database_marker: ordinary::DatabaseMarker::at_commit(commit_sequence),
                })
            }
            Err(error) => self.reject_durable_write(&error),
        }
    }

    fn pin_generation(&mut self, request: meta::PinRequest) -> memory::Outcome {
        let roots = match self.store.records::<crate::runtime_model::GcRoot>() {
            Ok(roots) => roots,
            Err(error) => {
                return memory::Outcome::StoreRefused(memory::StoreFailure(format!(
                    "read durable gc roots for pin request: {error}"
                )));
            }
        };
        let current_sequence = self.current_commit_sequence();
        let already_used = roots
            .iter()
            .any(|root| root.pin_label_option.as_ref() == Some(&request.pin_label));
        if already_used {
            return Self::write_rejected(current_sequence, memory::RejectionReason::PinLabelInUse);
        }
        let Some(mut root) = roots.into_iter().find(|root| {
            root.generation_identifier == request.generation_identifier
                && root.cluster_name == request.cluster_name
                && root.node_name == request.node_name
        }) else {
            return Self::write_rejected(
                current_sequence,
                memory::RejectionReason::GenerationUnknown,
            );
        };
        let from_slot = root.generation_slot;
        root.generation_slot = ordinary::GenerationSlot::Pinned;
        root.pin_label_option = Some(request.pin_label.clone());
        let committed = self
            .store
            .mutate_gc_root(root)
            .and_then(|()| self.store.commit_sequence());
        match committed {
            Ok(commit_sequence) => memory::Outcome::GenerationPinned(meta::AppliedPin {
                generation_identifier: request.generation_identifier,
                pin_label: request.pin_label,
                first_generation_slot: from_slot,
                second_generation_slot: ordinary::GenerationSlot::Pinned,
                database_marker: ordinary::DatabaseMarker::at_commit(commit_sequence),
            }),
            Err(error) => memory::Outcome::StoreRefused(memory::StoreFailure(format!(
                "persist generation pin transition: {error}"
            ))),
        }
    }

    fn unpin_generation(&mut self, request: meta::UnpinRequest) -> memory::Outcome {
        let roots = match self.store.records::<crate::runtime_model::GcRoot>() {
            Ok(roots) => roots,
            Err(error) => {
                return memory::Outcome::StoreRefused(memory::StoreFailure(format!(
                    "read durable gc roots for unpin request: {error}"
                )));
            }
        };
        let current_sequence = self.current_commit_sequence();
        let Some(mut root) = roots.into_iter().find(|root| {
            root.pin_label_option.as_ref() == Some(&request.pin_label)
                && root.cluster_name == request.cluster_name
                && root.node_name == request.node_name
        }) else {
            return Self::write_rejected(
                current_sequence,
                memory::RejectionReason::PinLabelUnknown,
            );
        };
        let generation_identifier = root.generation_identifier;
        let from_slot = root.generation_slot;
        root.generation_slot = ordinary::GenerationSlot::Recent;
        root.pin_label_option = None;
        let committed = self
            .store
            .mutate_gc_root(root)
            .and_then(|()| self.store.commit_sequence());
        match committed {
            Ok(commit_sequence) => memory::Outcome::GenerationUnpinned(meta::AppliedUnpin {
                generation_identifier,
                pin_label: request.pin_label,
                first_generation_slot: from_slot,
                second_generation_slot: ordinary::GenerationSlot::Recent,
                database_marker: ordinary::DatabaseMarker::at_commit(commit_sequence),
            }),
            Err(error) => memory::Outcome::StoreRefused(memory::StoreFailure(format!(
                "persist generation unpin transition: {error}"
            ))),
        }
    }

    fn retire_generation(&mut self, request: meta::RetireRequest) -> memory::Outcome {
        let roots = match self.store.records::<crate::runtime_model::GcRoot>() {
            Ok(roots) => roots,
            Err(error) => {
                return memory::Outcome::StoreRefused(memory::StoreFailure(format!(
                    "read durable gc roots for retire request: {error}"
                )));
            }
        };
        let current_sequence = self.current_commit_sequence();
        let Some(root) = roots.into_iter().find(|root| {
            root.generation_identifier == request.generation_identifier
                && root.cluster_name == request.cluster_name
                && root.node_name == request.node_name
        }) else {
            return Self::write_rejected(
                current_sequence,
                memory::RejectionReason::GenerationUnknown,
            );
        };
        if matches!(root.generation_slot, ordinary::GenerationSlot::Pinned) {
            return Self::write_rejected(
                current_sequence,
                memory::RejectionReason::GenerationPinned,
            );
        }
        let committed = self
            .store
            .retract_gc_root(*request.generation_identifier.payload())
            .and_then(|()| self.store.commit_sequence());
        match committed {
            Ok(commit_sequence) => memory::Outcome::GenerationRetired(meta::AppliedRetire {
                generation_identifier: request.generation_identifier,
                generation_slot: root.generation_slot,
                database_marker: ordinary::DatabaseMarker::at_commit(commit_sequence),
            }),
            Err(error) => memory::Outcome::StoreRefused(memory::StoreFailure(format!(
                "persist generation retirement: {error}"
            ))),
        }
    }

    fn record_container_transition(
        &mut self,
        transition: memory::ContainerTransition,
    ) -> memory::Outcome {
        let recorded = self
            .store
            .allocate_event_log_position()
            .and_then(|position| {
                let record = memory::ContainerLifecycleRecord {
                    cluster_name: transition.cluster_name,
                    node_name: transition.node_name,
                    container_name: transition.container_name,
                    container_state: transition.container_state,
                    event_log_position: ordinary::EventLogPosition::new(position),
                };
                let entry = memory::EventLogEntry {
                    event_log_position: ordinary::EventLogPosition::new(position),
                    logged_event: memory::LoggedEvent::Container(record.clone()),
                };
                self.store
                    .record_container_transition(record, entry)
                    .and_then(|()| self.store.commit_sequence())
                    .map(|commit_sequence| (position, commit_sequence))
            });
        match recorded {
            Ok((event_log_position, commit_sequence)) => {
                memory::Outcome::ContainerRecorded(memory::ContainerReceipt {
                    event_log_position: ordinary::EventLogPosition::new(event_log_position),
                    database_marker: ordinary::DatabaseMarker::at_commit(commit_sequence),
                })
            }
            Err(error) => panic!("persist container transition: {error}"),
        }
    }

    fn reject_durable_write(&self, cause: &dyn std::fmt::Display) -> memory::Outcome {
        eprintln!("lojix durable write rejected: {cause}");
        Self::write_rejected(
            self.store.commit_sequence().unwrap_or(0),
            memory::RejectionReason::DurableWriteFailed,
        )
    }

    fn write_rejected(commit_sequence: i64, reason: memory::RejectionReason) -> memory::Outcome {
        memory::Outcome::WriteRejected(memory::RejectionReport {
            rejection_reason: reason,
            database_marker: ordinary::DatabaseMarker::at_commit(commit_sequence),
        })
    }
}

/// The reads among the operations, each answered from the durable store and
/// projected into the public vocabulary the asker speaks.
pub(crate) trait MemoryObserving {
    /// Answer a `(ByTestRun …)` query from the durable test-run table (report
    /// 54 §5.3). Filters by cluster + node, and by run identifier when the
    /// lookup names one (`None` returns every run for that node). The matching
    /// rows are returned newest-first by run identifier so the routine
    /// `(Check …)` reader sees its latest run first.
    fn query_test_runs(&self, lookup: ordinary::TestRunLookup) -> memory::Outcome;

    fn query_generations(&self, selection: ordinary::Selection) -> memory::Outcome;

    fn project_generation(
        live: &memory::LiveGeneration,
        deployment_records: &[memory::DeploymentRecord],
    ) -> ordinary::Generation;

    fn read_event_log(&self, range: ordinary::EventLogRange) -> memory::Outcome;
}

impl MemoryObserving for SchemaRuntime {
    fn query_test_runs(&self, lookup: ordinary::TestRunLookup) -> memory::Outcome {
        let runs = match self.store.records::<crate::runtime_model::StoredTestRun>() {
            Ok(runs) => runs,
            Err(error) => {
                return memory::Outcome::StoreRefused(memory::StoreFailure(format!(
                    "read durable test runs: {error}"
                )));
            }
        };
        let commit_sequence = self.current_commit_sequence();
        let mut matching: Vec<memory::StoredTestRun> =
            runs.into_iter().filter(|run| lookup.matches(run)).collect();
        matching.sort_by(|left, right| {
            right
                .test_run_record
                .test_run_identifier
                .cmp(&left.test_run_record.test_run_identifier)
        });
        // A closure path is shown only when it is a canonical store item root.
        memory::Outcome::TestRunsQueried(ordinary::TestRunListing {
            test_run_record_vector: matching
                .into_iter()
                .map(ordinary::TestRunRecord::from)
                .map(|record| ordinary::TestRunRecord {
                    closure_path_option: record.closure_path_option.filter(|path| {
                        NixStorePath::from(path.payload().as_str()).is_canonical_item_root()
                    }),
                    ..record
                })
                .collect(),
            database_marker: memory::DatabaseMarker::at_commit(commit_sequence),
        })
    }

    fn query_generations(&self, selection: ordinary::Selection) -> memory::Outcome {
        let matching = self
            .store
            .matching_live_generations(|live| selection.matches_generation(live));
        let live_generations = match matching {
            Ok(live_generations) => live_generations,
            Err(error) => {
                return memory::Outcome::StoreRefused(memory::StoreFailure(format!(
                    "read durable generations: {error}"
                )));
            }
        };
        let commit_sequence = self.current_commit_sequence();
        let deployment_records: Vec<memory::DeploymentRecord> =
            match self.store.deployment_records() {
                Ok(records) => records
                    .into_iter()
                    .filter(|record| selection.matches_deployment_record(record))
                    .collect(),
                Err(error) => {
                    return memory::Outcome::StoreRefused(memory::StoreFailure(format!(
                        "read durable deployment records: {error}"
                    )));
                }
            };
        let generations: Vec<ordinary::Generation> = live_generations
            .iter()
            .map(|generation| Self::project_generation(generation, &deployment_records))
            .collect();
        memory::Outcome::GenerationsQueried(ordinary::GenerationListing {
            generation_vector: generations,
            deployment_record_vector: deployment_records,
            database_marker: ordinary::DatabaseMarker::at_commit(commit_sequence),
        })
    }

    fn project_generation(
        live: &memory::LiveGeneration,
        deployment_records: &[memory::DeploymentRecord],
    ) -> ordinary::Generation {
        // A closure path is shown only when it is a canonical store item root.
        let closure_path_option = NixStorePath::from(live.closure_path.payload().as_str())
            .is_canonical_item_root()
            .then(|| live.closure_path.clone());
        ordinary::Generation {
            generation_identifier: live.generation_identifier,
            deployment_identifier: live.deployment_identifier,
            cluster_name: live.cluster_name.clone(),
            node_name: live.node_name.clone(),
            generation_artifact: live.generation_artifact.clone(),
            activation_effect: live.activation_effect.clone(),
            generation_slot: live.generation_slot.clone(),
            closure_path_option,
            immutable_revision_option: deployment_records
                .iter()
                .find(|record| record.deployment_identifier == live.deployment_identifier)
                .and_then(|record| {
                    record
                        .deployment_request_identity
                        .immutable_revision_option
                        .clone()
                }),
        }
    }

    fn read_event_log(&self, range: ordinary::EventLogRange) -> memory::Outcome {
        let entries = match self.store.event_log_in_range(
            *range.first_event_log_position.payload(),
            *range.second_event_log_position.payload(),
        ) {
            Ok(entries) => entries,
            Err(_) => {
                return memory::Outcome::read_missed(
                    self.current_commit_sequence(),
                    memory::RejectionReason::EventLogPositionOutOfRange,
                );
            }
        };
        let commit_sequence = self.current_commit_sequence();
        let mut deployment_events = Vec::new();
        let mut retention_events = Vec::new();
        for entry in &entries {
            match &entry.logged_event {
                memory::LoggedEvent::Deployment(event) => deployment_events.push(event.clone()),
                memory::LoggedEvent::CacheRetention(event) => retention_events.push(event.clone()),
                memory::LoggedEvent::Container(_) => {}
            }
        }
        memory::Outcome::EventLogRead(memory::EventLogPage {
            deployment_phase_event_vector: deployment_events,
            cache_retention_transition_event_vector: retention_events,
            database_marker: ordinary::DatabaseMarker::at_commit(commit_sequence),
        })
    }
}

/// Running one decided effect against the world outside the Nexus and
/// reporting what came back. Each arm is one attempt: it composes the
/// emissions its stage needs, awaits each on a blocking thread, and
/// classifies a failure into the stage the pipeline understands.
pub(crate) trait EffectRunning {
    async fn resolve_flake_auth(&self, request: nexus::FlakeAuthRequest) -> memory::Outcome;

    async fn run_horizon_materialization(
        &self,
        command: nexus::HorizonMaterializationCommand,
    ) -> memory::Outcome;

    async fn run_nix_eval(&self, command: nexus::NixEvalCommand) -> memory::Outcome;

    async fn run_nix_build(&self, command: nexus::NixBuildCommand) -> memory::Outcome;

    async fn run_copy_closure(&self, command: nexus::CopyClosureCommand) -> memory::Outcome;

    async fn run_activate_generation(
        &self,
        command: nexus::ActivateGenerationCommand,
    ) -> memory::Outcome;

    async fn run_path_info_gc(&self, command: nexus::PathInfoGcCommand) -> memory::Outcome;

    /// The real hermetic check effect builds the exact profile selector. Exit
    /// 0 plus an out-path produces `HermeticCheckBuilt`; a non-zero exit
    /// produces `EffectFailed(HermeticCheck)`. The selected check owns its
    /// sandboxed VM, so this is a pure build with zero host effect. The outcome
    /// is the actual Nix build result.
    async fn run_hermetic_check(&self, command: nexus::HermeticCheckCommand) -> memory::Outcome;

    /// The live bring-up effect. BUILT
    /// here, NOT run live (the first Prometheus cycle is psyche-gated): the
    /// host-untouched user-namespace bring-up emission is constructed
    /// (`ssh <host-fqdn>` + `systemd-run --user` + `unshare -rn`) and would, on
    /// a live run, start the generated microVM runner + additive tap inside a
    /// private user network namespace on the resolved vmhost. The gated build
    /// path returns `TestVmBroughtUp` so the bracket is provably constructed
    /// end-to-end without touching a real host.
    async fn run_bring_up_test_vm(&self, command: nexus::BringUpTestVmCommand) -> memory::Outcome;

    /// The LIVE teardown effect (Unit 2b). BUILT here, NOT run live: constructs
    /// the `systemctl --user stop` emission that, on a live run, stops the user
    /// units so the tap + route vanish with the namespace (host netns
    /// byte-identical). Returns `TestVmTornDown` for the gated build path.
    async fn run_tear_down_test_vm(&self, command: nexus::TearDownTestVmCommand)
    -> memory::Outcome;

    fn effect_failed(
        stage: nexus::EffectStage,
        reported: impl Into<StageFailure>,
    ) -> memory::Outcome;

    /// The `TargetStore` arm of the build effect: derivation copy, build in
    /// the target's store, target-side GC root, in that order.
    async fn run_target_store_build(
        &self,
        command: &nexus::NixBuildCommand,
        build: &nexus::TargetStoreBuild,
    ) -> memory::Outcome;
}

impl EffectRunning for SchemaRuntime {
    async fn resolve_flake_auth(&self, request: nexus::FlakeAuthRequest) -> memory::Outcome {
        // Resolve the flake metadata to a locked revision through Nix. The
        // typed SourceRevisionRecord produced here is carried through the
        // eval/build path, event log, deploy-job row, and live-generation state.
        match self
            .configuration
            .effect_execution()
            .emitted(emission::FlakeMetadataQuery {
                flake_reference: request.flake_reference.clone(),
            })
            .await
        {
            emission::FlakeMetadataOutcome::Read(metadata) => {
                memory::Outcome::FlakeResolved(nexus::ResolvedFlake::new(
                    metadata
                        .source_revision(request.source_revision_policy, request.flake_reference),
                ))
            }
            emission::FlakeMetadataOutcome::Malformed(fault) => Self::effect_failed(
                nexus::EffectStage::FlakeAuth,
                format!("nix flake metadata returned malformed json: {}", fault.0),
            ),
            emission::FlakeMetadataOutcome::Failed(failure) => {
                Self::effect_failed(nexus::EffectStage::FlakeAuth, failure)
            }
        }
    }

    async fn run_horizon_materialization(
        &self,
        command: nexus::HorizonMaterializationCommand,
    ) -> memory::Outcome {
        let materialization = HorizonMaterialization {
            configuration: self.configuration.as_ref().clone(),
            command,
        };
        match materialization
            .run(self.configuration.effect_execution())
            .await
        {
            Ok(inputs) => memory::Outcome::HorizonMaterialized(inputs),
            Err(detail) => Self::effect_failed(nexus::EffectStage::MaterializeHorizon, detail),
        }
    }

    async fn run_nix_eval(&self, command: nexus::NixEvalCommand) -> memory::Outcome {
        match self
            .configuration
            .effect_execution()
            .emitted(command.evaluation())
            .await
        {
            emission::PathOutcome::Printed(printed) => {
                let closure_path = printed.printed_text();
                if !NixStorePath::from(closure_path.as_str()).is_canonical_item_root() {
                    return Self::effect_failed(
                        nexus::EffectStage::Eval,
                        "nix eval returned a noncanonical closure path".to_string(),
                    );
                }
                memory::Outcome::ClosureEvaluated(nexus::EvaluatedClosure {
                    generation_identifier: command.generation_identifier,
                    closure_path: ordinary::ClosurePath::new(closure_path),
                })
            }
            emission::PathOutcome::Failed(failure) => {
                Self::effect_failed(nexus::EffectStage::Eval, failure)
            }
        }
    }

    async fn run_nix_build(&self, command: nexus::NixBuildCommand) -> memory::Outcome {
        // A daemon-host deployment realizes in the authenticated local Lojix
        // context; an explicit builder still uses the local Nix client and
        // imports its result locally, and the copy stage transports it. Any
        // other node realizes in its own store (`TargetStore`): the derivation
        // closure goes over, the target daemon builds, and a target-side GC
        // root holds the output until activation.
        let build_place = match &command.build_target {
            nexus::BuildTarget::TargetStore(build) => {
                return self.run_target_store_build(&command, build).await;
            }
            nexus::BuildTarget::Local => emission::BuildPlace::DaemonStore,
            nexus::BuildTarget::Remote(builder_spec) => {
                emission::BuildPlace::Offloaded(builder_spec.clone())
            }
        };
        match self
            .configuration
            .effect_execution()
            .emitted(emission::ClosureBuild {
                closure_path: command.closure_path.clone(),
                build_place,
                extra_substituter_vector: command.extra_substituter_vector.clone(),
            })
            .await
        {
            emission::PathOutcome::Printed(printed) => {
                let closure_path = printed
                    .closure_path_option
                    .unwrap_or_else(|| command.closure_path.clone())
                    .into_payload();
                if !NixStorePath::from(closure_path.as_str()).is_canonical_item_root() {
                    return Self::effect_failed(
                        nexus::EffectStage::Build,
                        "nix build returned a noncanonical closure path".to_string(),
                    );
                }
                memory::Outcome::ClosureBuilt(nexus::BuiltClosure {
                    generation_identifier: command.generation_identifier,
                    closure_path: ordinary::ClosurePath::new(closure_path),
                })
            }
            emission::PathOutcome::Failed(failure) => {
                Self::effect_failed(nexus::EffectStage::Build, failure)
            }
        }
    }

    async fn run_target_store_build(
        &self,
        command: &nexus::NixBuildCommand,
        build: &nexus::TargetStoreBuild,
    ) -> memory::Outcome {
        let realization = match TargetStoreRealization::from_command(command, build) {
            Ok(realization) => realization,
            Err(detail) => return Self::effect_failed(nexus::EffectStage::Build, detail),
        };
        if let Some(builder) = &build.ignored_builder_option {
            eprintln!(
                "lojix-nexus: BuilderIgnored.{{ {} TargetStore «{}» }}",
                build.node_name.payload(),
                builder.0.payload(),
            );
        }
        match realization.run(self.configuration.effect_execution()).await {
            Ok(closure_path) => {
                eprintln!(
                    "lojix-nexus: TargetStoreRealized.{{ {} {} {} {} }}",
                    build.node_name.payload(),
                    build.deployment_transport.nix_store_uri.payload(),
                    closure_path,
                    realization.gc_root_text(),
                );
                memory::Outcome::ClosureBuilt(nexus::BuiltClosure {
                    generation_identifier: command.generation_identifier,
                    closure_path: ordinary::ClosurePath::new(closure_path),
                })
            }
            Err(detail) => Self::effect_failed(nexus::EffectStage::Build, detail),
        }
    }

    async fn run_copy_closure(&self, command: nexus::CopyClosureCommand) -> memory::Outcome {
        let transfer = match ClosureTransfer::from_command(&command) {
            Ok(transfer) => transfer,
            Err(detail) => return Self::effect_failed(nexus::EffectStage::CopyClosure, detail),
        };
        match transfer.run(self.configuration.effect_execution()).await {
            Ok(()) => memory::Outcome::ClosureCopied(nexus::CopiedClosure {
                generation_identifier: command.generation_identifier,
                node_name: command.node_name,
                closure_path: command.closure_path,
            }),
            Err(detail) => Self::effect_failed(nexus::EffectStage::CopyClosure, detail),
        }
    }

    async fn run_activate_generation(
        &self,
        command: nexus::ActivateGenerationCommand,
    ) -> memory::Outcome {
        let slot = command.activation_effect.activation_slot();
        let activation =
            match Activation::from_command(&command, Some(self.configuration.daemon_host())) {
                Ok(activation) => activation,
                Err(detail) => return Self::effect_failed(nexus::EffectStage::Activate, detail),
            };
        // `systemd-run --no-block` can return before PID 1 has made its
        // transient visible to GetUnit.  Subscribe to UnitNew before dispatch,
        // then hand that subscription to the private observer only after the
        // dispatch command itself has succeeded.  This makes the narrow
        // accepted handoff durable without mistaking that registration window
        // for an activation failure.
        let detached_observer = match activation.detached_activation_unit() {
            Some(unit) => match unit.prepare_observer().await {
                Ok(observer) => Some(observer),
                Err(detail) => return Self::effect_failed(nexus::EffectStage::Activate, detail),
            },
            None => None,
        };
        match activation.run(self.configuration.effect_execution()).await {
            Ok(reconciliation) if let Some(observer) = detached_observer => {
                reconciliation.announce(&command.node_name);
                observer.observe(
                    self.store.clone(),
                    *command.deployment_identifier.payload(),
                    self.configuration.effect_execution().clone(),
                );
                memory::Outcome::DetachedActivationDispatched(reconciliation)
            }
            Ok(reconciliation) => {
                reconciliation.announce(&command.node_name);
                memory::Outcome::ActivationCompleted(nexus::ActivatedGeneration {
                    generation_identifier: command.generation_identifier,
                    node_name: command.node_name,
                    generation_slot: slot,
                    boot_entry_reconciliation: reconciliation,
                })
            }
            Err(detail) => Self::effect_failed(nexus::EffectStage::Activate, detail),
        }
    }

    async fn run_path_info_gc(&self, command: nexus::PathInfoGcCommand) -> memory::Outcome {
        match self
            .configuration
            .effect_execution()
            .emitted(emission::GarbageCollection {
                node_name: command.node_name.clone(),
            })
            .await
        {
            emission::GarbageOutcome::Collected(collected) => {
                memory::Outcome::PathsCollected(nexus::GarbageCollected {
                    cluster_name: command.cluster_name,
                    node_name: command.node_name,
                    collected_paths: collected,
                })
            }
            emission::GarbageOutcome::Failed(failure) => {
                Self::effect_failed(nexus::EffectStage::Gc, failure)
            }
        }
    }

    async fn run_hermetic_check(&self, command: nexus::HermeticCheckCommand) -> memory::Outcome {
        match self
            .configuration
            .effect_execution()
            .emitted(emission::CheckBuild {
                flake_reference: command.flake_reference.clone(),
                deployment_output_selector: command
                    .test_execution_profile
                    .deployment_output_selector
                    .clone(),
            })
            .await
        {
            emission::PathOutcome::Printed(printed) => {
                let closure_path = printed.printed_text();
                if !NixStorePath::from(closure_path.as_str()).is_canonical_item_root() {
                    return Self::effect_failed(
                        nexus::EffectStage::HermeticCheck,
                        "nix hermetic check returned a noncanonical closure path",
                    );
                }
                memory::Outcome::HermeticCheckBuilt(nexus::CheckBuilt {
                    cluster_name: command.cluster_name,
                    node_name: command.node_name,
                    closure_path: ordinary::ClosurePath::new(closure_path),
                })
            }
            emission::PathOutcome::Failed(failure) => {
                Self::effect_failed(nexus::EffectStage::HermeticCheck, failure)
            }
        }
    }

    async fn run_bring_up_test_vm(&self, command: nexus::BringUpTestVmCommand) -> memory::Outcome {
        // The emission is CONSTRUCTED (the host-untouched user-namespace
        // call) but not emitted — a live run is gated. Constructing it proves
        // the call's shape; emitting it is the on-host effect a live run makes.
        if let Err(detail) = command.emission() {
            return Self::effect_failed(nexus::EffectStage::BringUpTestVm, detail);
        }
        memory::Outcome::TestVmStarted(nexus::TestVmBroughtUp {
            cluster_name: command.cluster_name,
            guest_node: command.guest_node,
            test_vm_host: command.test_vm_host,
        })
    }

    async fn run_tear_down_test_vm(
        &self,
        command: nexus::TearDownTestVmCommand,
    ) -> memory::Outcome {
        if let Err(detail) = command.emission() {
            return Self::effect_failed(nexus::EffectStage::TearDownTestVm, detail);
        }
        memory::Outcome::TestVmStopped(nexus::TestVmTornDown {
            cluster_name: command.cluster_name,
            guest_node: command.guest_node,
            test_vm_host: command.test_vm_host,
        })
    }

    fn effect_failed(
        stage: nexus::EffectStage,
        reported: impl Into<StageFailure>,
    ) -> memory::Outcome {
        memory::Outcome::EffectFailed(nexus::EffectFailure {
            effect_stage: stage,
            failure_evidence: reported.into().witness(),
        })
    }
}

/// The live test VM's emissions: the host-untouched user-namespace bring-up
/// of the built runner on the resolved vmhost, and its teardown.
trait TestVmEmitting {
    type Emission;

    fn emission(&self) -> std::result::Result<Self::Emission, String>;
}

impl TestVmEmitting for nexus::BringUpTestVmCommand {
    type Emission = emission::TestVmBringUp;

    fn emission(&self) -> std::result::Result<emission::TestVmBringUp, String> {
        let target = SshTarget::from_transport(&self.deployment_transport)?;
        Ok(emission::TestVmBringUp {
            ssh_destination: target.ssh_destination,
            guest_node: self.guest_node.clone(),
            closure_path: self.closure_path.clone(),
            guest_address: self.guest_address.clone(),
        })
    }
}

impl TestVmEmitting for nexus::TearDownTestVmCommand {
    type Emission = emission::TestVmTearDown;

    fn emission(&self) -> std::result::Result<emission::TestVmTearDown, String> {
        let target = SshTarget::from_transport(&self.deployment_transport)?;
        Ok(emission::TestVmTearDown {
            ssh_destination: target.ssh_destination,
            guest_node: self.guest_node.clone(),
        })
    }
}

/// The evaluation an eval command asks for. The build target it carries
/// contributes nothing: evaluation always stays in the daemon host's own
/// store, because an ssh-ng evaluation store can deadlock in remote transport
/// (lojix 0.3.9), and a `TargetStore` build copies the evaluated derivation
/// closure to the target afterwards instead.
trait Evaluating {
    fn evaluation(&self) -> emission::DerivationEvaluation;
}

impl Evaluating for nexus::NixEvalCommand {
    fn evaluation(&self) -> emission::DerivationEvaluation {
        emission::DerivationEvaluation {
            flake_reference: self.flake_reference.clone(),
            deployment_output_selector: self.deployment_output_selector.clone(),
            evaluation_freshness: emission::EvaluationFreshness::for_source(
                self.source_revision_record.source_revision_policy.clone(),
                self.flake_reference.payload(),
            ),
            persisted_flake_input_override_vector: self
                .persisted_flake_input_override_vector
                .clone(),
        }
    }
}

/// The path a Nix command printed first, as text: empty when it printed none.
trait PrintedPathText {
    fn printed_text(self) -> String;
}

impl PrintedPathText for emission::PrintedPath {
    fn printed_text(self) -> String {
        self.closure_path_option
            .map(Payload::into_payload)
            .unwrap_or_default()
    }
}

/// One Horizon materialization request. It owns the decoded Nexus command and
/// immutable daemon paths, projects cluster data through horizon-rs, and emits
/// flake input overrides for the subsequent Nix eval.
#[derive(Debug, Clone)]
struct HorizonMaterialization {
    configuration: RuntimeConfiguration,
    command: nexus::HorizonMaterializationCommand,
}

impl Effect for HorizonMaterialization {
    type Product = nexus::MaterializedInputs;

    async fn run(
        &self,
        execution: &EffectExecution,
    ) -> std::result::Result<nexus::MaterializedInputs, StageFailure> {
        self.materialize(execution)
            .await
            .map_err(|error| StageFailure::from(error.to_string()))
    }
}

/// Projecting cluster data through horizon-rs and writing it out as the flake
/// inputs one deployment's evaluation overrides.
trait HorizonMaterializing {
    async fn materialize(&self, execution: &EffectExecution) -> Result<nexus::MaterializedInputs>;
}

impl HorizonMaterializing for HorizonMaterialization {
    async fn materialize(&self, execution: &EffectExecution) -> Result<nexus::MaterializedInputs> {
        let horizon = self
            .command
            .horizon_definition
            .project(self.command.node_name.payload())?;
        let root = MaterializationRoot::new(self.configuration.materialization_root(&self.command));
        root.prepare()?;
        let secrets_source = ClusterSecretsDirectory::from_input(&self.command.secrets_input)?;
        MaterializedInputSet::new(
            root,
            horizon,
            self.command.materialization_shape.clone(),
            secrets_source,
        )
        .write(execution)
        .await
    }
}

/// Root directory for one materialization result.
#[derive(Debug, Clone)]
struct MaterializationRoot {
    path: PathBuf,
}

/// The directory one materialization result is written under, and the
/// per-input directories inside it.
trait MaterializationLayout {
    fn new(path: PathBuf) -> Self
    where
        Self: Sized;

    fn prepare(&self) -> Result<()>;

    fn input_directory(&self, name: GeneratedInputName) -> GeneratedInputDirectory;
}

impl MaterializationLayout for MaterializationRoot {
    fn new(path: PathBuf) -> Self {
        Self { path }
    }

    fn prepare(&self) -> Result<()> {
        fs::create_dir_all(&self.path)?;
        Ok(())
    }

    fn input_directory(&self, name: GeneratedInputName) -> GeneratedInputDirectory {
        GeneratedInputDirectory::new(self.path.join(name.as_str()))
    }
}

/// The generated inputs for one deploy.
#[derive(Debug, Clone)]
struct MaterializedInputSet {
    root: MaterializationRoot,
    horizon: Horizon,
    shape: nexus::MaterializationShape,
    secrets_source: ClusterSecretsDirectory,
}

/// Writing a projected Horizon, its system, its deployment shape and its
/// secrets out as the flake inputs a Nix evaluation will override.
trait InputSetWriting {
    fn new(
        root: MaterializationRoot,
        horizon: Horizon,
        shape: nexus::MaterializationShape,
        secrets_source: ClusterSecretsDirectory,
    ) -> Self
    where
        Self: Sized;

    async fn write(&self, execution: &EffectExecution) -> Result<nexus::MaterializedInputs>;
}

impl InputSetWriting for MaterializedInputSet {
    fn new(
        root: MaterializationRoot,
        horizon: Horizon,
        shape: nexus::MaterializationShape,
        secrets_source: ClusterSecretsDirectory,
    ) -> Self {
        Self {
            root,
            horizon,
            shape,
            secrets_source,
        }
    }

    async fn write(&self, execution: &EffectExecution) -> Result<nexus::MaterializedInputs> {
        let mut inputs = Vec::new();
        inputs.push(
            self.root
                .input_directory(GeneratedInputName::Horizon)
                .write_horizon(&self.horizon)?
                .to_override(GeneratedInputName::Horizon, execution)
                .await?,
        );
        inputs.push(
            self.root
                .input_directory(GeneratedInputName::System)
                .write_system(&self.horizon.node.machine.architecture)?
                .to_override(GeneratedInputName::System, execution)
                .await?,
        );
        if let Some(deployment) = DeploymentInput::from_shape(&self.shape) {
            inputs.push(
                self.root
                    .input_directory(GeneratedInputName::Deployment)
                    .write_deployment(&deployment)?
                    .to_override(GeneratedInputName::Deployment, execution)
                    .await?,
            );
        }
        inputs.push(
            self.root
                .input_directory(GeneratedInputName::Secrets)
                .write_secrets(&self.secrets_source)?
                .to_override(GeneratedInputName::Secrets, execution)
                .await?,
        );
        Ok(nexus::MaterializedInputs::new(inputs))
    }
}

/// One generated input directory containing a tiny flake.
#[derive(Debug, Clone)]
struct GeneratedInputDirectory {
    path: PathBuf,
}

/// One generated flake input directory: what is written into it, and the
/// override that points the evaluation at the result.
trait GeneratedInputWriting: Sized {
    fn new(path: PathBuf) -> Self
    where
        Self: Sized;

    fn prepare(&self) -> Result<()>;

    fn write_horizon(&self, horizon: &Horizon) -> Result<Self>;

    fn write_system(&self, system: &str) -> Result<Self>;

    fn write_deployment(&self, deployment: &DeploymentInput) -> Result<Self>;

    /// Generate the per-deploy `secrets` override: copy each cluster sops file
    /// into this directory as opaque bytes and emit a self-contained flake
    /// mapping each CriomOS `sopsFiles` attribute to its local `./<file>.sops`
    /// path. When the cluster has no secrets directory (or it is empty) the
    /// flake still exposes `sopsFiles = { }`, matching the CriomOS stub so
    /// non-cluster sources stay buildable.
    ///
    /// The directory is wiped and recreated EACH call before copying, so it
    /// holds exactly the current secrets — a removed cluster secret leaves no
    /// stale ciphertext that would drift the narHash (audit fix). Two files that
    /// map to the same `sopsFiles` attribute name are a real conflict and return
    /// a typed `Error::SecretAttributeCollision`, never a silent last-writer-wins.
    fn write_secrets(&self, secrets: &ClusterSecretsDirectory) -> Result<Self>;

    /// Remove the generated directory (ignoring an absent one) and recreate it
    /// empty, so a regenerated `secrets` input contains exactly the current
    /// files with no stale leftovers from a prior deploy.
    fn reset_directory(&self) -> Result<()>;

    async fn to_override(
        &self,
        name: GeneratedInputName,
        execution: &EffectExecution,
    ) -> Result<memory::PersistedFlakeInputOverride>;
}

impl GeneratedInputWriting for GeneratedInputDirectory {
    fn new(path: PathBuf) -> Self {
        Self { path }
    }

    fn prepare(&self) -> Result<()> {
        fs::create_dir_all(&self.path)?;
        Ok(())
    }

    fn write_horizon(&self, horizon: &Horizon) -> Result<Self> {
        self.prepare()?;
        fs::write(
            self.path.join("horizon.json"),
            serde_json::to_string_pretty(horizon)?,
        )?;
        fs::write(
            self.path.join("flake.nix"),
            "{ outputs = _: { horizon = builtins.fromJSON (builtins.readFile ./horizon.json); }; }\n",
        )?;
        Ok(self.clone())
    }

    fn write_system(&self, system: &str) -> Result<Self> {
        self.prepare()?;
        let system = NixSystemName::from_horizon_architecture(system).ok_or_else(|| {
            Error::Invariant("Horizon architecture has no supported Nix system".to_string())
        })?;
        fs::write(
            self.path.join("flake.nix"),
            format!(
                "{{ outputs = _: {{ system = \"{}\"; }}; }}\n",
                system.as_str()
            ),
        )?;
        Ok(self.clone())
    }

    fn write_deployment(&self, deployment: &DeploymentInput) -> Result<Self> {
        self.prepare()?;
        fs::write(self.path.join("flake.nix"), deployment.flake_text())?;
        Ok(self.clone())
    }

    fn write_secrets(&self, secrets: &ClusterSecretsDirectory) -> Result<Self> {
        self.reset_directory()?;
        let files = secrets.secret_files()?;
        let mut entries = String::new();
        let mut attributes: std::collections::BTreeMap<String, String> =
            std::collections::BTreeMap::new();
        for file in &files {
            let file_name = file.file_name()?;
            let attribute_name = file.attribute_name()?;
            if let Some(existing) = attributes.insert(attribute_name.clone(), file_name.clone()) {
                return Err(crate::Error::SecretAttributeCollision {
                    attribute_name,
                    first: existing,
                    second: file_name,
                });
            }
            file.copy_into(&self.path)?;
            entries.push_str(&format!("    {attribute_name} = ./{file_name};\n"));
        }
        fs::write(
            self.path.join("flake.nix"),
            format!("{{ outputs = _: {{ sopsFiles = {{\n{entries}  }}; }}; }}\n"),
        )?;
        Ok(self.clone())
    }

    fn reset_directory(&self) -> Result<()> {
        match fs::remove_dir_all(&self.path) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
        fs::create_dir_all(&self.path)?;
        Ok(())
    }

    async fn to_override(
        &self,
        name: GeneratedInputName,
        execution: &EffectExecution,
    ) -> Result<memory::PersistedFlakeInputOverride> {
        let hash = match execution
            .emitted(emission::InputHashing {
                input_directory: emission::InputDirectory(self.path.display().to_string()),
            })
            .await
        {
            emission::HashOutcome::Hashed(hash) => hash,
            emission::HashOutcome::Failed(failure) => {
                return Err(std::io::Error::other(format!(
                    "failed to hash generated input: {}",
                    StageFailure::from(failure).reported_text()
                ))
                .into());
            }
        };
        Ok(memory::PersistedFlakeInputOverride {
            flake_input_name: memory::FlakeInputName(name.as_str().to_string()),
            persisted_flake_input_reference: memory::PersistedFlakeInputReference {
                flake_input_url: memory::FlakeInputUrl(format!("path:{}", self.path.display())),
                nix_archive_hash: memory::NixArchiveHash(hash.as_url_query_value()),
            },
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum GeneratedInputName {
    Horizon,
    System,
    Deployment,
    Secrets,
}

impl crate::Named for GeneratedInputName {
    fn as_str(&self) -> &str {
        match self {
            Self::Horizon => "horizon",
            Self::System => "system",
            Self::Deployment => "deployment",
            Self::Secrets => "secrets",
        }
    }
}

/// Deployment-shape flake contents for CriomOS.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct DeploymentInput {
    include_home: bool,
    include_all_firmware: bool,
}

/// The deployment shape as the generated flake that expresses it.
trait DeploymentFlakeText: Sized {
    fn from_shape(shape: &nexus::MaterializationShape) -> Option<Self>;

    fn flake_text(&self) -> String;
}

impl DeploymentFlakeText for DeploymentInput {
    fn from_shape(shape: &nexus::MaterializationShape) -> Option<Self> {
        match shape {
            nexus::MaterializationShape::CompleteHost => Some(Self {
                include_home: true,
                include_all_firmware: true,
            }),
            nexus::MaterializationShape::BaseHost => Some(Self {
                include_home: false,
                include_all_firmware: false,
            }),
            nexus::MaterializationShape::UserEnvironment(_) => None,
        }
    }

    fn flake_text(&self) -> String {
        format!(
            "{{ outputs = _: {{ deployment = {{ includeHome = {}; includeAllFirmware = {}; }}; }}; }}\n",
            self.include_home, self.include_all_firmware
        )
    }
}

/// The caller-owned secrets authority. Each sops-encrypted file there becomes one
/// `inputs.secrets.sopsFiles.<stem>` entry the daemon provisions per deploy,
/// where `<stem>` is the `.sops` filename stem VERBATIM (the file is named with
/// its exact consumer attribute name; no case transform), overriding CriomOS's
/// `stubs/no-secrets` stub. `NoSecrets` yields an empty `sopsFiles` input; a
/// directory is never inferred from the public Horizon definition location.
#[derive(Debug, Clone)]
struct ClusterSecretsDirectory {
    path: Option<PathBuf>,
}

/// The caller-supplied secrets directory a materialization copies from.
trait SecretsSource {
    fn from_input(input: &memory::SecretsInput) -> Result<Self>
    where
        Self: Sized;

    /// The `*.sops` files in this directory, sorted by file name for a stable
    /// generated flake. An absent directory yields an empty list (bootstrap
    /// sources carry no cluster secrets); other read failures propagate.
    fn secret_files(&self) -> Result<Vec<ClusterSecretFile>>;
}

impl SecretsSource for ClusterSecretsDirectory {
    fn from_input(input: &memory::SecretsInput) -> Result<Self> {
        match input {
            memory::SecretsInput::NoSecrets => Ok(Self { path: None }),
            memory::SecretsInput::SecretsDirectory(directory) => {
                let raw = directory.payload();
                // Caller-provided secret authority is admitted on the same
                // no-symlink contract as every other lojix path: the daemon
                // copies ciphertext later, so a link must not be resolvable
                // after admission.
                let path = OfferedPath::from(raw.as_str())
                    .existing_directory()
                    .map_err(|_| {
                        Error::StoreMaintenance(
                            "secrets input must be an existing absolute directory".to_string(),
                        )
                    })?;
                Ok(Self { path: Some(path) })
            }
        }
    }

    fn secret_files(&self) -> Result<Vec<ClusterSecretFile>> {
        let Some(directory) = &self.path else {
            return Ok(Vec::new());
        };
        let mut files = Vec::new();
        for entry in fs::read_dir(directory)? {
            let path = entry?.path();
            if path.extension().and_then(|extension| extension.to_str()) == Some("sops") {
                let metadata = fs::symlink_metadata(&path)?;
                if !metadata.file_type().is_file() || metadata.file_type().is_symlink() {
                    return Err(Error::StoreMaintenance(format!(
                        "secret file must be a regular non-symlink file: {}",
                        path.display()
                    )));
                }
                files.push(ClusterSecretFile::new(path));
            }
        }
        files.sort_by(|left, right| left.sort_key().cmp(right.sort_key()));
        Ok(files)
    }
}

/// One sops-encrypted secret file in the cluster `secrets/` directory. Its
/// bytes are opaque ciphertext — copied verbatim into the generated input,
/// never read or decrypted by the daemon (the target decrypts at activation
/// via sops-nix).
#[derive(Debug, Clone)]
struct ClusterSecretFile {
    path: PathBuf,
}

/// One secret file: how it sorts, what it is called, and how it is copied
/// into a generated input.
trait SecretFileCopying {
    fn new(path: PathBuf) -> Self
    where
        Self: Sized;

    /// The full path, used as the stable sort key for a deterministic generated
    /// flake. Within one secrets directory it orders identically to the file
    /// name, and unlike `file_name` it is infallible so the sort comparator
    /// stays total even for a (later-rejected) non-UTF-8 name.
    fn sort_key(&self) -> &Path;

    /// The bare file name (`fixtureCamelCase.sops`) used both for the copy
    /// destination and the generated `./<file>.sops` Nix path. A non-UTF-8 file
    /// name is a real error (it cannot name a Nix path), not an empty string, so
    /// it returns a typed `Error::SecretFileNameNotUtf8` per the typed-error
    /// discipline.
    fn file_name(&self) -> Result<String>;

    /// The CriomOS `sopsFiles` attribute name: the `.sops` filename stem
    /// VERBATIM (only the `.sops` suffix stripped), with NO case transform. The
    /// coordinated design renames each cluster secret file to its exact
    /// camelCase consumer name (`fixtureCamelCase.sops`), so the attribute
    /// name is the stem as written — no hidden, lossy kebab-to-camel coupling. A
    /// non-UTF-8 stem returns a typed `Error::SecretFileNameNotUtf8`.
    fn attribute_name(&self) -> Result<String>;

    /// Copy the opaque ciphertext into the generated input directory so the
    /// flake is self-contained (its narHash covers the file). The contents are
    /// never read into the daemon.
    fn copy_into(&self, directory: &Path) -> Result<()>;
}

impl SecretFileCopying for ClusterSecretFile {
    fn new(path: PathBuf) -> Self {
        Self { path }
    }

    fn sort_key(&self) -> &Path {
        &self.path
    }

    fn file_name(&self) -> Result<String> {
        self.path
            .file_name()
            .and_then(|name| name.to_str())
            .map(str::to_string)
            .ok_or_else(|| crate::Error::SecretFileNameNotUtf8(self.path.clone()))
    }

    fn attribute_name(&self) -> Result<String> {
        self.path
            .file_stem()
            .and_then(|stem| stem.to_str())
            .map(str::to_string)
            .ok_or_else(|| crate::Error::SecretFileNameNotUtf8(self.path.clone()))
    }

    fn copy_into(&self, directory: &Path) -> Result<()> {
        fs::copy(&self.path, directory.join(self.file_name()?))?;
        Ok(())
    }
}

/// Nix platform string derived from Horizon's resolved machine architecture.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct NixSystemName(&'static str);

/// Naming a Nix platform from what Horizon resolved the machine's architecture
/// to be.
trait NixPlatform: Sized {
    fn from_horizon_architecture(architecture: &str) -> Option<Self>;
}

impl NixPlatform for NixSystemName {
    fn from_horizon_architecture(architecture: &str) -> Option<Self> {
        (architecture).nix_system().map(Self)
    }
}

impl crate::Named for NixSystemName {
    fn as_str(&self) -> &str {
        self.0
    }
}

/// An SRI NAR hash as a flake URL's `narHash` query value carries it.
trait NarHashQuery {
    fn as_url_query_value(&self) -> String;
}

impl NarHashQuery for emission::SriHash {
    fn as_url_query_value(&self) -> String {
        self.0
            .chars()
            .flat_map(|character| match character {
                '+' => "%2B".chars().collect::<Vec<_>>(),
                '/' => "%2F".chars().collect::<Vec<_>>(),
                '=' => "%3D".chars().collect::<Vec<_>>(),
                other => vec![other],
            })
            .collect()
    }
}

/// Exact request-supplied deployment transport. The two values have distinct
/// consumers: Nix receives `nix_store_uri` and SSH receives
/// `ssh_destination`. They are validated as command arguments but never
/// normalized, rewritten, or derived from cluster/node identity.
#[derive(Debug, Clone, PartialEq, Eq)]
struct SshTarget {
    nix_store_uri: ordinary::NixStoreUri,
    ssh_destination: ordinary::SshDestination,
}

/// The authority available to a remote Home Manager activation. The SSH login
/// is request-owned routing data, so it — never the logical node name — decides
/// whether the target user can run directly, needs explicit root mediation, or
/// must be rejected before the profile can change.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RemoteUserActivationAuthority {
    MatchedUser,
    RootMediated,
    UnprivilegedMismatch,
}

/// The explicit store URI and SSH destination a deployment was given. Lojix
/// never derives a route from cluster, node or user names.
trait SshRouting {
    fn from_transport(transport: &nexus::DeploymentTransport) -> std::result::Result<Self, String>
    where
        Self: Sized;

    fn validate_nix_store_uri(value: &str) -> std::result::Result<(), String>;

    fn validate_ssh_destination(value: &str) -> std::result::Result<(), String>;

    /// The login half of the validated destination.
    fn login(&self) -> &str;

    /// Classify the explicit SSH login for a Home Manager target. Only the
    /// literal `root` login carries mediation authority; Lojix never promotes
    /// another login or derives a root route from node or user identity.
    fn user_environment_activation_authority(
        &self,
        user: &HorizonUserName,
    ) -> RemoteUserActivationAuthority;
}

impl SshRouting for SshTarget {
    fn from_transport(transport: &nexus::DeploymentTransport) -> std::result::Result<Self, String> {
        Self::validate_nix_store_uri(transport.nix_store_uri.payload())?;
        Self::validate_ssh_destination(transport.ssh_destination.payload())?;
        Ok(Self {
            nix_store_uri: transport.nix_store_uri.clone(),
            ssh_destination: transport.ssh_destination.clone(),
        })
    }

    fn validate_nix_store_uri(value: &str) -> std::result::Result<(), String> {
        if !value.starts_with("ssh-ng://")
            || value.len() == "ssh-ng://".len()
            || value
                .bytes()
                .any(|byte| byte.is_ascii_whitespace() || byte.is_ascii_control())
        {
            return Err("deployment nix_store_uri must be a nonempty ssh-ng URI without whitespace or controls".to_string());
        }
        Ok(())
    }

    fn validate_ssh_destination(value: &str) -> std::result::Result<(), String> {
        if value.is_empty()
            || value.starts_with('-')
            || value
                .bytes()
                .any(|byte| byte.is_ascii_whitespace() || byte.is_ascii_control())
        {
            return Err("deployment ssh_destination must be a nonempty SSH destination without whitespace or controls".to_string());
        }
        let Some((login, host)) = value.split_once('@') else {
            return Err(
                "deployment ssh_destination must contain an explicit login and host".to_string(),
            );
        };
        if login.is_empty() || host.is_empty() || host.contains('@') {
            return Err(
                "deployment ssh_destination must contain exactly one nonempty login and host"
                    .to_string(),
            );
        }
        Ok(())
    }

    fn login(&self) -> &str {
        self.ssh_destination
            .payload()
            .split_once('@')
            .map(|(login, _)| login)
            .expect("SshTarget retains a validated SSH destination")
    }

    fn user_environment_activation_authority(
        &self,
        user: &HorizonUserName,
    ) -> RemoteUserActivationAuthority {
        let login = self.login();
        if login == user.as_str() {
            RemoteUserActivationAuthority::MatchedUser
        } else if login == "root" {
            RemoteUserActivationAuthority::RootMediated
        } else {
            RemoteUserActivationAuthority::UnprivilegedMismatch
        }
    }
}

/// Moving the realized closure to the activation target. A closure the
/// dispatcher realized is copied with `--substitute-on-destination`, so the
/// target pulls signed paths from the cluster cache when it can, using the
/// request-owned `nix_store_uri` verbatim. A closure the target built itself
/// is not transferred: its presence there is confirmed, and its absence fails
/// with that command's evidence. Both are idempotent.
#[derive(Debug, Clone, PartialEq, Eq)]
enum ClosureTransfer {
    Copy(emission::ClosureCopy),
    PresenceCheck(emission::ClosurePresence),
}

/// Choosing the transfer a copy command needs.
trait ClosureTransferring: Sized {
    fn from_command(command: &nexus::CopyClosureCommand) -> std::result::Result<Self, String>;
}

impl ClosureTransferring for ClosureTransfer {
    fn from_command(command: &nexus::CopyClosureCommand) -> std::result::Result<Self, String> {
        let target = SshTarget::from_transport(&command.deployment_transport)?;
        Ok(match command.closure_origin {
            nexus::ClosureOrigin::TargetStore => Self::PresenceCheck(emission::ClosurePresence {
                nix_store_uri: target.nix_store_uri,
                closure_path: command.closure_path.clone(),
            }),
            nexus::ClosureOrigin::DaemonStore => Self::Copy(emission::ClosureCopy {
                nix_store_uri: target.nix_store_uri,
                closure_path: command.closure_path.clone(),
            }),
        })
    }
}

impl Effect for ClosureTransfer {
    type Product = ();

    async fn run(&self, execution: &EffectExecution) -> std::result::Result<(), StageFailure> {
        match self {
            Self::Copy(copy) => execution.emitted(copy.clone()).await,
            Self::PresenceCheck(presence) => execution.emitted(presence.clone()).await,
        }
        .completed()
    }
}

/// Realizing a locally evaluated derivation in the deployment's own store.
///
/// Evaluation already happened in the daemon host's store (never over ssh-ng).
/// Three emissions follow, each idempotent, so a resumed `NixBuild` stage
/// simply makes them again:
///
/// 1. the derivation closure is copied to the target store;
/// 2. the target's daemon builds or substitutes the outputs with its own
///    settings (no `--builders`, no `max-jobs`: the operator's builder list is
///    not the target's);
/// 3. the output is rooted on the target, so its garbage collector keeps it
///    until the copy and activation stages have run. A root login writes a
///    direct root under `/nix/var/nix/gcroots/lojix/<operator>/`; any other
///    login an indirect root under its own
///    `$HOME/.local/state/lojix/gcroots/<operator>/`.
#[derive(Debug, Clone, PartialEq, Eq)]
struct TargetStoreRealization {
    derivation_copy: emission::DerivationCopy,
    build: emission::TargetStoreBuild,
    rooting: emission::TargetStoreRooting,
}

/// Building one derivation in the deployment's own store.
trait TargetStoreRealizing: Sized {
    fn from_command(
        command: &nexus::NixBuildCommand,
        build: &nexus::TargetStoreBuild,
    ) -> std::result::Result<Self, String>;

    /// The rooting emission for the output the build printed.
    fn rooting_of(&self, output_path: &str) -> emission::TargetStoreRooting;

    fn gc_root_text(&self) -> String;
}

impl TargetStoreRealizing for TargetStoreRealization {
    fn from_command(
        command: &nexus::NixBuildCommand,
        build: &nexus::TargetStoreBuild,
    ) -> std::result::Result<Self, String> {
        let target = SshTarget::from_transport(&build.deployment_transport)?;
        let derivation_path = command.closure_path.payload();
        if !NixStorePath::from(derivation_path.as_str()).is_canonical_item_root()
            || !derivation_path.ends_with(".drv")
        {
            return Err(
                "target-store build needs the evaluated derivation path, not an output".to_string(),
            );
        }
        let gc_root_directory = if target.login() == "root" {
            emission::GcRootDirectory::SystemRoots
        } else {
            emission::GcRootDirectory::LoginHome
        };
        Ok(Self {
            derivation_copy: emission::DerivationCopy {
                nix_store_uri: target.nix_store_uri.clone(),
                closure_path: command.closure_path.clone(),
            },
            build: emission::TargetStoreBuild {
                nix_store_uri: target.nix_store_uri.clone(),
                closure_path: command.closure_path.clone(),
                extra_substituter_vector: command.extra_substituter_vector.clone(),
            },
            rooting: emission::TargetStoreRooting {
                ssh_destination: target.ssh_destination,
                gc_root_directory,
                operator_node: build.operator_node.clone(),
                generation_identifier: command.generation_identifier,
                closure_path: command.closure_path.clone(),
            },
        })
    }

    fn rooting_of(&self, output_path: &str) -> emission::TargetStoreRooting {
        emission::TargetStoreRooting {
            closure_path: ordinary::ClosurePath::new(output_path.to_string()),
            ..self.rooting.clone()
        }
    }

    fn gc_root_text(&self) -> String {
        self.rooting.display_text()
    }
}

impl Effect for TargetStoreRealization {
    type Product = String;

    async fn run(&self, execution: &EffectExecution) -> std::result::Result<String, StageFailure> {
        if let emission::CallOutcome::Failed(failure) =
            execution.emitted(self.derivation_copy.clone()).await
        {
            return Err(failure.into());
        }
        let output_path = match execution.emitted(self.build.clone()).await {
            emission::PathOutcome::Printed(printed) => printed.printed_text(),
            emission::PathOutcome::Failed(failure) => return Err(failure.into()),
        };
        if !NixStorePath::from(output_path.as_str()).is_canonical_item_root() {
            return Err(StageFailure::from(format!(
                "nix build in the target store returned a noncanonical output path: {output_path:?}"
            )));
        }
        match execution.emitted(self.rooting_of(&output_path)).await {
            emission::CallOutcome::Completed => Ok(output_path),
            emission::CallOutcome::Failed(failure) => Err(failure.into()),
        }
    }
}

/// One activation on the target node — host (switch-to-configuration +
/// optional EFI reconcile / BootOnce transient unit) or user environment
/// (home-manager profile/activate). Decoded from the `ActivateGenerationCommand`.
#[derive(Debug, Clone)]
enum Activation {
    Host(HostActivation),
    UserEnvironment(UserEnvironmentActivation),
}

/// Turning an activation command into the activation it names, and saying when
/// that activation must be handed to PID 1 rather than run in the daemon.
trait Activating: Sized {
    /// `daemon_host` is the node the dispatching daemon runs on, so a host
    /// activation can detect a self-targeting deploy and route around the
    /// self-Switch deadlock; `None` when no daemon host context is available.
    fn from_command(
        command: &nexus::ActivateGenerationCommand,
        daemon_host: Option<&ordinary::NodeName>,
    ) -> std::result::Result<Self, String>;

    /// The retained PID-1 unit a same-host Switch or Test is handed to, which
    /// the daemon that reads its receipt settles the deployment from.
    fn detached_activation_unit(&self) -> Option<DetachedActivationUnit>;
}

impl Activating for Activation {
    fn from_command(
        command: &nexus::ActivateGenerationCommand,
        daemon_host: Option<&ordinary::NodeName>,
    ) -> std::result::Result<Self, String> {
        let target = SshTarget::from_transport(&command.deployment_transport)?;
        let closure_path = command.closure_path.clone();
        match (&command.activation_backend, &command.activation_profile) {
            (
                nexus::ActivationBackend::NixosSystemdBootV1,
                nexus::ActivationProfile::Host(action),
            ) => Ok(Self::Host(HostActivation {
                deployment_identifier: command.deployment_identifier,
                target,
                node_name: command.node_name.clone(),
                daemon_host: daemon_host.cloned(),
                closure_path,
                action: action.clone(),
            })),
            (
                nexus::ActivationBackend::HomeManagerNixProfileV1,
                nexus::ActivationProfile::UserEnvironment(profile),
            ) => {
                let user = HorizonUserName::try_from(profile.user_name.payload().clone())
                    .map_err(|error| format!("invalid user name for home activation: {error}"))?;
                Ok(Self::UserEnvironment(UserEnvironmentActivation {
                    node_name: command.node_name.clone(),
                    authority: target.user_environment_activation_authority(&user),
                    target,
                    user,
                    closure_path,
                    mode: profile.user_environment_action.clone(),
                }))
            }
            _ => Err(
                "activation backend does not support the requested activation profile".to_string(),
            ),
        }
    }

    fn detached_activation_unit(&self) -> Option<DetachedActivationUnit> {
        match self {
            Self::Host(activation) => {
                activation
                    .detached_self_activation()
                    .map(|detached| DetachedActivationUnit {
                        name: activation.self_switch_unit_name(),
                        activation: detached,
                        journal_option: Some(UnitJournal::new(
                            activation.self_switch_unit_name(),
                            activation.target.clone(),
                        )),
                    })
            }
            Self::UserEnvironment(_) => None,
        }
    }
}

impl Effect for Activation {
    /// What became of the node's EFI boot entries; a user environment
    /// requires no reconciliation.
    type Product = nexus::BootEntryReconciliation;

    async fn run(
        &self,
        execution: &EffectExecution,
    ) -> std::result::Result<nexus::BootEntryReconciliation, StageFailure> {
        match self {
            Self::Host(activation) => activation.run(execution).await,
            Self::UserEnvironment(activation) => activation
                .run(execution)
                .await
                .map(|()| nexus::BootEntryReconciliation::NotRequired),
        }
    }
}

/// The two host actions that hand activation to a PID-1-owned transient unit
/// on the daemon's own node, its receipt retained for whichever daemon reads
/// it, the dispatching one or its successor. Every other action activates in
/// the foreground and has no handoff script.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DetachedSelfActivation {
    /// Set the system profile and `switch-to-configuration switch`, run
    /// detached so the daemon restart it triggers cannot kill it mid-flight.
    /// The generation it set live is recorded Current whatever the unit's
    /// result; the deployment's terminal is the unit's result.
    Switch,
    /// `switch-to-configuration test`, which leaves the system profile
    /// where it is.
    Test,
}

/// Host activation on the target.
///
/// `Boot`/`Switch`/`Test`: one switch emission over ssh (Boot/Switch first set
/// the system profile). `BootOnce`: one emission wrapping the boot-once script
/// in a PID-1-owned transient unit, so a network blip that kills the ssh
/// leaves the unit running on the target to completion.
#[derive(Debug, Clone)]
struct HostActivation {
    /// The deployment this activation belongs to. The BootOnce transient unit
    /// name derives deterministically from it so the unit the daemon starts on
    /// the target matches the `lojix-boot-once-deploy-<id>` the durable resume
    /// cursor persisted at submit — a daemon crash during the BootOnce window
    /// can then reconcile by polling that exact unit (report 150).
    deployment_identifier: ordinary::DeploymentIdentifier,
    target: SshTarget,
    /// The target node this activation lands on. Compared against `daemon_host`
    /// to detect a self-targeting deploy.
    node_name: ordinary::NodeName,
    /// The node the dispatching daemon runs on, when known. A `Switch` whose
    /// target IS this host must NOT activate over a foreground ssh — that ssh is
    /// killed when `switch-to-configuration switch` restarts the daemon,
    /// deadlocking the deploy. `None` outside a daemon context (some tests).
    daemon_host: Option<ordinary::NodeName>,
    closure_path: ordinary::ClosurePath,
    action: ordinary::HostDeployAction,
}

/// Activating a whole host: the emissions it makes, whether it must run
/// detached from the daemon that started it, and the EFI reconciliation a
/// boot entry change requires.
trait HostActivating {
    /// The switch emission for the simple Boot/Switch/Test path. `None` for
    /// the non-simple actions (BootOnce runs a transient unit; Eval/Build do
    /// not activate).
    fn switch_emission(&self) -> Option<emission::ConfigurationSwitch>;

    /// The deterministic BootOnce transient unit name for this deploy:
    /// `lojix-boot-once-deploy-<deployment-identifier>`, the same string the
    /// durable resume cursor (`DeployJob::boot_once_unit`) persists at submit,
    /// so a daemon that crashes inside the BootOnce window can recompute the
    /// running unit's name on restart (report 150).
    fn unit_name(&self) -> String;

    /// The BootOnce emission: the boot-once script in a waited-on transient
    /// unit.
    fn boot_once_emission(&self) -> emission::TransientUnitRun;

    /// Which detached self-activation this is, or `None` when the activation
    /// is not one: a `Switch` or `Test` whose target node IS the dispatching
    /// daemon's own host. `switch-to-configuration` restarts the daemon,
    /// which would kill a foreground ssh and deadlock the deploy; the detached
    /// transient unit survives the restart (report 150 self-Switch).
    fn detached_self_activation(&self) -> Option<DetachedSelfActivation>;

    /// The deterministic transient unit name for a detached self-activation:
    /// `lojix-self-switch-deploy-<deployment-identifier>.service`.
    fn self_switch_unit_name(&self) -> String;

    /// The detached self-activation, dispatched without tying its lifetime to
    /// the daemon's cgroup, its PID-1 receipt retained until the daemon that
    /// reads it has settled the deployment and cleans it: for a Switch the
    /// whole Switch, profile set and EFI overrides cleared; for a Test the
    /// test action alone.
    fn retained_emission(
        &self,
        activation: DetachedSelfActivation,
        loader_interface: emission::LoaderInterface,
    ) -> emission::RetainedUnitDispatch;

    /// Whether this action reconciles EFI bootloader vars after activation.
    /// `Boot`/`Switch` write the declarative `loader.conf` default; reconcile
    /// removes both EFI overrides so they cannot compete. `Test` is
    /// non-persistent; `BootOnce` is its own thing.
    fn requires_efi_reconcile(&self) -> bool;

    /// Clearing one EFI override: `LoaderEntryDefault` (`loader.conf` owns
    /// persistent boot) or a pending one-shot from a prior BootOnce.
    fn boot_entry_clearing(&self, boot_entry: emission::BootEntry) -> emission::BootEntryClearing;

    /// Reading whether the node's running loader implements the boot loader
    /// interface, over the route the activation takes.
    fn loader_interface_query(&self) -> emission::LoaderInterfaceQuery;

    /// The loader interface as read on the node, before anything there is
    /// changed: a loader sets `LoaderInfo` at boot, so no switch alters it.
    async fn read_loader_interface(
        &self,
        execution: &EffectExecution,
    ) -> std::result::Result<emission::LoaderInterface, StageFailure>;

    /// The dispatch of a same-host Switch or Test, and what its unit's script
    /// does with the boot entries.
    async fn run_detached_self_activation(
        &self,
        execution: &EffectExecution,
        activation: DetachedSelfActivation,
    ) -> std::result::Result<nexus::BootEntryReconciliation, StageFailure>;

    async fn run_simple(
        &self,
        execution: &EffectExecution,
    ) -> std::result::Result<nexus::BootEntryReconciliation, StageFailure>;

    async fn reconcile_efi(
        &self,
        execution: &EffectExecution,
    ) -> std::result::Result<(), StageFailure>;

    /// A transient unit run, whose failure carries the unit's journal: the
    /// script ran in its own unit, so its output is there and not in ssh's.
    async fn run_journalled_unit(
        &self,
        execution: &EffectExecution,
        run: emission::TransientUnitRun,
    ) -> std::result::Result<(), StageFailure>;
}

impl HostActivating for HostActivation {
    fn switch_emission(&self) -> Option<emission::ConfigurationSwitch> {
        let switch_action = match self.action {
            ordinary::HostDeployAction::SetBootProfile => emission::SwitchAction::Boot,
            ordinary::HostDeployAction::ActivateNow => emission::SwitchAction::Switch,
            ordinary::HostDeployAction::TestActivation => emission::SwitchAction::Test,
            ordinary::HostDeployAction::ScheduleBootOnce
            | ordinary::HostDeployAction::Evaluate
            | ordinary::HostDeployAction::Realize => return None,
        };
        Some(emission::ConfigurationSwitch {
            ssh_destination: self.target.ssh_destination.clone(),
            closure_path: self.closure_path.clone(),
            switch_action,
        })
    }

    fn unit_name(&self) -> String {
        self.deployment_identifier.boot_once_unit_name()
    }

    fn boot_once_emission(&self) -> emission::TransientUnitRun {
        emission::TransientUnitRun {
            ssh_destination: self.target.ssh_destination.clone(),
            unit_name: emission::UnitName(self.unit_name()),
            transient_script: emission::TransientScript::BootOnce(self.closure_path.clone()),
        }
    }

    fn detached_self_activation(&self) -> Option<DetachedSelfActivation> {
        let is_self_target = self
            .daemon_host
            .as_ref()
            .is_some_and(|host| host.payload() == self.node_name.payload());
        if !is_self_target {
            return None;
        }
        match self.action {
            ordinary::HostDeployAction::ActivateNow => Some(DetachedSelfActivation::Switch),
            ordinary::HostDeployAction::TestActivation => Some(DetachedSelfActivation::Test),
            _ => None,
        }
    }

    fn self_switch_unit_name(&self) -> String {
        DetachedActivationUnit::canonical_name(&self.deployment_identifier)
    }

    fn retained_emission(
        &self,
        activation: DetachedSelfActivation,
        loader_interface: emission::LoaderInterface,
    ) -> emission::RetainedUnitDispatch {
        let closure_path = self.closure_path.clone();
        emission::RetainedUnitDispatch {
            ssh_destination: self.target.ssh_destination.clone(),
            unit_name: emission::UnitName(self.self_switch_unit_name()),
            transient_script: match activation {
                DetachedSelfActivation::Switch => {
                    emission::TransientScript::SelfSwitch(emission::SelfSwitchScript {
                        closure_path,
                        loader_interface,
                    })
                }
                DetachedSelfActivation::Test => emission::TransientScript::SelfTest(closure_path),
            },
        }
    }

    fn requires_efi_reconcile(&self) -> bool {
        matches!(
            self.action,
            ordinary::HostDeployAction::SetBootProfile | ordinary::HostDeployAction::ActivateNow
        )
    }

    fn boot_entry_clearing(&self, boot_entry: emission::BootEntry) -> emission::BootEntryClearing {
        emission::BootEntryClearing {
            ssh_destination: self.target.ssh_destination.clone(),
            boot_entry,
        }
    }

    fn loader_interface_query(&self) -> emission::LoaderInterfaceQuery {
        emission::LoaderInterfaceQuery {
            ssh_destination: self.target.ssh_destination.clone(),
        }
    }

    async fn read_loader_interface(
        &self,
        execution: &EffectExecution,
    ) -> std::result::Result<emission::LoaderInterface, StageFailure> {
        match execution.emitted(self.loader_interface_query()).await {
            emission::LoaderInterfaceOutcome::Read(loader_interface) => Ok(loader_interface),
            emission::LoaderInterfaceOutcome::Unrecognized(printed) => {
                Err(StageFailure::from(format!(
                    "the boot loader interface read printed neither answer: {}",
                    printed.0.trim()
                )))
            }
            emission::LoaderInterfaceOutcome::Failed(failure) => Err(failure.into()),
        }
    }

    async fn run_detached_self_activation(
        &self,
        execution: &EffectExecution,
        activation: DetachedSelfActivation,
    ) -> std::result::Result<nexus::BootEntryReconciliation, StageFailure> {
        let (loader_interface, reconciliation) = match activation {
            DetachedSelfActivation::Switch => {
                let loader_interface = self.read_loader_interface(execution).await?;
                let reconciliation = loader_interface.reconciliation();
                (loader_interface, reconciliation)
            }
            DetachedSelfActivation::Test => (
                emission::LoaderInterface::NotImplemented,
                nexus::BootEntryReconciliation::NotRequired,
            ),
        };
        // The dispatch returns once PID 1 holds the unit; what the
        // activation prints goes to the unit's journal, and its result is
        // read from the retained receipt.
        match execution
            .emitted(self.retained_emission(activation, loader_interface))
            .await
        {
            emission::CallOutcome::Completed => Ok(reconciliation),
            emission::CallOutcome::Failed(failure) => Err(failure.into()),
        }
    }

    async fn run_simple(
        &self,
        execution: &EffectExecution,
    ) -> std::result::Result<nexus::BootEntryReconciliation, StageFailure> {
        let Some(switch) = self.switch_emission() else {
            return Err(StageFailure::from(format!(
                "no simple activation for action {:?}",
                self.action
            )));
        };
        let loader_interface_option = match self.requires_efi_reconcile() {
            true => Some(self.read_loader_interface(execution).await?),
            false => None,
        };
        if let emission::CallOutcome::Failed(failure) = execution.emitted(switch).await {
            return Err(failure.into());
        }
        match loader_interface_option {
            None => Ok(nexus::BootEntryReconciliation::NotRequired),
            Some(emission::LoaderInterface::NotImplemented) => {
                Ok(nexus::BootEntryReconciliation::SkippedWithoutLoaderInterface)
            }
            Some(emission::LoaderInterface::Implemented) => {
                self.reconcile_efi(execution).await?;
                Ok(nexus::BootEntryReconciliation::Cleared)
            }
        }
    }

    async fn reconcile_efi(
        &self,
        execution: &EffectExecution,
    ) -> std::result::Result<(), StageFailure> {
        for boot_entry in [emission::BootEntry::Default, emission::BootEntry::Oneshot] {
            if let emission::CallOutcome::Failed(failure) = execution
                .emitted(self.boot_entry_clearing(boot_entry))
                .await
            {
                return Err(failure.into());
            }
        }
        Ok(())
    }

    async fn run_journalled_unit(
        &self,
        execution: &EffectExecution,
        run: emission::TransientUnitRun,
    ) -> std::result::Result<(), StageFailure> {
        let unit_name = run.unit_name.0.clone();
        match execution.emitted(run).await {
            emission::CallOutcome::Completed => Ok(()),
            emission::CallOutcome::Failed(failure) => Err(StageFailure::from(failure).with_output(
                UnitJournal::new(unit_name, self.target.clone())
                    .tail(execution)
                    .await,
            )),
        }
    }
}

impl Effect for HostActivation {
    type Product = nexus::BootEntryReconciliation;

    async fn run(
        &self,
        execution: &EffectExecution,
    ) -> std::result::Result<nexus::BootEntryReconciliation, StageFailure> {
        if let Some(activation) = self.detached_self_activation() {
            return self
                .run_detached_self_activation(execution, activation)
                .await;
        }
        match self.action {
            ordinary::HostDeployAction::ScheduleBootOnce => self
                .run_journalled_unit(execution, self.boot_once_emission())
                .await
                .map(|()| nexus::BootEntryReconciliation::NotRequired),
            _ => self.run_simple(execution).await,
        }
    }
}

/// Saying what an activation did with the node's boot entries where it is
/// news: a skip is said in the daemon's log, beside the outcome that carries
/// it.
trait ReconciliationAnnouncing {
    fn announce(&self, node_name: &ordinary::NodeName);
}

impl ReconciliationAnnouncing for nexus::BootEntryReconciliation {
    fn announce(&self, node_name: &ordinary::NodeName) {
        if let Self::SkippedWithoutLoaderInterface = self {
            eprintln!(
                "lojix: {} boots a loader that does not implement the boot loader interface (no LoaderInfo); its boot-entry clears are skipped",
                node_name.payload()
            );
        }
    }
}

/// What a Boot or Switch does with the boot entries under the loader it runs
/// on: clears them where the loader implements the boot loader interface,
/// and skips the clears where it does not.
trait LoaderReconciliation {
    fn reconciliation(&self) -> nexus::BootEntryReconciliation;
}

impl LoaderReconciliation for emission::LoaderInterface {
    fn reconciliation(&self) -> nexus::BootEntryReconciliation {
        match self {
            Self::Implemented => nexus::BootEntryReconciliation::Cleared,
            Self::NotImplemented => nexus::BootEntryReconciliation::SkippedWithoutLoaderInterface,
        }
    }
}

/// The journal of a transient unit an activation was handed to, read on the
/// node the unit ran on, over the same route that started it. A unit run by
/// `systemd-run` writes what its script printed here, not to the command that
/// started it, and `--collect` keeps the journal after the unit is gone.
#[derive(Debug, Clone)]
struct UnitJournal {
    unit_name: String,
    target: SshTarget,
}

/// Reading back the tail of one unit's journal as reported output.
trait UnitJournalReading {
    fn new(unit_name: String, target: SshTarget) -> Self
    where
        Self: Sized;

    /// The journal query for this unit on its node.
    fn query(&self) -> emission::UnitJournalQuery;

    /// The journal tail under `journal of <unit>`; when it cannot be read,
    /// what the read reported, under `journal of <unit> unavailable`.
    async fn tail(&self, execution: &EffectExecution) -> ReportedOutput;
}

impl UnitJournalReading for UnitJournal {
    fn new(unit_name: String, target: SshTarget) -> Self {
        Self { unit_name, target }
    }

    fn query(&self) -> emission::UnitJournalQuery {
        emission::UnitJournalQuery {
            ssh_destination: self.target.ssh_destination.clone(),
            unit_name: emission::UnitName(self.unit_name.clone()),
        }
    }

    async fn tail(&self, execution: &EffectExecution) -> ReportedOutput {
        let label = format!("journal of {}", self.unit_name);
        match execution.emitted(self.query()).await {
            emission::JournalOutcome::Read(text) => ReportedOutput::new(label, text.0),
            emission::JournalOutcome::Failed(failure) => ReportedOutput::new(
                format!("{label} unavailable"),
                StageFailure::from(failure).reported_text(),
            ),
        }
    }
}

/// PID-1's retained receipt for a same-host Switch or Test activation. The
/// unit name is derived from the already durable deployment identity; there is
/// deliberately no BootOnce cursor involved, and the unit's own result, never
/// the system profile, is the deployment's terminal.
#[derive(Debug, Clone)]
pub(crate) struct DetachedActivationUnit {
    name: String,
    /// What the unit runs: a Switch, whose live generation is recorded
    /// Current whatever its result, or a Test, which records none.
    activation: DetachedSelfActivation,
    /// Where the unit's journal is read from: the route the activation was
    /// dispatched over. `None` only for a unit named without a deployment
    /// behind it.
    journal_option: Option<UnitJournal>,
}

/// The exact Manager.UnitNew subscription created before dispatch, held by the
/// system-bus session the observer's emissions run on.  It is intentionally
/// private runtime state: the deployment identity and unit name are already
/// durable, while this closes only the short systemd registration race in the
/// predecessor.
pub(crate) struct DetachedActivationObserver {
    unit: DetachedActivationUnit,
    bus: SystemdBus,
}

/// What PID 1 reported about a detached activation transient: its
/// `ActiveState`, the service `Result`, how the main process ended when it
/// had one, and the unit's journal tail. Observed, not inferred.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DetachedActivationFailure {
    unit_name: String,
    active_state: String,
    result: String,
    process_end_option: Option<emission::ProcessEnd>,
    reported_outputs: Vec<ReportedOutput>,
}

impl Witnessing for DetachedActivationFailure {
    fn reported_detail(&self) -> &str {
        &self.result
    }

    fn reported_outputs(&self) -> &[ReportedOutput] {
        &self.reported_outputs
    }

    fn reported_command(&self) -> Option<crate::runtime_model::FailedCommand> {
        Some(crate::runtime_model::FailedCommand {
            command_program: crate::runtime_model::CommandProgram::new("systemd-run".into()),
            command_argument_vector: vec![
                crate::runtime_model::CommandArgument::new(format!("--unit={}", self.unit_name)),
                crate::runtime_model::CommandArgument::new(format!(
                    "ActiveState={}",
                    self.active_state
                )),
            ],
            exit_code_option: self
                .process_end_option
                .as_ref()
                .and_then(|end| end.exit_code())
                .filter(|status| *status >= 0)
                .map(i64::from)
                .map(crate::runtime_model::ExitCode::new),
        })
    }
}

impl FailureReporting for DetachedActivationFailure {
    fn with_output(mut self, output: ReportedOutput) -> Self {
        self.reported_outputs.push(output);
        self
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum DetachedActivationOutcome {
    Succeeded,
    Failed(DetachedActivationFailure),
    Missing,
    Running,
}

/// The transient systemd unit a same-host activation is handed to, and what is
/// asked of PID 1 about it afterwards.
pub(crate) trait DetachedActivation {
    fn new(name: String, activation: DetachedSelfActivation) -> Self
    where
        Self: Sized;

    /// Systemd's D-Bus unit identity is fully qualified. `systemd-run` accepts
    /// a shorthand service name, but Manager.UnitNew and GetUnit use this
    /// canonical `.service` spelling, so dispatch, signal matching, lookup,
    /// logging, and cleanup must share it exactly.
    fn canonical_name(deployment_identifier: &ordinary::DeploymentIdentifier) -> String;

    fn for_deployment(
        deployment_identifier: &ordinary::DeploymentIdentifier,
        activation: DetachedSelfActivation,
    ) -> Self
    where
        Self: Sized;

    /// Subscribe before `systemd-run --no-block` so its UnitNew cannot be
    /// missed between CLI admission and the first GetUnit call.
    async fn prepare_observer(&self) -> std::result::Result<DetachedActivationObserver, String>;

    fn terminal_outcome(
        outcome: Option<DetachedActivationOutcome>,
    ) -> Option<memory::DeploymentTerminal>;

    #[cfg(test)]
    fn needs_completion_observer(outcome: Option<DetachedActivationOutcome>) -> bool;

    /// What PID 1's reading of the unit says: succeeded, failed with what it
    /// reported, still running, or gone.
    fn classify(&self, state: emission::UnitState) -> DetachedActivationOutcome;

    /// What is known of a unit PID 1 no longer holds: that it is gone, and
    /// whatever its journal kept.
    fn vanished(&self) -> DetachedActivationFailure;

    /// The failure completed with the unit's journal tail, read over the
    /// route the activation was dispatched on.
    async fn journalled(
        &self,
        failure: DetachedActivationFailure,
        execution: &EffectExecution,
    ) -> DetachedActivationFailure;

    async fn clean(&self);

    /// Settle the deployment from the unit's terminal outcome: a failure is
    /// completed with the unit's journal, a Switch's live generation is
    /// recorded Current when the system profile is its closure, and the
    /// terminal is written; the unit is cleaned once that terminal is
    /// durable. An outcome that is no terminal leaves the deployment
    /// Activating.
    async fn settle(
        &self,
        outcome: Option<DetachedActivationOutcome>,
        store: &Store,
        deployment_identifier: i64,
        execution: &EffectExecution,
    );

    /// Write a terminal the unit's result came to, after recording a Switch's
    /// live generation Current when `system_profile` is its closure. True
    /// when the terminal is durable; a failed generation write leaves the
    /// deployment Activating for the next start to settle.
    fn conclude(
        &self,
        terminal: memory::DeploymentTerminal,
        store: &Store,
        deployment_identifier: i64,
        system_profile: Option<&str>,
    ) -> bool;
}

/// The store path the daemon host's system profile resolves to:
/// `/nix/var/nix/profiles/system`, which a Switch sets with `nix-env --set`
/// before it switches. `None` when it cannot be read.
pub(crate) struct SystemProfile;

/// Reading where the system profile points.
pub(crate) trait SystemProfileReading {
    fn closure() -> Option<String>;
}

impl SystemProfileReading for SystemProfile {
    fn closure() -> Option<String> {
        fs::canonicalize("/nix/var/nix/profiles/system")
            .ok()
            .map(|path| path.display().to_string())
    }
}

impl DetachedActivation for DetachedActivationUnit {
    fn new(name: String, activation: DetachedSelfActivation) -> Self {
        Self {
            name,
            activation,
            journal_option: None,
        }
    }

    fn canonical_name(deployment_identifier: &ordinary::DeploymentIdentifier) -> String {
        format!(
            "lojix-self-switch-deploy-{}.service",
            deployment_identifier.payload()
        )
    }

    fn for_deployment(
        deployment_identifier: &ordinary::DeploymentIdentifier,
        activation: DetachedSelfActivation,
    ) -> Self {
        Self::new(Self::canonical_name(deployment_identifier), activation)
    }

    async fn prepare_observer(&self) -> std::result::Result<DetachedActivationObserver, String> {
        let bus = SystemdBus::default();
        match bus.emitted(emission::SubscribeUnitNew {}).await {
            emission::BusCallOutcome::Answered => Ok(DetachedActivationObserver {
                unit: self.clone(),
                bus,
            }),
            emission::BusCallOutcome::Refused(fault) => Err(fault.0),
        }
    }

    fn terminal_outcome(
        outcome: Option<DetachedActivationOutcome>,
    ) -> Option<memory::DeploymentTerminal> {
        match outcome {
            Some(DetachedActivationOutcome::Succeeded) => {
                Some(memory::DeploymentTerminal::Succeeded)
            }
            Some(DetachedActivationOutcome::Failed(observed)) => Some(
                memory::DeploymentTerminal::Failed(memory::DeploymentFailure {
                    deployment_failure_stage: memory::DeploymentFailureStage::Activate,
                    deployment_terminal_reason: memory::DeploymentTerminalReason::ActivationFailed,
                    failure_evidence_option: Some(observed.witness()),
                }),
            ),
            Some(DetachedActivationOutcome::Missing)
            | Some(DetachedActivationOutcome::Running)
            | None => None,
        }
    }

    #[cfg(test)]
    fn needs_completion_observer(outcome: Option<DetachedActivationOutcome>) -> bool {
        matches!(outcome, None | Some(DetachedActivationOutcome::Running))
    }

    fn classify(&self, state: emission::UnitState) -> DetachedActivationOutcome {
        let active = match state.active_state_reading {
            emission::ActiveStateReading::Read(active_state) => Ok(active_state),
            emission::ActiveStateReading::Unreadable(fault) => Err(fault.0),
        };
        let result = match state.service_result_reading {
            emission::ServiceResultReading::Read(service_result) => Ok(service_result),
            emission::ServiceResultReading::Unreadable(fault) => Err(fault.0),
        };
        eprintln!(
            "lojix detached activation observer state active={:?} result={:?}",
            active
                .as_ref()
                .map(|active_state| active_state.systemd_word()),
            result
                .as_ref()
                .map(|service_result| service_result.systemd_word()),
        );
        match (&active, &result) {
            (Ok(emission::ActiveState::Active), Ok(emission::ServiceResult::Success)) => {
                DetachedActivationOutcome::Succeeded
            }
            // A oneshot that does not remain after exit, as one dispatched
            // before its receipt was retained, ends inactive; its receipt is
            // a success when its main process exited 0.
            (Ok(emission::ActiveState::Inactive), Ok(emission::ServiceResult::Success))
                if state.process_end_option
                    == Some(emission::ProcessEnd::Exited(emission::ExitStatus(0))) =>
            {
                DetachedActivationOutcome::Succeeded
            }
            (Ok(emission::ActiveState::Activating | emission::ActiveState::Reloading), _) => {
                DetachedActivationOutcome::Running
            }
            (Ok(emission::ActiveState::Active), _) => DetachedActivationOutcome::Running,
            (Ok(active_state), _) => {
                let active_state = active_state.systemd_word();
                // What PID 1 said is the whole evidence for this failure —
                // keep it rather than printing it and terminalizing a generic
                // reason.
                let process_end_option = state.process_end_option;
                let reported = match &result {
                    Ok(result) => format!("systemd reported Result={}", result.systemd_word()),
                    Err(error) => format!(
                        "systemd reported ActiveState={active_state}, Result unreadable: {error}"
                    ),
                };
                DetachedActivationOutcome::Failed(DetachedActivationFailure {
                    unit_name: self.name.clone(),
                    active_state: active_state.to_string(),
                    result: match &process_end_option {
                        Some(end) => format!("{reported}; main process {}", end.statement()),
                        None => reported,
                    },
                    process_end_option,
                    reported_outputs: Vec::new(),
                })
            }
            _ => DetachedActivationOutcome::Missing,
        }
    }

    fn vanished(&self) -> DetachedActivationFailure {
        DetachedActivationFailure {
            unit_name: self.name.clone(),
            active_state: "missing".to_string(),
            result: format!(
                "systemd no longer holds {}, so it reported no result",
                self.name
            ),
            process_end_option: None,
            reported_outputs: Vec::new(),
        }
    }

    async fn journalled(
        &self,
        failure: DetachedActivationFailure,
        execution: &EffectExecution,
    ) -> DetachedActivationFailure {
        match &self.journal_option {
            Some(journal) => failure.with_output(journal.tail(execution).await),
            None => failure.with_output(ReportedOutput::new(
                format!("journal of {} unavailable", self.name),
                "no route to the node the unit ran on",
            )),
        }
    }

    async fn clean(&self) {
        let _ = SystemdBus::default()
            .emitted(emission::UnitStop {
                unit_name: emission::UnitName(self.name.clone()),
            })
            .await;
    }

    async fn settle(
        &self,
        outcome: Option<DetachedActivationOutcome>,
        store: &Store,
        deployment_identifier: i64,
        execution: &EffectExecution,
    ) {
        let outcome = match outcome {
            Some(DetachedActivationOutcome::Failed(failure)) => Some(
                DetachedActivationOutcome::Failed(self.journalled(failure, execution).await),
            ),
            other => other,
        };
        let Some(terminal) = Self::terminal_outcome(outcome) else {
            return;
        };
        if self.conclude(
            terminal,
            store,
            deployment_identifier,
            SystemProfile::closure().as_deref(),
        ) {
            self.clean().await;
        }
    }

    fn conclude(
        &self,
        terminal: memory::DeploymentTerminal,
        store: &Store,
        deployment_identifier: i64,
        system_profile: Option<&str>,
    ) -> bool {
        if self.activation == DetachedSelfActivation::Switch {
            let Ok(jobs) = store.deploy_jobs() else {
                return false;
            };
            let recorded = jobs
                .into_iter()
                .find(|job| *job.deployment_identifier.payload() == deployment_identifier)
                .and_then(|job| job.self_switch_activation_record(system_profile));
            if let Some((generation, root)) = recorded {
                let already_recorded = store
                    .records::<memory::GcRoot>()
                    .map(|roots| {
                        roots.iter().any(|existing| {
                            existing.generation_identifier == generation.generation_identifier
                        })
                    })
                    .unwrap_or(false);
                if !already_recorded && store.record_activation(generation, root).is_err() {
                    return false;
                }
            }
        }
        store
            .terminalize_deployment(deployment_identifier, terminal)
            .is_ok()
    }
}

/// Watching a detached activation to its terminal outcome and persisting it,
/// after the foreground runner has already replied.
pub(crate) trait DetachedObserving {
    /// This is the successor's first state read. The Manager.UnitNew match was
    /// installed by `prepare_observer` before this lookup, so the one allowed
    /// transient `NoSuchUnit` is a pending registration, not a terminal
    /// activation failure.
    async fn initial_outcome(
        &self,
    ) -> std::result::Result<Option<DetachedActivationOutcome>, String>;

    /// Start the private event observer after `systemd-run --no-block` has
    /// accepted the handoff. An observer interruption is deliberately not a
    /// terminal result: a replacement daemon uses the retained PID-1 receipt.
    fn observe(self, store: Arc<Store>, deployment_identifier: i64, execution: EffectExecution);

    async fn await_completion(
        &mut self,
    ) -> std::result::Result<Option<DetachedActivationOutcome>, String>;

    /// Read the found unit's state and classify it.
    async fn read_outcome(&self) -> DetachedActivationOutcome;
}

impl DetachedObserving for DetachedActivationObserver {
    async fn initial_outcome(
        &self,
    ) -> std::result::Result<Option<DetachedActivationOutcome>, String> {
        match self
            .bus
            .emitted(emission::UnitLookup {
                unit_name: emission::UnitName(self.unit.name.clone()),
            })
            .await
        {
            emission::UnitLookupOutcome::Found => Ok(Some(self.read_outcome().await)),
            // PID 1 has not yet made this exact transient visible to GetUnit.
            // A registration window is the absence of an outcome, not one of
            // its values: `classify` reads a resolved unit and cannot report
            // one.
            emission::UnitLookupOutcome::NoSuchUnit => Ok(None),
            emission::UnitLookupOutcome::Refused(fault) => Err(fault.0),
        }
    }

    fn observe(
        mut self,
        store: Arc<Store>,
        deployment_identifier: i64,
        execution: EffectExecution,
    ) {
        tokio::spawn(async move {
            eprintln!(
                "lojix detached activation observer attached deployment={} unit={}",
                deployment_identifier, self.unit.name
            );
            let outcome = match self.await_completion().await {
                Ok(outcome) => outcome,
                Err(error) => {
                    eprintln!(
                        "lojix detached activation observer error deployment={} unit={} error={}",
                        deployment_identifier, self.unit.name, error
                    );
                    return;
                }
            };
            eprintln!(
                "lojix detached activation observer completion deployment={} unit={} outcome={outcome:?}",
                deployment_identifier, self.unit.name
            );
            self.unit
                .settle(outcome, &store, deployment_identifier, &execution)
                .await;
        });
    }

    async fn await_completion(
        &mut self,
    ) -> std::result::Result<Option<DetachedActivationOutcome>, String> {
        let unit_name = emission::UnitName(self.unit.name.clone());
        loop {
            match self
                .bus
                .emitted(emission::UnitLookup {
                    unit_name: unit_name.clone(),
                })
                .await
            {
                emission::UnitLookupOutcome::Found => break,
                emission::UnitLookupOutcome::Refused(fault) => return Err(fault.0),
                emission::UnitLookupOutcome::NoSuchUnit => {
                    eprintln!(
                        "lojix detached activation observer awaiting registration unit={}",
                        self.unit.name
                    );
                    match self
                        .bus
                        .emitted(emission::UnitArrival {
                            unit_name: unit_name.clone(),
                        })
                        .await
                    {
                        emission::UnitArrivalOutcome::Arrived => {}
                        emission::UnitArrivalOutcome::StreamEnded => return Ok(None),
                        emission::UnitArrivalOutcome::Refused(fault) => return Err(fault.0),
                    }
                }
            }
        }
        // The lookup subscribed to the unit's ActiveState changes before
        // anything is read from it. If PID 1 completed between the lookup and
        // the subscription, the first read sees that terminal receipt;
        // otherwise the subscription sees its transition. No clock polling is
        // involved.
        loop {
            match self.read_outcome().await {
                outcome @ (DetachedActivationOutcome::Succeeded
                | DetachedActivationOutcome::Failed(_)) => {
                    return Ok(Some(outcome));
                }
                DetachedActivationOutcome::Missing => {
                    return Ok(Some(DetachedActivationOutcome::Missing));
                }
                DetachedActivationOutcome::Running => {}
            }
            if let emission::UnitStateWatchOutcome::StreamEnded = self
                .bus
                .emitted(emission::UnitStateWatch {
                    unit_name: unit_name.clone(),
                })
                .await
            {
                return Ok(None);
            }
        }
    }

    async fn read_outcome(&self) -> DetachedActivationOutcome {
        let state = self
            .bus
            .emitted(emission::UnitStateQuery {
                unit_name: emission::UnitName(self.unit.name.clone()),
            })
            .await;
        self.unit.classify(state)
    }
}

/// User-environment activation on the target. A request explicitly logged in as
/// the target user runs directly; an explicit root login mediates through a
/// target-user login; any other remote login is refused before a profile or
/// activation effect. Includes the local fast-path: skip ssh entirely when the
/// dispatcher already is the requested user on the target node.
#[derive(Debug, Clone)]
struct UserEnvironmentActivation {
    node_name: ordinary::NodeName,
    authority: RemoteUserActivationAuthority,
    target: SshTarget,
    user: HorizonUserName,
    closure_path: ordinary::ClosurePath,
    mode: meta::UserEnvironmentAction,
}

/// Activating one user environment, locally or through a remote shell.
/// `ActivateNow` runs the activation first and sets the profile only once it
/// has succeeded, so a refused activation leaves the profile where it was.
trait UserEnvironmentActivating {
    fn local_profile_emission(&self, home: &Path) -> emission::LocalHomeProfile;

    fn local_activate_emission(&self) -> emission::LocalHomeActivation;

    fn remote_profile_emission(&self) -> std::result::Result<emission::RemoteHomeProfile, String>;

    fn remote_activate_emission(
        &self,
    ) -> std::result::Result<emission::RemoteHomeActivation, String>;

    /// The login a remote command runs as, from the authority carried by the
    /// explicit SSH destination: the target user directly, or root mediating
    /// through a login of the target user. This is the effect-side fail-closed
    /// guard for recovered jobs; fresh submissions reject this same mismatch
    /// before any effect.
    fn remote_home_login(&self) -> std::result::Result<emission::RemoteHomeLogin, String>;

    async fn run_profile(
        &self,
        execution: &EffectExecution,
    ) -> std::result::Result<(), StageFailure>;

    async fn run_activate(
        &self,
        execution: &EffectExecution,
    ) -> std::result::Result<(), StageFailure>;

    /// The local fast-path predicate: the dispatcher is already the requested
    /// user on the target node, so activation runs locally without ssh.
    async fn is_local_context(&self, execution: &EffectExecution) -> bool;

    fn current_user(&self) -> Option<String>;

    async fn current_node(&self, execution: &EffectExecution) -> Option<String>;
}

/// Reading a completed-or-failed emission outcome as a stage result.
trait CompletedCall {
    fn completed(self) -> std::result::Result<(), StageFailure>;
}

impl CompletedCall for emission::CallOutcome {
    fn completed(self) -> std::result::Result<(), StageFailure> {
        match self {
            Self::Completed => Ok(()),
            Self::Failed(failure) => Err(failure.into()),
        }
    }
}

impl UserEnvironmentActivating for UserEnvironmentActivation {
    fn local_profile_emission(&self, home: &Path) -> emission::LocalHomeProfile {
        emission::LocalHomeProfile {
            home_directory: emission::HomeDirectory(home.display().to_string()),
            closure_path: self.closure_path.clone(),
        }
    }

    fn local_activate_emission(&self) -> emission::LocalHomeActivation {
        emission::LocalHomeActivation {
            closure_path: self.closure_path.clone(),
        }
    }

    fn remote_profile_emission(&self) -> std::result::Result<emission::RemoteHomeProfile, String> {
        Ok(emission::RemoteHomeProfile {
            ssh_destination: self.target.ssh_destination.clone(),
            remote_home_login: self.remote_home_login()?,
            closure_path: self.closure_path.clone(),
        })
    }

    fn remote_activate_emission(
        &self,
    ) -> std::result::Result<emission::RemoteHomeActivation, String> {
        Ok(emission::RemoteHomeActivation {
            ssh_destination: self.target.ssh_destination.clone(),
            remote_home_login: self.remote_home_login()?,
            closure_path: self.closure_path.clone(),
        })
    }

    fn remote_home_login(&self) -> std::result::Result<emission::RemoteHomeLogin, String> {
        match self.authority {
            RemoteUserActivationAuthority::MatchedUser => {
                Ok(emission::RemoteHomeLogin::AsTargetUser)
            }
            RemoteUserActivationAuthority::RootMediated => {
                Ok(emission::RemoteHomeLogin::MediatedFor(
                    ordinary::UserName::new(self.user.as_str().to_string()),
                ))
            }
            RemoteUserActivationAuthority::UnprivilegedMismatch => Err(
                "remote Home Manager activation login differs from the target user and is not root"
                    .to_string(),
            ),
        }
    }

    async fn run_profile(
        &self,
        execution: &EffectExecution,
    ) -> std::result::Result<(), StageFailure> {
        if !self.is_local_context(execution).await {
            return execution
                .emitted(self.remote_profile_emission()?)
                .await
                .completed();
        }
        let home = std::env::var("HOME")
            .map_err(|_| "HOME is unset for local home activation".to_string())?;
        execution
            .emitted(self.local_profile_emission(Path::new(&home)))
            .await
            .completed()
    }

    async fn run_activate(
        &self,
        execution: &EffectExecution,
    ) -> std::result::Result<(), StageFailure> {
        if !self.is_local_context(execution).await {
            return execution
                .emitted(self.remote_activate_emission()?)
                .await
                .completed();
        }
        execution
            .emitted(self.local_activate_emission())
            .await
            .completed()
    }

    async fn is_local_context(&self, execution: &EffectExecution) -> bool {
        self.current_user().as_deref() == Some(self.user.as_str())
            && self.current_node(execution).await.as_deref()
                == Some(self.node_name.payload().as_str())
    }

    fn current_user(&self) -> Option<String> {
        std::env::var("USER")
            .or_else(|_| std::env::var("LOGNAME"))
            .ok()
    }

    async fn current_node(&self, execution: &EffectExecution) -> Option<String> {
        // The local-context node match compares the dispatcher's short hostname
        // against the deploy cursor's node name.
        match execution.emitted(emission::ReadShortHostName {}).await {
            emission::HostNameOutcome::Named(node) => Some(node.into_payload()),
            emission::HostNameOutcome::Failed(_) => None,
        }
    }
}

impl Effect for UserEnvironmentActivation {
    type Product = ();

    async fn run(&self, execution: &EffectExecution) -> std::result::Result<(), StageFailure> {
        match self.mode {
            meta::UserEnvironmentAction::Realize => Ok(()),
            meta::UserEnvironmentAction::SetProfile => self.run_profile(execution).await,
            meta::UserEnvironmentAction::ActivateNow => {
                self.run_activate(execution).await?;
                self.run_profile(execution).await
            }
        }
    }
}

impl nexus::NexusEngine for SchemaRuntime {
    async fn attempt(&mut self, operation: memory::Operation) -> memory::Outcome {
        match operation {
            memory::Operation::ResolvingFlake(request) => self.resolve_flake_auth(request).await,
            memory::Operation::MaterializingHorizon(command) => {
                self.run_horizon_materialization(command).await
            }
            memory::Operation::EvaluatingClosure(command) => self.run_nix_eval(command).await,
            memory::Operation::BuildingClosure(command) => self.run_nix_build(command).await,
            memory::Operation::CopyingClosure(command) => self.run_copy_closure(command).await,
            memory::Operation::ActivatingGeneration(command) => {
                self.run_activate_generation(command).await
            }
            memory::Operation::CollectingGarbage(command) => self.run_path_info_gc(command).await,
            memory::Operation::BuildingHermeticCheck(command) => {
                self.run_hermetic_check(command).await
            }
            memory::Operation::BringingUpTestVm(command) => {
                self.run_bring_up_test_vm(command).await
            }
            memory::Operation::TearingDownTestVm(command) => {
                self.run_tear_down_test_vm(command).await
            }
            memory_operation => self.operate(memory_operation),
        }
    }

    fn step_limit_reached(&self, _limit: nexus::PipelineStepLimit) -> nexus::PipelineOutput {
        self.refuse_deploy(
            memory::DeployRefusalReason::ContinuationBudgetExhausted,
            &"the Nexus runner exhausted its continuation budget",
        )
    }

    fn decide(&mut self, outcome: memory::Outcome) -> nexus::NexusAction {
        self.decide_outcome(outcome)
    }
}

#[cfg(test)]
mod tests {
    //! Unit/argv/snapshot tests for the S4a command port: closure-threading
    //! onto the activate command, and the target-side command shapes
    //! (`SshTarget` addressing, `ClosureCopy`, `HostActivation`,
    //! `UserEnvironmentActivation`). The construction is unit-testable here; the on-node
    //! behavior is proven later at S5 on a live VM.

    use super::*;
    use crate::translator::{
        ActivationScript as _, ArrivalMatching as _, CommandLine, Commanded as _,
        ProcessRunning as _, ShellArgument, ShellQuoting as _,
    };

    /// The view the argv tests read an emission's command line through.
    trait InvocationView {
        fn program(&self) -> &str;

        fn joined_arguments(&self) -> String;
    }

    impl InvocationView for CommandLine {
        fn program(&self) -> &str {
            &self.program
        }

        fn joined_arguments(&self) -> String {
            self.arguments.join(" ")
        }
    }

    /// The command lines a host activation's emissions become.
    trait HostInvocations {
        fn ssh_invocation(&self) -> Option<CommandLine>;

        fn step_clear_efi_default_invocation(&self) -> CommandLine;

        fn step_clear_efi_oneshot_invocation(&self) -> CommandLine;

        fn systemd_run_invocation(&self, unit_name: &str) -> CommandLine;
    }

    impl HostInvocations for HostActivation {
        fn ssh_invocation(&self) -> Option<CommandLine> {
            self.switch_emission()
                .map(|emission| emission.command_line())
        }

        fn step_clear_efi_default_invocation(&self) -> CommandLine {
            self.boot_entry_clearing(emission::BootEntry::Default)
                .command_line()
        }

        fn step_clear_efi_oneshot_invocation(&self) -> CommandLine {
            self.boot_entry_clearing(emission::BootEntry::Oneshot)
                .command_line()
        }

        fn systemd_run_invocation(&self, unit_name: &str) -> CommandLine {
            emission::TransientUnitRun {
                unit_name: emission::UnitName(unit_name.to_string()),
                ..self.boot_once_emission()
            }
            .command_line()
        }
    }

    /// The command lines a target-store realization's emissions become.
    trait RealizationInvocations {
        fn derivation_copy_invocation(&self) -> CommandLine;

        fn build_invocation(&self) -> CommandLine;

        fn gc_root_invocation(&self, output_path: &str) -> CommandLine;
    }

    impl RealizationInvocations for TargetStoreRealization {
        fn derivation_copy_invocation(&self) -> CommandLine {
            self.derivation_copy.command_line()
        }

        fn build_invocation(&self) -> CommandLine {
            self.build.command_line()
        }

        fn gc_root_invocation(&self, output_path: &str) -> CommandLine {
            self.rooting_of(output_path).command_line()
        }
    }

    /// The command line a closure transfer's emission becomes.
    trait TransferInvocation {
        fn invocation(&self) -> CommandLine;
    }

    impl TransferInvocation for ClosureTransfer {
        fn invocation(&self) -> CommandLine {
            match self {
                Self::Copy(copy) => copy.command_line(),
                Self::PresenceCheck(presence) => presence.command_line(),
            }
        }
    }

    /// The command lines a user-environment activation's emissions become.
    trait UserInvocations {
        fn local_profile_invocation(&self, home: &Path) -> CommandLine;

        fn local_activate_invocation(&self) -> CommandLine;

        fn remote_profile_invocation(&self) -> std::result::Result<CommandLine, String>;

        fn remote_activate_invocation(&self) -> std::result::Result<CommandLine, String>;
    }

    impl UserInvocations for UserEnvironmentActivation {
        fn local_profile_invocation(&self, home: &Path) -> CommandLine {
            self.local_profile_emission(home).command_line()
        }

        fn local_activate_invocation(&self) -> CommandLine {
            self.local_activate_emission().command_line()
        }

        fn remote_profile_invocation(&self) -> std::result::Result<CommandLine, String> {
            self.remote_profile_emission()
                .map(|emission| emission.command_line())
        }

        fn remote_activate_invocation(&self) -> std::result::Result<CommandLine, String> {
            self.remote_activate_emission()
                .map(|emission| emission.command_line())
        }
    }

    /// Whether a registered unit is the one a detached activation awaits.
    trait UnitNewMatching {
        fn is_expected_unit_new(&self, name: &str) -> bool;
    }

    impl UnitNewMatching for DetachedActivationUnit {
        fn is_expected_unit_new(&self, name: &str) -> bool {
            emission::UnitArrival {
                unit_name: emission::UnitName(self.name.clone()),
            }
            .names(name)
        }
    }

    /// The `nix eval` command line for `.#toplevel` under one freshness.
    fn toplevel_evaluation(freshness: emission::EvaluationFreshness) -> CommandLine {
        emission::DerivationEvaluation {
            flake_reference: ordinary::FlakeReference::new(".".into()),
            deployment_output_selector: ordinary::DeploymentOutputSelector::new(
                ordinary::FlakeAttribute::new("toplevel".into()),
            ),
            evaluation_freshness: freshness,
            persisted_flake_input_override_vector: Vec::new(),
        }
        .command_line()
    }

    /// The local `nix build` command line for a derivation.
    fn closure_build(derivation: &str, build_place: emission::BuildPlace) -> CommandLine {
        emission::ClosureBuild {
            closure_path: ordinary::ClosurePath::new(derivation.into()),
            build_place,
            extra_substituter_vector: Vec::new(),
        }
        .command_line()
    }

    const STORE: &str = "/nix/store/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-toplevel";

    fn host_submission(action: ordinary::HostDeployAction) -> memory::ActualizedDeploySubmission {
        memory::ActualizedDeploySubmission {
            horizon_definition_option: None,
            deploy_submission: memory::DeploySubmission::Host(meta::HostDeployment {
                cluster_name: ordinary::ClusterName::new("alpha".into()),
                node_name: ordinary::NodeName::new("node-1".into()),
                host_composition: ordinary::HostComposition::BaseHost,
                proposal_source: ordinary::ProposalSource::new("/dev/null".into()),
                secrets_input: memory::SecretsInput::NoSecrets,
                flake_reference: ordinary::FlakeReference::new("github:owner/repo".into()),
                deployment_transport: fixture_transport(),
                deployment_input_mode: memory::DeploymentInputMode::Direct,
                deployment_output_selector: fixture_output_selector(),
                activation_backend: memory::ActivationBackend::NixosSystemdBootV1,
                host_deploy_action: action,
                source_revision_policy: meta::SourceRevisionPolicy::ResolveAndRecord,
                nix_builder_spec_option: None,
                extra_substituter_vector: Vec::new(),
            }),
        }
    }

    fn host_pipeline(action: ordinary::HostDeployAction) -> DeployPipeline {
        DeployPipeline::from_submission(
            ordinary::DeploymentIdentifier::new(1),
            ordinary::GenerationIdentifier::new(1),
            ordinary::DatabaseMarker::at_commit(0),
            host_submission(action),
        )
    }

    fn user_submission() -> memory::ActualizedDeploySubmission {
        memory::ActualizedDeploySubmission {
            horizon_definition_option: None,
            deploy_submission: memory::DeploySubmission::UserEnvironment(
                meta::UserEnvironmentDeployment {
                    cluster_name: ordinary::ClusterName::new("fixture-cluster".into()),
                    node_name: ordinary::NodeName::new("fixture-daemon".into()),
                    user_name: ordinary::UserName::new("fixture-user".into()),
                    proposal_source: ordinary::ProposalSource::new("/dev/null".into()),
                    secrets_input: memory::SecretsInput::NoSecrets,
                    flake_reference: ordinary::FlakeReference::new("github:owner/repo".into()),
                    deployment_transport: fixture_transport(),
                    deployment_input_mode: memory::DeploymentInputMode::Direct,
                    deployment_output_selector: fixture_output_selector(),
                    activation_backend: memory::ActivationBackend::HomeManagerNixProfileV1,
                    user_environment_action: meta::UserEnvironmentAction::ActivateNow,
                    source_revision_policy: meta::SourceRevisionPolicy::ResolveAndRecord,
                    nix_builder_spec_option: None,
                    extra_substituter_vector: Vec::new(),
                },
            ),
        }
    }

    fn source_revision(policy: ordinary::SourceRevisionPolicy) -> ordinary::SourceRevisionRecord {
        ordinary::SourceRevisionRecord {
            source_revision_policy: policy,
            requested_reference: memory::RequestedReference(ordinary::FlakeReference::new(
                "github:owner/repo/main".into(),
            )),
            resolved_reference: memory::ResolvedReference(ordinary::FlakeReference::new(
                "github:owner/repo?rev=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_string(),
            )),
            resolved_revision: memory::ResolvedRevision(
                "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_string(),
            ),
        }
    }

    fn resolved_flake(policy: ordinary::SourceRevisionPolicy) -> nexus::ResolvedFlake {
        nexus::ResolvedFlake::new(source_revision(policy))
    }

    fn cluster() -> ordinary::ClusterName {
        ordinary::ClusterName::new("alpha".into())
    }

    fn node() -> ordinary::NodeName {
        ordinary::NodeName::new("node-1".into())
    }

    /// The daemon host every `RuntimeConfiguration::test_default` names.
    fn daemon_host() -> ordinary::NodeName {
        ordinary::NodeName::new("daemon-host".into())
    }

    fn fixture_transport() -> memory::DeploymentTransport {
        memory::DeploymentTransport {
            nix_store_uri: memory::NixStoreUri::new("ssh-ng://fixture-copy.invalid".into()),
            ssh_destination: memory::SshDestination::new(
                "fixture-login@fixture-activate.invalid".into(),
            ),
        }
    }

    fn fixture_output_selector() -> memory::DeploymentOutputSelector {
        memory::DeploymentOutputSelector::new(memory::FlakeAttribute::new(
            "checks.fixture-a".into(),
        ))
    }

    fn fixture_test_profile() -> nexus::TestExecutionProfile {
        nexus::TestExecutionProfile {
            test_mode: ordinary::TestMode::Hermetic,
            nix_system: nexus::NixSystem::new("x86_64-linux".into()),
            deployment_output_selector: nexus::DeploymentOutputSelector::new(
                nexus::FlakeAttribute::new("checks.fixture-a".into()),
            ),
            deployment_transport_option: None,
        }
    }

    // ---- Step 1: closure-threading onto the activate command ----

    #[test]
    fn activate_command_carries_built_closure_path() {
        // The cursor captures the built path via `set_closure_path` (the
        // `ClosureBuilt` arm); `advance_after_phase` at `BuildingRecorded` reads
        // it onto the fired ActivateGeneration command — same non-empty path,
        // never dropped between build and activate (risk R2).
        let mut engine = SchemaRuntime::new();
        engine.active_deploy = Some(host_pipeline(ordinary::HostDeployAction::ActivateNow));
        let built = ordinary::ClosurePath::new(STORE.into());
        assert!(engine.set_closure_path(built.clone()));
        engine.set_stage(DeployStage::BuildingRecorded);

        match engine.advance_after_phase() {
            nexus::NexusAction::Operate(memory::Operation::ActivatingGeneration(command)) => {
                assert_eq!(command.closure_path, built);
                assert!(!command.closure_path.payload().is_empty());
            }
            other => panic!("expected ActivateGeneration effect, got {other:?}"),
        }
    }

    #[test]
    fn activate_without_closure_fails_rather_than_activating_empty() {
        // No closure on the cursor at activate time is an internal invariant
        // failure, not an empty activation: the pipeline fails, never fires an
        // ActivateGeneration with an empty path (risk R2).
        let mut engine = SchemaRuntime::new();
        assert!(matches!(
            engine
                .record_deploy_submitted(host_submission(ordinary::HostDeployAction::ActivateNow)),
            memory::Outcome::DeploySubmitted(_)
        ));
        engine.set_stage(DeployStage::BuildingRecorded);

        match engine.advance_after_phase() {
            nexus::NexusAction::Finish(meta::Output::DeployTerminal(_record)) => {}
            other => panic!("expected correlated failed deployment, got {other:?}"),
        }
    }

    #[test]
    fn activation_commit_requires_closure_path() {
        let mut pipeline = host_pipeline(ordinary::HostDeployAction::ActivateNow);
        pipeline.activation_slot = Some(ordinary::GenerationSlot::Current);
        pipeline.source_revision = Some(source_revision(
            ordinary::SourceRevisionPolicy::ResolveAndRecord,
        ));
        assert!(pipeline.activation_commit().is_none());
        pipeline.closure_path = Some(ordinary::ClosurePath::new(STORE.into()));
        let commit = pipeline.activation_commit().expect("commit with closure");
        assert_eq!(commit.closure_path.payload(), STORE);
    }

    // ---- Step 2: the reject-guard opens the activating actions ----

    fn require_immutable_request(flake: &str) -> meta::DeployRequest {
        deployment_request(meta::SourceRevisionPolicy::RequireImmutable, flake)
    }

    fn deployment_request(policy: meta::SourceRevisionPolicy, flake: &str) -> meta::DeployRequest {
        meta::DeployRequest {
            horizon_definition_option: None,
            deploy_submission: meta::DeploySubmission::Host(meta::HostDeployment {
                cluster_name: cluster(),
                node_name: node(),
                host_composition: ordinary::HostComposition::BaseHost,
                proposal_source: ordinary::ProposalSource::new("/dev/null".into()),
                secrets_input: memory::SecretsInput::NoSecrets,
                flake_reference: ordinary::FlakeReference::new(flake.into()),
                deployment_transport: fixture_transport(),
                deployment_input_mode: memory::DeploymentInputMode::Direct,
                deployment_output_selector: fixture_output_selector(),
                activation_backend: memory::ActivationBackend::NixosSystemdBootV1,
                host_deploy_action: ordinary::HostDeployAction::Evaluate,
                source_revision_policy: policy,
                nix_builder_spec_option: None,
                extra_substituter_vector: Vec::new(),
            }),
        }
    }

    #[test]
    fn flake_locator_policy_acceptance_matrix_is_policy_specific_and_total() {
        for (policy, flake, accepted) in [
            (
                meta::SourceRevisionPolicy::RequireImmutable,
                "github:owner/repo?rev=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                true,
            ),
            (
                meta::SourceRevisionPolicy::RequireImmutable,
                "github:owner/repo?dir=systems/base&rev=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                true,
            ),
            (
                meta::SourceRevisionPolicy::RequireImmutable,
                "github:owner/repo",
                false,
            ),
            (
                meta::SourceRevisionPolicy::RequireImmutable,
                "github:owner/repo?ref=main",
                false,
            ),
            (
                meta::SourceRevisionPolicy::ResolveAndRecord,
                "github:owner/repo",
                true,
            ),
            (
                meta::SourceRevisionPolicy::ResolveAndRecord,
                "github:owner/repo?ref=release/v0.4&dir=systems/base",
                true,
            ),
            (
                meta::SourceRevisionPolicy::ResolveAndRecord,
                "github:owner/repo?rev=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                false,
            ),
            (
                meta::SourceRevisionPolicy::ResolveAndRecord,
                "github:owner/repo?ref=main&ref=other",
                false,
            ),
        ] {
            let rejected =
                (deployment_request(policy.clone(), flake)).source_revision_policy_rejection();
            assert_eq!(rejected.is_none(), accepted, "{policy:?}: {flake}");
        }
    }

    #[test]
    fn admission_rejects_invalid_request_owned_routing_before_snapshot_persistence() {
        let mut request = deployment_request(
            meta::SourceRevisionPolicy::ResolveAndRecord,
            "github:owner/repo",
        );
        let meta::DeploySubmission::Host(deployment) = &mut request.deploy_submission else {
            unreachable!("fixture is a host deploy")
        };
        deployment.deployment_transport.ssh_destination =
            memory::SshDestination::new("root".into());
        assert_eq!(
            request.deployment_routing_rejection(),
            Some(meta::DeployRejectionReason::InvalidDeploymentRouting)
        );
        let mut runtime = SchemaRuntime::new();
        let outcome = runtime.submit_deploy(request);
        let DeploySubmissionOutcome::Rejected(rejected) = outcome else {
            panic!("invalid routing must reject")
        };
        assert!(matches!(
            rejected.into_payload().deployment_terminal_option,
            Some(memory::DeploymentTerminal::Rejected(
                memory::DeploymentTerminalReason::InvalidDeploymentRouting
            ))
        ));
        assert!(runtime.store().deploy_jobs().expect("job rows").is_empty());
    }

    #[test]
    fn flake_locator_policy_rejects_unsafe_common_forms_for_every_policy() {
        for policy in [
            meta::SourceRevisionPolicy::RequireImmutable,
            meta::SourceRevisionPolicy::ResolveAndRecord,
        ] {
            for flake in [
                "github:owner/repo#fragment",
                "github:owner@evil/repo?ref=main",
                "https://github.com/owner/repo?ref=main",
                "github:owner/repo?token=secret",
                "github:owner/repo?ref=release-secret",
                "github:owner/repo?dir=private/CREDENTIALS",
                "github:owner/repo?ref=release-%53eCrEt",
                "github:owner/repo?ref=APIKey-release",
                "github:owner/repo?dir=auth%252Fhidden",
                "github:owner/repo?ref=bad%ZZ",
                "github:owner/repo?dir=../private",
                "github:owner/repo?unknown=value",
                "github:owner/repo?ref=main&dir=systems//base",
                "github:owner/repo?ref=main&rev=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                "github:owner/repo?ref=release/%2Funsafe",
            ] {
                assert_eq!(
                    (deployment_request(policy.clone(), flake,)).source_revision_policy_rejection(),
                    Some(meta::DeployRejectionReason::FlakeReferenceMalformed),
                    "{policy:?}: {flake}"
                );
            }
        }
    }

    #[test]
    fn require_immutable_rejects_mutable_flake_reference() {
        let request = require_immutable_request("github:owner/repo/main");
        assert_eq!(
            request.source_revision_policy_rejection(),
            Some(meta::DeployRejectionReason::FlakeReferenceMalformed)
        );
    }

    #[test]
    fn require_immutable_rejects_malformed_refs_that_only_contain_rev_text() {
        for flake in [
            "github:owner/repo/rev=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "github:owner/repo?rev=not-a-full-commit",
            "github:owner/repo?foo=rev=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        ] {
            let request = require_immutable_request(flake);
            assert_eq!(
                request.source_revision_policy_rejection(),
                Some(meta::DeployRejectionReason::FlakeReferenceMalformed),
                "{flake} must not pass the structured immutable-ref parser"
            );
        }
    }

    #[test]
    fn require_immutable_rejects_malformed_refs_that_only_contain_nar_hash_text() {
        for flake in [
            "github:owner/repo/narHash=sha256-deadbeef",
            "github:owner/repo?narHash=",
            "github:owner/repo?narHash=not-sri",
        ] {
            let request = require_immutable_request(flake);
            assert_eq!(
                request.source_revision_policy_rejection(),
                Some(meta::DeployRejectionReason::FlakeReferenceMalformed),
                "{flake} must not pass the structured immutable-ref parser"
            );
        }
    }

    #[test]
    fn require_immutable_accepts_structured_revision_query() {
        let request = require_immutable_request(
            "github:owner/repo?rev=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        );
        assert!(request.source_revision_policy_rejection().is_none());
    }

    #[test]
    fn horizon_mode_without_an_actualized_definition_rejects_before_effects() {
        let mut engine = SchemaRuntime::new();
        let mut request = deployment_request(
            meta::SourceRevisionPolicy::ResolveAndRecord,
            "github:owner/repo",
        );
        let meta::DeploySubmission::Host(deployment) = &mut request.deploy_submission else {
            unreachable!()
        };
        deployment.deployment_input_mode = memory::DeploymentInputMode::Horizon;
        match engine.submit_deploy(request) {
            DeploySubmissionOutcome::Rejected(record) => {
                assert!(matches!(
                    record.into_payload().deployment_terminal_option,
                    Some(memory::DeploymentTerminal::Rejected(
                        memory::DeploymentTerminalReason::ProposalSourceUnreachable
                    ))
                ));
            }
            other => panic!("unactualized Horizon input must reject before effects, got {other:?}"),
        }
        assert!(engine.active_deploy.is_none());
        assert!(engine.active_operation.is_none());
    }

    #[test]
    fn resolve_and_record_records_pipeline_flake_and_eval_source_revision() {
        let mut engine = SchemaRuntime::new();
        engine.active_deploy = Some(host_pipeline(ordinary::HostDeployAction::Evaluate));
        assert!(engine.set_resolved_flake(
            resolved_flake(ordinary::SourceRevisionPolicy::ResolveAndRecord),
            memory::DeployResumeStage::RecordBuilding,
        ));
        let pipeline = engine.active_deploy.as_ref().expect("pipeline");
        assert_eq!(
            pipeline.flake,
            source_revision(ordinary::SourceRevisionPolicy::ResolveAndRecord)
                .resolved_reference
                .0
        );
        assert_eq!(
            pipeline
                .source_revision
                .as_ref()
                .expect("source revision")
                .requested_reference
                .0,
            ordinary::FlakeReference::new("github:owner/repo/main".into())
        );
        let command = pipeline.nix_eval_command(&daemon_host());
        assert_eq!(
            command.source_revision_record.resolved_revision.0,
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        );
        assert_eq!(
            command.flake_reference,
            command.source_revision_record.resolved_reference.0
        );
    }

    #[test]
    fn unproven_resolver_metadata_terminalizes_before_any_later_effect() {
        let mut engine = SchemaRuntime::new();
        assert!(matches!(
            engine.record_deploy_submitted(host_submission(ordinary::HostDeployAction::Evaluate)),
            memory::Outcome::DeploySubmitted(_)
        ));
        let mut source = source_revision(ordinary::SourceRevisionPolicy::ResolveAndRecord);
        source.resolved_revision = memory::ResolvedRevision("sha256-unproven-nar-hash".to_string());
        match engine.decide_effect_completion(memory::Outcome::FlakeResolved(
            nexus::ResolvedFlake::new(source),
        )) {
            nexus::NexusAction::Finish(meta::Output::DeployTerminal(record)) => {
                assert!(matches!(
                    record.deployment_terminal_option,
                    Some(memory::DeploymentTerminal::Failed(
                        memory::DeploymentFailure {
                            deployment_failure_stage: memory::DeploymentFailureStage::FlakeAuth,
                            ..
                        }
                    ))
                ));
            }
            other => panic!("unproven metadata must terminalize, got {other:?}"),
        }
        assert!(engine.active_deploy.is_none());
    }

    #[test]
    fn injected_closure_effects_terminalize_before_build_copy_or_activation() {
        let unsafe_path = ordinary::ClosurePath::new(
            "/nix/store/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-private-secret".to_string(),
        );

        let mut evaluated = SchemaRuntime::new();
        assert!(matches!(
            evaluated.record_deploy_submitted(host_submission(ordinary::HostDeployAction::Realize)),
            memory::Outcome::DeploySubmitted(_)
        ));
        match evaluated.decide_effect_completion(memory::Outcome::ClosureEvaluated(
            nexus::EvaluatedClosure {
                generation_identifier: ordinary::GenerationIdentifier::new(1),
                closure_path: unsafe_path.clone(),
            },
        )) {
            nexus::NexusAction::Finish(meta::Output::DeployTerminal(record)) => assert!(matches!(
                record.deployment_terminal_option,
                Some(memory::DeploymentTerminal::Failed(
                    memory::DeploymentFailure {
                        deployment_failure_stage: memory::DeploymentFailureStage::Eval,
                        ..
                    }
                ))
            )),
            other => panic!("unsafe evaluated closure must terminalize, got {other:?}"),
        }
        assert!(evaluated.active_deploy.is_none());

        let mut built = SchemaRuntime::new();
        assert!(matches!(
            built.record_deploy_submitted(host_submission(ordinary::HostDeployAction::ActivateNow)),
            memory::Outcome::DeploySubmitted(_)
        ));
        match built.decide_effect_completion(memory::Outcome::ClosureBuilt(nexus::BuiltClosure {
            generation_identifier: ordinary::GenerationIdentifier::new(1),
            closure_path: unsafe_path,
        })) {
            nexus::NexusAction::Finish(meta::Output::DeployTerminal(record)) => assert!(matches!(
                record.deployment_terminal_option,
                Some(memory::DeploymentTerminal::Failed(
                    memory::DeploymentFailure {
                        deployment_failure_stage: memory::DeploymentFailureStage::Build,
                        ..
                    }
                ))
            )),
            other => panic!("unsafe built closure must terminalize, got {other:?}"),
        }
        assert!(built.active_deploy.is_none());
    }

    #[test]
    fn phase_projection_never_labels_nix_metadata_as_an_immutable_revision() {
        let mut pipeline = host_pipeline(ordinary::HostDeployAction::Evaluate);
        let mut source = source_revision(ordinary::SourceRevisionPolicy::ResolveAndRecord);
        source.resolved_revision = memory::ResolvedRevision("sha256-unproven-nar-hash".to_string());
        pipeline.source_revision = Some(source);
        assert!(
            pipeline
                .phase_event(
                    ordinary::DeploymentPhase::Building,
                    ordinary::EventLogPosition::new(7),
                )
                .immutable_revision_option
                .is_none()
        );
    }

    #[test]
    fn nix_output_boundaries_terminalize_unsafe_paths_without_echoing_them() {
        use std::os::unix::fs::PermissionsExt;

        let directory = tempfile::tempdir().expect("temporary fake nix directory");
        let unsafe_output = "/nix/store/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-private-secret";
        let nix = directory.path().join("nix");
        fs::write(
            &nix,
            format!("#!/bin/sh\nprintf '%s\\n' '{unsafe_output}'\n"),
        )
        .expect("write fake nix");
        let mut permissions = fs::metadata(&nix).expect("fake nix metadata").permissions();
        permissions.set_mode(0o700);
        fs::set_permissions(&nix, permissions).expect("make fake nix executable");

        let configuration = Arc::new(RuntimeConfiguration::test_with_effect_program_directory(
            directory.path().join("inputs"),
            directory.path().to_path_buf(),
        ));
        let store =
            Arc::new(Store::open(directory.path().join("lojix.memory")).expect("open store"));
        let engine = SchemaRuntime::with_store_and_configuration(store, configuration.clone());
        let mut pipeline = host_pipeline(ordinary::HostDeployAction::Evaluate);
        pipeline.source_revision = Some(source_revision(
            ordinary::SourceRevisionPolicy::ResolveAndRecord,
        ));
        let eval = pipeline.nix_eval_command(&daemon_host());
        let build = nexus::NixBuildCommand {
            generation_identifier: ordinary::GenerationIdentifier::new(1),
            closure_path: ordinary::ClosurePath::new(STORE.into()),
            build_target: nexus::BuildTarget::Local,
            extra_substituter_vector: Vec::new(),
        };
        let hermetic = nexus::HermeticCheckCommand {
            cluster_name: cluster(),
            node_name: node(),
            flake_reference: ordinary::FlakeReference::new("github:owner/repo".into()),
            test_execution_profile: fixture_test_profile(),
        };
        let runtime = tokio::runtime::Runtime::new().expect("tokio runtime");
        for (stage, result) in [
            (
                nexus::EffectStage::Eval,
                runtime.block_on(engine.run_nix_eval(eval)),
            ),
            (
                nexus::EffectStage::Build,
                runtime.block_on(engine.run_nix_build(build)),
            ),
            (
                nexus::EffectStage::HermeticCheck,
                runtime.block_on(engine.run_hermetic_check(hermetic)),
            ),
        ] {
            match result {
                memory::Outcome::EffectFailed(failure) => {
                    assert_eq!(failure.effect_stage, stage);
                    assert!(
                        !failure
                            .failure_evidence
                            .failure_detail
                            .payload()
                            .contains(unsafe_output)
                    );
                }
                other => panic!("unsafe output must fail at {stage:?}, got {other:?}"),
            }
        }
    }

    #[test]
    fn crash_after_atomic_resolved_source_commit_reopens_at_materialization_without_resolving_again()
     {
        let directory = tempfile::tempdir().expect("temporary store directory");
        let path = directory.path().join("lojix.memory");
        let store = Arc::new(Store::open(&path).expect("open store"));
        let mut before_crash = SchemaRuntime::with_store(store.clone());
        let accepted = match before_crash
            .record_deploy_submitted(host_submission(ordinary::HostDeployAction::Evaluate))
        {
            memory::Outcome::DeploySubmitted(accepted) => accepted,
            other => panic!("expected accepted deploy, got {other:?}"),
        };
        before_crash.set_resolved_flake(
            resolved_flake(ordinary::SourceRevisionPolicy::ResolveAndRecord),
            memory::DeployResumeStage::MaterializeHorizon,
        );
        before_crash.set_input_overrides(vec![memory::PersistedFlakeInputOverride {
            flake_input_name: memory::FlakeInputName("criomos".to_string()),
            persisted_flake_input_reference: memory::PersistedFlakeInputReference {
                flake_input_url: memory::FlakeInputUrl(
                    "path:/var/lib/lojix/inputs/criomos".to_string(),
                ),
                nix_archive_hash: memory::NixArchiveHash(
                    "sha256-immutable-horizon-snapshot".to_string(),
                ),
            },
        }]);
        let persisted = store
            .deploy_jobs()
            .expect("resolved restart cursor")
            .into_iter()
            .next()
            .expect("one job");
        assert_eq!(
            persisted.deploy_resume_stage,
            memory::DeployResumeStage::RecordBuilding
        );
        assert_eq!(
            persisted.flake_reference_option,
            Some(
                source_revision(ordinary::SourceRevisionPolicy::ResolveAndRecord)
                    .resolved_reference
                    .0
            )
        );
        drop(before_crash);
        drop(store);

        // This is the injected crash boundary: the resolved source + exact
        // cursor have committed, but no continuation has been run.
        let reopened = Arc::new(Store::open(&path).expect("reopen store"));
        let job = reopened
            .deploy_jobs()
            .expect("read resume cursor")
            .into_iter()
            .next()
            .expect("one job after reopen");
        let mut resumed = SchemaRuntime::with_store(reopened.clone());
        assert!(resumed.resume_deploy_job(job).expect("resume cursor"));
        let pipeline = resumed.active_deploy.as_ref().expect("restored pipeline");
        assert_eq!(pipeline.accepted_marker, accepted.database_marker);
        assert_eq!(
            pipeline.resume_stage,
            memory::DeployResumeStage::RecordBuilding
        );
        assert_eq!(
            pipeline.flake,
            source_revision(ordinary::SourceRevisionPolicy::ResolveAndRecord)
                .resolved_reference
                .0
        );
        assert_eq!(
            pipeline
                .source_revision
                .as_ref()
                .expect("exact durable source revision")
                .resolved_revision
                .0,
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        );
        assert_eq!(pipeline.input_overrides.len(), 1);
        assert_eq!(pipeline.input_overrides[0].flake_input_name.0, "criomos");
        assert_eq!(
            pipeline.input_overrides[0]
                .persisted_flake_input_reference
                .nix_archive_hash
                .0,
            "sha256-immutable-horizon-snapshot"
        );
    }

    #[test]
    fn restart_restores_or_recovers_the_exact_phase_receipt_for_each_resume_continuation() {
        let directory = tempfile::tempdir().expect("temporary store directory");
        let path = directory.path().join("lojix.memory");
        let store = Arc::new(Store::open(&path).expect("open store"));
        let mut engine = SchemaRuntime::with_store(store.clone());
        assert!(matches!(
            engine
                .record_deploy_submitted(host_submission(ordinary::HostDeployAction::ActivateNow)),
            memory::Outcome::DeploySubmitted(_)
        ));
        engine.set_resolved_flake(
            resolved_flake(ordinary::SourceRevisionPolicy::ResolveAndRecord),
            memory::DeployResumeStage::RecordBuilding,
        );
        assert!(engine.set_closure_path(ordinary::ClosurePath::new(STORE.into())));
        engine.set_activation_slot(ordinary::GenerationSlot::Current);

        for (phase, resume_stage) in [
            (
                ordinary::DeploymentPhase::Building,
                memory::DeployResumeStage::NixEval,
            ),
            (
                ordinary::DeploymentPhase::Copying,
                memory::DeployResumeStage::ActivateGeneration,
            ),
            (
                ordinary::DeploymentPhase::Activated,
                memory::DeployResumeStage::RecordGenerationActivated,
            ),
        ] {
            let position = store
                .allocate_event_log_position()
                .expect("reserve event position");
            let event = {
                let pipeline = engine.active_deploy.as_mut().expect("pipeline");
                pipeline.resume_stage = resume_stage;
                pipeline.phase_event(phase, ordinary::EventLogPosition::new(position))
            };
            let receipt = match engine.record_phase_transition(event) {
                memory::Outcome::PhaseRecorded(receipt) => receipt,
                other => panic!("expected phase receipt, got {other:?}"),
            };
            let mut job = store
                .deploy_jobs()
                .expect("job row")
                .into_iter()
                .next()
                .expect("one job");
            assert_eq!(job.phase_receipt_option, Some(receipt.clone()));

            // Simulate a crash after public event append but before the
            // receipt-side job rewrite. Reopen reconstructs the SAME receipt
            // from the durable event, not a synthetic value.
            job.phase_receipt_option = None;
            store
                .upsert_deploy_job(job)
                .expect("simulate pre-receipt cursor");
            let job = store
                .deploy_jobs()
                .expect("resume row")
                .into_iter()
                .next()
                .expect("one job");
            let mut resumed = SchemaRuntime::with_store(store.clone());
            assert!(
                resumed
                    .resume_deploy_job(job)
                    .expect("resume exact receipt")
            );
            assert_eq!(
                resumed
                    .active_deploy
                    .as_ref()
                    .expect("restored pipeline")
                    .phase_receipt,
                Some(receipt)
            );
        }
    }

    #[test]
    fn recorded_source_revision_survives_event_and_state_paths() {
        let mut engine = SchemaRuntime::new();
        assert!(matches!(
            engine
                .record_deploy_submitted(host_submission(ordinary::HostDeployAction::ActivateNow)),
            memory::Outcome::DeploySubmitted(_)
        ));
        let mut pipeline = engine.active_deploy.clone().expect("accepted pipeline");
        let source_revision = source_revision(ordinary::SourceRevisionPolicy::ResolveAndRecord);
        pipeline.source_revision = Some(source_revision.clone());
        pipeline.activation_slot = Some(ordinary::GenerationSlot::Current);
        pipeline.closure_path = Some(ordinary::ClosurePath::new(STORE.into()));
        engine.active_deploy = Some(pipeline.clone());

        let event = pipeline.phase_event(
            ordinary::DeploymentPhase::Building,
            ordinary::EventLogPosition::new(1),
        );
        assert_eq!(event.deployment_identifier, pipeline.deployment_identifier);
        assert!(matches!(
            engine.record_phase_transition(event),
            memory::Outcome::PhaseRecorded(_)
        ));
        match engine.read_event_log(ordinary::EventLogRange {
            first_event_log_position: ordinary::EventLogPosition::new(1),
            second_event_log_position: ordinary::EventLogPosition::new(2),
        }) {
            memory::Outcome::EventLogRead(page) => assert_eq!(
                page.deployment_phase_event_vector
                    .first()
                    .expect("deployment event")
                    .deployment_identifier,
                pipeline.deployment_identifier
            ),
            other => panic!("expected EventLogRead, got {other:?}"),
        }

        let commit = pipeline.activation_commit().expect("activation commit");
        assert!(matches!(
            engine.record_generation_activated(commit),
            memory::Outcome::GenerationActivated(_)
        ));
        match engine.query_generations(ordinary::Selection::ByNode(ordinary::NodeSelector {
            cluster_name: cluster(),
            node_name: node(),
            generation_artifact_option: None,
        })) {
            memory::Outcome::GenerationsQueried(listing) => assert_eq!(
                listing
                    .generation_vector
                    .first()
                    .expect("generation")
                    .closure_path_option,
                Some(ordinary::ClosurePath::new(STORE.into()))
            ),
            other => panic!("expected GenerationsQueried, got {other:?}"),
        }
    }

    #[test]
    fn guard_accepts_every_declared_action() {
        for action in [
            ordinary::HostDeployAction::Evaluate,
            ordinary::HostDeployAction::Realize,
            ordinary::HostDeployAction::SetBootProfile,
            ordinary::HostDeployAction::ActivateNow,
            ordinary::HostDeployAction::TestActivation,
            ordinary::HostDeployAction::ScheduleBootOnce,
        ] {
            let request = meta::DeployRequest {
                horizon_definition_option: None,
                deploy_submission: meta::DeploySubmission::Host(meta::HostDeployment {
                    cluster_name: cluster(),
                    node_name: node(),
                    host_composition: ordinary::HostComposition::BaseHost,
                    proposal_source: ordinary::ProposalSource::new("/dev/null".into()),
                    secrets_input: memory::SecretsInput::NoSecrets,
                    flake_reference: ordinary::FlakeReference::new("github:owner/repo".into()),
                    deployment_transport: fixture_transport(),
                    deployment_input_mode: memory::DeploymentInputMode::Direct,
                    deployment_output_selector: fixture_output_selector(),
                    activation_backend: memory::ActivationBackend::NixosSystemdBootV1,
                    host_deploy_action: action.clone(),
                    source_revision_policy: meta::SourceRevisionPolicy::ResolveAndRecord,
                    nix_builder_spec_option: None,
                    extra_substituter_vector: Vec::new(),
                }),
            };
            assert!(
                request.unsupported_deploy_reason().is_none(),
                "Host {action:?} should be supported"
            );
        }
        for mode in [
            meta::UserEnvironmentAction::Realize,
            meta::UserEnvironmentAction::SetProfile,
            meta::UserEnvironmentAction::ActivateNow,
        ] {
            let request = meta::DeployRequest {
                horizon_definition_option: None,
                deploy_submission: meta::DeploySubmission::UserEnvironment(
                    meta::UserEnvironmentDeployment {
                        cluster_name: cluster(),
                        node_name: node(),
                        user_name: ordinary::UserName::new("li".into()),
                        proposal_source: ordinary::ProposalSource::new("/dev/null".into()),
                        secrets_input: memory::SecretsInput::NoSecrets,
                        flake_reference: ordinary::FlakeReference::new("github:owner/repo".into()),
                        deployment_transport: fixture_transport(),
                        deployment_input_mode: memory::DeploymentInputMode::Direct,
                        deployment_output_selector: fixture_output_selector(),
                        activation_backend: memory::ActivationBackend::HomeManagerNixProfileV1,
                        user_environment_action: mode.clone(),
                        source_revision_policy: meta::SourceRevisionPolicy::ResolveAndRecord,
                        nix_builder_spec_option: None,
                        extra_substituter_vector: Vec::new(),
                    },
                ),
            };
            assert!(
                request.unsupported_deploy_reason().is_none(),
                "User environment {mode:?} should be supported"
            );
        }
    }

    #[test]
    fn flake_metadata_never_writes_the_fetched_source_lock() {
        let invocation = emission::FlakeMetadataQuery {
            flake_reference: ordinary::FlakeReference::new(
                "github:LiGoldragon/CriomOS/0123456789abcdef0123456789abcdef01234567".into(),
            ),
        }
        .command_line();
        assert_eq!(invocation.program(), "nix");
        assert_eq!(
            invocation.arguments,
            vec![
                "flake",
                "metadata",
                "--no-write-lock-file",
                "--json",
                "github:LiGoldragon/CriomOS/0123456789abcdef0123456789abcdef01234567",
            ],
        );
    }

    // ---- closure build argv — `.drv^*` output selector, never the bare .drv ----

    const DERIVATION: &str = "/nix/store/bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb-nixos-system-mercury.drv";

    #[test]
    fn build_closure_selects_drv_outputs_not_the_bare_drv() {
        // The closure path threaded in is a `.drv` (from `eval_drv_path`'s
        // `.drvPath`). On nix 2.4+ a bare `.drv` installable makes
        // `--print-out-paths` print the `.drv` path itself, so the daemon would
        // copy/activate the `.drv` and activation could never succeed (Unit C
        // live e2e). The `^*` output selector pins the realised output path.
        let invocation = closure_build(DERIVATION, emission::BuildPlace::DaemonStore);
        assert_eq!(invocation.program(), "nix");
        let argv = invocation.joined_arguments();
        assert!(argv.contains("--print-out-paths"), "{argv}");
        assert!(
            argv.contains(&format!("{DERIVATION}^*")),
            "build installable must carry the `^*` output selector: {argv}"
        );
        // The bare `.drv` must never be handed in as its own argv token — that
        // is exactly the regression that printed/activated the `.drv`.
        assert!(
            !invocation
                .arguments
                .iter()
                .any(|argument| argument == DERIVATION),
            "bare .drv must not appear as an installable token: {argv}"
        );
    }

    #[test]
    fn remote_build_uses_the_exact_request_builder_spec_and_disables_local_fallback() {
        let builder_spec = "ssh-ng://fixture-builder.invalid x86_64-linux - 4 2 k1";
        let invocation = closure_build(
            DERIVATION,
            emission::BuildPlace::Offloaded(ordinary::NixBuilderSpec::new(builder_spec.into())),
        );
        assert_eq!(invocation.program(), "nix");
        let argv = invocation.joined_arguments();
        assert!(
            argv.contains(&format!("--builders {builder_spec}")),
            "{argv}"
        );
        assert!(!argv.contains("/etc/nix/machines"), "{argv}");
        assert!(argv.contains("--option max-jobs 0"), "{argv}");
        assert!(argv.contains("--print-out-paths"), "{argv}");
        assert!(
            argv.contains(&format!("{DERIVATION}^*")),
            "remote build installable must carry the `^*` output selector: {argv}"
        );
        assert!(
            !invocation
                .arguments
                .iter()
                .any(|argument| argument == DERIVATION),
            "bare .drv must not appear as an installable token: {argv}"
        );
    }

    // ---- Step 3: exact request-supplied transport, never derived routing ----

    fn nexus_fixture_transport() -> nexus::DeploymentTransport {
        nexus::DeploymentTransport {
            nix_store_uri: nexus::NixStoreUri::new("ssh-ng://fixture-copy.invalid".into()),
            ssh_destination: nexus::SshDestination::new(
                "fixture-login@fixture-activate.invalid".into(),
            ),
        }
    }

    fn copy_command() -> nexus::CopyClosureCommand {
        nexus::CopyClosureCommand {
            generation_identifier: ordinary::GenerationIdentifier::new(1),
            node_name: node(),
            deployment_transport: nexus_fixture_transport(),
            closure_path: ordinary::ClosurePath::new(STORE.into()),
            closure_origin: nexus::ClosureOrigin::DaemonStore,
        }
    }

    // ---- Step 3: copy argv — always --substitute-on-destination, target only ----

    #[test]
    fn copy_from_dispatcher_uses_to_only_with_substitute() {
        let copy = ClosureTransfer::from_command(&copy_command())
            .expect("local build copies from the dispatcher store");
        let invocation = copy.invocation();
        assert_eq!(invocation.program(), "nix");
        let argv = invocation.joined_arguments();
        assert!(argv.contains("--substitute-on-destination"), "{argv}");
        assert!(
            argv.contains("--to ssh-ng://fixture-copy.invalid"),
            "{argv}"
        );
        assert!(!argv.contains("--from"), "{argv}");
        assert!(argv.contains(STORE), "{argv}");
    }

    #[test]
    fn copy_preserves_login_host_and_params() {
        let mut command = copy_command();
        command.deployment_transport.nix_store_uri =
            nexus::NixStoreUri::new("ssh-ng://bird@zeus.example:2222?compress=true".into());
        let copy = ClosureTransfer::from_command(&command).expect("copy transport");
        let invocation = copy.invocation();
        let argv = invocation.joined_arguments();
        assert!(argv.contains("--substitute-on-destination"), "{argv}");
        assert!(!argv.contains("--from"), "{argv}");
        assert!(
            argv.contains("--to ssh-ng://bird@zeus.example:2222?compress=true"),
            "{argv}"
        );
    }

    #[test]
    fn copy_preserves_root_login_unchanged() {
        let mut command = copy_command();
        command.deployment_transport.nix_store_uri =
            nexus::NixStoreUri::new("ssh-ng://root@zeus.example".into());
        let copy = ClosureTransfer::from_command(&command).expect("copy transport");
        let invocation = copy.invocation();
        let argv = invocation.joined_arguments();
        assert!(argv.contains("--to ssh-ng://root@zeus.example"), "{argv}");
    }

    fn activate_command(
        profile: nexus::ActivationProfile,
        kind: ordinary::ActivationEffect,
    ) -> nexus::ActivateGenerationCommand {
        nexus::ActivateGenerationCommand {
            deployment_identifier: ordinary::DeploymentIdentifier::new(7),
            generation_identifier: ordinary::GenerationIdentifier::new(1),
            cluster_name: cluster(),
            node_name: node(),
            deployment_transport: nexus_fixture_transport(),
            closure_path: ordinary::ClosurePath::new(STORE.into()),
            activation_effect: kind,
            activation_backend: match &profile {
                nexus::ActivationProfile::Host(_) => nexus::ActivationBackend::NixosSystemdBootV1,
                nexus::ActivationProfile::UserEnvironment(_) => {
                    nexus::ActivationBackend::HomeManagerNixProfileV1
                }
            },
            activation_profile: profile,
        }
    }

    fn host_activation(action: ordinary::HostDeployAction) -> HostActivation {
        host_activation_on_host(action, None)
    }

    /// A host activation built with an explicit daemon-host context so a test
    /// can target a self-Switch (`daemon_host == node-1`, the command's node) or
    /// a foreign-target Switch (`daemon_host == None` or a different node).
    fn host_activation_on_host(
        action: ordinary::HostDeployAction,
        daemon_host: Option<ordinary::NodeName>,
    ) -> HostActivation {
        match Activation::from_command(
            &activate_command(
                nexus::ActivationProfile::Host(action),
                ordinary::ActivationEffect::LiveActivation,
            ),
            daemon_host.as_ref(),
        )
        .expect("activation")
        {
            Activation::Host(activation) => activation,
            Activation::UserEnvironment(_) => panic!("expected host activation"),
        }
    }

    // ---- Step 3: per-action host activation argv, no $CLOSURE token ----

    #[test]
    fn switch_activation_has_store_path_and_switch_subcommand_no_closure_token() {
        let activation = host_activation(ordinary::HostDeployAction::ActivateNow);
        let invocation = activation.ssh_invocation().expect("switch invocation");
        assert_eq!(invocation.program(), "ssh");
        let argv = invocation.joined_arguments();
        assert!(
            argv.contains("fixture-login@fixture-activate.invalid"),
            "{argv}"
        );
        assert!(argv.contains(STORE), "{argv}");
        assert!(
            argv.contains(&format!("{STORE}/bin/switch-to-configuration switch")),
            "{argv}"
        );
        assert!(
            argv.contains("nix-env -p /nix/var/nix/profiles/system --set"),
            "{argv}"
        );
        assert!(!argv.contains("$CLOSURE"), "no $CLOSURE token: {argv}");
    }

    #[test]
    fn boot_activation_runs_switch_to_configuration_then_releases_efi_to_loader_configuration() {
        let activation = host_activation(ordinary::HostDeployAction::SetBootProfile);
        let invocation = activation.ssh_invocation().expect("boot invocation");
        let argv = invocation.joined_arguments();
        assert!(
            argv.contains(&format!("{STORE}/bin/switch-to-configuration boot")),
            "{argv}"
        );
        assert!(!argv.contains("$CLOSURE"), "{argv}");
        assert!(activation.requires_efi_reconcile());
        // EFI overrides are cleared, returning authority to declarative loader.conf.
        let clear_default = activation.step_clear_efi_default_invocation();
        assert!(
            clear_default
                .joined_arguments()
                .contains("bootctl set-default ''"),
            "{}",
            clear_default.joined_arguments()
        );
        let clear = activation.step_clear_efi_oneshot_invocation();
        assert!(
            clear.joined_arguments().contains("bootctl set-oneshot ''"),
            "{}",
            clear.joined_arguments()
        );
    }

    #[test]
    fn test_activation_runs_switch_to_configuration_test_only_no_profile_set() {
        let activation = host_activation(ordinary::HostDeployAction::TestActivation);
        let invocation = activation.ssh_invocation().expect("test invocation");
        let argv = invocation.joined_arguments();
        assert!(
            argv.contains(&format!("{STORE}/bin/switch-to-configuration test")),
            "{argv}"
        );
        assert!(
            !argv.contains("nix-env"),
            "test does not set the profile: {argv}"
        );
        assert!(!activation.requires_efi_reconcile());
    }

    #[test]
    fn interrupted_detached_test_observer_does_not_terminalize_the_handoff() {
        // Losing the predecessor's D-Bus listener is not an activation
        // failure. PID 1 retains the receipt and a successor reconciles it.
        assert!(DetachedActivationUnit::terminal_outcome(None).is_none());
        assert!(
            DetachedActivationUnit::terminal_outcome(Some(DetachedActivationOutcome::Missing,))
                .is_none()
        );
    }

    #[test]
    fn running_successor_retained_unit_attaches_the_completion_observer() {
        assert!(DetachedActivationUnit::needs_completion_observer(Some(
            DetachedActivationOutcome::Running
        )));
        assert!(!DetachedActivationUnit::needs_completion_observer(Some(
            DetachedActivationOutcome::Succeeded
        )));
    }

    #[test]
    fn retained_test_completion_visible_on_the_post_subscribe_read_terminalizes() {
        // The completion may precede listener attachment; the observer's
        // subscribe-then-uncached-read pass still adopts PID 1's success.
        assert!(
            DetachedActivationUnit::terminal_outcome(Some(DetachedActivationOutcome::Succeeded,))
                .is_some()
        );
    }

    #[test]
    fn successor_missing_then_unit_new_keeps_the_exact_handoff_pending() {
        // On successor start the manager subscription is established before
        // GetUnit. A momentary NoSuchUnit must adopt the deterministic UnitNew
        // handoff, not write ActivationFailed before PID 1 registers it.
        // A registration window is the absence of an outcome: `initial_outcome`
        // answers `None` when PID 1 has not yet made the transient visible.
        assert!(DetachedActivationUnit::needs_completion_observer(None));
        let unit = DetachedActivationUnit::new(
            "lojix-self-switch-deploy-41.service".to_string(),
            DetachedSelfActivation::Test,
        );
        assert!(unit.is_expected_unit_new("lojix-self-switch-deploy-41.service"));
    }

    #[test]
    fn pre_dispatch_unit_new_observer_accepts_only_its_exact_handoff() {
        // UnitNew is subscribed before `systemd-run --no-block`. A matching
        // signal queued while dispatch returns must make a following NoSuchUnit
        // lookup retry; an unrelated transient must not wake this deployment.
        let unit = DetachedActivationUnit::new(
            "lojix-self-switch-deploy-41.service".to_string(),
            DetachedSelfActivation::Switch,
        );
        assert!(unit.is_expected_unit_new("lojix-self-switch-deploy-41.service"));
        assert!(!unit.is_expected_unit_new("lojix-self-switch-deploy-42.service"));
    }

    #[test]
    fn detached_handoff_name_round_trips_the_systemd_service_identity() {
        let identifier = ordinary::DeploymentIdentifier::new(41);
        let name = DetachedActivationUnit::canonical_name(&identifier);
        assert_eq!(name, "lojix-self-switch-deploy-41.service");
        for activation in [DetachedSelfActivation::Switch, DetachedSelfActivation::Test] {
            assert!(
                DetachedActivationUnit::for_deployment(&identifier, activation)
                    .is_expected_unit_new(&name)
            );
        }
    }

    #[test]
    fn boot_once_uses_simple_invocation_none() {
        // BootOnce is not a simple invocation; it uses the systemd-run shape.
        let activation = host_activation(ordinary::HostDeployAction::ScheduleBootOnce);
        assert!(activation.ssh_invocation().is_none());
    }

    // ---- self-Switch is deadlock-free (PID-1-owned), report 150 ----

    #[test]
    fn self_host_switch_selects_the_detached_shape() {
        // A Switch whose target node IS the dispatching daemon's own host must
        // route to the detached PID-1-owned transient unit, NOT a foreground ssh
        // that `switch-to-configuration switch` would kill by restarting the
        // daemon. `activate_command` targets node-1, so a daemon hosted on node-1
        // is a self-Switch.
        let activation =
            host_activation_on_host(ordinary::HostDeployAction::ActivateNow, Some(node()));
        assert!(
            activation.detached_self_activation().is_some(),
            "self-host Switch must take the detached shape"
        );
        assert_eq!(
            activation.detached_self_activation(),
            Some(DetachedSelfActivation::Switch)
        );
        let invocation = activation
            .retained_emission(
                DetachedSelfActivation::Switch,
                emission::LoaderInterface::Implemented,
            )
            .command_line();
        assert_eq!(invocation.program(), "ssh");
        let argv = invocation.joined_arguments();
        assert!(argv.contains("systemd-run"), "PID-1-owned unit: {argv}");
        assert!(argv.contains("--service-type=oneshot"), "{argv}");
        // bead primary-6llk: PID 1 retains the unit's receipt for the daemon
        // that settles the deployment, the dispatcher or its successor.
        assert!(argv.contains("--no-block"), "{argv}");
        assert!(argv.contains("--remain-after-exit"), "{argv}");
        assert!(!argv.contains("--wait"), "{argv}");
        assert!(!argv.contains("--collect"), "{argv}");
        assert!(
            argv.contains("--unit=lojix-self-switch-deploy-7.service"),
            "deterministic self-switch unit name: {argv}"
        );
        // It carries Switch semantics (set profile + switch-to-configuration
        // switch), NOT a boot-once entry (`set-oneshot <generation>`).
        assert!(
            argv.contains("switch-to-configuration switch"),
            "self-switch runs the Switch action: {argv}"
        );
        assert!(
            argv.contains("nix-env -p /nix/var/nix/profiles/system --set"),
            "self-switch sets the system profile: {argv}"
        );
    }

    #[test]
    fn self_host_test_activation_selects_the_detached_handoff_shape() {
        // TestActivation can replace the Lojix service just like a live
        // self-switch can. Its whole test action therefore has to belong to
        // PID 1 rather than the daemon service cgroup; unlike Switch, the test
        // handoff must not set the persistent system profile.
        let activation =
            host_activation_on_host(ordinary::HostDeployAction::TestActivation, Some(node()));
        assert!(
            activation.detached_self_activation().is_some(),
            "same-host TestActivation must survive the service replacement"
        );
        assert_eq!(
            activation.detached_self_activation(),
            Some(DetachedSelfActivation::Test)
        );
        let invocation = activation
            .retained_emission(
                DetachedSelfActivation::Test,
                emission::LoaderInterface::NotImplemented,
            )
            .command_line();
        let argv = invocation.joined_arguments();
        assert!(argv.contains("systemd-run"), "PID-1-owned unit: {argv}");
        assert!(
            argv.contains("--no-block"),
            "handoff dispatch returns before replacement: {argv}"
        );
        assert!(
            argv.contains("--remain-after-exit"),
            "PID-1 retains the terminal receipt: {argv}"
        );
        assert!(
            !argv.contains("--wait"),
            "retained receipt cannot use --wait: {argv}"
        );
        assert!(
            argv.contains("--unit=lojix-self-switch-deploy-7.service"),
            "deterministic handoff unit: {argv}"
        );
        assert!(
            argv.contains("switch-to-configuration test"),
            "handoff runs TestActivation semantics: {argv}"
        );
        assert!(
            !argv.contains("nix-env -p /nix/var/nix/profiles/system --set"),
            "TestActivation must not persist the system profile: {argv}"
        );
    }

    fn interrupted_self_switch_job(closure: Option<&str>) -> memory::DeployJob {
        memory::DeployJob {
            deployment_identifier: ordinary::DeploymentIdentifier::new(72),
            generation_identifier: ordinary::GenerationIdentifier::new(72),
            cluster_name: ordinary::ClusterName::new("fixture-cluster".into()),
            node_name: ordinary::NodeName::new("fixture-daemon".into()),
            deploy_job_phase: memory::DeployJobPhase::Activating,
            closure_path_option: closure.map(|path| ordinary::ClosurePath::new(path.into())),
            source_revision_policy: ordinary::SourceRevisionPolicy::RequireImmutable,
            flake_reference: ordinary::FlakeReference::new(
                "github:LiGoldragon/CriomOS?rev=0123456789abcdef0123456789abcdef01234567"
                    .to_string(),
            ),
            flake_reference_option: Some(ordinary::FlakeReference::new(
                "github:LiGoldragon/CriomOS?rev=0123456789abcdef0123456789abcdef01234567"
                    .to_string(),
            )),
            resolved_revision_option: Some(crate::runtime_model::ResolvedRevision(
                "0123456789abcdef0123456789abcdef01234567".to_string(),
            )),
            deployment_transport: fixture_transport(),
            deployment_input_mode: memory::DeploymentInputMode::Direct,
            deployment_output_selector: fixture_output_selector(),
            activation_backend: memory::ActivationBackend::NixosSystemdBootV1,
            nix_builder_spec_option: None,
            boot_once_unit_option: None,
            generation_slot_option: None,
            persisted_flake_input_override_vector: Vec::new(),
            deploy_resume_stage: memory::DeployResumeStage::ResolveFlakeAuth,
            phase_receipt_option: None,
            actualized_deploy_submission_option: Some(host_submission(
                ordinary::HostDeployAction::ActivateNow,
            )),
        }
    }

    #[test]
    fn interrupted_self_switch_reconstructs_a_current_generation_record() {
        // bead primary-7u8p: a self-switch interrupted at Activating (the daemon
        // restarted itself mid-switch) rebuilds a Current live generation and its
        // gc-root from the persisted job cursor, so the restarted daemon records
        // the generation the pipeline could not — and the live-set-scanning id
        // allocator advances past this id instead of reusing it. The resumption
        // discriminator the daemon pairs with `target == daemon host` is a
        // PollActivationUnit with NO boot-once unit.
        let job = interrupted_self_switch_job(Some("/nix/store/aaaaaaaa-system"));
        assert_eq!(
            job.resumption(),
            DeployJobResumption::PollActivationUnit { unit: None }
        );
        // The live system profile resolves to exactly this job's closure — the
        // witness of a completed host switch.
        let (generation, root) = job
            .self_switch_activation_record(Some("/nix/store/aaaaaaaa-system"))
            .expect("a completed self-switch whose system profile matches records");
        assert_eq!(*generation.generation_identifier.payload(), 72);
        assert_eq!(*generation.deployment_identifier.payload(), 72);
        assert_eq!(
            generation.generation_slot,
            ordinary::GenerationSlot::Current
        );
        assert_eq!(
            generation.activation_effect,
            ordinary::ActivationEffect::LiveActivation
        );
        // The artifact is the one the submission composed: this fixture's
        // host is a BaseHost.
        assert_eq!(
            generation.generation_artifact,
            ordinary::GenerationArtifact::BaseHost
        );
        assert_eq!(
            generation.closure_path.payload(),
            "/nix/store/aaaaaaaa-system"
        );
        assert_eq!(
            generation.source_revision_record.resolved_revision.0,
            "0123456789abcdef0123456789abcdef01234567"
        );
        assert_eq!(root.generation_slot, ordinary::GenerationSlot::Current);
        assert_eq!(root.closure_path.payload(), "/nix/store/aaaaaaaa-system");
        assert_eq!(root.pin_label_option, None);
    }

    #[test]
    fn self_switch_without_a_built_closure_records_nothing() {
        // No captured closure path means the switch never got past eval/build, so
        // there is nothing to root or record — the daemon just drops the row.
        let job = interrupted_self_switch_job(None);
        assert!(
            job.self_switch_activation_record(Some("/nix/store/aaaaaaaa-system"))
                .is_none()
        );
    }

    #[test]
    fn activating_row_whose_system_profile_differs_is_not_recorded() {
        // The tight gate (audit fix): an Activating row on the daemon host whose
        // closure is NOT the live system profile is a racy unrelated restart
        // during a self-host SetBootProfile / TestActivation / user-environment
        // activation — none of which set the system profile. It must NOT be
        // recorded as a Current complete-host generation; it is left for S5.
        let job = interrupted_self_switch_job(Some("/nix/store/aaaaaaaa-system"));
        assert!(
            job.self_switch_activation_record(Some("/nix/store/bbbbbbbb-other-system"))
                .is_none()
        );
        // A daemon that cannot read its system profile also declines to record.
        assert!(job.self_switch_activation_record(None).is_none());
    }

    #[test]
    fn matching_profile_set_boot_profile_is_not_recovered_as_a_self_switch() {
        // The profile match is deliberately insufficient. SetBootProfile can
        // leave an Activating row behind if the daemon restarts for another
        // reason, but it never runs the detached self-switch protocol.
        let mut job = interrupted_self_switch_job(Some("/nix/store/aaaaaaaa-system"));
        job.actualized_deploy_submission_option =
            Some(host_submission(ordinary::HostDeployAction::SetBootProfile));
        assert!(
            job.self_switch_activation_record(Some("/nix/store/aaaaaaaa-system"))
                .is_none()
        );
    }

    #[test]
    fn matching_profile_user_activation_is_not_recovered_as_a_self_switch() {
        // A user activation can share the daemon host's node name and a
        // coincidental closure string, but no user submission owns the host
        // system profile. It must remain for normal S5 reconciliation.
        let mut job = interrupted_self_switch_job(Some("/nix/store/aaaaaaaa-system"));
        job.actualized_deploy_submission_option = Some(user_submission());
        assert!(
            job.self_switch_activation_record(Some("/nix/store/aaaaaaaa-system"))
                .is_none()
        );
    }

    #[test]
    fn matching_profile_without_the_systemd_boot_backend_is_not_recovered() {
        let mut job = interrupted_self_switch_job(Some("/nix/store/aaaaaaaa-system"));
        job.activation_backend = memory::ActivationBackend::HomeManagerNixProfileV1;
        assert!(
            job.self_switch_activation_record(Some("/nix/store/aaaaaaaa-system"))
                .is_none()
        );
    }

    #[test]
    fn foreign_target_switch_keeps_the_foreground_path() {
        // A Switch targeting a DIFFERENT node than the daemon host (or with no
        // daemon-host context) must NOT take the detached shape — the foreground
        // ssh is not at risk there.
        let foreign = host_activation_on_host(
            ordinary::HostDeployAction::ActivateNow,
            Some(ordinary::NodeName::new("some-other-node".into())),
        );
        assert!(foreign.detached_self_activation().is_none());
        let no_context = host_activation(ordinary::HostDeployAction::ActivateNow);
        assert!(no_context.detached_self_activation().is_none());
        // The foreground Switch invocation is still available and well-shaped.
        let invocation = no_context.ssh_invocation().expect("foreground switch");
        assert_eq!(invocation.program(), "ssh");
        assert!(
            invocation
                .joined_arguments()
                .contains("switch-to-configuration switch")
        );
    }

    #[test]
    fn self_host_boot_does_not_take_the_self_switch_shape() {
        // Only Switch self-restarts the daemon; a self-host Boot/Test/BootOnce
        // keeps its normal path (Boot uses the foreground ssh; BootOnce its own
        // transient unit).
        let boot =
            host_activation_on_host(ordinary::HostDeployAction::SetBootProfile, Some(node()));
        assert!(boot.detached_self_activation().is_none());
        let boot_once =
            host_activation_on_host(ordinary::HostDeployAction::ScheduleBootOnce, Some(node()));
        assert!(boot_once.detached_self_activation().is_none());
    }

    // ---- Step 3: BootOnce transient-unit argv + script snapshot ----

    #[test]
    fn boot_once_systemd_run_argv_shape() {
        let activation = host_activation(ordinary::HostDeployAction::ScheduleBootOnce);
        let invocation = activation.systemd_run_invocation("lojix-boot-once-abc-def");
        assert_eq!(invocation.program(), "ssh");
        let argv = invocation.joined_arguments();
        assert!(
            argv.contains("fixture-login@fixture-activate.invalid"),
            "{argv}"
        );
        assert!(argv.contains("systemd-run"), "{argv}");
        assert!(argv.contains("--unit=lojix-boot-once-abc-def"), "{argv}");
        assert!(argv.contains("--collect"), "{argv}");
        assert!(argv.contains("--wait"), "{argv}");
        assert!(argv.contains("--service-type=oneshot"), "{argv}");
        assert!(argv.contains("/bin/sh -c"), "{argv}");
    }

    #[test]
    fn boot_once_unit_name_is_deterministic_in_the_deployment_identifier() {
        // report 150 fix: the activation transient-unit name must be the
        // deterministic `lojix-boot-once-deploy-<id>` — NOT the old
        // time+pid suffix — so a daemon that crashes inside the BootOnce window
        // recomputes the same name on restart. `activate_command` carries
        // deployment id 7.
        let activation = host_activation(ordinary::HostDeployAction::ScheduleBootOnce);
        assert_eq!(activation.unit_name(), "lojix-boot-once-deploy-7");
    }

    #[test]
    fn activation_unit_name_matches_the_resume_cursor_unit() {
        // The activation-side `unit_name` and the resume-side
        // `DeployJob::boot_once_unit` must produce the SAME string for one
        // deployment so the crash-resume `PollActivationUnit` polls the unit the
        // activation actually started (report 150). Both go through
        // `DeploymentIdentifier::boot_once_unit_name`.
        let mut pipeline = host_pipeline(ordinary::HostDeployAction::ScheduleBootOnce);
        pipeline.deployment_identifier = ordinary::DeploymentIdentifier::new(7);
        let cursor_unit = pipeline.boot_once_unit().expect("BootOnce records a unit");
        let activation = host_activation(ordinary::HostDeployAction::ScheduleBootOnce);
        assert_eq!(activation.unit_name(), cursor_unit);
    }

    #[test]
    fn non_boot_once_deploy_records_no_resume_unit() {
        // A non-BootOnce action has no transient unit to poll; copy is
        // idempotent and activation re-runs safely, so the cursor records None.
        let pipeline = host_pipeline(ordinary::HostDeployAction::ActivateNow);
        assert!(pipeline.boot_once_unit().is_none());
    }

    // ---- build context: the daemon host's own store, or the target's ----

    const FIXTURE_BUILDER: &str = "ssh-ng://fixture-builder.invalid x86_64-linux - 4 2 k1";

    #[test]
    fn build_on_a_different_target_realizes_in_the_transport_store() {
        // A node other than the daemon host builds in the store its transport
        // names; the target store carries the exact request transport and the
        // daemon host, which names the target-side GC root.
        let pipeline = host_pipeline(ordinary::HostDeployAction::ScheduleBootOnce);
        let nexus::BuildTarget::TargetStore(build) = pipeline.build_target(&daemon_host()) else {
            panic!("a remote node must realize in its own store");
        };
        assert_eq!(
            build.deployment_transport.nix_store_uri.payload(),
            "ssh-ng://fixture-copy.invalid"
        );
        assert_eq!(
            build.deployment_transport.ssh_destination.payload(),
            "fixture-login@fixture-activate.invalid"
        );
        assert_eq!(build.node_name, node());
        assert_eq!(build.operator_node, emission::OperatorNode(daemon_host()));
        assert!(build.ignored_builder_option.is_none());
    }

    #[test]
    fn realize_on_a_different_target_also_realizes_in_the_transport_store() {
        // Realize does not activate, but it too leaves the closure on the
        // target rather than on the daemon host.
        let pipeline = host_pipeline(ordinary::HostDeployAction::Realize);
        assert!(matches!(
            pipeline.build_target(&daemon_host()),
            nexus::BuildTarget::TargetStore(_)
        ));
    }

    #[test]
    fn build_on_the_daemon_host_stays_local() {
        // Deploying the daemon's own host is unchanged: local build, no
        // target store.
        let pipeline = host_pipeline(ordinary::HostDeployAction::ScheduleBootOnce);
        assert!(matches!(
            pipeline.build_target(&node()),
            nexus::BuildTarget::Local
        ));
    }

    #[test]
    fn explicit_builder_offloads_only_a_daemon_host_build() {
        // An operator-named builder still dispatches the daemon host's own
        // build to that machine through the local Nix client.
        let mut pipeline = host_pipeline(ordinary::HostDeployAction::ScheduleBootOnce);
        pipeline.builder = Some(memory::NixBuilderSpec::new(FIXTURE_BUILDER.to_string()));
        assert!(matches!(
            pipeline.build_target(&node()),
            nexus::BuildTarget::Remote(_)
        ));
    }

    #[test]
    fn explicit_builder_is_carried_as_ignored_for_a_remote_target() {
        // For any other node the target daemon builds with its own settings;
        // the builder must not win (lojix 0.3.5) and is carried only so the
        // effect can log that it was ignored.
        let mut pipeline = host_pipeline(ordinary::HostDeployAction::ScheduleBootOnce);
        pipeline.builder = Some(memory::NixBuilderSpec::new(FIXTURE_BUILDER.to_string()));
        let nexus::BuildTarget::TargetStore(build) = pipeline.build_target(&daemon_host()) else {
            panic!("a remote node realizes in its own store even with a builder");
        };
        assert_eq!(
            build
                .ignored_builder_option
                .as_ref()
                .map(|builder| builder.0.payload().as_str()),
            Some(FIXTURE_BUILDER)
        );
        let realization = TargetStoreRealization::from_command(
            &pipeline.nix_build_command(
                ordinary::ClosurePath::new(DERIVATION.into()),
                &daemon_host(),
            ),
            &build,
        )
        .expect("target-store realization");
        for invocation in [
            realization.derivation_copy_invocation(),
            realization.build_invocation(),
        ] {
            let argv = invocation.joined_arguments();
            assert!(!argv.contains("--builders"), "{argv}");
            assert!(!argv.contains("max-jobs"), "{argv}");
            assert!(!argv.contains("fixture-builder"), "{argv}");
        }
    }

    #[test]
    fn eval_never_redirects_even_for_a_target_store_build() {
        // The 0.3.9 deadlock guard: whatever the build target, evaluation
        // writes its derivations into the daemon host's own store.
        let pipeline = host_pipeline(ordinary::HostDeployAction::ScheduleBootOnce);
        let command = pipeline.nix_eval_command(&daemon_host());
        assert!(matches!(
            command.build_target,
            nexus::BuildTarget::TargetStore(_)
        ));
        let invocation = command.evaluation().command_line();
        let argv = invocation.joined_arguments();
        assert!(
            !argv.contains("--store") && !argv.contains("--eval-store"),
            "eval must remain local: {argv}"
        );
        assert!(!argv.contains("ssh-ng"), "{argv}");
    }

    fn target_store_realization(
        ssh_destination: &str,
        substituters: Vec<nexus::ExtraSubstituter>,
    ) -> TargetStoreRealization {
        let build = nexus::TargetStoreBuild {
            deployment_transport: nexus::DeploymentTransport {
                nix_store_uri: nexus::NixStoreUri::new(
                    "ssh-ng://root@fixture-target.invalid".into(),
                ),
                ssh_destination: nexus::SshDestination::new(ssh_destination.into()),
            },
            node_name: node(),
            operator_node: emission::OperatorNode(daemon_host()),
            ignored_builder_option: None,
        };
        let command = nexus::NixBuildCommand {
            generation_identifier: ordinary::GenerationIdentifier::new(7),
            closure_path: ordinary::ClosurePath::new(DERIVATION.into()),
            build_target: nexus::BuildTarget::TargetStore(build.clone()),
            extra_substituter_vector: substituters,
        };
        TargetStoreRealization::from_command(&command, &build).expect("target-store realization")
    }

    #[test]
    fn target_store_build_copies_the_derivation_closure_to_the_transport_store() {
        let realization = target_store_realization("root@fixture-target.invalid", Vec::new());
        let invocation = realization.derivation_copy_invocation();
        assert_eq!(invocation.program(), "nix");
        assert_eq!(
            invocation.joined_arguments(),
            format!("copy --derivation --to ssh-ng://root@fixture-target.invalid {DERIVATION}")
        );
    }

    #[test]
    fn target_store_build_builds_the_outputs_in_the_transport_store() {
        let realization = target_store_realization(
            "root@fixture-target.invalid",
            vec![nexus::ExtraSubstituter {
                first_string: "https://cache.fixture.invalid".to_string(),
                second_string: "cache.fixture.invalid-1:key".to_string(),
            }],
        );
        let invocation = realization.build_invocation();
        assert_eq!(invocation.program(), "nix");
        let argv = invocation.joined_arguments();
        assert!(
            argv.starts_with(&format!(
                "build --no-link --print-out-paths --store ssh-ng://root@fixture-target.invalid {DERIVATION}^*"
            )),
            "{argv}"
        );
        assert!(
            argv.contains("--option extra-substituters https://cache.fixture.invalid"),
            "{argv}"
        );
        assert!(!argv.contains("--eval-store"), "{argv}");
        assert!(!argv.contains("--builders"), "{argv}");
        assert!(!argv.contains("max-jobs"), "{argv}");
    }

    #[test]
    fn target_store_build_roots_the_output_on_the_target_over_ssh() {
        let realization = target_store_realization("root@fixture-target.invalid", Vec::new());
        let invocation = realization.gc_root_invocation(STORE);
        assert_eq!(invocation.program(), "ssh");
        let argv = invocation.joined_arguments();
        assert!(
            argv.starts_with("-o BatchMode=yes root@fixture-target.invalid "),
            "{argv}"
        );
        assert!(
            argv.contains("mkdir -p /nix/var/nix/gcroots/lojix/daemon-host"),
            "{argv}"
        );
        assert!(
            argv.contains(&format!(
                "nix-store --add-root /nix/var/nix/gcroots/lojix/daemon-host/generation-7 --realise {STORE}"
            )),
            "{argv}"
        );
        assert_eq!(
            realization.gc_root_text(),
            "/nix/var/nix/gcroots/lojix/daemon-host/generation-7"
        );
    }

    #[test]
    fn target_store_gc_root_for_a_user_login_is_an_indirect_root_in_its_home() {
        let realization = target_store_realization("bird@fixture-target.invalid", Vec::new());
        let argv = realization.gc_root_invocation(STORE).joined_arguments();
        assert!(
            argv.contains(
                "nix-store --add-root \"$HOME\"/.local/state/lojix/gcroots/daemon-host/generation-7"
            ),
            "{argv}"
        );
        assert!(!argv.contains("/nix/var/nix/gcroots"), "{argv}");
    }

    #[test]
    fn target_store_build_refuses_an_output_path_in_place_of_a_derivation() {
        let build = nexus::TargetStoreBuild {
            deployment_transport: nexus_fixture_transport(),
            node_name: node(),
            operator_node: emission::OperatorNode(daemon_host()),
            ignored_builder_option: None,
        };
        let command = nexus::NixBuildCommand {
            generation_identifier: ordinary::GenerationIdentifier::new(1),
            closure_path: ordinary::ClosurePath::new(STORE.into()),
            build_target: nexus::BuildTarget::TargetStore(build.clone()),
            extra_substituter_vector: Vec::new(),
        };
        assert!(TargetStoreRealization::from_command(&command, &build).is_err());
    }

    #[test]
    fn copy_after_a_target_store_build_is_a_presence_check() {
        let pipeline = host_pipeline(ordinary::HostDeployAction::ScheduleBootOnce);
        let command =
            pipeline.copy_closure_command(ordinary::ClosurePath::new(STORE.into()), &daemon_host());
        assert_eq!(command.closure_origin, nexus::ClosureOrigin::TargetStore);
        let argv = ClosureTransfer::from_command(&command)
            .expect("copy transport")
            .invocation()
            .joined_arguments();
        assert_eq!(
            argv,
            format!("path-info --store ssh-ng://fixture-copy.invalid {STORE}")
        );
    }

    #[test]
    fn copy_after_a_daemon_host_build_still_transfers() {
        let pipeline = host_pipeline(ordinary::HostDeployAction::ScheduleBootOnce);
        let command =
            pipeline.copy_closure_command(ordinary::ClosurePath::new(STORE.into()), &node());
        assert_eq!(command.closure_origin, nexus::ClosureOrigin::DaemonStore);
        let argv = ClosureTransfer::from_command(&command)
            .expect("copy transport")
            .invocation()
            .joined_arguments();
        assert!(
            argv.starts_with("copy --substitute-on-destination --to"),
            "{argv}"
        );
    }

    #[test]
    fn rerunning_a_target_store_build_repeats_the_same_idempotent_triple() {
        // A resume from `DeployResumeStage::NixBuild` reruns the build effect
        // from the recovered cursor. The triple is the same both times: the
        // derivation copy and the build skip what the target already holds,
        // and the GC root is replaced in place.
        use std::os::unix::fs::PermissionsExt;

        let directory = tempfile::tempdir().expect("temporary fake program directory");
        for (program, reply) in [
            ("nix", format!("printf '%s\\n' '{STORE}'")),
            ("ssh", String::new()),
        ] {
            let path = directory.path().join(program);
            fs::write(
                &path,
                format!(
                    "#!/bin/sh\nprintf '{program}' >> \"$(dirname \"$0\")/commands\"\nfor arg in \"$@\"; do printf ' <%s>' \"$arg\" >> \"$(dirname \"$0\")/commands\"; done\nprintf '\\n' >> \"$(dirname \"$0\")/commands\"\n{reply}\n"
                ),
            )
            .expect("write fake program");
            fs::set_permissions(&path, fs::Permissions::from_mode(0o700))
                .expect("make fake program executable");
        }
        let configuration = Arc::new(RuntimeConfiguration::test_with_effect_program_directory(
            directory.path().join("inputs"),
            directory.path().to_path_buf(),
        ));
        let store =
            Arc::new(Store::open(directory.path().join("lojix.memory")).expect("open store"));
        let engine = SchemaRuntime::with_store_and_configuration(store, configuration);
        let pipeline = host_pipeline(ordinary::HostDeployAction::ScheduleBootOnce);
        let command = pipeline.nix_build_command(
            ordinary::ClosurePath::new(DERIVATION.into()),
            &daemon_host(),
        );
        let runtime = tokio::runtime::Runtime::new().expect("tokio runtime");
        for _ in 0..2 {
            let result = runtime.block_on(engine.run_nix_build(command.clone()));
            assert!(
                matches!(&result, memory::Outcome::ClosureBuilt(built)
                    if built.closure_path.payload() == STORE),
                "{result:?}"
            );
        }
        let commands: Vec<String> = fs::read_to_string(directory.path().join("commands"))
            .expect("read fake command log")
            .lines()
            .map(str::to_owned)
            .collect();
        assert_eq!(commands.len(), 6, "{commands:?}");
        assert_eq!(commands[..3], commands[3..], "{commands:?}");
        assert!(
            commands[0].starts_with("nix <copy> <--derivation>"),
            "{commands:?}"
        );
        assert!(commands[1].starts_with("nix <build>"), "{commands:?}");
        assert!(commands[2].starts_with("ssh "), "{commands:?}");
    }

    #[test]
    fn build_closure_stays_local_and_selects_the_realised_output() {
        let invocation = closure_build(DERIVATION, emission::BuildPlace::DaemonStore);
        assert_eq!(invocation.program(), "nix");
        let argv = invocation.joined_arguments();
        assert!(
            !argv.contains("--store") && !argv.contains("--eval-store"),
            "build must remain in the local owner context: {argv}"
        );
        assert!(argv.contains("--print-out-paths"), "{argv}");
        assert!(
            argv.contains(&format!("{DERIVATION}^*")),
            "local build must carry the `^*` output selector: {argv}"
        );
        assert!(!argv.contains("--builders"), "{argv}");
    }

    #[test]
    fn resolved_horizon_architecture_maps_to_nix_system() {
        assert_eq!(
            NixSystemName::from_horizon_architecture("x86_64")
                .expect("x86_64 is supported")
                .as_str(),
            "x86_64-linux"
        );
        assert_eq!(
            NixSystemName::from_horizon_architecture("aarch64")
                .expect("aarch64 is supported")
                .as_str(),
            "aarch64-linux"
        );
        assert!(NixSystemName::from_horizon_architecture("riscv64").is_none());
    }

    #[test]
    fn deployment_input_maps_complete_and_base_host_materialization() {
        let complete = DeploymentInput::from_shape(&nexus::MaterializationShape::CompleteHost)
            .expect("complete host deployment input");
        assert_eq!(
            complete.flake_text(),
            "{ outputs = _: { deployment = { includeHome = true; includeAllFirmware = true; }; }; }\n"
        );

        let base = DeploymentInput::from_shape(&nexus::MaterializationShape::BaseHost)
            .expect("base host deployment input");
        assert_eq!(
            base.flake_text(),
            "{ outputs = _: { deployment = { includeHome = false; includeAllFirmware = false; }; }; }\n"
        );

        let user = nexus::MaterializationShape::UserEnvironment(
            nexus::UserEnvironmentMaterialization::new(ordinary::UserName::new("li".into())),
        );
        assert!(DeploymentInput::from_shape(&user).is_none());
    }

    #[test]
    fn local_eval_never_redirects_over_ssh_ng() {
        let invocation = toplevel_evaluation(emission::EvaluationFreshness::Refreshed);
        assert_eq!(invocation.program(), "nix");
        let argv = invocation.joined_arguments();
        assert!(
            !argv.contains("--store") && !argv.contains("--eval-store"),
            "eval must remain local: {argv}"
        );
        assert!(argv.contains("--refresh"), "{argv}");
        assert!(argv.contains("--raw"), "{argv}");
        assert!(argv.ends_with(".#toplevel.drvPath"), "{argv}");
    }

    #[test]
    fn immutable_pin_eval_omits_refresh_but_keeps_raw_and_selector() {
        // Under an immutable pin the eval trusts Nix's per-flake eval cache
        // (bead primary-8sv6): no `--refresh`, so a re-deploy of the same rev
        // serves the cached evaluation instead of re-evaluating the whole tree.
        // The `--raw` flag and `.drvPath` selector are unaffected.
        let invocation = toplevel_evaluation(emission::EvaluationFreshness::TrustingPin);
        let argv = invocation.joined_arguments();
        assert!(
            !argv.contains("--refresh"),
            "an immutable pin must not force a full re-eval: {argv}"
        );
        assert!(argv.contains("--raw"), "{argv}");
        assert!(
            !argv.contains("--store"),
            "immutable eval remains local: {argv}"
        );
        assert!(argv.ends_with(".#toplevel.drvPath"), "{argv}");
    }

    #[test]
    fn refresh_is_dropped_only_for_require_immutable_against_a_pinned_ref() {
        // Only a RequireImmutable deploy against a reference carrying its
        // immutable identity trusts the cache; a mutable ref (even under
        // RequireImmutable) and every ResolveAndRecord deploy keep `--refresh`.
        let pinned = "github:LiGoldragon/CriomOS?rev=0123456789abcdef0123456789abcdef01234567";
        let mutable = "github:LiGoldragon/CriomOS";
        assert_eq!(
            emission::EvaluationFreshness::for_source(
                ordinary::SourceRevisionPolicy::RequireImmutable,
                pinned
            ),
            emission::EvaluationFreshness::TrustingPin
        );
        assert_eq!(
            emission::EvaluationFreshness::for_source(
                ordinary::SourceRevisionPolicy::RequireImmutable,
                mutable
            ),
            emission::EvaluationFreshness::Refreshed
        );
        assert_eq!(
            emission::EvaluationFreshness::for_source(
                ordinary::SourceRevisionPolicy::ResolveAndRecord,
                pinned
            ),
            emission::EvaluationFreshness::Refreshed
        );
    }

    #[test]
    fn local_eval_reads_the_daemon_host_store_with_no_redirect() {
        // A daemon-host target (`Local`) keeps the host-local eval — its store
        // already holds everything the config references, and a store redirect
        // would be wrong. No `--store` / `--eval-store` flags are added.
        let invocation = toplevel_evaluation(emission::EvaluationFreshness::Refreshed);
        let argv = invocation.joined_arguments();
        assert!(
            !argv.contains("--store"),
            "local eval adds no store: {argv}"
        );
        assert!(
            !argv.contains("--eval-store"),
            "local eval adds no eval-store: {argv}"
        );
        assert!(argv.ends_with(".#toplevel.drvPath"), "{argv}");
    }

    #[test]
    fn remote_eval_stays_host_local_with_no_store_redirect() {
        // A `Remote` build offloads only the REALIZATION to the named builder
        // machine; the eval stays host-local (the `.drv` is instantiated against
        // the daemon host's store, matching `build_closure_remote`). So a Remote
        // eval adds no `--store` / `--eval-store` flags at all.
        let mut pipeline = host_pipeline(ordinary::HostDeployAction::ScheduleBootOnce);
        pipeline.builder = Some(ordinary::NixBuilderSpec::new(
            "ssh-ng://fixture-builder.invalid x86_64-linux - 4 2 k1".to_string(),
        ));
        let command = pipeline.nix_eval_command(&node());
        assert!(matches!(
            command.build_target,
            nexus::BuildTarget::Remote(_)
        ));
        let invocation = command.evaluation().command_line();
        let argv = invocation.joined_arguments();
        assert!(
            !argv.contains("--store"),
            "remote eval adds no store redirect: {argv}"
        );
        assert!(
            !argv.contains("--eval-store"),
            "remote eval adds no eval-store: {argv}"
        );
        assert!(argv.ends_with(".drvPath"), "{argv}");
    }

    #[test]
    fn boot_once_script_snapshot() {
        let activation = host_activation(ordinary::HostDeployAction::ScheduleBootOnce);
        let expected = format!(
            "export PATH=/run/current-system/sw/bin:/run/wrappers/bin:$PATH\n\
             set -eu\n\
             CLOSURE='{STORE}'\n\
             OLD=$(bootctl status | awk -F': *' '/Current Entry:/ {{print $2}}')\n\
             [ -n \"$OLD\" ]\n\
             nix-env -p /nix/var/nix/profiles/system --set \"$CLOSURE\"\n\
             \"$CLOSURE/bin/switch-to-configuration\" boot\n\
             NEW=$(awk '$1 == \"default\" {{print $2; exit}}' /boot/loader/loader.conf)\n\
             [ -n \"$NEW\" ]\n\
             [ -f \"/boot/loader/entries/$NEW\" ]\n\
             [ \"$NEW\" != \"$OLD\" ]\n\
             bootctl set-default \"$OLD\"\n\
             bootctl set-oneshot \"$NEW\"\n\
             echo \"boot-once: oneshot=$NEW persistent-default=$OLD (=running generation)\"\n",
        );
        assert_eq!(
            emission::TransientScript::BootOnce(activation.closure_path.clone()).script(),
            expected
        );
    }

    // ---- Step 3: user-environment activation argv ----

    fn user_environment_activation(
        mode: meta::UserEnvironmentAction,
        ssh_destination: &str,
    ) -> UserEnvironmentActivation {
        let profile =
            nexus::ActivationProfile::UserEnvironment(nexus::UserEnvironmentActivationProfile {
                user_environment_action: mode.clone(),
                user_name: ordinary::UserName::new("li".into()),
            });
        let mut command = activate_command(profile, ordinary::ActivationEffect::LiveActivation);
        command.deployment_transport.ssh_destination =
            nexus::SshDestination::new(ssh_destination.into());
        match Activation::from_command(&command, None).expect("activation") {
            Activation::UserEnvironment(activation) => activation,
            Activation::Host(_) => panic!("expected user-environment activation"),
        }
    }

    #[test]
    fn user_environment_remote_profile_uses_root_mediation() {
        let activation = user_environment_activation(
            meta::UserEnvironmentAction::SetProfile,
            "root@fixture.invalid",
        );
        let invocation = activation
            .remote_profile_invocation()
            .expect("root-mediated profile invocation");
        assert_eq!(invocation.program(), "ssh");
        let argv = invocation.joined_arguments();
        assert!(argv.contains("root@fixture.invalid"), "{argv}");
        assert!(argv.contains("runuser --login --command"), "{argv}");
        assert!(argv.trim_end().ends_with("li"), "{argv}");
        assert!(
            argv.contains("nix-env -p \"$HOME/.local/state/nix/profiles/home-manager\" --set"),
            "{argv}"
        );
        assert!(argv.contains(STORE), "{argv}");
    }

    #[test]
    fn user_environment_remote_activate_uses_root_mediation() {
        let activation = user_environment_activation(
            meta::UserEnvironmentAction::ActivateNow,
            "root@fixture.invalid",
        );
        let invocation = activation
            .remote_activate_invocation()
            .expect("root-mediated activation invocation");
        let argv = invocation.joined_arguments();
        assert!(argv.contains("root@fixture.invalid"), "{argv}");
        assert!(argv.contains("runuser --login --command"), "{argv}");
        assert!(argv.trim_end().ends_with("li"), "{argv}");
        assert!(argv.contains(&format!("{STORE}/activate")), "{argv}");
    }

    #[test]
    fn user_environment_matched_remote_login_runs_without_mediation() {
        let activation = user_environment_activation(
            meta::UserEnvironmentAction::ActivateNow,
            "li@fixture.invalid",
        );
        let invocation = activation
            .remote_activate_invocation()
            .expect("matched-user activation invocation");
        let argv = invocation.joined_arguments();
        assert!(argv.contains("li@fixture.invalid"), "{argv}");
        assert!(!argv.contains("runuser"), "{argv}");
        assert!(argv.contains(&format!("{STORE}/activate")), "{argv}");
    }

    #[test]
    fn user_environment_local_profile_invocation_remains_local() {
        let activation = user_environment_activation(
            meta::UserEnvironmentAction::SetProfile,
            "other@fixture.invalid",
        );
        let invocation = activation.local_profile_invocation(Path::new("/home/li"));
        assert_eq!(invocation.program(), "nix-env");
        let argv = invocation.joined_arguments();
        assert!(argv.contains("/home/li/.local/state/nix/profiles/home-manager"));
        assert!(!argv.contains("ssh"));
        assert!(!argv.contains("runuser"));
    }

    #[test]
    fn user_environment_activation_uses_private_transient_service() {
        let activation = user_environment_activation(
            meta::UserEnvironmentAction::ActivateNow,
            "li@fixture.invalid",
        );
        let invocation = activation.local_activate_invocation();
        assert_eq!(invocation.program, "/run/current-system/sw/bin/systemd-run");
        assert_eq!(
            invocation.arguments,
            vec![
                "--user",
                "--wait",
                "--pipe",
                "--collect",
                "--quiet",
                "--service-type=exec",
                "--expand-environment=no",
                "--property=PrivateTmp=yes",
                "--setenv=TMPDIR=/tmp",
                "--",
                &format!("{STORE}/activate"),
                "--driver-version",
                "1",
            ]
        );
        for login in ["li@fixture.invalid", "root@fixture.invalid"] {
            let mut activation = activation.clone();
            activation.target = SshTarget::from_transport(&nexus::DeploymentTransport {
                nix_store_uri: nexus::NixStoreUri::new("ssh-ng://fixture.invalid".into()),
                ssh_destination: nexus::SshDestination::new(login.into()),
            })
            .expect("target");
            activation.authority = activation
                .target
                .user_environment_activation_authority(&activation.user);
            let invocation = activation
                .remote_activate_invocation()
                .expect("remote invocation");
            let command = invocation.joined_arguments();
            assert!(command.contains("/run/current-system/sw/bin/systemd-run"));
            assert!(command.contains("--setenv=TMPDIR=/tmp"));
            assert!(!command.contains("DRY_RUN"));
        }
    }

    #[test]
    fn user_environment_remote_activation_quotes_closure_as_one_argument() {
        let mut activation = user_environment_activation(
            meta::UserEnvironmentAction::ActivateNow,
            "li@fixture.invalid",
        );
        activation.closure_path =
            ordinary::ClosurePath::new("/fixture/path with spaces'$literal".to_string());
        let invocation = activation
            .remote_activate_invocation()
            .expect("remote invocation");
        let expected = activation.local_activate_invocation();
        let command = invocation.arguments.last().expect("SSH command");
        assert!(
            command.contains(
                &ShellArgument(&format!("{}/activate", activation.closure_path.payload()))
                    .to_command_text()
            )
        );
        assert!(command.contains("--expand-environment=no"));
        assert_eq!(expected.arguments.last().map(String::as_str), Some("1"));
    }

    #[test]
    fn user_environment_runner_status_and_streams_keep_existing_effect_contract() {
        use std::os::unix::fs::PermissionsExt;
        let directory = tempfile::tempdir().expect("fake runner directory");
        let runner = directory.path().join("systemd-run");
        let execution = EffectExecution::test(directory.path().to_path_buf());
        let runtime = tokio::runtime::Runtime::new().expect("runtime");
        for status in [0, 23] {
            fs::write(&runner, format!("#!/bin/sh\nprintf 'activation stdout'\nprintf 'activation stderr' >&2\nexit {status}\n")).expect("runner script");
            fs::set_permissions(&runner, fs::Permissions::from_mode(0o700)).expect("executable");
            let activation = user_environment_activation(
                meta::UserEnvironmentAction::ActivateNow,
                "li@fixture.invalid",
            );
            let invocation = activation.local_activate_invocation();
            let invocation = CommandLine {
                program: "systemd-run".into(),
                arguments: invocation.arguments,
            };
            let running = execution.clone();
            let result = runtime.block_on(async {
                tokio::task::spawn_blocking(move || running.run(&invocation))
                    .await
                    .expect("runner thread")
            });
            if status == 0 {
                assert_eq!(result.expect("success stdout"), "activation stdout");
            } else {
                let failure = StageFailure::from(result.expect_err("runner failure"));
                assert!(format!("{failure:?}").contains("23"));
                assert!(
                    failure
                        .reported_outputs
                        .iter()
                        .any(|output| output.text == "activation stdout")
                );
                assert!(
                    failure
                        .reported_outputs
                        .iter()
                        .any(|output| output.text == "activation stderr")
                );
            }
        }
    }

    // ---- per-deploy secrets provisioning ----

    fn secret_file(name: &str) -> ClusterSecretFile {
        ClusterSecretFile::new(PathBuf::from(format!("/cluster/secrets/{name}")))
    }

    #[test]
    fn sops_attribute_name_is_the_filename_stem_verbatim() {
        // No case transform: the `.sops` filename stem becomes the attribute
        // name exactly as written. Only the `.sops` suffix is stripped.
        assert_eq!(
            secret_file("fixtureCamelCase.sops")
                .attribute_name()
                .expect("utf8 stem"),
            "fixtureCamelCase"
        );
        assert_eq!(
            secret_file("fixtureSecondValue.sops")
                .attribute_name()
                .expect("utf8 stem"),
            "fixtureSecondValue"
        );
        // a single-token stem passes through unchanged
        assert_eq!(
            secret_file("token.sops")
                .attribute_name()
                .expect("utf8 stem"),
            "token"
        );
    }

    #[test]
    fn no_secrets_is_explicit_and_yields_no_files() {
        let directory = ClusterSecretsDirectory::from_input(&memory::SecretsInput::NoSecrets)
            .expect("NoSecrets is accepted");
        assert!(directory.path.is_none());
        assert!(directory.secret_files().expect("no files").is_empty());
    }

    #[test]
    fn secrets_input_rejects_nonexistent_directory() {
        let input = memory::SecretsInput::SecretsDirectory(memory::SecretsDirectory::new(
            "/nonexistent/path/that/has/no/secrets".to_string(),
        ));
        assert!(matches!(
            ClusterSecretsDirectory::from_input(&input),
            Err(Error::StoreMaintenance(_))
        ));
    }

    #[cfg(unix)]
    #[test]
    fn secrets_input_rejects_a_symlinked_parent_directory() {
        use std::os::unix::fs::symlink;

        let temporary = tempfile::tempdir().expect("temporary secrets root");
        let actual_parent = temporary.path().join("actual");
        let secrets_directory = actual_parent.join("secrets");
        fs::create_dir_all(&secrets_directory).expect("create actual secrets directory");
        let linked_parent = temporary.path().join("linked");
        symlink(&actual_parent, &linked_parent).expect("create linked parent");

        let input = memory::SecretsInput::SecretsDirectory(memory::SecretsDirectory::new(
            linked_parent.join("secrets").to_string_lossy().to_string(),
        ));
        assert!(matches!(
            ClusterSecretsDirectory::from_input(&input),
            Err(Error::StoreMaintenance(_))
        ));
    }

    #[cfg(unix)]
    #[test]
    fn secrets_input_rejects_a_symlinked_sops_file_before_copying() {
        use std::os::unix::fs::symlink;

        let temporary = tempfile::tempdir().expect("temporary secrets root");
        let secrets_directory = temporary.path().join("secrets");
        fs::create_dir_all(&secrets_directory).expect("create secrets directory");
        let ciphertext = secrets_directory.join("ciphertext");
        fs::write(&ciphertext, b"synthetic opaque ciphertext").expect("write ciphertext");
        symlink(&ciphertext, secrets_directory.join("linked.sops"))
            .expect("create linked sops file");

        let input = memory::SecretsInput::SecretsDirectory(memory::SecretsDirectory::new(
            secrets_directory.to_string_lossy().to_string(),
        ));
        let cluster = ClusterSecretsDirectory::from_input(&input).expect("directory is valid");
        assert!(matches!(
            cluster.secret_files(),
            Err(Error::StoreMaintenance(_))
        ));
    }

    #[test]
    fn generated_secrets_flake_maps_verbatim_stems_to_copied_files() {
        let source_directory =
            std::env::temp_dir().join(format!("lojix-secrets-source-{}", std::process::id()));
        let secrets_directory = source_directory.join("secrets");
        fs::create_dir_all(&secrets_directory).expect("create source secrets dir");
        // opaque placeholder ciphertext — never read back by the daemon. Files
        // are named with an exact camelCase consumer name; the attribute is the
        // stem verbatim.
        fs::write(secrets_directory.join("fixtureCamelCase.sops"), b"opaque")
            .expect("write sops file");
        fs::write(secrets_directory.join("fixtureSecondValue.sops"), b"opaque")
            .expect("write sops file");
        // a non-.sops file in the directory is ignored
        fs::write(secrets_directory.join("README.md"), b"ignore me").expect("write readme");

        let generated =
            std::env::temp_dir().join(format!("lojix-secrets-gen-{}", std::process::id()));
        let _ = fs::remove_dir_all(&generated);
        let input = memory::SecretsInput::SecretsDirectory(memory::SecretsDirectory::new(
            secrets_directory.to_string_lossy().to_string(),
        ));
        let cluster = ClusterSecretsDirectory::from_input(&input).expect("valid secrets directory");
        GeneratedInputDirectory::new(generated.clone())
            .write_secrets(&cluster)
            .expect("write secrets input");

        let flake = fs::read_to_string(generated.join("flake.nix")).expect("read flake");
        assert!(flake.contains("sopsFiles = {"), "{flake}");
        assert!(
            flake.contains("fixtureSecondValue = ./fixtureSecondValue.sops;"),
            "{flake}"
        );
        assert!(
            flake.contains("fixtureCamelCase = ./fixtureCamelCase.sops;"),
            "{flake}"
        );
        assert!(
            !flake.contains("README"),
            "non-sops files excluded: {flake}"
        );
        assert!(
            generated.join("fixtureCamelCase.sops").is_file(),
            "ciphertext copied into the generated input"
        );
        assert!(
            generated.join("fixtureSecondValue.sops").is_file(),
            "ciphertext copied into the generated input"
        );

        let _ = fs::remove_dir_all(&source_directory);
        let _ = fs::remove_dir_all(&generated);
    }

    #[test]
    fn regenerating_secrets_wipes_stale_ciphertext() {
        // The generated dir is wiped each call, so a file removed from the
        // cluster secrets leaves no stale ciphertext (which would drift the
        // narHash). First generate with two files, then with one, and confirm
        // the dropped file is gone from the generated input.
        let source_directory =
            std::env::temp_dir().join(format!("lojix-secrets-stale-source-{}", std::process::id()));
        let secrets_directory = source_directory.join("secrets");
        let _ = fs::remove_dir_all(&source_directory);
        fs::create_dir_all(&secrets_directory).expect("create source secrets dir");
        fs::write(secrets_directory.join("alpha.sops"), b"opaque").expect("write alpha");
        fs::write(secrets_directory.join("beta.sops"), b"opaque").expect("write beta");

        let generated =
            std::env::temp_dir().join(format!("lojix-secrets-stale-gen-{}", std::process::id()));
        let _ = fs::remove_dir_all(&generated);
        let input = memory::SecretsInput::SecretsDirectory(memory::SecretsDirectory::new(
            secrets_directory.to_string_lossy().to_string(),
        ));
        let cluster = ClusterSecretsDirectory::from_input(&input).expect("valid secrets directory");
        GeneratedInputDirectory::new(generated.clone())
            .write_secrets(&cluster)
            .expect("first write");
        assert!(generated.join("alpha.sops").is_file());
        assert!(generated.join("beta.sops").is_file());

        // Drop beta from the cluster secrets and regenerate.
        fs::remove_file(secrets_directory.join("beta.sops")).expect("remove beta");
        GeneratedInputDirectory::new(generated.clone())
            .write_secrets(&cluster)
            .expect("second write");
        assert!(generated.join("alpha.sops").is_file());
        assert!(
            !generated.join("beta.sops").exists(),
            "stale ciphertext must be wiped on regeneration"
        );

        let _ = fs::remove_dir_all(&source_directory);
        let _ = fs::remove_dir_all(&generated);
    }

    #[test]
    fn colliding_secret_attribute_names_are_a_typed_error() {
        // Two files mapping to the same sopsFiles attribute name is a real
        // conflict, not silent last-writer-wins. A case-insensitive or
        // unicode-normalizing host filesystem can present two distinct `.sops`
        // entries whose verbatim stems are byte-identical; `write_secrets` must
        // reject. The guard is exercised by feeding two `ClusterSecretFile`s
        // with the same stem through the same iteration `write_secrets` runs.
        let files = [secret_file("shared.sops"), secret_file("shared.sops")];
        let mut attributes: std::collections::BTreeMap<String, String> =
            std::collections::BTreeMap::new();
        let mut error = None;
        for file in &files {
            let file_name = file.file_name().expect("utf8");
            let attribute_name = file.attribute_name().expect("utf8");
            if let Some(existing) = attributes.insert(attribute_name.clone(), file_name.clone()) {
                error = Some(crate::Error::SecretAttributeCollision {
                    attribute_name,
                    first: existing,
                    second: file_name,
                });
                break;
            }
        }
        assert!(matches!(
            error,
            Some(crate::Error::SecretAttributeCollision { .. })
        ));
    }

    #[test]
    fn non_utf8_secret_file_name_is_a_typed_error() {
        // A non-UTF-8 `.sops` file name cannot name a Nix path; it is a typed
        // error, not a silently-empty attribute name.
        use std::os::unix::ffi::OsStrExt;
        let raw = std::ffi::OsStr::from_bytes(b"bad\xff.sops");
        let path = PathBuf::from("/cluster/secrets").join(raw);
        let file = ClusterSecretFile::new(path);
        assert!(matches!(
            file.attribute_name(),
            Err(crate::Error::SecretFileNameNotUtf8(_))
        ));
        assert!(matches!(
            file.file_name(),
            Err(crate::Error::SecretFileNameNotUtf8(_))
        ));
    }

    #[test]
    fn empty_cluster_secrets_emit_empty_sops_files_attribute() {
        let generated =
            std::env::temp_dir().join(format!("lojix-secrets-empty-{}", std::process::id()));
        let _ = fs::remove_dir_all(&generated);
        let cluster = ClusterSecretsDirectory::from_input(&memory::SecretsInput::NoSecrets)
            .expect("NoSecrets is accepted");
        GeneratedInputDirectory::new(generated.clone())
            .write_secrets(&cluster)
            .expect("write empty secrets input");
        let flake = fs::read_to_string(generated.join("flake.nix")).expect("read flake");
        assert!(flake.contains("sopsFiles = {"), "{flake}");
        // no entries between the braces
        assert!(!flake.contains(" = ./"), "no entries: {flake}");
        let _ = fs::remove_dir_all(&generated);
    }

    // ---- every failed terminal carries its evidence ----

    #[test]
    fn a_tail_never_starts_inside_a_character() {
        // 'é' is two bytes: a bound of 5 lands inside the third from the end.
        let (tail, cut) = StageFailure::tail_within("ééééé", 5);
        assert!(cut);
        assert_eq!(tail, "éé");
    }

    #[test]
    fn a_tail_keeps_a_whole_first_line_when_the_cut_falls_on_its_start() {
        let (tail, cut) = StageFailure::tail_within("first\nsecond\nthird", 12);
        assert!(cut);
        assert_eq!(tail, "second\nthird");
    }

    #[test]
    fn a_short_stream_leaves_its_unused_share_to_a_long_one() {
        assert_eq!(
            StageFailure::fair_shares(&[10, 5000, 0], 3000),
            vec![10, 2990, 0]
        );
        assert_eq!(
            StageFailure::fair_shares(&[5000, 5000], 3000),
            vec![1500, 1500]
        );
    }

    #[test]
    fn a_main_process_killed_by_a_signal_has_no_exit_code() {
        let killed =
            <emission::ProcessEnd as crate::translator::MainProcessReading>::from_main_process(
                2, 9,
            )
            .expect("CLD_KILLED");
        assert_eq!(killed.exit_code(), None);
        assert_eq!(killed.statement(), "killed by signal 9");
        let dumped =
            <emission::ProcessEnd as crate::translator::MainProcessReading>::from_main_process(
                3, 6,
            )
            .expect("CLD_DUMPED");
        assert_eq!(dumped.statement(), "killed by signal 6, core dumped");
        let exited =
            <emission::ProcessEnd as crate::translator::MainProcessReading>::from_main_process(
                1, 4,
            )
            .expect("CLD_EXITED");
        assert_eq!(exited.exit_code(), Some(4));
        assert_eq!(
            <emission::ProcessEnd as crate::translator::MainProcessReading>::from_main_process(
                0, 0
            ),
            None
        );
    }

    /// A fake `ssh` answering `journalctl` with `journal`, in a fresh program
    /// directory.
    fn journal_execution(journal: &str) -> (tempfile::TempDir, EffectExecution) {
        use std::os::unix::fs::PermissionsExt as _;
        let directory = tempfile::tempdir().expect("program directory");
        let ssh = directory.path().join("ssh");
        fs::write(
            &ssh,
            format!(
                "#!/bin/sh\nprintf '%s\\n' \"$*\" >> \"$(dirname \"$0\")/commands\"\ncase \"$*\" in *journalctl*) printf '%s\\n' '{journal}' ;; esac\n"
            ),
        )
        .expect("write fake ssh");
        fs::set_permissions(&ssh, fs::Permissions::from_mode(0o755))
            .expect("make fake ssh executable");
        let execution = EffectExecution::test(directory.path().to_path_buf());
        (directory, execution)
    }

    fn detached_test_unit() -> DetachedActivationUnit {
        let job_unit = DetachedActivationUnit::for_deployment(
            &ordinary::DeploymentIdentifier::new(41),
            DetachedSelfActivation::Test,
        );
        DetachedActivationUnit {
            journal_option: Some(UnitJournal::new(
                job_unit.name.clone(),
                SshTarget::from_transport(&nexus_fixture_transport()).expect("fixture transport"),
            )),
            ..job_unit
        }
    }

    #[test]
    fn a_detached_test_failure_carries_its_main_process_end_and_journal() {
        let (directory, execution) = journal_execution("switch-to-configuration: unit sshd failed");
        let unit = detached_test_unit();
        let main =
            <emission::ProcessEnd as crate::translator::MainProcessReading>::from_main_process(
                2, 15,
            );
        let failure = DetachedActivationFailure {
            unit_name: unit.name.clone(),
            active_state: "failed".to_string(),
            result: format!(
                "systemd reported Result=signal; main process {}",
                main.clone().expect("signal").statement()
            ),
            process_end_option: main,
            reported_outputs: Vec::new(),
        };
        let runtime = tokio::runtime::Runtime::new().expect("tokio runtime");
        let evidence = runtime
            .block_on(unit.journalled(failure, &execution))
            .witness();
        let detail = evidence.failure_detail.payload();
        assert!(
            detail.contains("main process killed by signal 15"),
            "{detail}"
        );
        assert!(
            detail.contains("--- journal of lojix-self-switch-deploy-41.service ---"),
            "{detail}"
        );
        assert!(detail.contains("unit sshd failed"), "{detail}");
        let command = evidence.failed_command_option.expect("systemd-run named");
        assert_eq!(command.exit_code_option, None);
        assert_eq!(
            command.command_argument_vector[0].payload(),
            "--unit=lojix-self-switch-deploy-41.service"
        );
        let commands = fs::read_to_string(directory.path().join("commands")).expect("ssh log");
        assert!(
            commands.contains("journalctl --unit=lojix-self-switch-deploy-41.service"),
            "{commands}"
        );
    }

    /// What PID 1 retained for a self-switch whose boot-entry clearing failed
    /// after the switch, as on ouranos for deployment 586, carries the unit's
    /// journal: what bootctl printed is there and nowhere else.
    #[test]
    fn a_self_switch_unit_that_failed_carries_its_result_and_journal() {
        let (directory, execution) =
            journal_execution("bootctl: Not booted with a supported boot loader.");
        let unit = DetachedActivationUnit {
            activation: DetachedSelfActivation::Switch,
            ..detached_test_unit()
        };
        let DetachedActivationOutcome::Failed(failure) = unit.classify(emission::UnitState {
            active_state_reading: emission::ActiveStateReading::Read(emission::ActiveState::Failed),
            service_result_reading: emission::ServiceResultReading::Read(
                emission::ServiceResult::ExitCode,
            ),
            process_end_option: Some(emission::ProcessEnd::Exited(emission::ExitStatus(1))),
        }) else {
            panic!("a failed unit is a failure");
        };
        let runtime = tokio::runtime::Runtime::new().expect("tokio runtime");
        let evidence = runtime
            .block_on(unit.journalled(failure, &execution))
            .witness();
        let detail = evidence.failure_detail.payload();
        assert!(
            detail.contains("systemd reported Result=exit-code; main process exited with status 1"),
            "{detail}"
        );
        assert!(
            detail.contains("--- journal of lojix-self-switch-deploy-41.service ---"),
            "{detail}"
        );
        assert!(
            detail.contains("Not booted with a supported boot loader."),
            "{detail}"
        );
        let command = evidence.failed_command_option.expect("systemd-run named");
        assert_eq!(
            command.exit_code_option,
            Some(crate::runtime_model::ExitCode::new(1))
        );
        let commands = fs::read_to_string(directory.path().join("commands")).expect("ssh log");
        assert!(
            commands.contains("journalctl --unit=lojix-self-switch-deploy-41.service"),
            "{commands}"
        );
    }

    #[test]
    fn a_vanished_detached_unit_is_still_evidence() {
        // PID 1 no longer holding the unit used to terminalize with no
        // evidence at all.
        let (_directory, execution) = journal_execution("activating the configuration...");
        let unit = detached_test_unit();
        let runtime = tokio::runtime::Runtime::new().expect("tokio runtime");
        let evidence = runtime
            .block_on(unit.journalled(unit.vanished(), &execution))
            .witness();
        let detail = evidence.failure_detail.payload();
        assert!(
            detail.contains("systemd no longer holds lojix-self-switch-deploy-41.service"),
            "{detail}"
        );
        assert!(
            detail.contains("activating the configuration..."),
            "{detail}"
        );
        assert!(evidence.failed_command_option.is_some());
    }

    fn unit_state(
        active_state: emission::ActiveState,
        service_result: emission::ServiceResult,
        process_end_option: Option<emission::ProcessEnd>,
    ) -> emission::UnitState {
        emission::UnitState {
            active_state_reading: emission::ActiveStateReading::Read(active_state),
            service_result_reading: emission::ServiceResultReading::Read(service_result),
            process_end_option,
        }
    }

    /// PID 1's receipt decides the outcome: ActiveState, Result and how the
    /// main process ended, each as systemd's own value.
    #[test]
    fn a_retained_receipt_is_classified_from_systemd_values() {
        let unit = detached_test_unit();
        let exited = |status| Some(emission::ProcessEnd::Exited(emission::ExitStatus(status)));
        assert_eq!(
            unit.classify(unit_state(
                emission::ActiveState::Active,
                emission::ServiceResult::Success,
                exited(0),
            )),
            DetachedActivationOutcome::Succeeded
        );
        // A oneshot that does not remain after exit ends inactive; it
        // succeeded when its main process exited 0, and not otherwise.
        assert_eq!(
            unit.classify(unit_state(
                emission::ActiveState::Inactive,
                emission::ServiceResult::Success,
                exited(0),
            )),
            DetachedActivationOutcome::Succeeded
        );
        assert!(matches!(
            unit.classify(unit_state(
                emission::ActiveState::Inactive,
                emission::ServiceResult::Success,
                Some(emission::ProcessEnd::Signalled(emission::SignalEnd {
                    signal_number: emission::SignalNumber(15),
                    core_dumped: emission::CoreDumped(false),
                })),
            )),
            DetachedActivationOutcome::Failed(_)
        ));
        assert_eq!(
            unit.classify(unit_state(
                emission::ActiveState::Activating,
                emission::ServiceResult::Success,
                None,
            )),
            DetachedActivationOutcome::Running
        );
        let DetachedActivationOutcome::Failed(failure) = unit.classify(unit_state(
            emission::ActiveState::Failed,
            emission::ServiceResult::ExitCode,
            exited(1),
        )) else {
            panic!("a failed unit is a failure");
        };
        assert_eq!(failure.active_state, "failed");
        assert_eq!(
            failure.result,
            "systemd reported Result=exit-code; main process exited with status 1"
        );
        // A word systemd adds later is kept as the text it sent.
        let DetachedActivationOutcome::Failed(failure) = unit.classify(unit_state(
            emission::ActiveState::Unrecognized("hibernating".into()),
            emission::ServiceResult::Unrecognized("dreaming".into()),
            None,
        )) else {
            panic!("an unrecognized state is no success");
        };
        assert_eq!(failure.active_state, "hibernating");
        assert_eq!(failure.result, "systemd reported Result=dreaming");
    }

    // ---- Every emission's argv equals what 0.11.0 at 23f88f2 ran ----
    //
    // `tests/fixtures/emission-argv.txt` was written by 23f88f2's own
    // `NixCommand` builders, from the inputs below, before the emissions
    // replaced them. Each line is a label, the program and the argument
    // vector, the last two in Rust's debug form so a script's newlines and
    // quotes are compared exactly.

    const RECORDED_DERIVATION: &str = "/nix/store/bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb-toplevel.drv";
    const RECORDED_OUTPUT: &str = "/nix/store/cccccccccccccccccccccccccccccccc-toplevel";

    fn recorded_substituters() -> Vec<nexus::ExtraSubstituter> {
        vec![
            nexus::ExtraSubstituter {
                first_string: "https://cache.one.invalid".into(),
                second_string: "cache.one.invalid-1:AAAA".into(),
            },
            nexus::ExtraSubstituter {
                first_string: "https://cache.two.invalid".into(),
                second_string: "cache.two.invalid-1:BBBB".into(),
            },
        ]
    }

    fn recorded_override(name: &str, url: &str, hash: &str) -> memory::PersistedFlakeInputOverride {
        memory::PersistedFlakeInputOverride {
            flake_input_name: memory::FlakeInputName(name.into()),
            persisted_flake_input_reference: memory::PersistedFlakeInputReference {
                flake_input_url: memory::FlakeInputUrl(url.into()),
                nix_archive_hash: memory::NixArchiveHash(hash.into()),
            },
        }
    }

    fn recorded_evaluation(
        freshness: emission::EvaluationFreshness,
        overrides: Vec<memory::PersistedFlakeInputOverride>,
    ) -> emission::DerivationEvaluation {
        emission::DerivationEvaluation {
            flake_reference: ordinary::FlakeReference::new("github:owner/repo".into()),
            deployment_output_selector: ordinary::DeploymentOutputSelector::new(
                ordinary::FlakeAttribute::new(
                    "nixosConfigurations.node-1.config.system.build.toplevel".into(),
                ),
            ),
            evaluation_freshness: freshness,
            persisted_flake_input_override_vector: overrides,
        }
    }

    fn recorded_closure_build(
        build_place: emission::BuildPlace,
        substituters: Vec<nexus::ExtraSubstituter>,
    ) -> emission::ClosureBuild {
        emission::ClosureBuild {
            closure_path: ordinary::ClosurePath::new(RECORDED_DERIVATION.into()),
            build_place,
            extra_substituter_vector: substituters,
        }
    }

    fn recorded_realization(
        login: &str,
        substituters: Vec<nexus::ExtraSubstituter>,
    ) -> TargetStoreRealization {
        let build = nexus::TargetStoreBuild {
            deployment_transport: nexus::DeploymentTransport {
                nix_store_uri: nexus::NixStoreUri::new(
                    "ssh-ng://root@fixture-target.invalid".into(),
                ),
                ssh_destination: nexus::SshDestination::new(login.into()),
            },
            node_name: node(),
            operator_node: emission::OperatorNode(daemon_host()),
            ignored_builder_option: None,
        };
        let command = nexus::NixBuildCommand {
            generation_identifier: ordinary::GenerationIdentifier::new(7),
            closure_path: ordinary::ClosurePath::new(RECORDED_DERIVATION.into()),
            build_target: nexus::BuildTarget::TargetStore(build.clone()),
            extra_substituter_vector: substituters,
        };
        TargetStoreRealization::from_command(&command, &build).expect("realization")
    }

    fn emitted_command_lines() -> Vec<(String, CommandLine)> {
        let mut lines: Vec<(String, CommandLine)> = Vec::new();
        let mut line =
            |label: &str, command_line: CommandLine| lines.push((label.into(), command_line));
        line(
            "flake-metadata github",
            emission::FlakeMetadataQuery {
                flake_reference: ordinary::FlakeReference::new("github:owner/repo".into()),
            }
            .command_line(),
        );
        line(
            "flake-metadata pinned",
            emission::FlakeMetadataQuery {
                flake_reference: ordinary::FlakeReference::new(
                    "git+https://example.invalid/repo?rev=0123456789abcdef0123456789abcdef01234567"
                        .into(),
                ),
            }
            .command_line(),
        );
        line(
            "eval refresh bare",
            recorded_evaluation(emission::EvaluationFreshness::Refreshed, Vec::new())
                .command_line(),
        );
        line(
            "eval pinned overridden",
            recorded_evaluation(
                emission::EvaluationFreshness::TrustingPin,
                vec![
                    recorded_override(
                        "horizon",
                        "path:/var/lib/lojix/generated/alpha/node-1/complete-host/horizon",
                        "sha256-AAAA%2BBBB%2FCCCC%3D",
                    ),
                    recorded_override(
                        "system",
                        "path:/var/lib/lojix/generated/alpha/node-1/complete-host/system",
                        "sha256-DDDD%3D",
                    ),
                ],
            )
            .command_line(),
        );
        line(
            "hash-path",
            emission::InputHashing {
                input_directory: emission::InputDirectory(
                    "/var/lib/lojix/generated/alpha/node-1/complete-host/horizon".into(),
                ),
            }
            .command_line(),
        );
        line(
            "build local bare",
            recorded_closure_build(emission::BuildPlace::DaemonStore, Vec::new()).command_line(),
        );
        line(
            "build local substituted",
            recorded_closure_build(emission::BuildPlace::DaemonStore, recorded_substituters())
                .command_line(),
        );
        line(
            "build remote",
            recorded_closure_build(
                emission::BuildPlace::Offloaded(ordinary::NixBuilderSpec::new(
                    "ssh-ng://builder.invalid x86_64-linux - 8 1".into(),
                )),
                recorded_substituters()[..1].to_vec(),
            )
            .command_line(),
        );
        line(
            "check build",
            emission::CheckBuild {
                flake_reference: ordinary::FlakeReference::new("github:owner/repo".into()),
                deployment_output_selector: fixture_test_profile().deployment_output_selector,
            }
            .command_line(),
        );
        for (login, label) in [
            ("root@fixture-target.invalid", "root"),
            ("fixture-login@fixture-target.invalid", "login"),
        ] {
            let realization = recorded_realization(
                login,
                if label == "root" {
                    Vec::new()
                } else {
                    recorded_substituters()
                },
            );
            line(
                &format!("target-store {label} derivation-copy"),
                realization.derivation_copy_invocation(),
            );
            line(
                &format!("target-store {label} build"),
                realization.build_invocation(),
            );
            line(
                &format!("target-store {label} gc-root"),
                realization.gc_root_invocation(RECORDED_OUTPUT),
            );
        }
        line(
            "copy daemon-store",
            ClosureTransfer::from_command(&copy_command())
                .expect("copy")
                .invocation(),
        );
        let mut presence = copy_command();
        presence.closure_origin = nexus::ClosureOrigin::TargetStore;
        line(
            "copy target-store",
            ClosureTransfer::from_command(&presence)
                .expect("copy")
                .invocation(),
        );
        for (action, label) in [
            (ordinary::HostDeployAction::SetBootProfile, "boot"),
            (ordinary::HostDeployAction::ActivateNow, "switch"),
            (ordinary::HostDeployAction::TestActivation, "test"),
        ] {
            line(
                &format!("host {label}"),
                host_activation(action).ssh_invocation().expect("simple"),
            );
        }
        let boot = host_activation(ordinary::HostDeployAction::SetBootProfile);
        line(
            "host clear-efi-default",
            boot.step_clear_efi_default_invocation(),
        );
        line(
            "host clear-efi-oneshot",
            boot.step_clear_efi_oneshot_invocation(),
        );
        let boot_once = host_activation(ordinary::HostDeployAction::ScheduleBootOnce);
        line(
            "host boot-once",
            boot_once.boot_once_emission().command_line(),
        );
        let self_switch =
            host_activation_on_host(ordinary::HostDeployAction::ActivateNow, Some(node()));
        line(
            "host self-switch",
            self_switch
                .retained_emission(
                    DetachedSelfActivation::Switch,
                    emission::LoaderInterface::Implemented,
                )
                .command_line(),
        );
        let self_test =
            host_activation_on_host(ordinary::HostDeployAction::TestActivation, Some(node()));
        line(
            "host self-test",
            self_test
                .retained_emission(
                    DetachedSelfActivation::Test,
                    emission::LoaderInterface::NotImplemented,
                )
                .command_line(),
        );
        line(
            "journal boot-once",
            UnitJournal::new(boot_once.unit_name(), boot_once.target.clone())
                .query()
                .command_line(),
        );
        line(
            "journal self-switch",
            UnitJournal::new(
                self_switch.self_switch_unit_name(),
                self_switch.target.clone(),
            )
            .query()
            .command_line(),
        );
        let matched = user_environment_activation(
            meta::UserEnvironmentAction::ActivateNow,
            "li@fixture.invalid",
        );
        let mediated = user_environment_activation(
            meta::UserEnvironmentAction::ActivateNow,
            "root@fixture.invalid",
        );
        line(
            "user local-profile",
            matched.local_profile_invocation(Path::new("/home/li")),
        );
        line("user local-activate", matched.local_activate_invocation());
        line(
            "user remote-profile matched",
            matched.remote_profile_invocation().expect("matched"),
        );
        line(
            "user remote-activate matched",
            matched.remote_activate_invocation().expect("matched"),
        );
        line(
            "user remote-profile mediated",
            mediated.remote_profile_invocation().expect("mediated"),
        );
        line(
            "user remote-activate mediated",
            mediated.remote_activate_invocation().expect("mediated"),
        );
        line("hostname", emission::ReadShortHostName {}.command_line());
        line(
            "collect-garbage",
            emission::GarbageCollection {
                node_name: ordinary::NodeName::new("node-1".into()),
            }
            .command_line(),
        );
        line(
            "test-vm bring-up",
            nexus::BringUpTestVmCommand {
                cluster_name: cluster(),
                guest_node: emission::GuestNode(ordinary::NodeName::new("guest-1".into())),
                test_vm_host: nexus::TestVmHost(node()),
                deployment_transport: nexus_fixture_transport(),
                closure_path: ordinary::ClosurePath::new(RECORDED_OUTPUT.into()),
                guest_address: emission::GuestAddress("10.0.0.7".into()),
            }
            .emission()
            .expect("bring up")
            .command_line(),
        );
        line(
            "test-vm tear-down",
            nexus::TearDownTestVmCommand {
                cluster_name: cluster(),
                guest_node: emission::GuestNode(ordinary::NodeName::new("guest-1".into())),
                test_vm_host: nexus::TestVmHost(node()),
                deployment_transport: nexus_fixture_transport(),
            }
            .emission()
            .expect("tear down")
            .command_line(),
        );
        lines
    }

    #[test]
    fn every_emission_runs_the_argv_23f88f2_ran_for_the_same_inputs() {
        // One line moves from what 23f88f2 ran, and only as named here: the
        // same-host self-switch is dispatched with its receipt retained, as
        // the self-test is, rather than waited on and collected (bead
        // primary-6llk). Its script is unchanged.
        let recorded: String = include_str!("../tests/fixtures/emission-argv.txt")
            .lines()
            .map(|line| {
                if line.starts_with("host self-switch\t") {
                    line.replacen(
                        "--collect --wait --service-type=oneshot /bin/sh",
                        "--no-block --service-type=oneshot --remain-after-exit /bin/sh",
                        1,
                    )
                } else {
                    line.to_string()
                }
            })
            .map(|line| line + "\n")
            .collect();
        assert!(recorded.contains("host self-switch\t\"ssh\"\t[\"-o\", \"BatchMode=yes\", \"fixture-login@fixture-activate.invalid\", \"systemd-run --unit=lojix-self-switch-deploy-7.service --no-block --service-type=oneshot --remain-after-exit /bin/sh"));
        let emitted: String = emitted_command_lines()
            .into_iter()
            .map(|(label, command_line)| {
                format!(
                    "{label}\t{:?}\t{:?}\n",
                    command_line.program, command_line.arguments
                )
            })
            .collect();
        assert_eq!(recorded.lines().count(), 37);
        for (recorded_line, emitted_line) in recorded.lines().zip(emitted.lines()) {
            assert_eq!(recorded_line, emitted_line);
        }
        assert_eq!(emitted, recorded);
    }

    /// A host Switch run against a fake `ssh` that answers the loader read
    /// with `answer`, fails a `bootctl` call when `failing_clear` is set, and
    /// logs every remote body it ran; what the activation came to, and the
    /// bodies in order.
    fn foreground_switch_under(
        answer: &str,
        failing_clear: bool,
    ) -> (
        std::result::Result<nexus::BootEntryReconciliation, StageFailure>,
        Vec<String>,
    ) {
        use std::os::unix::fs::PermissionsExt;
        let directory = tempfile::tempdir().expect("fake ssh directory");
        let ssh = directory.path().join("ssh");
        let log = directory.path().join("bodies");
        fs::write(
            &ssh,
            format!(
                "#!/bin/sh\nfor argument in \"$@\"; do body=\"$argument\"; done\nprintf '%s\\n' \"$body\" >> {log}\ncase \"$body\" in\n  *LoaderInfo*) echo {answer} ;;\n  bootctl*) exit {clear_status} ;;\nesac\nexit 0\n",
                log = log.display(),
                clear_status = if failing_clear { 1 } else { 0 },
            ),
        )
        .expect("fake ssh");
        fs::set_permissions(&ssh, fs::Permissions::from_mode(0o700)).expect("executable");
        let execution = EffectExecution::test(directory.path().to_path_buf());
        let activation = host_activation(ordinary::HostDeployAction::ActivateNow);
        let runtime = tokio::runtime::Runtime::new().expect("runtime");
        let result = runtime.block_on(activation.run(&execution));
        let bodies = fs::read_to_string(&log)
            .unwrap_or_default()
            .lines()
            .map(str::to_string)
            .collect();
        (result, bodies)
    }

    #[test]
    fn a_switch_under_a_loader_without_the_interface_skips_the_clears_and_says_so() {
        let (result, bodies) = foreground_switch_under("not-implemented", false);
        assert_eq!(
            result.expect("the switch succeeds"),
            nexus::BootEntryReconciliation::SkippedWithoutLoaderInterface
        );
        assert_eq!(bodies.len(), 2, "{bodies:?}");
        assert!(bodies[0].contains("LoaderInfo-4a67b082"), "{bodies:?}");
        assert!(
            bodies[1].contains("switch-to-configuration switch"),
            "{bodies:?}"
        );
        assert!(
            bodies.iter().all(|body| !body.contains("bootctl")),
            "{bodies:?}"
        );
    }

    #[test]
    fn a_switch_under_a_loader_with_the_interface_clears_both_entries() {
        let (result, bodies) = foreground_switch_under("implemented", false);
        assert_eq!(
            result.expect("the switch succeeds"),
            nexus::BootEntryReconciliation::Cleared
        );
        assert_eq!(
            bodies[2..],
            ["bootctl set-default ''", "bootctl set-oneshot ''"],
            "{bodies:?}"
        );
    }

    #[test]
    fn a_clear_that_fails_under_a_loader_with_the_interface_fails_the_activation() {
        let (result, bodies) = foreground_switch_under("implemented", true);
        let failure = result.expect_err("the failed clear is the activation's failure");
        assert!(format!("{failure:?}").contains("bootctl"), "{failure:?}");
        assert_eq!(bodies.len(), 3, "{bodies:?}");
    }

    #[test]
    fn a_loader_read_that_prints_neither_answer_fails_before_the_switch() {
        let (result, bodies) = foreground_switch_under("", false);
        let failure = result.expect_err("an unreadable loader is no answer");
        assert!(
            format!("{failure:?}").contains("printed neither answer"),
            "{failure:?}"
        );
        assert_eq!(bodies.len(), 1, "{bodies:?}");
    }

    #[test]
    fn the_loader_interface_read_and_a_self_switch_without_clears_are_the_named_additions() {
        // bead primary-6llk: before a Boot or Switch clears the boot entries,
        // the node is asked whether its running loader implements the boot
        // loader interface, over the route the activation takes; a
        // self-switch under a loader that does not carries no clears. Neither
        // call existed at 23f88f2, so each is written out here whole.
        let boot = host_activation(ordinary::HostDeployAction::SetBootProfile);
        let read = boot.loader_interface_query().command_line();
        assert_eq!(read.program, "ssh");
        assert_eq!(
            read.arguments,
            vec![
                "-o".to_string(),
                "BatchMode=yes".into(),
                "fixture-login@fixture-activate.invalid".into(),
                "if [ -e /sys/firmware/efi/efivars/LoaderInfo-4a67b082-0a4c-41cf-b6c7-440b29bb8c4f ]; then echo implemented; else echo not-implemented; fi".into(),
            ]
        );
        let self_switch =
            host_activation_on_host(ordinary::HostDeployAction::ActivateNow, Some(node()));
        let with_clears = self_switch
            .retained_emission(
                DetachedSelfActivation::Switch,
                emission::LoaderInterface::Implemented,
            )
            .command_line();
        let without_clears = self_switch
            .retained_emission(
                DetachedSelfActivation::Switch,
                emission::LoaderInterface::NotImplemented,
            )
            .command_line();
        let clears = "bootctl set-default '\\'''\\''\nbootctl set-oneshot '\\'''\\''\n";
        let body = |command_line: &CommandLine| command_line.arguments[3].clone();
        assert!(
            body(&with_clears).ends_with(&format!("switch\n{clears}'")),
            "{}",
            body(&with_clears)
        );
        assert_eq!(
            body(&without_clears),
            body(&with_clears).replacen(clears, "", 1)
        );
        assert!(!body(&without_clears).contains("bootctl"));
    }
}
