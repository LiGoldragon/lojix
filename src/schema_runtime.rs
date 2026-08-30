//! Hand-implemented `SchemaRuntime` noun — the single data-bearing type that
//! implements the handwritten `nexus::NexusEngine` decision boundary.
//!
//! `decide` is the routing brain (port plan §4.2): ordinary reads route to
//! `SemaRead`, ordinary subscription verbs reply with the token handshake, and
//! meta mutations route to `SemaWrite`. A meta `Deploy` opens the effect
//! pipeline (port plan §4.3): the write completion drives a chain of
//! `RunEffect` continuations — resolve flake auth, eval, build, copy, activate
//! — recording a phase transition between stages and finally replying
//! `DeployAccepted`. `run_effect` does real `nix` IO through `tokio::process::Command`
//! so actor-native request tasks await child processes directly instead of
//! routing Nexus execution through a blocking-pool bridge.

use std::fs;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::Arc;

use datomic::TextEdge;
use horizon_lib::name::{
    ClusterName as HorizonClusterName, NodeName as HorizonNodeName, UserName as HorizonUserName,
};
use horizon_lib::{ClusterProposal, Horizon, Viewpoint};
use protos::Text;
use tokio::io::AsyncReadExt;
use tokio::process::Command;

use crate::runtime_flow::{self as nexus, NexusEngine};
use crate::runtime_model as sema;

const CANONICAL_PROPOSAL_ARTIFACT: &str = "proposal.datom";

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

// The engine has no public-contract nouns. These small local facades retain
// local names while lowering them to the private
// ingress/egress roots.  They are deliberately value-only shims: no wire type
// can cross this module boundary.
#[allow(clippy::new_ret_no_self)]
mod ordinary {
    pub use crate::runtime_model::*;

    pub type Input = OrdinaryIngress;
    pub type Output = OrdinaryEgress;

    pub struct Watching;
    impl Watching {
        pub fn new(payload: SubscriptionOpened) -> SubscriptionOpened {
            payload
        }
    }
    pub struct WatchRejected;
    impl WatchRejected {
        pub fn new(payload: RejectedWatch) -> RejectedWatch {
            payload
        }
    }
    pub struct Unwatched;
    impl Unwatched {
        pub fn new(payload: SubscriptionClosed) -> SubscriptionClosed {
            payload
        }
    }
    pub struct Queried;
    impl Queried {
        pub fn new(payload: GenerationListing) -> GenerationListing {
            payload
        }
    }
    pub struct KeyMaterialChecked;
    impl KeyMaterialChecked {
        pub fn new(payload: KeyMaterialReport) -> KeyMaterialReport {
            payload
        }
    }
    pub struct TestRunsQueried;
    impl TestRunsQueried {
        pub fn new(payload: TestRunListing) -> TestRunListing {
            payload
        }
    }
    pub struct DeploymentEventsQueried;
    impl DeploymentEventsQueried {
        pub fn new(payload: EventLogPage) -> EventLogPage {
            payload
        }
    }
    pub struct QueryRejected;
    impl QueryRejected {
        pub fn new(payload: RejectedQuery) -> RejectedQuery {
            payload
        }
    }
}

#[allow(clippy::new_ret_no_self)]
mod meta {
    pub use crate::runtime_model::*;

    pub type Input = MetaIngress;
    pub type Output = MetaEgress;
    pub type DeployRequest = DeploySubmission;

    /// Private classification used only while lowering a failed deploy into a
    /// correlated local `DeploymentRecord`. It is never a wire type.
    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    pub enum DeployRejectionReason {
        ClusterUnknown,
        NodeUnknown,
        ProposalSourceUnreachable,
        FlakeReferenceMalformed,
        InvalidDeploymentRouting,
        BuilderUnreachable,
        DeploymentInFlight,
        UnsupportedDeployAction,
        InternalError,
        ActivationFailed,
    }

    pub struct DeployAccepted;
    impl DeployAccepted {
        pub fn new(payload: DeployHandle) -> DeployHandle {
            payload
        }
    }
    pub struct DeployRejected;
    impl DeployRejected {
        pub fn new(payload: RejectedDeploy) -> RejectedDeploy {
            payload
        }
    }
    pub struct Pinned;
    impl Pinned {
        pub fn new(payload: AppliedPin) -> AppliedPin {
            payload
        }
    }
    pub struct PinRejected;
    impl PinRejected {
        pub fn new(payload: RejectedPin) -> RejectedPin {
            payload
        }
    }
    pub struct Unpinned;
    impl Unpinned {
        pub fn new(payload: AppliedUnpin) -> AppliedUnpin {
            payload
        }
    }
    pub struct UnpinRejected;
    impl UnpinRejected {
        pub fn new(payload: RejectedUnpin) -> RejectedUnpin {
            payload
        }
    }
    pub struct Retired;
    impl Retired {
        pub fn new(payload: AppliedRetire) -> AppliedRetire {
            payload
        }
    }
    pub struct RetireRejected;
    impl RetireRejected {
        pub fn new(payload: RejectedRetire) -> RejectedRetire {
            payload
        }
    }
    pub struct Tested;
    impl Tested {
        pub fn new(payload: AcceptedTest) -> AcceptedTest {
            payload
        }
    }
    pub struct TestRejected;
    impl TestRejected {
        pub fn new(payload: RejectedTest) -> RejectedTest {
            payload
        }
    }
    pub struct Test;
    impl Test {
        pub fn new(payload: TestRequest) -> TestRequest {
            payload
        }
    }
}
use crate::{DaemonConfiguration, Error, Result, Store};

/// The lojix engine noun. Carries the durable `Store` (the four sema tables)
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
    /// behavior. Deploy evaluation/build is local for every target and does
    /// not use this name to select a remote Nix store.
    daemon_host: ordinary::NodeName,
    /// Program resolution shared by every external Nix, SSH, and process
    /// effect. Production uses the declarative service PATH; focused tests use
    /// an isolated fixture directory.
    effect_execution: EffectExecution,
    /// A test-only gate that the deploy pipeline awaits before its first effect
    /// runs. `None` in production (the pipeline runs straight through). A test
    /// holds the barrier closed to prove the daemon replies the accepted handle
    /// while the pipeline is still parked, then opens it to let the pipeline
    /// complete on the daemon-owned executor — the up9b decoupling witness.
    effect_barrier: Option<EffectBarrier>,
    /// The test-op defaults, projected from the daemon's binary startup
    /// configuration (report 54). `decide_meta_input` reads these to lower a
    /// `(Check …)` shorthand into a full `TestRun` — cluster, host, and mode
    /// all default from here. `None` when the daemon was configured without
    /// test defaults; a `(Check …)` then rejects with `NoTestDefaults` rather
    /// than guessing.
    test_defaults: Option<TestDefaults>,
}

/// Bounded execution settings for commands that leave the daemon process.
///
/// The optional program directory is a hermetic test seam: production resolves
/// command names through the daemon service PATH; focused tests install fake
/// `nix` and `ssh` programs in their own directory without modifying process
/// environment shared by other tests.
#[derive(Debug, Clone)]
struct EffectExecution {
    program_directory: Option<PathBuf>,
}

impl EffectExecution {
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
    test_nix_system: sema::NixSystem,
    test_output_selector: sema::DeploymentOutputSelector,
    /// The canonical ClusterProposal artifact projected to validate `(OnHost h)`
    /// against the node's declared host-set and to resolve `All` to the
    /// cluster's test-VM nodes. Empty when host-set validation is not
    /// configured.
    proposal_source: ordinary::ProposalSource,
}

impl TestDefaults {
    /// Lower one `TestRequest` to the resolved test targets it names. A
    /// `(Run …)` carries cluster/host/mode explicitly; a `(Check …)` fills all
    /// three from these defaults — the routine `(Check mercury)` form (report
    /// 54 decision D). `(Nodes [n …])` expands to one resolved run per named
    /// node; `All` is a projection sweep deferred to Unit 2b and resolves to no
    /// targets here (the caller rejects an empty resolution honestly rather
    /// than faking a run). Returns the resolved targets so the caller mints an
    /// identifier and records a Pending row per target.
    fn lower(&self, request: meta::TestRequest) -> Vec<ResolvedTestRun> {
        match request {
            meta::TestRequest::Run(run) => self.lower_run(run),
            meta::TestRequest::Check(check) => self.lower_check(check),
        }
    }

    /// Lower a full `(Run …)`: explicit cluster + host selection + mode, one
    /// resolved run per node in the selection.
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

    /// Lower a routine `(Check [n …])`: cluster, host, and mode all from these
    /// defaults; one resolved run per named node.
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

    /// Resolve a `HostSelection` against these defaults: `DefaultHost` reads
    /// the config default vm-host; `(OnHost h)` overrides to the named host
    /// (the declared-host-set membership check is a Unit-2b projection gate).
    fn resolve_host(&self, selection: ordinary::HostSelection) -> ordinary::NodeName {
        match selection {
            ordinary::HostSelection::DefaultHost => self.default_vm_host.clone(),
            ordinary::HostSelection::OnHost(host) => host,
        }
    }

    /// The explicit node list of a `NodeSelection`. `All` resolves to the
    /// cluster's test-VM nodes by projecting the configured proposal source
    /// (Unit 2b): every Pod node whose primary host (`super_node`) declares a
    /// `VmHost` service. An unconfigured or unreadable proposal source resolves
    /// `All` to no nodes, so the caller rejects an empty resolution honestly
    /// rather than faking a sweep.
    fn nodes_of(&self, selection: meta::NodeSelection) -> Vec<ordinary::NodeName> {
        match selection {
            meta::NodeSelection::Nodes(nodes) => nodes,
            meta::NodeSelection::All => self.all_test_vm_nodes(),
        }
    }

    /// Sweep the configured proposal for the cluster's test-VM-host nodes —
    /// every node whose `Machine::host_set` (its primary `super_node`, plus the
    /// additive `super_nodes` once Unit 1 lands on horizon main) is non-empty,
    /// i.e. a Pod hosted on a vmhost. Empty when no proposal source is
    /// configured or it fails to project.
    fn all_test_vm_nodes(&self) -> Vec<ordinary::NodeName> {
        ClusterProjection::from_source(&self.proposal_source)
            .map(|projection| projection.hosted_pod_nodes())
            .unwrap_or_default()
    }

    /// Synthetic defaults used only by in-process tests. Production startup
    /// configuration supplies no test fixture.
    fn test_default() -> Self {
        Self {
            cluster: ordinary::ClusterName::new("fixture-cluster"),
            default_vm_host: ordinary::NodeName::new("fixture-vm-host"),
            default_mode: ordinary::TestMode::Hermetic,
            test_flake: ordinary::FlakeReference::new("github:fixture-owner/fixture-test-flake"),
            test_nix_system: sema::NixSystem::new("x86_64-linux"),
            test_output_selector: sema::DeploymentOutputSelector::new(sema::FlakeAttribute::new(
                "checks.fixture-a",
            )),
            proposal_source: ordinary::ProposalSource::new(""),
        }
    }

    fn default_profile(&self) -> sema::TestExecutionProfile {
        sema::TestExecutionProfile {
            test_mode: self.default_mode,
            nix_system: self.test_nix_system.clone(),
            deployment_output_selector: self.test_output_selector.clone(),
            optional_deployment_transport: None,
        }
    }

    /// The configured proposal projection, if a proposal source is set. The
    /// host-set validation and the `All` sweep both read it; absent (empty)
    /// when host-set validation is not configured.
    fn projection(&self) -> Option<ClusterProjection> {
        ClusterProjection::from_source(&self.proposal_source)
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
            test_nix_system: sema::NixSystem::new(defaults.test_nix_system.clone()),
            test_output_selector: sema::DeploymentOutputSelector::new(sema::FlakeAttribute::new(
                defaults.test_output_selector.clone(),
            )),
            proposal_source: ordinary::ProposalSource::new(defaults.proposal_source.clone()),
        }
    }
}

/// A pipeline-pause gate the deploy job awaits once before its first effect.
/// Test-only: production [`RuntimeConfiguration`] carries `None`. Backed by a
/// semaphore that starts with zero permits (the pipeline parks on `acquire`)
/// until the test `open`s it. Lets a test prove ordering deterministically
/// without shelling out to `nix`.
#[derive(Debug, Clone)]
pub struct EffectBarrier {
    gate: Arc<tokio::sync::Semaphore>,
}

impl Default for EffectBarrier {
    fn default() -> Self {
        Self::held()
    }
}

impl EffectBarrier {
    /// A barrier that starts CLOSED — the pipeline parks at its first effect
    /// until [`Self::open`] is called.
    pub fn held() -> Self {
        Self {
            gate: Arc::new(tokio::sync::Semaphore::new(0)),
        }
    }

    /// Release the barrier so the parked pipeline proceeds. Idempotent enough
    /// for a test: adds a generous permit budget so every awaiter passes.
    pub fn open(&self) {
        self.gate.add_permits(1024);
    }

    /// Park until the barrier opens. The acquired permit is forgotten so the
    /// budget is not returned, keeping the barrier open for any later awaiter.
    async fn wait(&self) {
        if let Ok(permit) = self.gate.acquire().await {
            permit.forget();
        }
    }
}

/// Which single-write meta mutation is in flight, so a `WriteRejected` from the
/// SEMA engine routes back to the matching typed rejection reply
/// (`PinRejected` / `UnpinRejected` / `RetireRejected` / `DeployRejected`).
/// Deploy is multi-step and additionally tracked by `active_deploy`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MetaOperation {
    Deploy,
    Pin,
    Unpin,
    Retire,
    Test,
}

/// A fully-resolved test target — the `(Check …)`/`(Run …)` request lowered to
/// the concrete cluster, node, host, and mode the daemon records and (in Unit
/// 2b) dispatches. The `(Check …)` shorthand fills cluster/host/mode from
/// `TestDefaults`; the `(Run …)` full form carries them explicitly. This is the
/// spirit `State`->`Record` precedent: a distinct typed lowering, never an
/// under-filled wire struct.
#[derive(Debug, Clone)]
struct ResolvedTestRun {
    cluster: ordinary::ClusterName,
    node: ordinary::NodeName,
    host: ordinary::NodeName,
    profile: sema::TestExecutionProfile,
    /// The flake paired with the exact output selector the hermetic dispatch
    /// builds (and whose built microVM runner the live path brings up).
    flake: ordinary::FlakeReference,
}

impl ResolvedTestRun {
    /// The durable test-run row at acceptance: phase `Submitted`, outcome
    /// `Pending`, no closure yet. The decoupled executor rewrites it through
    /// the real phases (`BringingUp`/`Deploying`/…) to a terminal `Passed`
    /// (with the built closure) or `Failed(stage)` — never a faked pass.
    fn pending_record(&self, identifier: ordinary::TestRunIdentifier) -> ordinary::TestRunRecord {
        ordinary::TestRunRecord {
            test_run_identifier: identifier,
            cluster_name: self.cluster.clone(),
            node: self.node.clone(),
            host: self.host.clone(),
            test_mode: self.profile.test_mode,
            test_run_phase: ordinary::TestRunPhase::Submitted,
            test_outcome: ordinary::TestOutcome::Pending,
            optional_closure_path: None,
        }
    }

    /// The hermetic-check effect command for this run: build
    /// `<flake>#<request selector>`. The execution profile supplies the exact
    /// selector and Nix system; the daemon does not infer a node-shaped output.
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
            test_mode: self.profile.test_mode,
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
            optional_deployment_transport: self.profile.optional_deployment_transport.as_ref().map(
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

    /// The live bring-up command for this run: the report-51 host-untouched
    /// user-namespace bring-up of the built microVM runner on the resolved
    /// vmhost. The runner closure and guest IP are filled by the live path's
    /// preceding build; BUILT but not run live here (gated).
    fn bring_up_command(&self, runner: ordinary::ClosurePath) -> nexus::BringUpTestVmCommand {
        nexus::BringUpTestVmCommand {
            cluster_name: self.cluster.clone(),
            node: self.node.clone(),
            host: self.host.clone(),
            deployment_transport: self
                .nexus_test_execution_profile()
                .optional_deployment_transport
                .expect("live test transport is validated before dispatch"),
            closure_path: runner,
            string: String::new(),
        }
    }

    /// The live teardown command for this run: stop the user units so the tap +
    /// route vanish with the namespace, host netns byte-identical.
    fn tear_down_command(&self) -> nexus::TearDownTestVmCommand {
        nexus::TearDownTestVmCommand {
            cluster_name: self.cluster.clone(),
            node: self.node.clone(),
            host: self.host.clone(),
            deployment_transport: self
                .nexus_test_execution_profile()
                .optional_deployment_transport
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
struct TestPipeline {
    run: ResolvedTestRun,
    identifier: ordinary::TestRunIdentifier,
    stage: TestStage,
    /// The accepted database marker, replayed on the terminal reply.
    accepted_marker: ordinary::StateMarker,
}

/// The test pipeline cursor stage — the step that has just completed. The
/// executor reads it to emit the next effect or the terminal outcome write.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TestStage {
    /// Test accepted; the first effect (hermetic check, or live bring-up) runs
    /// next.
    Submitted,
    /// (Live) the VM was brought up; the deploy chain + assert run next.
    BroughtUp,
    /// (Live) the deploy + assert finished; teardown runs next.
    Asserted,
}

impl TestPipeline {
    /// The cursor at acceptance — stage `Submitted`, the marker stand-in filled
    /// at the durable write.
    fn accepted(run: ResolvedTestRun, identifier: ordinary::TestRunIdentifier) -> Self {
        Self {
            run,
            identifier,
            stage: TestStage::Submitted,
            accepted_marker: ordinary::StateMarker {
                commit_sequence: ordinary::CommitSequence::new(0),
                state_digest: ordinary::StateDigest::new(0),
            },
        }
    }

    /// The durable row at a given phase/outcome, carrying the run identity and
    /// (once built) the closure under test. Rewritten at every transition so a
    /// `(Query (ByTestRun …))` reads the latest committed step (Unit 2b
    /// observability).
    fn record_at(
        &self,
        phase: ordinary::TestRunPhase,
        outcome: ordinary::TestOutcome,
        closure_path: Option<ordinary::ClosurePath>,
    ) -> ordinary::TestRunRecord {
        ordinary::TestRunRecord {
            test_run_identifier: self.identifier.clone(),
            cluster_name: self.run.cluster.clone(),
            node: self.run.node.clone(),
            host: self.run.host.clone(),
            test_mode: self.run.profile.test_mode,
            test_run_phase: phase,
            test_outcome: outcome,
            optional_closure_path: closure_path,
        }
    }

    /// The container-lifecycle transition for a live bring-up/teardown state
    /// change — the driver the report-47 §2 `ContainerLifecycleRecord` table
    /// was scaffolded for. The container is named `vm-<node>`, the on-demand
    /// microVM this test brings up.
    fn container_transition(&self, state: sema::ContainerState) -> sema::ContainerTransition {
        sema::ContainerTransition {
            cluster_name: self.run.cluster.clone(),
            node_name: self.run.node.clone(),
            container_name: sema::ContainerName::new(format!("vm-{}", self.run.node.payload())),
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

/// A projected cluster — the canonical ClusterProposal artifact the daemon reads to validate
/// `(OnHost h)` against a node's declared host-set and to resolve `All` to the
/// cluster's test-VM-host nodes (Unit 2b host/node selection). Wraps the parsed
/// `ClusterProposal`; the host-set is read from each node's `Machine` primary
/// host (`super_node`), the single-host majority Unit 1 keeps byte-identical
/// (the additive `super_nodes` extends it the moment Unit 1 lands on horizon
/// main).
#[derive(Debug, Clone)]
struct ClusterProjection {
    proposal: ClusterProposal,
}

impl ClusterProjection {
    /// Load + parse the configured proposal.  An unavailable source never
    /// silently disables host-set validation: deploy admission rejects it, and
    /// this optional test-only projection simply has no trustworthy data.
    fn from_source(source: &ordinary::ProposalSource) -> Option<Self> {
        let proposal = ProposalFile::available(source)?.load().ok()?;
        Some(Self { proposal })
    }

    /// Validate that `host` is in `node`'s declared host-set. Rejects
    /// `NodeUnknown` if the node is absent from the projection, or
    /// `VmHostNotDeclaredForNode` if the resolved host is not the node's
    /// declared primary host (`super_node`). The single-host host-set is
    /// exactly `{super_node}`; once Unit 1 lands, `∪ super_nodes` widens it.
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

    /// The declared host-set of a node by name — its primary `super_node`,
    /// deduped. `None` when the node is not in the projection. (The additive
    /// `super_nodes` join is the Unit-1-on-main follow-on; today the pinned
    /// horizon-lib carries only `super_node`.)
    fn host_set_of(&self, node: &ordinary::NodeName) -> Option<Vec<String>> {
        let name = HorizonNodeName::try_new(node.payload().clone()).ok()?;
        let proposal = self.proposal.nodes.get(&name)?;
        Some(
            proposal
                .machine
                .super_node
                .as_ref()
                .map(|primary| vec![primary.as_str().to_string()])
                .unwrap_or_default(),
        )
    }

    /// Every Pod node hosted on a vmhost — a node whose declared host-set is
    /// non-empty (i.e. a `super_node` is set). The `All` selection sweeps these.
    /// The configured test profile selects its exact output, so a hosted-Pod
    /// predicate remains the right `All` expansion set.
    fn hosted_pod_nodes(&self) -> Vec<ordinary::NodeName> {
        self.proposal
            .nodes
            .iter()
            .filter(|(_, proposal)| proposal.machine.super_node.is_some())
            .map(|(name, _)| ordinary::NodeName::new(name.as_str().to_string()))
            .collect()
    }
}

/// One hermetic-check build — the real `nix build <exact-selector>
/// --print-out-paths` the daemon runs as the hermetic test effect. The selected
/// check owns its sandboxed VM, so this is a pure build: exit 0 + an out-path
/// = Passed; a non-zero exit = Failed(HermeticCheck). No SSH, tap, or live
/// host is inferred by the daemon.
#[derive(Debug, Clone)]
struct HermeticCheck {
    command: nexus::HermeticCheckCommand,
}

impl HermeticCheck {
    fn new(command: nexus::HermeticCheckCommand) -> Self {
        Self { command }
    }

    /// The exact `<flake>#<selector>` installable from the execution profile.
    fn installable(&self) -> String {
        format!(
            "{}#{}",
            self.command.flake_reference.payload(),
            self.command
                .test_execution_profile
                .deployment_output_selector
                .payload()
                .payload(),
        )
    }

    /// Run the real `nix build <installable> --print-out-paths`. On exit 0 the
    /// first printed line is the realised check out-path (the closure under
    /// test); on a non-zero exit the build/test failed.
    async fn run(
        &self,
        execution: &EffectExecution,
    ) -> std::result::Result<ordinary::ClosurePath, String> {
        let output = NixCommand::build_check(&self.installable())
            .run(execution)
            .await?;
        let closure_path = NixCommand::first_line(&output);
        canonical_nix_store_root(&closure_path)
            .then(|| ordinary::ClosurePath::new(closure_path))
            .ok_or_else(|| "nix hermetic check returned a noncanonical closure path".to_string())
    }
}

/// The live host-untouched VM lifecycle.
/// — the report-51 user-namespace bring-up/teardown of the built microVM
/// runner on the resolved vmhost. BUILT here, NOT run live (the first
/// Prometheus cycle is psyche-gated): the invocation shapes are constructed so
/// the bracket is provably end-to-end, but a live run is gated.
///
/// Bring-up `ssh <host-fqdn>` runs a `systemd-run --user` durable unit that
/// `unshare -rn`'s a private network namespace, creates the additive tap
/// inside it, and `nsenter`s the built runner — no sudo, no
/// switch-to-configuration, host netns byte-identical. Teardown
/// `systemctl --user stop`s the units so the tap + route vanish with the
/// namespace.
#[derive(Debug, Clone)]
struct LiveTestVm {
    target: SshTarget,
    node: ordinary::NodeName,
    runner: String,
    guest_ip: String,
}

impl LiveTestVm {
    fn from_bring_up(command: &nexus::BringUpTestVmCommand) -> std::result::Result<Self, String> {
        Ok(Self {
            target: SshTarget::from_transport(&command.deployment_transport)?,
            node: command.node.clone(),
            runner: command.closure_path.payload().clone(),
            guest_ip: command.string.clone(),
        })
    }

    fn from_tear_down(command: &nexus::TearDownTestVmCommand) -> std::result::Result<Self, String> {
        Ok(Self {
            target: SshTarget::from_transport(&command.deployment_transport)?,
            node: command.node.clone(),
            runner: String::new(),
            guest_ip: String::new(),
        })
    }

    /// The durable `--user` unit name for this guest's namespace bring-up
    /// (`lojix-test-vm-<node>`), the unit teardown stops.
    fn unit_name(&self) -> String {
        format!("lojix-test-vm-{}", self.node.payload())
    }

    /// The host-untouched bring-up invocation (report 51 §3): a `--user`
    /// systemd-run unit that `unshare -rn`s a private netns, brings up the
    /// additive tap inside it, and `nsenter`s the built runner. Constructed
    /// here; on a live (gated) run this is `.run().await`'d.
    fn bring_up_invocation(&self) -> NixCommand {
        let script = format!(
            "set -eu\n\
             systemd-run --user --unit={unit} --collect --service-type=notify \
             unshare -rn /bin/sh -c {body}\n",
            unit = self.unit_name(),
            body = ShellArgument::new(self.bring_up_body()).to_command_text(),
        );
        self.target
            .remote_invocation(ShellCommand::from_raw(script))
    }

    /// The in-namespace bring-up body: create the tap, route to the guest IP,
    /// then `nsenter` the built runner. The tap design maps one-to-one onto
    /// the C2-emitted `.network` content (report 51 §2), applied in the netns
    /// instead of host networkd.
    fn bring_up_body(&self) -> String {
        format!(
            "ip tuntap add dev vmt0 mode tap; \
             ip addr add 169.254.100.1/32 dev vmt0; \
             ip link set vmt0 up; \
             ip route add {guest_ip} dev vmt0; \
             exec {runner}",
            guest_ip = self.guest_ip,
            runner = self.runner,
        )
    }

    /// The host-untouched teardown invocation: stop the user units so the tap +
    /// route vanish with the namespace (host netns byte-identical).
    fn tear_down_invocation(&self) -> NixCommand {
        self.target
            .remote_invocation(ShellCommand::from_raw(format!(
                "systemctl --user stop {unit} || true",
                unit = self.unit_name(),
            )))
    }
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
    Rejected(meta::RejectedDeploy),
}

impl RuntimeConfiguration {
    pub fn from_daemon_configuration(configuration: &DaemonConfiguration) -> Self {
        Self {
            generated_inputs_directory: PathBuf::from(&configuration.state_directory_path)
                .join("generated-inputs"),
            daemon_host: ordinary::NodeName::new(configuration.daemon_host.clone()),
            effect_execution: EffectExecution::production(),
            effect_barrier: None,
            test_defaults: configuration.test_defaults.as_ref().map(TestDefaults::from),
        }
    }

    pub fn test_default() -> Self {
        Self {
            generated_inputs_directory: std::env::temp_dir().join("lojix-generated-inputs"),
            daemon_host: ordinary::NodeName::new("daemon-host"),
            effect_execution: EffectExecution::production(),
            effect_barrier: None,
            test_defaults: Some(TestDefaults::test_default()),
        }
    }

    /// A test configuration whose deploy pipeline parks at its first effect on
    /// the given barrier — the seam the up9b decoupling tests drive.
    pub fn test_with_effect_barrier(barrier: EffectBarrier) -> Self {
        Self {
            generated_inputs_directory: std::env::temp_dir().join("lojix-generated-inputs"),
            daemon_host: ordinary::NodeName::new("daemon-host"),
            effect_execution: EffectExecution::production(),
            effect_barrier: Some(barrier),
            test_defaults: Some(TestDefaults::test_default()),
        }
    }

    /// A hermetic focused-test configuration whose external command names are
    /// resolved from `program_directory`.
    /// Production configuration cannot set this directory; it resolves command
    /// names through the declarative service PATH.
    pub fn test_with_effect_program_directory(
        generated_inputs_directory: PathBuf,
        program_directory: PathBuf,
    ) -> Self {
        Self {
            generated_inputs_directory,
            daemon_host: ordinary::NodeName::new("daemon-host"),
            effect_execution: EffectExecution::test(program_directory),
            effect_barrier: None,
            test_defaults: Some(TestDefaults::test_default()),
        }
    }

    fn effect_barrier(&self) -> Option<&EffectBarrier> {
        self.effect_barrier.as_ref()
    }

    fn effect_execution(&self) -> &EffectExecution {
        &self.effect_execution
    }

    /// The node this daemon runs on — used to detect a self-targeting deploy so
    /// activation routes around the self-Switch deadlock (a foreground ssh that
    /// `switch-to-configuration switch` kills by restarting the daemon).
    pub fn daemon_host(&self) -> &ordinary::NodeName {
        &self.daemon_host
    }

    /// The configured test-op defaults, if the daemon was started with them.
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
struct DeployPipeline {
    deployment_identifier: ordinary::DeploymentIdentifier,
    generation_identifier: ordinary::GenerationIdentifier,
    cluster_name: ordinary::ClusterName,
    node_name: ordinary::NodeName,
    generation_artifact: ordinary::GenerationArtifact,
    activation_effect: ordinary::ActivationEffect,
    activation_slot: Option<ordinary::GenerationSlot>,
    source: ordinary::ProposalSource,
    requested_flake: ordinary::FlakeReference,
    flake: ordinary::FlakeReference,
    /// Exact, request-owned deployment routing. These values are persisted on
    /// the job cursor and copied to every effect; cluster/node identity never
    /// supplies an implicit route or output name.
    deployment_transport: sema::DeploymentTransport,
    deployment_input_mode: sema::DeploymentInputMode,
    deployment_output_selector: sema::DeploymentOutputSelector,
    activation_backend: sema::ActivationBackend,
    /// The deploy action (host action, or user-environment action + user). Owns the
    /// produces-closure / activates / target-attribute decisions so the
    /// pipeline asks the action rather than storing derived booleans.
    action: DeployAction,
    source_revision_policy: meta::SourceRevisionPolicy,
    source_revision: Option<ordinary::SourceRevisionRecord>,
    builder: Option<sema::NixBuilderSpec>,
    substituters: Vec<nexus::ExtraSubstituter>,
    input_overrides: Vec<nexus::FlakeInputOverride>,
    closure_path: Option<ordinary::ClosurePath>,
    accepted_marker: ordinary::StateMarker,
    stage: DeployStage,
    /// The exact next continuation action, durably mirrored on `DeployJob`.
    /// It advances only after the preceding materialization/result is stored.
    resume_stage: sema::DeployResumeStage,
    /// Exact receipt of the immediately preceding phase transition. It is
    /// persisted only after Store returns the commit receipt; a restart uses
    /// this real receipt to enter the runner continuation, never a zero or
    /// predicted marker.
    phase_receipt: Option<sema::PhaseReceipt>,
    submission: sema::DeploySubmission,
}

/// The deploy pipeline cursor. Each value names the stage that has just
/// completed; after a phase-transition write commits, `advance_after_phase`
/// reads it to emit the next effect (or the final activation-record write).
/// The chain is: Submitted -> (FlakeAuth) -> Building/Eval -> Build -> Copy ->
/// (Copying) -> Activate -> (Activated) -> RecordGenerationActivated -> DeployAccepted.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DeployStage {
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

impl DeployAction {
    /// `false` only for host `Evaluate` (derivation path only, no realised
    /// closure). User-environment actions always build a closure.
    fn produces_closure(&self) -> bool {
        match self {
            Self::Host(action) => !matches!(action, ordinary::HostDeployAction::Evaluate),
            Self::UserEnvironment { .. } => true,
        }
    }

    /// Whether the action copies + activates after the build: host
    /// SetBootProfile/ActivateNow/TestActivation/ScheduleBootOnce, or
    /// user-environment SetProfile/ActivateNow. Host Evaluate/Realize and
    /// user-environment Realize stop at the realised closure.
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

    /// The activation profile carried on the activate command — the shape that
    /// decides which target-side activation runs (host switch-to-configuration
    /// vs user-environment home-manager profile/activate).
    fn activation_profile(&self) -> nexus::ActivationProfile {
        match self {
            Self::Host(action) => nexus::ActivationProfile::Host(*action),
            Self::UserEnvironment { action, user } => {
                nexus::ActivationProfile::UserEnvironment(nexus::UserEnvironmentActivationProfile {
                    user_environment_action: *action,
                    user_name: user.clone(),
                })
            }
        }
    }
}

struct FlakeReferencePolicy<'a> {
    reference: &'a str,
}

impl<'a> FlakeReferencePolicy<'a> {
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
                || Self::credential_like(key)
                || Self::credential_like(value)
            {
                return false;
            }
            match key {
                "rev" if revision.replace(value).is_none() => {}
                "dir" if directory.replace(value).is_none() && Self::safe_relative_dir(value) => {}
                _ => return false,
            }
        }
        revision.is_some_and(|value| crate::immutable_revision(value).is_some())
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
                || Self::credential_like(key)
                || Self::credential_like(value)
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

    fn credential_like(value: &str) -> bool {
        let Some(value) = Self::percent_decode_once(value) else {
            return true;
        };
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

    /// Decode percent escapes exactly once before inspecting query values for
    /// credential-like material. A remaining `%` would require a second decode
    /// and is rejected rather than normalized ambiguously.
    fn percent_decode_once(value: &str) -> Option<String> {
        let bytes = value.as_bytes();
        let mut decoded = Vec::with_capacity(bytes.len());
        let mut index = 0;
        while index < bytes.len() {
            if bytes[index] != b'%' {
                decoded.push(bytes[index]);
                index += 1;
                continue;
            }
            let high = *bytes.get(index + 1)?;
            let low = *bytes.get(index + 2)?;
            let hex = |byte| match byte {
                b'0'..=b'9' => Some(byte - b'0'),
                b'a'..=b'f' => Some(byte - b'a' + 10),
                b'A'..=b'F' => Some(byte - b'A' + 10),
                _ => None,
            };
            decoded.push((hex(high)? << 4) | hex(low)?);
            index += 3;
        }
        let decoded = String::from_utf8(decoded).ok()?;
        (!decoded.contains('%')).then_some(decoded)
    }

    fn immutable_revision(&self) -> Option<sema::ImmutableRevision> {
        self.is_immutable().then(|| {
            self.reference
                .split_once('?')
                .and_then(|(_, query)| {
                    query
                        .split('&')
                        .find_map(|parameter| parameter.strip_prefix("rev="))
                })
                .and_then(crate::immutable_revision)
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
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum EvalRefresh {
    ForceRefresh,
    TrustImmutablePin,
}

impl EvalRefresh {
    /// The refresh decision for one eval, from the deploy's source-revision
    /// policy and its resolved flake reference. Only a `RequireImmutable`
    /// deploy whose reference actually carries an immutable identity may trust
    /// the eval cache; every other case refreshes.
    fn for_source(policy: ordinary::SourceRevisionPolicy, flake: &str) -> Self {
        match policy {
            ordinary::SourceRevisionPolicy::RequireImmutable
                if FlakeReferencePolicy::new(flake).is_immutable() =>
            {
                Self::TrustImmutablePin
            }
            _ => Self::ForceRefresh,
        }
    }

    fn adds_refresh_flag(self) -> bool {
        matches!(self, Self::ForceRefresh)
    }
}

struct NixFlakeMetadata {
    value: serde_json::Value,
}

impl NixFlakeMetadata {
    fn parse(text: &str) -> std::result::Result<Self, String> {
        serde_json::from_str(text)
            .map(|value| Self { value })
            .map_err(|error| format!("nix flake metadata returned malformed json: {error}"))
    }

    fn source_revision(
        &self,
        policy: ordinary::SourceRevisionPolicy,
        requested_ref: ordinary::FlakeReference,
    ) -> ordinary::SourceRevisionRecord {
        let resolved_ref = self
            .string_at(&["url"])
            .or_else(|| self.string_at(&["resolvedUrl"]))
            .map(ordinary::FlakeReference::new)
            .unwrap_or_else(|| requested_ref.clone());
        ordinary::SourceRevisionRecord {
            source_revision_policy: policy,
            requested_ref,
            resolved_ref,
            string: self.resolved_revision(),
        }
    }

    fn resolved_revision(&self) -> String {
        self.string_at(&["locked", "rev"])
            .or_else(|| self.string_at(&["revision"]))
            .or_else(|| self.string_at(&["rev"]))
            .or_else(|| self.string_at(&["locked", "narHash"]))
            .or_else(|| self.string_at(&["narHash"]))
            .unwrap_or_default()
    }

    fn string_at(&self, path: &[&str]) -> Option<String> {
        let mut value = &self.value;
        for key in path {
            value = value.get(*key)?;
        }
        value.as_str().map(ToOwned::to_owned)
    }
}

impl DeployPipeline {
    fn deployment_request_identity(
        submission: &sema::DeploySubmission,
    ) -> sema::DeploymentRequestIdentity {
        match submission {
            sema::DeploySubmission::Host(deployment) => sema::DeploymentRequestIdentity {
                deployment_environment: sema::DeploymentEnvironment::HostEnvironment,
                cluster_name: deployment.cluster_name.clone(),
                node_name: deployment.node_name.clone(),
                generation_artifact: Self::host_generation_artifact(deployment.host_composition),
                requested_deployment_action: sema::RequestedDeploymentAction::Host(
                    deployment.host_deploy_action,
                ),
                activation_effect: Self::host_activation_effect(deployment.host_deploy_action),
                source_revision_policy: deployment.source_revision_policy,
                optional_immutable_revision: FlakeReferencePolicy::new(
                    deployment.flake_reference.payload(),
                )
                .immutable_revision(),
            },
            sema::DeploySubmission::UserEnvironment(deployment) => {
                sema::DeploymentRequestIdentity {
                    deployment_environment: sema::DeploymentEnvironment::UserEnvironment(
                        deployment.user_name.clone(),
                    ),
                    cluster_name: deployment.cluster_name.clone(),
                    node_name: deployment.node_name.clone(),
                    generation_artifact: sema::GenerationArtifact::UserEnvironment,
                    requested_deployment_action: sema::RequestedDeploymentAction::UserEnvironment(
                        deployment.user_environment_action,
                    ),
                    activation_effect: Self::user_environment_activation_effect(
                        deployment.user_environment_action,
                    ),
                    source_revision_policy: deployment.source_revision_policy,
                    optional_immutable_revision: FlakeReferencePolicy::new(
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
        accepted_marker: ordinary::StateMarker,
        submission: sema::DeploySubmission,
    ) -> Self {
        let durable_submission = submission.clone();
        match submission {
            sema::DeploySubmission::Host(deployment) => Self {
                deployment_identifier,
                generation_identifier,
                cluster_name: deployment.cluster_name,
                node_name: deployment.node_name,
                generation_artifact: Self::host_generation_artifact(deployment.host_composition),
                activation_effect: Self::host_activation_effect(deployment.host_deploy_action),
                activation_slot: None,
                source: deployment.proposal_source,
                requested_flake: deployment.flake_reference.clone(),
                flake: deployment.flake_reference,
                deployment_transport: deployment.deployment_transport,
                deployment_input_mode: deployment.deployment_input_mode,
                deployment_output_selector: deployment.deployment_output_selector,
                activation_backend: deployment.activation_backend,
                action: DeployAction::Host(deployment.host_deploy_action),
                source_revision_policy: deployment.source_revision_policy,
                source_revision: None,
                builder: deployment.optional_nix_builder_spec,
                substituters: Self::convert_substituters(deployment.extra_substituter_vector),
                input_overrides: Vec::new(),
                closure_path: None,
                accepted_marker,
                stage: DeployStage::Submitted,
                resume_stage: sema::DeployResumeStage::ResolveFlakeAuth,
                phase_receipt: None,
                submission: durable_submission,
            },
            sema::DeploySubmission::UserEnvironment(deployment) => Self {
                deployment_identifier,
                generation_identifier,
                cluster_name: deployment.cluster_name,
                node_name: deployment.node_name,
                generation_artifact: ordinary::GenerationArtifact::UserEnvironment,
                activation_effect: Self::user_environment_activation_effect(
                    deployment.user_environment_action,
                ),
                activation_slot: None,
                source: deployment.proposal_source,
                requested_flake: deployment.flake_reference.clone(),
                flake: deployment.flake_reference,
                deployment_transport: deployment.deployment_transport,
                deployment_input_mode: deployment.deployment_input_mode,
                deployment_output_selector: deployment.deployment_output_selector,
                activation_backend: deployment.activation_backend,
                action: DeployAction::UserEnvironment {
                    action: deployment.user_environment_action,
                    user: deployment.user_name,
                },
                source_revision_policy: deployment.source_revision_policy,
                source_revision: None,
                builder: deployment.optional_nix_builder_spec,
                substituters: Self::convert_substituters(deployment.extra_substituter_vector),
                input_overrides: Vec::new(),
                closure_path: None,
                accepted_marker,
                stage: DeployStage::Submitted,
                resume_stage: sema::DeployResumeStage::ResolveFlakeAuth,
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

    fn convert_substituters(
        substituters: Vec<meta::ExtraSubstituter>,
    ) -> Vec<nexus::ExtraSubstituter> {
        substituters
            .into_iter()
            .map(|substituter| nexus::ExtraSubstituter {
                url: substituter.url,
                public_key: substituter.public_key,
            })
            .collect()
    }

    /// The build command runs through the local daemon Nix client. An explicit
    /// builder specification is passed verbatim to Nix; its result is imported
    /// locally before the explicit transport copies the exact closure onward.
    fn build_target(&self) -> nexus::BuildTarget {
        match &self.builder {
            Some(builder) => {
                nexus::BuildTarget::Remote(nexus::NixBuilderSpec::new(builder.payload().clone()))
            }
            None => nexus::BuildTarget::Local,
        }
    }

    fn flake_auth_request(&self) -> nexus::FlakeAuthRequest {
        nexus::FlakeAuthRequest {
            proposal_source: self.source.clone(),
            flake_reference: self.flake.clone(),
            source_revision_policy: self.source_revision_policy,
        }
    }

    fn needs_horizon_materialization(&self) -> bool {
        matches!(
            self.deployment_input_mode,
            sema::DeploymentInputMode::Horizon
        )
    }

    fn horizon_materialization_command(&self) -> nexus::HorizonMaterializationCommand {
        nexus::HorizonMaterializationCommand {
            cluster_name: self.cluster_name.clone(),
            node_name: self.node_name.clone(),
            proposal_source: self.source.clone(),
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

    fn nix_eval_command(&self) -> nexus::NixEvalCommand {
        nexus::NixEvalCommand {
            generation_identifier: self.generation_identifier.clone(),
            cluster_name: self.cluster_name.clone(),
            node_name: self.node_name.clone(),
            generation_artifact: self.generation_artifact,
            flake_reference: self.flake.clone(),
            source_revision_record: self.source_revision_record(),
            deployment_output_selector: nexus::DeploymentOutputSelector::new(
                nexus::FlakeAttribute::new(
                    self.deployment_output_selector.payload().payload().clone(),
                ),
            ),
            flake_input_override_vector: self.input_overrides.clone(),
            // The ordinary owner transport resolves and realizes locally, then
            // copies the exact closure to the target. This deliberately never
            // selects an ssh-ng evaluation store.
            build_target: self.build_target(),
        }
    }

    fn nix_build_command(&self, closure_path: ordinary::ClosurePath) -> nexus::NixBuildCommand {
        nexus::NixBuildCommand {
            generation_identifier: self.generation_identifier.clone(),
            closure_path,
            build_target: self.build_target(),
            extra_substituter_vector: self.substituters.clone(),
        }
    }

    fn copy_closure_command(
        &self,
        closure_path: ordinary::ClosurePath,
    ) -> nexus::CopyClosureCommand {
        nexus::CopyClosureCommand {
            generation_identifier: self.generation_identifier.clone(),
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
        }
    }

    fn activate_generation_command(
        &self,
        closure_path: ordinary::ClosurePath,
    ) -> nexus::ActivateGenerationCommand {
        nexus::ActivateGenerationCommand {
            deployment_identifier: self.deployment_identifier.clone(),
            generation_identifier: self.generation_identifier.clone(),
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
            activation_effect: self.activation_effect,
            activation_backend: match self.activation_backend {
                sema::ActivationBackend::NixosSystemdBootV1 => {
                    nexus::ActivationBackend::NixosSystemdBootV1
                }
                sema::ActivationBackend::HomeManagerNixProfileV1 => {
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
                source_revision_policy: self.source_revision_policy,
                requested_ref: self.requested_flake.clone(),
                resolved_ref: self.flake.clone(),
                string: String::new(),
            })
    }

    /// The activation-record write. The closure path, resolved source revision,
    /// and computed activation slot are mandatory: by the time the pipeline
    /// records activation they have been captured on the cursor (the activate
    /// command already required the closure and returned the slot), so `None`
    /// here is an internal invariant failure surfaced through
    /// `activation_commit` returning `None` rather than committing incomplete
    /// state into the live set.
    fn activation_commit(&self) -> Option<sema::ActivationCommit> {
        Some(sema::ActivationCommit {
            generation_identifier: self.generation_identifier.clone(),
            cluster_name: self.cluster_name.clone(),
            node_name: self.node_name.clone(),
            deployment_environment: self.deployment_environment(),
            generation_slot: self.activation_slot?,
            closure_path: self.closure_path.clone()?,
            source_revision_record: self.source_revision.clone()?,
        })
    }

    fn phase_event(
        &self,
        phase: ordinary::DeploymentPhase,
        event_log_position: ordinary::EventLogPosition,
        _detail: Option<String>,
    ) -> ordinary::DeploymentPhaseEvent {
        ordinary::DeploymentPhaseEvent {
            deployment_identifier: self.deployment_identifier.clone(),
            generation_identifier: self.generation_identifier.clone(),
            cluster_name: self.cluster_name.clone(),
            node_name: self.node_name.clone(),
            deployment_phase: phase,
            event_log_position,
            state_marker: self.accepted_marker.clone(),
            optional_immutable_revision: self
                .source_revision
                .as_ref()
                .and_then(|record| crate::immutable_revision(&record.string)),
            optional_deployment_terminal: None,
        }
    }

    fn deployment_environment(&self) -> sema::DeploymentEnvironment {
        match &self.action {
            DeployAction::Host(_) => sema::DeploymentEnvironment::HostEnvironment,
            DeployAction::UserEnvironment { user, .. } => {
                sema::DeploymentEnvironment::UserEnvironment(user.clone())
            }
        }
    }

    /// The BootOnce transient-unit name a resumed `Activating` job polls via
    /// `journalctl -u <unit>` instead of re-activating. Deterministic in the
    /// deployment identifier so the resumed daemon computes the same name that
    /// was persisted at submit, rather than a time/pid value it cannot
    /// reconstruct. `None` for non-BootOnce actions (which have no transient
    /// unit to poll; copy is idempotent and activation re-runs safely).
    fn boot_once_unit(&self) -> Option<String> {
        match &self.action {
            DeployAction::Host(ordinary::HostDeployAction::ScheduleBootOnce) => {
                Some(self.deployment_identifier.boot_once_unit_name())
            }
            _ => None,
        }
    }

    /// The durable in-flight job row at the given phase. Written on submit and
    /// rewritten at every phase transition (up9q): the persisted phase cursor,
    /// closure path (once built), exact private routing snapshot, and BootOnce
    /// unit name let a restarted daemon read the row without recomputing a
    /// route or selector.
    fn deploy_job(&self, phase: sema::DeployJobPhase) -> sema::DeployJob {
        sema::DeployJob {
            deployment_identifier: self.deployment_identifier.clone(),
            generation_identifier: self.generation_identifier.clone(),
            cluster_name: self.cluster_name.clone(),
            node_name: self.node_name.clone(),
            deploy_job_phase: phase,
            optional_closure_path: self.closure_path.clone(),
            source_revision_policy: self.source_revision_policy,
            flake_reference: self.requested_flake.clone(),
            optional_flake_reference: self
                .source_revision
                .as_ref()
                .map(|source_revision| source_revision.resolved_ref.clone()),
            resolved_revision: self
                .source_revision
                .as_ref()
                .map(|source_revision| source_revision.string.clone()),
            deployment_transport: self.deployment_transport.clone(),
            deployment_input_mode: self.deployment_input_mode,
            deployment_output_selector: self.deployment_output_selector.clone(),
            activation_backend: self.activation_backend,
            optional_nix_builder_spec: self.builder.clone(),
            boot_once_unit: self.boot_once_unit(),
            optional_generation_slot: self.activation_slot,
            persisted_flake_input_override_vector: self
                .input_overrides
                .iter()
                .map(|override_value| sema::PersistedFlakeInputOverride {
                    string: override_value.string.clone(),
                    persisted_flake_input_reference: sema::PersistedFlakeInputReference {
                        url: override_value.flake_input_reference.url.clone(),
                        nix_archive_hash: override_value
                            .flake_input_reference
                            .nix_archive_hash
                            .clone(),
                    },
                })
                .collect(),
            deploy_resume_stage: self.resume_stage,
            optional_phase_receipt: self.phase_receipt.clone(),
            optional_deploy_submission: Some(self.submission.clone()),
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

impl sema::DeployJob {
    /// The reconcile decision for this persisted job, read on daemon start. A
    /// pure projection of the persisted phase cursor — the daemon calls it once
    /// per resumed row and acts on the typed verdict (up9q resume scaffolding).
    pub fn resumption(&self) -> DeployJobResumption {
        match self.deploy_job_phase {
            sema::DeployJobPhase::Activating => DeployJobResumption::PollActivationUnit {
                unit: self.boot_once_unit.clone(),
            },
            sema::DeployJobPhase::Submitted
            | sema::DeployJobPhase::Building
            | sema::DeployJobPhase::Built
            | sema::DeployJobPhase::Copying => DeployJobResumption::RestartPipeline,
            sema::DeployJobPhase::Activated | sema::DeployJobPhase::Failed => {
                DeployJobResumption::AlreadyTerminal
            }
        }
    }

    /// The live-set + gc-root rows a restarted daemon persists for a detached
    /// self-switch it could not record before its own activation restarted it
    /// (bead primary-7u8p). A host `ActivateNow` whose target IS the daemon host
    /// runs `switch-to-configuration switch` inside a PID-1-owned unit that
    /// restarts the daemon mid-pipeline, so the terminal activation write never
    /// commits: the generation is lost, and because `next_deployment_identifier`
    /// scans only the live set, its id is reused on the next deploy.
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
    /// is the tight witness that this interrupted `Activating` job really is a
    /// finished host switch — genuinely `Current`. Every other row that could
    /// reach `Activating` on the daemon host (a racy unrelated restart during a
    /// self-host `SetBootProfile`/`TestActivation`, or a user-environment
    /// activation, none of which touch the system profile) fails the match and
    /// returns `None`, so it is left for S5 rather than mis-recorded as a
    /// Current complete-host generation. Also `None` when the job never captured
    /// a built closure path, so there is nothing to record.
    pub fn self_switch_activation_record(
        &self,
        daemon_host_system_closure: Option<&str>,
    ) -> Option<(sema::LiveGeneration, sema::GcRoot)> {
        if !self.is_persisted_host_activate_now() {
            return None;
        }
        let closure_path = self.optional_closure_path.clone()?;
        if daemon_host_system_closure != Some(closure_path.payload().as_str()) {
            return None;
        }
        let source_revision_record = ordinary::SourceRevisionRecord {
            source_revision_policy: self.source_revision_policy,
            requested_ref: self.flake_reference.clone(),
            resolved_ref: self
                .optional_flake_reference
                .clone()
                .unwrap_or_else(|| self.flake_reference.clone()),
            string: self.resolved_revision.clone().unwrap_or_default(),
        };
        let generation = sema::LiveGeneration {
            deployment_identifier: self.deployment_identifier.clone(),
            generation_identifier: self.generation_identifier.clone(),
            cluster_name: self.cluster_name.clone(),
            node_name: self.node_name.clone(),
            deployment_environment: sema::DeploymentEnvironment::HostEnvironment,
            // The detached self-switch is only reached by a host `ActivateNow`,
            // and a daemon host is a complete host, so the recorded artifact is
            // CompleteHost and the effect a live activation.
            generation_artifact: ordinary::GenerationArtifact::CompleteHost,
            activation_effect: ordinary::ActivationEffect::LiveActivation,
            generation_slot: ordinary::GenerationSlot::Current,
            closure_path: closure_path.clone(),
            source_revision_record,
        };
        let root = sema::GcRoot {
            generation_identifier: self.generation_identifier.clone(),
            cluster_name: self.cluster_name.clone(),
            node_name: self.node_name.clone(),
            generation_slot: ordinary::GenerationSlot::Current,
            closure_path,
            optional_pin_label: None,
        };
        Some((generation, root))
    }

    fn is_persisted_host_activate_now(&self) -> bool {
        self.activation_backend == sema::ActivationBackend::NixosSystemdBootV1
            && matches!(
                self.optional_deploy_submission.as_ref(),
                Some(sema::DeploySubmission::Host(host))
                    if host.host_deploy_action == ordinary::HostDeployAction::ActivateNow
                        && host.activation_backend == sema::ActivationBackend::NixosSystemdBootV1
            )
    }
}

impl From<ordinary::TestRunRecord> for sema::StoredTestRun {
    /// Project the wire test-run record onto the lojix-local durable row. The
    /// two carry identical fields (the LiveGeneration/Generation split): the
    /// daemon writes the durable row, the query reads it back as the wire shape.
    fn from(record: ordinary::TestRunRecord) -> Self {
        Self {
            test_run_identifier: record.test_run_identifier,
            cluster_name: record.cluster_name,
            node: record.node,
            host: record.host,
            test_mode: record.test_mode,
            test_run_phase: record.test_run_phase,
            test_outcome: record.test_outcome,
            optional_closure_path: record.optional_closure_path,
        }
    }
}

impl From<sema::StoredTestRun> for ordinary::TestRunRecord {
    /// Project the durable row back onto the wire record for the
    /// `(ByTestRun …)` query reply.
    fn from(run: sema::StoredTestRun) -> Self {
        Self {
            test_run_identifier: run.test_run_identifier,
            cluster_name: run.cluster_name,
            node: run.node,
            host: run.host,
            test_mode: run.test_mode,
            test_run_phase: run.test_run_phase,
            test_outcome: run.test_outcome,
            optional_closure_path: run.optional_closure_path,
        }
    }
}

impl From<ordinary::DeploymentPhase> for sema::DeployJobPhase {
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

impl SchemaRuntime {
    pub fn new() -> Self {
        Self::with_store(Arc::new(
            Store::open(Self::test_store_path()).expect("open sema store"),
        ))
    }

    /// A unique tempdir-backed `*.sema` path for a test-only `Store`. Each call
    /// names a fresh file under the temp directory (process id, a nanosecond
    /// timestamp, and a per-process counter) so parallel tests never collide and
    /// each starts from a virgin store.
    fn test_store_path() -> PathBuf {
        use std::sync::atomic::{AtomicU64, Ordering};
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let nanoseconds = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|elapsed| elapsed.as_nanos())
            .unwrap_or(0);
        let unique = COUNTER.fetch_add(1, Ordering::SeqCst);
        std::env::temp_dir().join("lojix-test-store").join(format!(
            "{}-{nanoseconds}-{unique}.sema",
            std::process::id()
        ))
    }

    /// Build an engine over a SHARED `Store`. The daemon constructs one per
    /// request from a single shared `Arc<Store>`, so concurrent requests share
    /// the durable tables but each owns its in-flight deploy cursor (intent 2alg).
    pub fn with_store(store: Arc<Store>) -> Self {
        Self::with_store_and_configuration(store, Arc::new(RuntimeConfiguration::test_default()))
    }

    pub fn with_store_and_configuration(
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

    pub fn store(&self) -> &Store {
        self.store.as_ref()
    }

    /// The deployment identifier currently on this engine's in-flight cursor, if
    /// any — the durable handle the job actor uses to track the deploy and that
    /// a watcher re-observes by. `None` outside an active deploy.
    pub fn active_deployment_identifier(&self) -> Option<ordinary::DeploymentIdentifier> {
        self.active_deploy
            .as_ref()
            .map(|pipeline| pipeline.deployment_identifier.clone())
    }

    /// Record a capacity refusal with the same durable correlation discipline
    /// as every other rejected deploy. The daemon calls this before any pipeline
    /// is created, so a full queue still cannot return an anonymous rejection.
    pub fn reject_deployment_in_flight(
        &self,
        request: sema::DeploySubmission,
    ) -> sema::RejectedDeploy {
        self.reject_submission(request, meta::DeployRejectionReason::DeploymentInFlight)
    }

    /// Run ONLY the synchronous submit step of a `Deploy` (up9q surface a): the
    /// reject-guard, restart-safe identifier issuance, in-flight job-row
    /// persistence at `Submitted`, and cursor construction. Returns the typed
    /// admission outcome immediately — the `DeployHandle` handle the daemon
    /// replies before the pipeline runs, or a typed rejection. On accept the
    /// in-flight cursor is left set on `self`, so the daemon hands this engine
    /// to the deploy-job actor, which drives the pipeline via
    /// [`Self::drive_submitted_deploy`]. The pipeline does NOT run here.
    pub fn submit_deploy(&mut self, request: meta::DeployRequest) -> DeploySubmissionOutcome {
        if let Some(reason) = Self::unsupported_deploy_reason(&request) {
            return DeploySubmissionOutcome::Rejected(self.reject_submission(request, reason));
        }
        if let Some(reason) = Self::deployment_routing_rejection(&request) {
            return DeploySubmissionOutcome::Rejected(self.reject_submission(request, reason));
        }
        if let Some(reason) = Self::proposal_source_rejection(&request) {
            return DeploySubmissionOutcome::Rejected(self.reject_submission(request, reason));
        }
        if let Some(reason) = Self::source_revision_policy_rejection(&request) {
            return DeploySubmissionOutcome::Rejected(self.reject_submission(request, reason));
        }
        self.active_operation = Some(MetaOperation::Deploy);
        let rejected_request = request.clone();
        let submission = match request {
            meta::DeployRequest::Host(deployment) => sema::DeploySubmission::Host(deployment),
            meta::DeployRequest::UserEnvironment(deployment) => {
                sema::DeploySubmission::UserEnvironment(deployment)
            }
        };
        match self.record_deploy_submitted(submission) {
            sema::SemaWriteOutput::DeploySubmitted(accepted) => {
                DeploySubmissionOutcome::Accepted(accepted)
            }
            sema::SemaWriteOutput::WriteRejected(report) => {
                self.active_operation = None;
                self.active_deploy = None;
                DeploySubmissionOutcome::Rejected(self.reject_submission(
                    rejected_request,
                    Self::deploy_reason(report.rejection_reason),
                ))
            }
            // `record_deploy_submitted` only ever returns the two arms above;
            // any other output is an internal invariant violation surfaced as a
            // typed rejection rather than a panic.
            _ => {
                self.active_operation = None;
                self.active_deploy = None;
                DeploySubmissionOutcome::Rejected(self.reject_submission(
                    rejected_request,
                    meta::DeployRejectionReason::InternalError,
                ))
            }
        }
    }

    /// Reconstruct an accepted deploy from its daemon-local persisted
    /// submission and correlation receipt. This never allocates a second
    /// identity or emits another Submitted event.
    pub fn resume_deploy_job(&mut self, job: sema::DeployJob) -> Result<bool> {
        let persisted_phase = job.deploy_job_phase;
        let Some(submission) = job.optional_deploy_submission.clone() else {
            return Ok(false);
        };
        if job
            .optional_closure_path
            .as_ref()
            .is_some_and(|closure_path| !canonical_nix_store_root(closure_path.payload()))
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
        let Some(admission) = record.optional_admission_marker else {
            return Err(Error::Invariant(
                "persisted deploy job lacks its admission receipt".to_string(),
            ));
        };
        if matches!(
            record.deployment_lifecycle,
            sema::DeploymentLifecycle::Completed
                | sema::DeploymentLifecycle::Rejected
                | sema::DeploymentLifecycle::Failed
        ) {
            return Ok(false);
        }
        let mut pipeline = DeployPipeline::from_submission(
            job.deployment_identifier.clone(),
            job.generation_identifier.clone(),
            admission.into_payload(),
            submission,
        );
        if pipeline.deployment_transport != job.deployment_transport
            || pipeline.deployment_input_mode != job.deployment_input_mode
            || pipeline.deployment_output_selector != job.deployment_output_selector
            || pipeline.activation_backend != job.activation_backend
            || pipeline.builder != job.optional_nix_builder_spec
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
        pipeline.builder = job.optional_nix_builder_spec.clone();
        pipeline.closure_path = job.optional_closure_path.clone();
        pipeline.activation_slot = job.optional_generation_slot;
        pipeline.input_overrides = job
            .persisted_flake_input_override_vector
            .iter()
            .map(|override_value| nexus::FlakeInputOverride {
                string: override_value.string.clone(),
                flake_input_reference: nexus::FlakeInputReference {
                    url: override_value.persisted_flake_input_reference.url.clone(),
                    nix_archive_hash: override_value
                        .persisted_flake_input_reference
                        .nix_archive_hash
                        .clone(),
                },
            })
            .collect();
        pipeline.resume_stage = job.deploy_resume_stage;
        pipeline.phase_receipt = job.optional_phase_receipt.clone();
        let mut recovered_phase_receipt = false;
        if pipeline.phase_receipt.is_none()
            && matches!(
                pipeline.resume_stage,
                sema::DeployResumeStage::NixEval
                    | sema::DeployResumeStage::ActivateGeneration
                    | sema::DeployResumeStage::RecordGenerationActivated
            )
        {
            let expected_phase = match pipeline.resume_stage {
                sema::DeployResumeStage::NixEval => ordinary::DeploymentPhase::Building,
                sema::DeployResumeStage::ActivateGeneration => ordinary::DeploymentPhase::Copying,
                sema::DeployResumeStage::RecordGenerationActivated => {
                    ordinary::DeploymentPhase::Activated
                }
                _ => unreachable!("phase receipt is required only for recorded phases"),
            };
            let matching_intents: Vec<_> = self
                .store
                .pending_transition_intents()?
                .into_iter()
                .filter(|intent| {
                    intent.deployment_identifier == pipeline.deployment_identifier
                        && intent.deployment_phase == expected_phase
                        && matches!(
                            intent.transition_intent_state,
                            sema::TransitionIntentState::Acknowledged
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
                .optional_transition_marker
                .ok_or_else(|| {
                    Error::Invariant("acknowledged phase intent lacks a marker".to_string())
                })?
                .into_payload();
            let matching_receipts: Vec<_> = self
                .store
                .event_log_in_range(0, u64::MAX)?
                .into_iter()
                .filter_map(|entry| match entry.logged_event {
                    sema::LoggedEvent::Deployment(event)
                        if event.deployment_identifier == pipeline.deployment_identifier
                            && event.event_log_position == intent.event_log_position
                            && event.state_marker == marker =>
                    {
                        Some(sema::PhaseReceipt {
                            event_log_position: event.event_log_position,
                            state_marker: event.state_marker,
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
        pipeline.stage = match pipeline.resume_stage {
            sema::DeployResumeStage::NixEval => DeployStage::Submitted,
            sema::DeployResumeStage::ActivateGeneration => DeployStage::BuildingRecorded,
            sema::DeployResumeStage::RecordGenerationActivated => DeployStage::CopyingRecorded,
            sema::DeployResumeStage::FinishDeployment => DeployStage::ActivatedRecorded,
            _ => DeployStage::Submitted,
        };
        pipeline.source_revision = job.optional_flake_reference.clone().map(|resolved_ref| {
            ordinary::SourceRevisionRecord {
                source_revision_policy: job.source_revision_policy,
                requested_ref: job.flake_reference.clone(),
                resolved_ref,
                string: job.resolved_revision.clone().unwrap_or_default(),
            }
        });
        if let Some(source_revision) = pipeline.source_revision.as_ref() {
            pipeline.flake = source_revision.resolved_ref.clone();
        }
        self.active_operation = Some(MetaOperation::Deploy);
        self.active_deploy = Some(pipeline);
        if recovered_phase_receipt {
            self.persist_job_phase(persisted_phase);
        }
        Ok(true)
    }

    /// Drive an already-submitted deploy's effect pipeline to its terminal
    /// reply (up9q surface a, the daemon-owned executor body). Requires the
    /// in-flight cursor to be set by a prior [`Self::submit_deploy`]; re-enters
    /// the handwritten runner at the persisted continuation. A newly submitted
    /// job starts at `ResolveFlakeAuth`; a restarted job seeds the handwritten
    /// runner with its durable predecessor result instead, so it never reruns
    /// resolver/Horizon work that already committed. The
    /// returned `meta::Output` is daemon-internal executor evidence for logging
    /// and tests; the client already has its admission handle and re-observes
    /// the outcome by deployment identifier.
    pub async fn drive_submitted_deploy(&mut self) -> meta::Output {
        let accepted = match self.active_deploy.as_ref() {
            Some(pipeline) => meta::DeployHandle {
                deployment_identifier: pipeline.deployment_identifier.clone(),
                state_marker: pipeline.accepted_marker.clone(),
            },
            None => {
                return meta::Output::DeployRejected(meta::DeployRejected::new(
                    self.deploy_rejection(meta::DeployRejectionReason::InternalError),
                ));
            }
        };
        let pipeline = self
            .active_deploy
            .as_ref()
            .expect("active deploy checked above");
        if matches!(
            pipeline.resume_stage,
            sema::DeployResumeStage::FinishDeployment
        ) {
            return match self.finish_deploy_pipeline() {
                nexus::NexusAction::ReplyToSignal(nexus::SignalOutput::MetaOutput(output)) => {
                    output
                }
                _ => meta::Output::DeployRejected(meta::DeployRejected::new(
                    self.deploy_rejection(meta::DeployRejectionReason::InternalError),
                )),
            };
        }
        let work = match pipeline.resume_stage {
            sema::DeployResumeStage::ResolveFlakeAuth => nexus::NexusWork::SemaWriteCompleted(
                sema::SemaWriteOutput::DeploySubmitted(accepted),
            ),
            sema::DeployResumeStage::MaterializeHorizon => {
                let Some(revision) = pipeline.source_revision.clone() else {
                    return meta::Output::DeployRejected(meta::DeployRejected::new(
                        self.deploy_rejection(meta::DeployRejectionReason::InternalError),
                    ));
                };
                nexus::NexusWork::EffectCompleted(nexus::EffectResult::flake_resolved(revision))
            }
            sema::DeployResumeStage::RecordBuilding => nexus::NexusWork::EffectCompleted(
                nexus::EffectResult::horizon_materialized(pipeline.input_overrides.clone()),
            ),
            sema::DeployResumeStage::NixEval => {
                let Some(receipt) = pipeline.phase_receipt.clone() else {
                    return meta::Output::DeployRejected(meta::DeployRejected::new(
                        self.deploy_rejection(meta::DeployRejectionReason::InternalError),
                    ));
                };
                nexus::NexusWork::SemaWriteCompleted(sema::SemaWriteOutput::PhaseRecorded(receipt))
            }
            sema::DeployResumeStage::NixBuild => {
                let Some(closure_path) = pipeline.closure_path.clone() else {
                    return meta::Output::DeployRejected(meta::DeployRejected::new(
                        self.deploy_rejection(meta::DeployRejectionReason::InternalError),
                    ));
                };
                nexus::NexusWork::EffectCompleted(nexus::EffectResult::closure_evaluated(
                    nexus::EvaluatedClosure {
                        generation_identifier: pipeline.generation_identifier.clone(),
                        closure_path,
                    },
                ))
            }
            sema::DeployResumeStage::CopyClosure => {
                let Some(closure_path) = pipeline.closure_path.clone() else {
                    return meta::Output::DeployRejected(meta::DeployRejected::new(
                        self.deploy_rejection(meta::DeployRejectionReason::InternalError),
                    ));
                };
                nexus::NexusWork::EffectCompleted(nexus::EffectResult::closure_built(
                    nexus::BuiltClosure {
                        generation_identifier: pipeline.generation_identifier.clone(),
                        closure_path,
                    },
                ))
            }
            sema::DeployResumeStage::ActivateGeneration
            | sema::DeployResumeStage::RecordGenerationActivated => {
                let Some(receipt) = pipeline.phase_receipt.clone() else {
                    return meta::Output::DeployRejected(meta::DeployRejected::new(
                        self.deploy_rejection(meta::DeployRejectionReason::InternalError),
                    ));
                };
                nexus::NexusWork::SemaWriteCompleted(sema::SemaWriteOutput::PhaseRecorded(receipt))
            }
            sema::DeployResumeStage::FinishDeployment => unreachable!("handled above"),
        }
        .with_origin_route(nexus::OriginRoute::new(0));
        match self.execute(work).await.into_root() {
            nexus::NexusAction::ReplyToSignal(nexus::SignalOutput::MetaOutput(output)) => output,
            _ => meta::Output::DeployRejected(meta::DeployRejected::new(
                self.deploy_rejection(meta::DeployRejectionReason::InternalError),
            )),
        }
    }

    /// Run ONLY the synchronous submit of a `Test` (Unit 2b, mirroring
    /// [`Self::submit_deploy`]): lower + validate, record the Pending row, set
    /// the in-flight test cursor, and return the `AcceptedTest` handle the
    /// daemon replies before the real dispatch runs. On accept the cursor is
    /// left set on `self` so the daemon hands this engine to the test-job actor,
    /// which drives the dispatch via [`Self::drive_submitted_test`]. The
    /// hermetic build / live cycle does NOT run here.
    pub async fn submit_test(&mut self, request: meta::TestRequest) -> TestSubmissionOutcome {
        let work = nexus::NexusWork::SignalArrived(nexus::SignalInput::MetaInput(
            meta::Input::Test(meta::Test::new(request)),
        ))
        .with_origin_route(nexus::OriginRoute::new(0));
        match self.execute(work).await.into_root() {
            nexus::NexusAction::ReplyToSignal(nexus::SignalOutput::MetaOutput(
                meta::Output::Tested(accepted),
            )) => {
                // Stamp the accepted marker onto the cursor so the terminal
                // outcome reply carries the acceptance marker (like deploy).
                if let Some(pipeline) = self.active_test.as_mut() {
                    pipeline.accepted_marker = accepted.state_marker.clone();
                }
                TestSubmissionOutcome::Accepted(accepted)
            }
            nexus::NexusAction::ReplyToSignal(nexus::SignalOutput::MetaOutput(
                meta::Output::TestRejected(rejected),
            )) => TestSubmissionOutcome::Rejected(rejected),
            _ => TestSubmissionOutcome::Rejected(
                self.test_rejection(meta::TestRejectionReason::InternalError),
            ),
        }
    }

    /// Drive an already-submitted test's REAL dispatch to its terminal outcome
    /// (Unit 2b, the daemon-owned executor body — mirrors
    /// [`Self::drive_submitted_deploy`]). Requires the in-flight test cursor set
    /// by a prior [`Self::submit_test`] (or `decide_test` for the
    /// in-process proof). Re-enters the handwritten runner at the cursor's first
    /// effect (the hermetic `nix build`, or the live bring-up), runs it for
    /// real, and rewrites the durable row through real phases to a terminal
    /// `Passed` (with the built closure) or `Failed(stage)` — never a faked
    /// pass. The returned `meta::Output` is the terminal `Tested`/`TestRejected`
    /// for logging/tests; the client already has its accepted handle and
    /// re-observes the outcome via `(Query (ByTestRun …))`.
    pub async fn drive_submitted_test(&mut self) -> meta::Output {
        let Some(pipeline) = self.active_test.clone() else {
            return meta::Output::TestRejected(meta::TestRejected::new(
                self.test_rejection(meta::TestRejectionReason::InternalError),
            ));
        };
        self.active_operation = Some(MetaOperation::Test);
        let first_effect = match pipeline.run.profile.test_mode {
            ordinary::TestMode::Hermetic => {
                nexus::EffectCommand::HermeticCheck(pipeline.run.hermetic_check_command())
            }
            // LIVE is BUILT but not run live here (gated). The bring-up effect
            // is constructed and dispatched; `run_effect` for the live effects
            // is the host-untouched user-namespace path (report 51 §3). A live
            // run is psyche-gated, so the daemon-integration proof exercises
            // Hermetic; this constructs the live first effect honestly.
            ordinary::TestMode::Live => nexus::EffectCommand::BringUpTestVm(
                pipeline
                    .run
                    .bring_up_command(ordinary::ClosurePath::new(String::new())),
            ),
        };
        // The cursor's first effect is fired directly through `run_effect` and
        // routed by `decide_test_effect_completion`, then `drive_to_terminal`
        // threads any further continuation hops to the terminal outcome write.
        let result = self.run_effect(first_effect).await;
        let action = self.decide_test_effect_completion(result);
        self.drive_to_terminal(action).await
    }

    /// Drive a test-pipeline `NexusAction` to its terminal `Tested`/
    /// `TestRejected` reply, threading any further effect / sema-write
    /// continuations through the handwritten runner. The hermetic path is a
    /// single effect then a terminal write, so this usually runs one or two
    /// hops; the live path threads bring-up → deploy → assert → teardown.
    async fn drive_to_terminal(&mut self, mut action: nexus::NexusAction) -> meta::Output {
        loop {
            match action {
                nexus::NexusAction::ReplyToSignal(nexus::SignalOutput::MetaOutput(output)) => {
                    return output;
                }
                nexus::NexusAction::CommandSemaWrite(input) => {
                    let output = self.apply_sema(input);
                    action = self.decide_write_completion(output);
                }
                nexus::NexusAction::CommandEffect(command) => {
                    let result = self.run_effect(command).await;
                    action = self.decide_test_effect_completion(result);
                }
                _ => {
                    return meta::Output::TestRejected(meta::TestRejected::new(
                        self.test_rejection(meta::TestRejectionReason::InternalError),
                    ));
                }
            }
        }
    }

    fn marker(commit_sequence: u64) -> ordinary::StateMarker {
        ordinary::StateMarker {
            commit_sequence: ordinary::CommitSequence::new(commit_sequence),
            state_digest: ordinary::StateDigest::new(commit_sequence),
        }
    }

    fn sema_marker(commit_sequence: u64) -> sema::StateMarker {
        sema::StateMarker {
            commit_sequence: sema::CommitSequence::new(commit_sequence),
            state_digest: sema::StateDigest::new(commit_sequence),
        }
    }

    /// Return a marker only when it can be read from the durable store.  The
    /// public protocol has no infrastructure-error variant, so manufacturing a
    /// zero marker would falsely correlate a reply to a state that was never
    /// observed.
    fn current_commit_sequence(&self) -> u64 {
        self.store
            .commit_sequence()
            .expect("read durable state marker before protocol reply")
    }

    // ---- decide: signal arrival routing (port plan §4.2) ----------------

    fn decide_signal_arrival(&mut self, input: nexus::SignalInput) -> nexus::NexusAction {
        match input {
            nexus::SignalInput::OrdinaryInput(input) => self.decide_ordinary_input(input),
            nexus::SignalInput::MetaInput(input) => self.decide_meta_input(input),
        }
    }

    fn decide_ordinary_input(&mut self, input: ordinary::Input) -> nexus::NexusAction {
        match input {
            ordinary::Input::Query(selection) => {
                // A (ByTestRun …) selection reads the durable test-run table;
                // every other selection reads the generation set. Routing here
                // keeps one Query verb covering both read planes (report 54).
                match selection {
                    ordinary::Selection::ByTestRun(lookup) => nexus::NexusAction::CommandSemaRead(
                        sema::SemaReadInput::QueryTestRuns(lookup),
                    ),
                    ordinary::Selection::ByEventLog(range) => nexus::NexusAction::CommandSemaRead(
                        sema::SemaReadInput::ReadEventLog(range),
                    ),
                    selection => nexus::NexusAction::CommandSemaRead(
                        sema::SemaReadInput::QueryGenerations(selection),
                    ),
                }
            }
            ordinary::Input::CheckHostKeyMaterial(query) => {
                nexus::NexusAction::CommandSemaRead(sema::SemaReadInput::CheckKeyMaterial(query))
            }
            ordinary::Input::WatchDeployments(_) | ordinary::Input::WatchCacheRetention(_) => {
                self.open_subscription()
            }
            ordinary::Input::Unwatch(close) => self.close_subscription(close),
        }
    }

    fn open_subscription(&mut self) -> nexus::NexusAction {
        let subscription_token = self.store.next_subscription_token();
        let reply = match self.store.commit_sequence() {
            Ok(commit_sequence) => {
                ordinary::Output::Watching(ordinary::Watching::new(ordinary::SubscriptionOpened {
                    subscription_token: ordinary::SubscriptionToken::new(subscription_token),
                    commit_sequence: ordinary::CommitSequence::new(commit_sequence),
                }))
            }
            Err(_) => ordinary::Output::WatchRejected(ordinary::WatchRejected::new(
                ordinary::RejectedWatch::new(ordinary::WatchRejectionReason::StreamUnavailable),
            )),
        };
        nexus::NexusAction::ReplyToSignal(nexus::SignalOutput::OrdinaryOutput(reply))
    }

    fn close_subscription(&mut self, close: ordinary::SubscriptionClose) -> nexus::NexusAction {
        let reply = ordinary::Output::Unwatched(ordinary::Unwatched::new(
            ordinary::SubscriptionClosed::new(close.into_payload()),
        ));
        nexus::NexusAction::ReplyToSignal(nexus::SignalOutput::OrdinaryOutput(reply))
    }

    fn decide_meta_input(&mut self, input: meta::Input) -> nexus::NexusAction {
        match input {
            meta::Input::Deploy(request) => {
                if let Some(reason) = Self::unsupported_deploy_reason(&request) {
                    return Self::reply_meta(meta::Output::DeployRejected(
                        meta::DeployRejected::new(self.deploy_rejection(reason)),
                    ));
                }
                if let Some(reason) = Self::deployment_routing_rejection(&request) {
                    return Self::reply_meta(meta::Output::DeployRejected(
                        meta::DeployRejected::new(self.deploy_rejection(reason)),
                    ));
                }
                if let Some(reason) = Self::proposal_source_rejection(&request) {
                    return Self::reply_meta(meta::Output::DeployRejected(
                        meta::DeployRejected::new(self.deploy_rejection(reason)),
                    ));
                }
                if let Some(reason) = Self::source_revision_policy_rejection(&request) {
                    return Self::reply_meta(meta::Output::DeployRejected(
                        meta::DeployRejected::new(self.deploy_rejection(reason)),
                    ));
                }
                self.active_operation = Some(MetaOperation::Deploy);
                let submission = match request {
                    meta::DeployRequest::Host(deployment) => {
                        sema::DeploySubmission::Host(deployment)
                    }
                    meta::DeployRequest::UserEnvironment(deployment) => {
                        sema::DeploySubmission::UserEnvironment(deployment)
                    }
                };
                nexus::NexusAction::CommandSemaWrite(sema::SemaWriteInput::RecordDeploySubmitted(
                    submission,
                ))
            }
            meta::Input::Pin(request) => {
                self.active_operation = Some(MetaOperation::Pin);
                nexus::NexusAction::CommandSemaWrite(sema::SemaWriteInput::PinGeneration(request))
            }
            meta::Input::Unpin(request) => {
                self.active_operation = Some(MetaOperation::Unpin);
                nexus::NexusAction::CommandSemaWrite(sema::SemaWriteInput::UnpinGeneration(request))
            }
            meta::Input::Retire(request) => {
                self.active_operation = Some(MetaOperation::Retire);
                nexus::NexusAction::CommandSemaWrite(sema::SemaWriteInput::RetireGeneration(
                    request,
                ))
            }
            meta::Input::Test(request) => self.decide_test(request),
        }
    }

    /// Synchronously SUBMIT a `Test` request (report 54, Unit 2b): lower it to
    /// resolved targets through `TestDefaults`, validate host-set membership,
    /// record the FIRST target's Pending row, set the in-flight cursor, and
    /// reply `AcceptedTest`. The REAL hermetic/live dispatch runs on the
    /// decoupled executor (`drive_submitted_test`), which rewrites the row to a
    /// terminal `Passed`/`Failed` — never a faked pass.
    ///
    /// The `(Check …)` shorthand fills cluster/host/mode from the configured
    /// `TestDefaults`; `(Run …)` carries them explicitly. Multi-target fan-out
    /// (`(Nodes [a b])`/`All`) records this submit's first target and returns
    /// the remaining targets so the daemon's executor admits one TestRun per
    /// node (the daemon loops `submit_test` per resolved run).
    fn decide_test(&mut self, request: meta::TestRequest) -> nexus::NexusAction {
        match self.resolve_and_validate(request) {
            Ok(mut resolved) => {
                let run = resolved.remove(0);
                self.active_operation = Some(MetaOperation::Test);
                let identifier = ordinary::TestRunIdentifier::new(
                    self.store.next_test_run_identifier().unwrap_or(1),
                );
                self.active_test = Some(TestPipeline::accepted(run.clone(), identifier.clone()));
                nexus::NexusAction::CommandSemaWrite(sema::SemaWriteInput::RecordTestRun(
                    run.pending_record(identifier),
                ))
            }
            Err(reason) => Self::reply_meta(meta::Output::TestRejected(meta::TestRejected::new(
                self.test_rejection(reason),
            ))),
        }
    }

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
            state_marker: Self::marker(self.current_commit_sequence()),
        }
    }

    /// The deploy reject-guard. Production host and user-environment eval/build
    /// are implemented through Horizon materialization, and the activating actions
    /// (host SetBootProfile/ActivateNow/TestActivation/ScheduleBootOnce,
    /// user-environment SetProfile/ActivateNow) now construct
    /// target-safe copy + activate commands (S4a), so every declared action is
    /// supported and enters the effect pipeline. `UnsupportedDeployAction`
    /// stays in the enum for honesty on any future not-yet-implemented shape;
    /// no current action returns it.
    fn unsupported_deploy_reason(
        request: &meta::DeployRequest,
    ) -> Option<meta::DeployRejectionReason> {
        match request {
            meta::DeployRequest::Host(deployment) => {
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
            meta::DeployRequest::UserEnvironment(deployment) => {
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

    fn source_revision_policy_rejection(
        request: &meta::DeployRequest,
    ) -> Option<meta::DeployRejectionReason> {
        let (policy, flake) = match request {
            meta::DeployRequest::Host(deployment) => (
                deployment.source_revision_policy,
                deployment.flake_reference.payload(),
            ),
            meta::DeployRequest::UserEnvironment(deployment) => (
                deployment.source_revision_policy,
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
    fn deployment_routing_rejection(
        request: &meta::DeployRequest,
    ) -> Option<meta::DeployRejectionReason> {
        let (transport, selector, backend, action, builder) = match request {
            meta::DeployRequest::Host(deployment) => (
                &deployment.deployment_transport,
                &deployment.deployment_output_selector,
                deployment.activation_backend,
                true,
                deployment.optional_nix_builder_spec.as_ref(),
            ),
            meta::DeployRequest::UserEnvironment(deployment) => (
                &deployment.deployment_transport,
                &deployment.deployment_output_selector,
                deployment.activation_backend,
                false,
                deployment.optional_nix_builder_spec.as_ref(),
            ),
        };
        let valid_backend = matches!(
            (action, backend),
            (true, sema::ActivationBackend::NixosSystemdBootV1)
                | (false, sema::ActivationBackend::HomeManagerNixProfileV1)
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
        let valid_user_environment_authority = match request {
            meta::DeployRequest::Host(_) => true,
            meta::DeployRequest::UserEnvironment(deployment) => {
                if matches!(
                    deployment.user_environment_action,
                    meta::UserEnvironmentAction::Realize
                ) {
                    true
                } else {
                    HorizonUserName::try_new(deployment.user_name.payload().clone())
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

    /// Reject an unusable proposal source before admitting a deploy or firing
    /// FlakeAuth/Horizon effects.  The public reason is deliberately stable
    /// and path-free; detailed filesystem/parser failures remain local.
    fn proposal_source_rejection(
        request: &meta::DeployRequest,
    ) -> Option<meta::DeployRejectionReason> {
        let source = match request {
            meta::DeployRequest::Host(deployment) => &deployment.proposal_source,
            meta::DeployRequest::UserEnvironment(deployment) => &deployment.proposal_source,
        };
        ProposalFile::available(source)
            .is_none()
            .then_some(meta::DeployRejectionReason::ProposalSourceUnreachable)
    }

    // ---- decide: sema read completion -----------------------------------

    fn decide_read_completion(&mut self, output: sema::SemaReadOutput) -> nexus::NexusAction {
        let reply = match output {
            sema::SemaReadOutput::GenerationsQueried(listing) => {
                ordinary::Output::Queried(ordinary::Queried::new(listing))
            }
            sema::SemaReadOutput::KeyMaterialChecked(report) => {
                ordinary::Output::KeyMaterialChecked(ordinary::KeyMaterialChecked::new(report))
            }
            sema::SemaReadOutput::TestRunsQueried(listing) => {
                ordinary::Output::TestRunsQueried(ordinary::TestRunsQueried::new(listing))
            }
            sema::SemaReadOutput::EventLogRead(page) => ordinary::Output::DeploymentEventsQueried(
                ordinary::DeploymentEventsQueried::new(page),
            ),
            sema::SemaReadOutput::ReadMissed(report) => ordinary::Output::QueryRejected(
                ordinary::QueryRejected::new(ordinary::RejectedQuery {
                    query_rejection_reason: ordinary::QueryRejectionReason::GenerationUnknown,
                    state_marker: report.state_marker,
                }),
            ),
        };
        nexus::NexusAction::ReplyToSignal(nexus::SignalOutput::OrdinaryOutput(reply))
    }

    // ---- decide: sema write completion (opens / advances pipeline) ------

    fn decide_write_completion(&mut self, output: sema::SemaWriteOutput) -> nexus::NexusAction {
        match output {
            sema::SemaWriteOutput::DeploySubmitted(accepted) => {
                self.begin_deploy_pipeline(accepted)
            }
            sema::SemaWriteOutput::PhaseRecorded(_) => self.advance_after_phase(),
            sema::SemaWriteOutput::GenerationActivated(_) => self.finish_deploy_pipeline(),
            sema::SemaWriteOutput::GenerationPinned(applied) => {
                self.active_operation = None;
                Self::reply_meta(meta::Output::Pinned(meta::Pinned::new(applied)))
            }
            sema::SemaWriteOutput::GenerationUnpinned(applied) => {
                self.active_operation = None;
                Self::reply_meta(meta::Output::Unpinned(meta::Unpinned::new(applied)))
            }
            sema::SemaWriteOutput::GenerationRetired(applied) => {
                self.active_operation = None;
                Self::reply_meta(meta::Output::Retired(meta::Retired::new(applied)))
            }
            sema::SemaWriteOutput::ContainerRecorded(_) => self.advance_after_phase(),
            sema::SemaWriteOutput::TestRunRecorded(accepted) => {
                // The accepted SUBMIT reply. `active_operation` is cleared (the
                // synchronous submit is done) but `active_test` stays set: the
                // decoupled executor (`drive_submitted_test`) re-enters to run
                // the real dispatch and rewrite the row to a terminal outcome.
                self.active_operation = None;
                Self::reply_meta(meta::Output::Tested(meta::Tested::new(accepted)))
            }
            sema::SemaWriteOutput::WriteRejected(report) => self.reject_active_or_meta(report),
        }
    }

    // ---- decide: TEST effect completion (drives the test dispatch) ------

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
    fn decide_test_effect_completion(&mut self, result: nexus::EffectResult) -> nexus::NexusAction {
        let Some(pipeline) = self.active_test.clone() else {
            return Self::reply_meta(meta::Output::TestRejected(meta::TestRejected::new(
                self.test_rejection(meta::TestRejectionReason::InternalError),
            )));
        };
        match result {
            nexus::EffectResult::HermeticCheckBuilt(built) => {
                // Real nix build succeeded: the out-path is the realised check
                // closure. Record Completed/Passed with it — the durable proof.
                self.record_test_terminal(
                    &pipeline,
                    ordinary::TestRunPhase::Completed,
                    ordinary::TestOutcome::Passed,
                    Some(built.closure_path),
                )
            }
            nexus::EffectResult::TestVmStarted(_) => {
                self.record_container(&pipeline, sema::ContainerState::Started);
                self.set_test_stage(TestStage::BroughtUp);
                // The deploy-into-VM + assert chain runs here in a live run
                // (gated). BUILT path advances straight to teardown so the
                // bracket is provably constructed end-to-end.
                self.set_test_stage(TestStage::Asserted);
                nexus::NexusAction::CommandEffect(nexus::EffectCommand::TearDownTestVm(
                    pipeline.run.tear_down_command(),
                ))
            }
            nexus::EffectResult::TestVmStopped(_) => {
                self.record_container(&pipeline, sema::ContainerState::Stopped);
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
            nexus::EffectResult::EffectFailed(failure) => self.fail_test_pipeline(failure),
            // No other effect result belongs to a test dispatch; treat it as an
            // internal invariant failure rather than a misleading pass.
            _ => self.fail_test_pipeline(nexus::EffectFailure {
                effect_stage: nexus::EffectStage::HermeticCheck,
                string: "unexpected effect result on the test pipeline".to_string(),
            }),
        }
    }

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
    ) -> nexus::NexusAction {
        let record = pipeline.record_at(phase, outcome, closure_path);
        let output = self.record_test_run(record);
        self.active_operation = None;
        self.active_test = None;
        match output {
            sema::SemaWriteOutput::TestRunRecorded(accepted) => {
                Self::reply_meta(meta::Output::Tested(meta::Tested::new(accepted)))
            }
            _ => Self::reply_meta(meta::Output::TestRejected(meta::TestRejected::new(
                self.test_rejection(meta::TestRejectionReason::InternalError),
            ))),
        }
    }

    /// Record a live VM container-lifecycle transition (Unit 2b): the report-47
    /// §2 `ContainerLifecycleRecord` table finally gets its driver. Best-effort,
    /// like the deploy job-row persistence — a record error never fakes the
    /// outcome.
    fn record_container(&mut self, pipeline: &TestPipeline, state: sema::ContainerState) {
        let _ = self.record_container_transition(pipeline.container_transition(state));
    }

    fn set_test_stage(&mut self, stage: TestStage) {
        if let Some(pipeline) = self.active_test.as_mut() {
            pipeline.stage = stage;
        }
    }

    /// Record a terminal `Failed(stage)` test outcome and reply. The stage maps
    /// the effect failure to the durable `FailureStage`, so a query sees
    /// exactly where the test failed (`HermeticCheck` vs `BringUp`/`TearDown`).
    /// NEVER a faked pass — a build/test failure is recorded as Failed.
    fn fail_test_pipeline(&mut self, failure: nexus::EffectFailure) -> nexus::NexusAction {
        eprintln!(
            "lojix test pipeline effect failed at {:?}",
            failure.effect_stage
        );
        let stage = Self::test_failure_stage(failure.effect_stage);
        let pipeline = match self.active_test.clone() {
            Some(pipeline) => pipeline,
            None => {
                return Self::reply_meta(meta::Output::TestRejected(meta::TestRejected::new(
                    self.test_rejection(meta::TestRejectionReason::InternalError),
                )));
            }
        };
        self.record_test_terminal(
            &pipeline,
            ordinary::TestRunPhase::Failed,
            ordinary::TestOutcome::Failed(stage),
            None,
        )
    }

    fn test_failure_stage(stage: nexus::EffectStage) -> ordinary::FailureStage {
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

    fn begin_deploy_pipeline(&mut self, accepted: meta::DeployHandle) -> nexus::NexusAction {
        let pipeline = match self.active_deploy.as_ref() {
            Some(pipeline) => pipeline.clone(),
            None => {
                return Self::reply_meta(meta::Output::DeployAccepted(meta::DeployAccepted::new(
                    accepted,
                )));
            }
        };
        // First effect of the chain: resolve the flake against the proposal
        // source. Subsequent effects are emitted from `decide_effect_completion`.
        nexus::NexusAction::CommandEffect(nexus::EffectCommand::ResolveFlakeAuth(
            pipeline.flake_auth_request(),
        ))
    }

    fn advance_after_phase(&mut self) -> nexus::NexusAction {
        // A phase-transition write committed mid-pipeline. The cursor `stage`
        // names which phase was just recorded; advance to the next effect or
        // the final activation-record write.
        let pipeline = match self.active_deploy.clone() {
            Some(pipeline) => pipeline,
            None => {
                return Self::reply_meta(meta::Output::DeployRejected(meta::DeployRejected::new(
                    self.deploy_rejection(meta::DeployRejectionReason::DeploymentInFlight),
                )));
            }
        };
        match pipeline.stage {
            DeployStage::Submitted => {
                self.set_stage(DeployStage::BuildingRecorded);
                nexus::NexusAction::CommandEffect(nexus::EffectCommand::NixEval(
                    pipeline.nix_eval_command(),
                ))
            }
            DeployStage::BuildingRecorded => {
                // The closure path is captured on the cursor by `set_closure_path`
                // during eval/build; an activating pipeline that reached this stage
                // without a closure is an internal invariant failure, not an empty
                // activation (risk R2). Fail the pipeline rather than activate "".
                let closure_path = match pipeline.closure_path.clone() {
                    Some(closure_path) if canonical_nix_store_root(closure_path.payload()) => {
                        closure_path
                    }
                    None => {
                        return self.fail_pipeline(nexus::EffectFailure {
                            effect_stage: nexus::EffectStage::Activate,
                            string: "activation reached without a built closure path".to_string(),
                        });
                    }
                    Some(_) => {
                        return self.fail_pipeline(nexus::EffectFailure {
                            effect_stage: nexus::EffectStage::Activate,
                            string: "activation reached with a noncanonical closure path"
                                .to_string(),
                        });
                    }
                };
                self.set_stage(DeployStage::CopyingRecorded);
                // Mark the durable job row Activating before the activate effect
                // runs (up9q): a daemon that crashes mid-activation reads this
                // phase on restart and reconciles via the BootOnce unit rather
                // than blindly re-activating. There is no Activating event-log
                // phase today; the job row is the resume cursor for this window.
                self.persist_job_phase(sema::DeployJobPhase::Activating);
                nexus::NexusAction::CommandEffect(nexus::EffectCommand::ActivateGeneration(
                    pipeline.activate_generation_command(closure_path),
                ))
            }
            DeployStage::CopyingRecorded => {
                let commit = match pipeline.activation_commit() {
                    Some(commit) => commit,
                    None => {
                        return self.fail_pipeline(nexus::EffectFailure {
                            effect_stage: nexus::EffectStage::Activate,
                            string: "activation record reached without a built closure path"
                                .to_string(),
                        });
                    }
                };
                self.set_stage(DeployStage::ActivatedRecorded);
                nexus::NexusAction::CommandSemaWrite(
                    sema::SemaWriteInput::RecordGenerationActivated(commit),
                )
            }
            DeployStage::ActivatedRecorded => self.finish_deploy_pipeline(),
        }
    }

    fn set_stage(&mut self, stage: DeployStage) {
        if let Some(pipeline) = self.active_deploy.as_mut() {
            pipeline.stage = stage;
        }
    }

    fn finish_deploy_pipeline(&mut self) -> nexus::NexusAction {
        let Some(pipeline) = self.active_deploy.clone() else {
            unreachable!("a deploy pipeline cannot finish without its correlation cursor");
        };
        let record = match self.store.terminalize_deployment(
            *pipeline.deployment_identifier.payload(),
            sema::DeploymentLifecycle::Completed,
            sema::DeploymentTerminal::Succeeded,
        ) {
            Ok(record) => record,
            Err(error) => {
                return self.fail_pipeline(nexus::EffectFailure {
                    effect_stage: nexus::EffectStage::Activate,
                    string: format!("could not persist correlated deployment success: {error}"),
                });
            }
        };
        self.active_operation = None;
        self.active_deploy = None;
        Self::reply_meta(meta::Output::DeployTerminal(record))
    }

    fn reject_active_or_meta(&mut self, report: sema::RejectionReport) -> nexus::NexusAction {
        // A write rejection aborts any in-flight deploy and replies a typed
        // meta rejection for the operation in flight, carrying the rejection
        // reason and current marker.
        self.active_deploy = None;
        let operation = self
            .active_operation
            .take()
            .unwrap_or(MetaOperation::Deploy);
        let marker = report.state_marker;
        let output = match operation {
            MetaOperation::Deploy => meta::Output::DeployRejected(meta::DeployRejected::new(
                self.deploy_rejection(Self::deploy_reason(report.rejection_reason)),
            )),
            MetaOperation::Pin => {
                meta::Output::PinRejected(meta::PinRejected::new(meta::RejectedPin {
                    pin_rejection_reason: Self::pin_reason(report.rejection_reason),
                    state_marker: marker,
                }))
            }
            MetaOperation::Unpin => {
                meta::Output::UnpinRejected(meta::UnpinRejected::new(meta::RejectedUnpin {
                    unpin_rejection_reason: Self::unpin_reason(report.rejection_reason),
                    state_marker: marker,
                }))
            }
            MetaOperation::Retire => {
                meta::Output::RetireRejected(meta::RetireRejected::new(meta::RejectedRetire {
                    retire_rejection_reason: Self::retire_reason(report.rejection_reason),
                    state_marker: marker,
                }))
            }
            MetaOperation::Test => {
                meta::Output::TestRejected(meta::TestRejected::new(meta::RejectedTest {
                    test_rejection_reason: Self::test_reason(report.rejection_reason),
                    state_marker: marker,
                }))
            }
        };
        Self::reply_meta(output)
    }

    /// Map a SEMA write-rejection reason to a typed test rejection. A reason
    /// with no test-domain meaning is an internal invariant failure (the Deploy
    /// precedent), never a misleading domain reason.
    fn test_reason(reason: sema::RejectionReason) -> meta::TestRejectionReason {
        match reason {
            sema::RejectionReason::ClusterUnknown => meta::TestRejectionReason::ClusterUnknown,
            sema::RejectionReason::NodeUnknown => meta::TestRejectionReason::NodeUnknown,
            _ => meta::TestRejectionReason::InternalError,
        }
    }

    fn pin_reason(reason: sema::RejectionReason) -> meta::PinRejectionReason {
        match reason {
            sema::RejectionReason::GenerationUnknown => meta::PinRejectionReason::GenerationUnknown,
            sema::RejectionReason::NodeUnknown => meta::PinRejectionReason::NodeUnknown,
            sema::RejectionReason::PinLabelInUse => meta::PinRejectionReason::PinLabelInUse,
            _ => meta::PinRejectionReason::InternalError,
        }
    }

    fn unpin_reason(reason: sema::RejectionReason) -> meta::UnpinRejectionReason {
        match reason {
            sema::RejectionReason::PinLabelUnknown => meta::UnpinRejectionReason::PinLabelUnknown,
            sema::RejectionReason::NodeUnknown => meta::UnpinRejectionReason::NodeUnknown,
            _ => meta::UnpinRejectionReason::GenerationNotPinned,
        }
    }

    fn retire_reason(reason: sema::RejectionReason) -> meta::RetireRejectionReason {
        match reason {
            sema::RejectionReason::GenerationUnknown => {
                meta::RetireRejectionReason::GenerationUnknown
            }
            sema::RejectionReason::NodeUnknown => meta::RetireRejectionReason::NodeUnknown,
            sema::RejectionReason::GenerationActive => {
                meta::RetireRejectionReason::GenerationActive
            }
            sema::RejectionReason::GenerationPinned => {
                meta::RetireRejectionReason::GenerationPinned
            }
            _ => meta::RetireRejectionReason::InternalError,
        }
    }

    fn deploy_reason(reason: sema::RejectionReason) -> meta::DeployRejectionReason {
        match reason {
            sema::RejectionReason::ClusterUnknown => meta::DeployRejectionReason::ClusterUnknown,
            sema::RejectionReason::NodeUnknown => meta::DeployRejectionReason::NodeUnknown,
            sema::RejectionReason::ProposalSourceUnreachable => {
                meta::DeployRejectionReason::ProposalSourceUnreachable
            }
            // A sema reason with no deploy-domain mapping is an internal
            // invariant failure (e.g. a poisoned lock), not "already deploying"
            // (audit C4).
            _ => meta::DeployRejectionReason::InternalError,
        }
    }

    fn terminal_reason(reason: meta::DeployRejectionReason) -> sema::DeploymentTerminalReason {
        match reason {
            meta::DeployRejectionReason::ClusterUnknown => {
                sema::DeploymentTerminalReason::ClusterUnknown
            }
            meta::DeployRejectionReason::NodeUnknown => sema::DeploymentTerminalReason::NodeUnknown,
            meta::DeployRejectionReason::ProposalSourceUnreachable => {
                sema::DeploymentTerminalReason::ProposalSourceUnreachable
            }
            meta::DeployRejectionReason::FlakeReferenceMalformed => {
                sema::DeploymentTerminalReason::FlakeReferenceMalformed
            }
            meta::DeployRejectionReason::InvalidDeploymentRouting => {
                sema::DeploymentTerminalReason::InvalidDeploymentRouting
            }
            meta::DeployRejectionReason::BuilderUnreachable => {
                sema::DeploymentTerminalReason::BuilderUnreachable
            }
            meta::DeployRejectionReason::DeploymentInFlight => {
                sema::DeploymentTerminalReason::DeploymentInFlight
            }
            meta::DeployRejectionReason::UnsupportedDeployAction => {
                sema::DeploymentTerminalReason::UnsupportedDeployAction
            }
            meta::DeployRejectionReason::InternalError => {
                sema::DeploymentTerminalReason::InternalError
            }
            meta::DeployRejectionReason::ActivationFailed => {
                sema::DeploymentTerminalReason::ActivationFailed
            }
        }
    }

    fn deployment_failure_stage(stage: nexus::EffectStage) -> sema::DeploymentFailureStage {
        match stage {
            nexus::EffectStage::FlakeAuth => sema::DeploymentFailureStage::FlakeAuth,
            nexus::EffectStage::MaterializeHorizon => {
                sema::DeploymentFailureStage::MaterializeHorizon
            }
            nexus::EffectStage::Eval => sema::DeploymentFailureStage::Eval,
            nexus::EffectStage::Build => sema::DeploymentFailureStage::Build,
            nexus::EffectStage::CopyClosure => sema::DeploymentFailureStage::CopyClosure,
            nexus::EffectStage::Activate => sema::DeploymentFailureStage::Activate,
            nexus::EffectStage::Gc
            | nexus::EffectStage::HermeticCheck
            | nexus::EffectStage::BringUpTestVm
            | nexus::EffectStage::TearDownTestVm => sema::DeploymentFailureStage::Daemon,
        }
    }

    fn deployment_lifecycle(phase: ordinary::DeploymentPhase) -> sema::DeploymentLifecycle {
        match phase {
            ordinary::DeploymentPhase::Submitted => sema::DeploymentLifecycle::Submitted,
            ordinary::DeploymentPhase::Building => sema::DeploymentLifecycle::Building,
            ordinary::DeploymentPhase::Built => sema::DeploymentLifecycle::Built,
            ordinary::DeploymentPhase::Copying => sema::DeploymentLifecycle::Copying,
            ordinary::DeploymentPhase::Activating => sema::DeploymentLifecycle::Activating,
            ordinary::DeploymentPhase::Activated => sema::DeploymentLifecycle::Activated,
            ordinary::DeploymentPhase::Completed => sema::DeploymentLifecycle::Completed,
            ordinary::DeploymentPhase::Rejected => sema::DeploymentLifecycle::Rejected,
            ordinary::DeploymentPhase::Failed => sema::DeploymentLifecycle::Failed,
        }
    }

    /// Reject a request only after allocating and terminalizing its durable
    /// correlation record. This is the rejection analogue of admission: no
    /// caller can receive a deploy rejection with a synthetic identifier.
    fn reject_submission(
        &self,
        submission: sema::DeploySubmission,
        reason: meta::DeployRejectionReason,
    ) -> meta::RejectedDeploy {
        let identity = DeployPipeline::deployment_request_identity(&submission);
        let record = self
            .store
            .reject_deployment_request(
                identity,
                sema::DeploymentTerminal::Rejected(Self::terminal_reason(reason)),
            )
            .expect("durable rejected deployment terminal record");
        meta::RejectedDeploy::new(record)
    }

    fn deploy_rejection(&self, reason: meta::DeployRejectionReason) -> meta::RejectedDeploy {
        let deployment_identifier = self
            .active_deploy
            .as_ref()
            .expect("deploy rejection requires an active correlated deployment")
            .deployment_identifier
            .clone();
        let record = self
            .store
            .terminalize_deployment(
                *deployment_identifier.payload(),
                sema::DeploymentLifecycle::Rejected,
                sema::DeploymentTerminal::Rejected(Self::terminal_reason(reason)),
            )
            .expect("durable active deployment rejection record");
        meta::RejectedDeploy::new(record)
    }

    fn reply_meta(output: meta::Output) -> nexus::NexusAction {
        nexus::NexusAction::ReplyToSignal(nexus::SignalOutput::MetaOutput(output))
    }

    // ---- decide: effect completion (drives the deploy chain) ------------

    fn decide_effect_completion(&mut self, result: nexus::EffectResult) -> nexus::NexusAction {
        let pipeline = match self.active_deploy.clone() {
            Some(pipeline) => pipeline,
            None => {
                // Effects outside a deploy (e.g. a standalone GC) just confirm;
                // an unexpected effect completion replies a rejection.
                return match result {
                    nexus::EffectResult::EffectFailed(failure) => self.fail_pipeline(failure),
                    _ => Self::reply_meta(meta::Output::DeployRejected(meta::DeployRejected::new(
                        self.deploy_rejection(meta::DeployRejectionReason::DeploymentInFlight),
                    ))),
                };
            }
        };
        match result {
            nexus::EffectResult::FlakeResolved(resolved) => {
                let next_stage = if pipeline.needs_horizon_materialization() {
                    sema::DeployResumeStage::MaterializeHorizon
                } else {
                    sema::DeployResumeStage::RecordBuilding
                };
                if !self.set_resolved_flake(resolved, next_stage) {
                    return self.fail_pipeline(nexus::EffectFailure {
                        effect_stage: nexus::EffectStage::FlakeAuth,
                        string: "flake resolver did not prove an immutable commit".to_string(),
                    });
                }
                if pipeline.needs_horizon_materialization() {
                    nexus::NexusAction::CommandEffect(nexus::EffectCommand::MaterializeHorizon(
                        pipeline.horizon_materialization_command(),
                    ))
                } else {
                    // Record Building (stage still Submitted). The phase write
                    // hops back through advance_after_phase, which fires NixEval.
                    self.record_phase(ordinary::DeploymentPhase::Building, None)
                }
            }
            nexus::EffectResult::HorizonMaterialized(inputs) => {
                self.set_input_overrides(inputs.into_payload());
                self.record_phase(ordinary::DeploymentPhase::Building, None)
            }
            nexus::EffectResult::ClosureEvaluated(evaluated) => {
                if !self.set_closure_path(evaluated.closure_path.clone()) {
                    return self.fail_pipeline(nexus::EffectFailure {
                        effect_stage: nexus::EffectStage::Eval,
                        string: "effect returned a noncanonical closure path".to_string(),
                    });
                }
                if pipeline.action.produces_closure() {
                    self.persist_job_cursor(
                        sema::DeployJobPhase::Building,
                        sema::DeployResumeStage::NixBuild,
                    );
                    nexus::NexusAction::CommandEffect(nexus::EffectCommand::NixBuild(
                        pipeline.nix_build_command(evaluated.closure_path),
                    ))
                } else {
                    // Host `Evaluate`: the derivation path is the result — finish
                    // the pipeline without building.
                    self.persist_job_cursor(
                        sema::DeployJobPhase::Built,
                        sema::DeployResumeStage::FinishDeployment,
                    );
                    self.finish_deploy_pipeline()
                }
            }
            nexus::EffectResult::ClosureBuilt(built) => {
                if !self.set_closure_path(built.closure_path.clone()) {
                    return self.fail_pipeline(nexus::EffectFailure {
                        effect_stage: nexus::EffectStage::Build,
                        string: "effect returned a noncanonical closure path".to_string(),
                    });
                }
                if pipeline.action.activates() {
                    self.persist_job_cursor(
                        sema::DeployJobPhase::Built,
                        sema::DeployResumeStage::CopyClosure,
                    );
                    nexus::NexusAction::CommandEffect(nexus::EffectCommand::CopyClosure(
                        pipeline.copy_closure_command(built.closure_path),
                    ))
                } else {
                    // Non-activating action (`Build`): the closure is realised —
                    // finish without copy/activate (which remain addressing-
                    // incomplete; that is the M2/M3 deploy work).
                    self.persist_job_cursor(
                        sema::DeployJobPhase::Built,
                        sema::DeployResumeStage::FinishDeployment,
                    );
                    self.finish_deploy_pipeline()
                }
            }
            nexus::EffectResult::ClosureCopied(_) => {
                // Record Copying (stage BuildingRecorded). The phase write hops
                // back through advance_after_phase, which fires ActivateGeneration.
                self.record_phase(ordinary::DeploymentPhase::Copying, None)
            }
            nexus::EffectResult::GenerationActivated(activated) => {
                // Record Activated (stage CopyingRecorded). The phase write hops
                // back through advance_after_phase, which fires the
                // RecordGenerationActivated write that commits the live set. The
                // slot returned by the activation effect is persisted on that
                // commit rather than re-defaulting to Current.
                self.set_activation_slot(activated.generation_slot);
                self.record_phase(ordinary::DeploymentPhase::Activated, None)
            }
            nexus::EffectResult::PathsCollected(_) => self.finish_deploy_pipeline(),
            // The test-dispatch effect results never reach the DEPLOY effect
            // router — `drive_submitted_test` routes them through
            // `decide_test_effect_completion`. One arriving here is an internal
            // invariant failure, surfaced as a deploy failure rather than a
            // misleading success.
            nexus::EffectResult::HermeticCheckBuilt(_)
            | nexus::EffectResult::TestVmStarted(_)
            | nexus::EffectResult::TestVmStopped(_) => self.fail_pipeline(nexus::EffectFailure {
                effect_stage: nexus::EffectStage::Build,
                string: "test effect result on the deploy pipeline".to_string(),
            }),
            nexus::EffectResult::EffectFailed(failure) => self.fail_pipeline(failure),
        }
    }

    fn set_activation_slot(&mut self, generation_slot: ordinary::GenerationSlot) {
        if let Some(pipeline) = self.active_deploy.as_mut() {
            pipeline.activation_slot = Some(generation_slot);
        }
    }

    /// Capture a closure only after validating it at the typed effect ingress.
    /// This is intentionally independent of Nix-command parsing: an injected
    /// `EffectResult` must not reach a later build, copy, or activation command.
    fn set_closure_path(&mut self, closure_path: ordinary::ClosurePath) -> bool {
        if !canonical_nix_store_root(closure_path.payload()) {
            return false;
        }
        if let Some(pipeline) = self.active_deploy.as_mut() {
            pipeline.closure_path = Some(closure_path);
            true
        } else {
            false
        }
    }

    fn set_input_overrides(&mut self, overrides: Vec<nexus::FlakeInputOverride>) {
        if let Some(pipeline) = self.active_deploy.as_mut() {
            pipeline.input_overrides = overrides;
        }
        self.persist_job_cursor(
            sema::DeployJobPhase::Submitted,
            sema::DeployResumeStage::RecordBuilding,
        );
    }

    fn set_resolved_flake(
        &mut self,
        resolved: nexus::ResolvedFlake,
        resume_stage: sema::DeployResumeStage,
    ) -> bool {
        let source_revision = resolved.into_payload();
        let Some(immutable_revision) = crate::immutable_revision(&source_revision.string) else {
            return false;
        };
        let mut snapshot = None;
        if let Some(pipeline) = self.active_deploy.as_mut() {
            if matches!(
                pipeline.source_revision_policy,
                meta::SourceRevisionPolicy::ResolveAndRecord
            ) {
                pipeline.flake = source_revision.resolved_ref.clone();
            }
            pipeline.source_revision = Some(source_revision);
            pipeline.resume_stage = resume_stage;
            snapshot = Some((
                *pipeline.deployment_identifier.payload(),
                pipeline.deploy_job(sema::DeployJobPhase::Submitted),
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

    fn record_phase(
        &mut self,
        phase: ordinary::DeploymentPhase,
        detail: Option<String>,
    ) -> nexus::NexusAction {
        let resume_stage = match phase {
            ordinary::DeploymentPhase::Building => sema::DeployResumeStage::NixEval,
            ordinary::DeploymentPhase::Copying => sema::DeployResumeStage::ActivateGeneration,
            ordinary::DeploymentPhase::Activated => {
                sema::DeployResumeStage::RecordGenerationActivated
            }
            _ => sema::DeployResumeStage::FinishDeployment,
        };
        if let Some(pipeline) = self.active_deploy.as_mut() {
            pipeline.resume_stage = resume_stage;
        }
        let event = match self.active_deploy.as_ref() {
            Some(pipeline) => {
                let position = match self.store.allocate_event_log_position() {
                    Ok(position) => position,
                    Err(error) => {
                        return self.fail_pipeline(nexus::EffectFailure {
                            effect_stage: nexus::EffectStage::Gc,
                            string: format!("could not reserve deployment event position: {error}"),
                        });
                    }
                };
                pipeline.phase_event(phase, ordinary::EventLogPosition::new(position), detail)
            }
            None => {
                return Self::reply_meta(meta::Output::DeployRejected(meta::DeployRejected::new(
                    self.deploy_rejection(meta::DeployRejectionReason::DeploymentInFlight),
                )));
            }
        };
        nexus::NexusAction::CommandSemaWrite(sema::SemaWriteInput::RecordPhaseTransition(event))
    }

    /// Rewrite the durable in-flight job row at `phase` from the active deploy
    /// cursor (up9q). Best-effort: a persistence error here must not abort the
    /// running deploy — the event log remains the authoritative phase record
    /// and the job row is the resume convenience. No-op when no deploy is
    /// active (e.g. a standalone effect).
    fn persist_job_phase(&mut self, phase: sema::DeployJobPhase) {
        let resume_stage = self
            .active_deploy
            .as_ref()
            .map(|pipeline| pipeline.resume_stage)
            .unwrap_or(sema::DeployResumeStage::FinishDeployment);
        self.persist_job_cursor(phase, resume_stage);
    }

    fn persist_job_cursor(
        &mut self,
        phase: sema::DeployJobPhase,
        resume_stage: sema::DeployResumeStage,
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

    fn fail_pipeline(&mut self, failure: nexus::EffectFailure) -> nexus::NexusAction {
        eprintln!(
            "lojix deploy pipeline effect failed at {:?}",
            failure.effect_stage
        );
        let pipeline = self.active_deploy.clone();
        // Clear BOTH in-flight slots symmetrically with the finish path (audit
        // R5) — a mid-pipeline effect failure must not leak `active_operation`.
        let reason = match failure.effect_stage {
            nexus::EffectStage::FlakeAuth => meta::DeployRejectionReason::ProposalSourceUnreachable,
            nexus::EffectStage::MaterializeHorizon => {
                meta::DeployRejectionReason::ProposalSourceUnreachable
            }
            nexus::EffectStage::Eval => meta::DeployRejectionReason::FlakeReferenceMalformed,
            nexus::EffectStage::Build => meta::DeployRejectionReason::FlakeReferenceMalformed,
            nexus::EffectStage::CopyClosure => meta::DeployRejectionReason::BuilderUnreachable,
            nexus::EffectStage::Activate => meta::DeployRejectionReason::ActivationFailed,
            nexus::EffectStage::Gc => meta::DeployRejectionReason::DeploymentInFlight,
            // The test-only effect stages never reach the DEPLOY pipeline's
            // failure path (`fail_test_pipeline` owns them); an internal
            // invariant failure rather than a misleading deploy reason.
            nexus::EffectStage::HermeticCheck
            | nexus::EffectStage::BringUpTestVm
            | nexus::EffectStage::TearDownTestVm => meta::DeployRejectionReason::InternalError,
        };
        let Some(pipeline) = pipeline else {
            unreachable!("a deploy effect failure cannot occur without a correlation cursor");
        };
        let terminal = sema::DeploymentTerminal::Failed(sema::DeploymentFailure {
            deployment_failure_stage: Self::deployment_failure_stage(failure.effect_stage),
            deployment_terminal_reason: Self::terminal_reason(reason),
        });
        let record = match self.store.terminalize_deployment(
            *pipeline.deployment_identifier.payload(),
            sema::DeploymentLifecycle::Failed,
            terminal,
        ) {
            Ok(record) => record,
            Err(error) => {
                unreachable!("could not persist correlated deployment failure: {error}");
            }
        };
        self.active_deploy = None;
        self.active_operation = None;
        Self::reply_meta(meta::Output::DeployTerminal(record))
    }

    // ---- sema apply / observe (the four tables) -------------------------

    fn apply_sema(&mut self, input: sema::SemaWriteInput) -> sema::SemaWriteOutput {
        match input {
            sema::SemaWriteInput::RecordDeploySubmitted(submission) => {
                self.record_deploy_submitted(submission)
            }
            sema::SemaWriteInput::RecordPhaseTransition(event) => {
                self.record_phase_transition(event)
            }
            sema::SemaWriteInput::RecordGenerationActivated(commit) => {
                self.record_generation_activated(commit)
            }
            sema::SemaWriteInput::PinGeneration(request) => self.pin_generation(request),
            sema::SemaWriteInput::UnpinGeneration(request) => self.unpin_generation(request),
            sema::SemaWriteInput::RetireGeneration(request) => self.retire_generation(request),
            sema::SemaWriteInput::RecordContainerTransition(transition) => {
                self.record_container_transition(transition)
            }
            sema::SemaWriteInput::RecordTestRun(record) => self.record_test_run(record),
        }
    }

    /// Persist one accepted test-run row (phase Submitted / outcome Pending)
    /// and reply the `AcceptedTest` handle. Mirrors `record_deploy_submitted`:
    /// the row is durable from acceptance, so a `(Query (ByTestRun …))` reads
    /// it immediately and a restarted daemon reconciles the in-flight test
    /// (Unit 2b). Unit 2a writes exactly this Pending row — no faked pass.
    fn record_test_run(&mut self, record: ordinary::TestRunRecord) -> sema::SemaWriteOutput {
        let identifier = record.test_run_identifier.clone();
        match self
            .store
            .upsert_test_run(sema::StoredTestRun::from(record))
            .and_then(|()| self.store.commit_sequence())
        {
            Ok(commit_sequence) => sema::SemaWriteOutput::TestRunRecorded(meta::AcceptedTest {
                test_run_identifier: identifier,
                state_marker: Self::marker(commit_sequence),
            }),
            Err(error) => panic!("persist durable test-run record: {error}"),
        }
    }

    fn record_deploy_submitted(
        &mut self,
        submission: sema::DeploySubmission,
    ) -> sema::SemaWriteOutput {
        // Identifier reservation, correlation-record creation, and the
        // restart cursor share one durable commit.  The public admission
        // marker is bound only afterwards from that commit's durable log.
        let identity = DeployPipeline::deployment_request_identity(&submission);
        let restart_cursor = DeployPipeline::from_submission(
            ordinary::DeploymentIdentifier::new(0),
            ordinary::GenerationIdentifier::new(0),
            Self::marker(0),
            submission.clone(),
        )
        .deploy_job(sema::DeployJobPhase::Submitted);
        let record = match self
            .store
            .allocate_deployment_record(identity, restart_cursor)
        {
            Ok(record) => record,
            Err(error) => {
                unreachable!("cannot issue an uncorrelated deployment rejection: {error}")
            }
        };
        let Some(admission_marker) = record.optional_admission_marker.clone() else {
            unreachable!("accepted deployment record is missing its admission marker");
        };
        let accepted_marker = admission_marker.into_payload();
        let deployment_identifier = record.deployment_identifier.clone();
        let pipeline = DeployPipeline::from_submission(
            deployment_identifier.clone(),
            record.generation_identifier,
            accepted_marker.clone(),
            submission,
        );
        // `allocate_deployment_record` does not return until the durable
        // admission intent has been bound from its exact commit-log receipt,
        // journalled, and locally acknowledged.  Do not synthesize a second
        // Submitted event here.
        self.active_deploy = Some(pipeline);
        sema::SemaWriteOutput::DeploySubmitted(meta::DeployHandle {
            deployment_identifier,
            state_marker: accepted_marker,
        })
    }

    fn record_phase_transition(
        &mut self,
        mut event: ordinary::DeploymentPhaseEvent,
    ) -> sema::SemaWriteOutput {
        let recorded_phase = event.deployment_phase;
        let event_log_position = event.event_log_position.clone();
        let Some(pipeline) = self.active_deploy.as_ref() else {
            unreachable!("deployment phase transition requires its active correlation cursor");
        };
        let job = pipeline.deploy_job(sema::DeployJobPhase::from(recorded_phase));
        let recorded = self.store.advance_deployment_phase(
            *event.deployment_identifier.payload(),
            Self::deployment_lifecycle(recorded_phase),
            job,
            event.clone(),
        );
        match recorded {
            Ok(marker) => {
                event.state_marker = marker.clone();
                let receipt = sema::PhaseReceipt {
                    event_log_position,
                    state_marker: marker,
                };
                let job = if let Some(pipeline) = self.active_deploy.as_mut() {
                    pipeline.phase_receipt = Some(receipt.clone());
                    Some(pipeline.deploy_job(sema::DeployJobPhase::from(recorded_phase)))
                } else {
                    None
                };
                if let Some(job) = job {
                    self.store
                        .upsert_deploy_job(job)
                        .expect("persist exact phase receipt before resuming its continuation");
                }
                sema::SemaWriteOutput::PhaseRecorded(receipt)
            }
            Err(error) => {
                unreachable!("cannot emit an uncorrelated deployment phase reply: {error}")
            }
        }
    }

    fn record_generation_activated(
        &mut self,
        commit: sema::ActivationCommit,
    ) -> sema::SemaWriteOutput {
        let Some(pipeline) = self.active_deploy.clone() else {
            unreachable!("generation activation requires an active correlated deployment");
        };
        let deployment_identifier = pipeline.deployment_identifier.clone();
        let generation_artifact = pipeline.generation_artifact;
        let activation_effect = pipeline.activation_effect;
        let generation = sema::LiveGeneration {
            deployment_identifier,
            generation_identifier: commit.generation_identifier.clone(),
            cluster_name: commit.cluster_name.clone(),
            node_name: commit.node_name.clone(),
            deployment_environment: commit.deployment_environment.clone(),
            generation_artifact,
            activation_effect,
            generation_slot: commit.generation_slot,
            closure_path: commit.closure_path.clone(),
            source_revision_record: commit.source_revision_record.clone(),
        };
        let root = sema::GcRoot {
            generation_identifier: commit.generation_identifier.clone(),
            cluster_name: commit.cluster_name.clone(),
            node_name: commit.node_name.clone(),
            generation_slot: commit.generation_slot,
            closure_path: commit.closure_path.clone(),
            optional_pin_label: None,
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
                    sema::DeployJobPhase::Activated,
                    sema::DeployResumeStage::FinishDeployment,
                );
                sema::SemaWriteOutput::GenerationActivated(sema::AppliedActivation {
                    generation_identifier: commit.generation_identifier,
                    generation_slot: commit.generation_slot,
                    state_marker: Self::sema_marker(commit_sequence),
                })
            }
            Err(error) => {
                unreachable!("cannot emit an uncorrelated generation activation reply: {error}")
            }
        }
    }

    fn pin_generation(&mut self, request: meta::PinRequest) -> sema::SemaWriteOutput {
        let roots = match self.store.gc_roots() {
            Ok(roots) => roots,
            Err(error) => panic!("read durable gc roots for pin request: {error}"),
        };
        let current_sequence = self.current_commit_sequence();
        let already_used = roots
            .iter()
            .any(|root| root.optional_pin_label.as_ref() == Some(&request.pin_label));
        if already_used {
            return Self::write_rejected(current_sequence, sema::RejectionReason::PinLabelInUse);
        }
        let Some(mut root) = roots.into_iter().find(|root| {
            root.generation_identifier == request.generation_identifier
                && root.cluster_name == request.cluster_name
                && root.node_name == request.node_name
        }) else {
            return Self::write_rejected(
                current_sequence,
                sema::RejectionReason::GenerationUnknown,
            );
        };
        let from_slot = root.generation_slot;
        root.generation_slot = ordinary::GenerationSlot::Pinned;
        root.optional_pin_label = Some(request.pin_label.clone());
        let committed = self
            .store
            .mutate_gc_root(root)
            .and_then(|()| self.store.commit_sequence());
        match committed {
            Ok(commit_sequence) => sema::SemaWriteOutput::GenerationPinned(meta::AppliedPin {
                generation_identifier: request.generation_identifier,
                pin_label: request.pin_label,
                from_slot,
                to_slot: ordinary::GenerationSlot::Pinned,
                state_marker: Self::marker(commit_sequence),
            }),
            Err(error) => panic!("persist generation pin transition: {error}"),
        }
    }

    fn unpin_generation(&mut self, request: meta::UnpinRequest) -> sema::SemaWriteOutput {
        let roots = match self.store.gc_roots() {
            Ok(roots) => roots,
            Err(error) => panic!("read durable gc roots for unpin request: {error}"),
        };
        let current_sequence = self.current_commit_sequence();
        let Some(mut root) = roots.into_iter().find(|root| {
            root.optional_pin_label.as_ref() == Some(&request.pin_label)
                && root.cluster_name == request.cluster_name
                && root.node_name == request.node_name
        }) else {
            return Self::write_rejected(current_sequence, sema::RejectionReason::PinLabelUnknown);
        };
        let generation_identifier = root.generation_identifier.clone();
        let from_slot = root.generation_slot;
        root.generation_slot = ordinary::GenerationSlot::Recent;
        root.optional_pin_label = None;
        let committed = self
            .store
            .mutate_gc_root(root)
            .and_then(|()| self.store.commit_sequence());
        match committed {
            Ok(commit_sequence) => sema::SemaWriteOutput::GenerationUnpinned(meta::AppliedUnpin {
                generation_identifier,
                pin_label: request.pin_label,
                from_slot,
                to_slot: ordinary::GenerationSlot::Recent,
                state_marker: Self::marker(commit_sequence),
            }),
            Err(error) => panic!("persist generation unpin transition: {error}"),
        }
    }

    fn retire_generation(&mut self, request: meta::RetireRequest) -> sema::SemaWriteOutput {
        let roots = match self.store.gc_roots() {
            Ok(roots) => roots,
            Err(error) => panic!("read durable gc roots for retire request: {error}"),
        };
        let current_sequence = self.current_commit_sequence();
        let Some(root) = roots.into_iter().find(|root| {
            root.generation_identifier == request.generation_identifier
                && root.cluster_name == request.cluster_name
                && root.node_name == request.node_name
        }) else {
            return Self::write_rejected(
                current_sequence,
                sema::RejectionReason::GenerationUnknown,
            );
        };
        if matches!(root.generation_slot, ordinary::GenerationSlot::Pinned) {
            return Self::write_rejected(current_sequence, sema::RejectionReason::GenerationPinned);
        }
        let committed = self
            .store
            .retract_gc_root(*request.generation_identifier.payload())
            .and_then(|()| self.store.commit_sequence());
        match committed {
            Ok(commit_sequence) => sema::SemaWriteOutput::GenerationRetired(meta::AppliedRetire {
                generation_identifier: request.generation_identifier,
                generation_slot: root.generation_slot,
                state_marker: Self::marker(commit_sequence),
            }),
            Err(error) => panic!("persist generation retirement: {error}"),
        }
    }

    fn record_container_transition(
        &mut self,
        transition: sema::ContainerTransition,
    ) -> sema::SemaWriteOutput {
        let recorded = self
            .store
            .allocate_event_log_position()
            .and_then(|position| {
                let record = sema::ContainerLifecycleRecord {
                    cluster_name: transition.cluster_name,
                    node_name: transition.node_name,
                    container_name: transition.container_name,
                    container_state: transition.container_state,
                    event_log_position: ordinary::EventLogPosition::new(position),
                };
                let entry = sema::EventLogEntry {
                    event_log_position: ordinary::EventLogPosition::new(position),
                    logged_event: sema::LoggedEvent::Container(record.clone()),
                };
                self.store
                    .record_container_transition(record, entry)
                    .and_then(|()| self.store.commit_sequence())
                    .map(|commit_sequence| (position, commit_sequence))
            });
        match recorded {
            Ok((event_log_position, commit_sequence)) => {
                sema::SemaWriteOutput::ContainerRecorded(sema::ContainerReceipt {
                    event_log_position: ordinary::EventLogPosition::new(event_log_position),
                    state_marker: Self::sema_marker(commit_sequence),
                })
            }
            Err(error) => panic!("persist container transition: {error}"),
        }
    }

    /// Build a write-rejection at a known commit sequence. The caller passes
    /// the sequence it already read under the store lock — this method never
    /// re-locks, so it is safe to call while the store guard is still held.
    fn write_rejected(
        commit_sequence: u64,
        reason: sema::RejectionReason,
    ) -> sema::SemaWriteOutput {
        sema::SemaWriteOutput::WriteRejected(sema::RejectionReport {
            rejection_reason: reason,
            state_marker: Self::sema_marker(commit_sequence),
        })
    }

    fn observe_sema(&self, input: sema::SemaReadInput) -> sema::SemaReadOutput {
        match input {
            sema::SemaReadInput::QueryGenerations(selection) => self.query_generations(selection),
            sema::SemaReadInput::ReadEventLog(range) => self.read_event_log(range),
            sema::SemaReadInput::CheckKeyMaterial(query) => self.check_key_material(query),
            sema::SemaReadInput::QueryTestRuns(lookup) => self.query_test_runs(lookup),
        }
    }

    /// Answer a `(ByTestRun …)` query from the durable test-run table (report
    /// 54 §5.3). Filters by cluster + node, and by run identifier when the
    /// lookup names one (`None` returns every run for that node). The matching
    /// rows are returned newest-first by run identifier so the routine
    /// `(Check …)` reader sees its latest run first.
    fn query_test_runs(&self, lookup: ordinary::TestRunLookup) -> sema::SemaReadOutput {
        let runs = match self.store.test_runs() {
            Ok(runs) => runs,
            Err(error) => panic!("read durable test runs: {error}"),
        };
        let commit_sequence = self.current_commit_sequence();
        let mut matching: Vec<sema::StoredTestRun> = runs
            .into_iter()
            .filter(|run| Self::test_run_matches(&lookup, run))
            .collect();
        matching.sort_by(|left, right| {
            right
                .test_run_identifier
                .payload()
                .cmp(left.test_run_identifier.payload())
        });
        sema::SemaReadOutput::TestRunsQueried(ordinary::TestRunListing {
            test_run_record_vector: matching
                .into_iter()
                .map(ordinary::TestRunRecord::from)
                .collect(),
            database_marker: sema::DatabaseMarker::new(Self::marker(commit_sequence)),
        })
    }

    fn test_run_matches(lookup: &ordinary::TestRunLookup, run: &sema::StoredTestRun) -> bool {
        lookup.cluster_name == run.cluster_name
            && lookup.node_name == run.node
            && lookup
                .optional_test_run_identifier
                .as_ref()
                .is_none_or(|identifier| identifier == &run.test_run_identifier)
    }

    fn query_generations(&self, selection: ordinary::Selection) -> sema::SemaReadOutput {
        let matching = self
            .store
            .matching_live_generations(|live| Self::generation_matches(&selection, live));
        let live_generations = match matching {
            Ok(live_generations) => live_generations,
            Err(error) => panic!("read durable generations: {error}"),
        };
        let commit_sequence = self.current_commit_sequence();
        let deployment_records: Vec<sema::DeploymentRecord> = match self.store.deployment_records()
        {
            Ok(records) => records
                .into_iter()
                .filter(|record| Self::deployment_record_matches(&selection, record))
                .collect(),
            Err(error) => panic!("read durable deployment records: {error}"),
        };
        let generations: Vec<ordinary::Generation> = live_generations
            .iter()
            .map(|generation| Self::project_generation(generation, &deployment_records))
            .collect();
        sema::SemaReadOutput::GenerationsQueried(ordinary::GenerationListing {
            generation_vector: generations,
            deployment_record_vector: deployment_records,
            state_marker: Self::marker(commit_sequence),
        })
    }

    fn generation_matches(selection: &ordinary::Selection, live: &sema::LiveGeneration) -> bool {
        match selection {
            ordinary::Selection::ByNode(selector) => {
                selector.cluster_name == live.cluster_name
                    && selector.node_name == live.node_name
                    && selector
                        .optional_generation_artifact
                        .as_ref()
                        .is_none_or(|artifact| artifact == &live.generation_artifact)
            }
            ordinary::Selection::ByGeneration(lookup) => {
                *lookup.payload() == live.generation_identifier
            }
            ordinary::Selection::ByDeployment(_) => false,
            ordinary::Selection::ByEventLog(_) => true,
            // A test-run selection never reads the generation set — it is
            // routed to QueryTestRuns before reaching here (decide_ordinary_input).
            ordinary::Selection::ByTestRun(_) => false,
        }
    }

    fn deployment_record_matches(
        selection: &ordinary::Selection,
        record: &sema::DeploymentRecord,
    ) -> bool {
        match selection {
            ordinary::Selection::ByNode(selector) => {
                record.deployment_request_identity.cluster_name == selector.cluster_name
                    && record.deployment_request_identity.node_name == selector.node_name
                    && selector
                        .optional_generation_artifact
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
            ordinary::Selection::ByEventLog(_) => true,
            ordinary::Selection::ByTestRun(_) => false,
        }
    }

    fn project_generation(
        live: &sema::LiveGeneration,
        deployment_records: &[sema::DeploymentRecord],
    ) -> ordinary::Generation {
        ordinary::Generation {
            generation_identifier: live.generation_identifier.clone(),
            deployment_identifier: live.deployment_identifier.clone(),
            cluster_name: live.cluster_name.clone(),
            node_name: live.node_name.clone(),
            generation_artifact: live.generation_artifact,
            activation_effect: live.activation_effect,
            generation_slot: live.generation_slot,
            closure_path: live.closure_path.clone(),
            optional_immutable_revision: deployment_records
                .iter()
                .find(|record| record.deployment_identifier == live.deployment_identifier)
                .and_then(|record| {
                    record
                        .deployment_request_identity
                        .optional_immutable_revision
                        .clone()
                }),
        }
    }

    fn read_event_log(&self, range: ordinary::EventLogRange) -> sema::SemaReadOutput {
        let entries = match self
            .store
            .event_log_in_range(*range.from.payload(), *range.until.payload())
        {
            Ok(entries) => entries,
            Err(_) => {
                return Self::read_missed(
                    self.current_commit_sequence(),
                    sema::RejectionReason::EventLogPositionOutOfRange,
                );
            }
        };
        let commit_sequence = self.current_commit_sequence();
        let mut deployment_events = Vec::new();
        let mut retention_events = Vec::new();
        for entry in &entries {
            match &entry.logged_event {
                sema::LoggedEvent::Deployment(event) => deployment_events.push(event.clone()),
                sema::LoggedEvent::CacheRetention(event) => retention_events.push(event.clone()),
                sema::LoggedEvent::Container(_) => {}
            }
        }
        sema::SemaReadOutput::EventLogRead(sema::EventLogPage {
            deployment_phase_event_vector: deployment_events,
            cache_retention_transition_event_vector: retention_events,
            state_marker: Self::sema_marker(commit_sequence),
        })
    }

    fn check_key_material(&self, query: ordinary::KeyMaterialQuery) -> sema::SemaReadOutput {
        let commit_sequence = self.current_commit_sequence();
        sema::SemaReadOutput::KeyMaterialChecked(ordinary::KeyMaterialReport {
            node_name: query.node_name,
            string_vector: Vec::new(),
            state_marker: Self::marker(commit_sequence),
        })
    }

    /// Build a read-miss at a known commit sequence. Like `write_rejected`,
    /// this never re-locks; the caller supplies the sequence.
    fn read_missed(commit_sequence: u64, reason: sema::RejectionReason) -> sema::SemaReadOutput {
        sema::SemaReadOutput::ReadMissed(sema::RejectionReport {
            rejection_reason: reason,
            state_marker: Self::sema_marker(commit_sequence),
        })
    }

    // ---- real nix IO (port plan §4.3) -----------------------------------

    async fn resolve_flake_auth(&self, request: nexus::FlakeAuthRequest) -> nexus::EffectResult {
        // Park on the test effect barrier (if any) before the first real effect
        // runs. Production carries no barrier and falls straight through; a
        // decoupling test holds it closed to prove the accepted handle is
        // replied while the pipeline is still parked here (up9b).
        if let Some(barrier) = self.configuration.effect_barrier() {
            barrier.wait().await;
        }
        // Resolve the flake metadata to a locked revision through Nix. The
        // typed SourceRevisionRecord produced here is carried through the
        // eval/build path, event log, deploy-job row, and live-generation state.
        match NixCommand::flake_metadata(request.flake_reference.payload())
            .run(self.configuration.effect_execution())
            .await
        {
            Ok(output) => match NixFlakeMetadata::parse(&output) {
                Ok(metadata) => nexus::EffectResult::FlakeResolved(nexus::ResolvedFlake::new(
                    metadata
                        .source_revision(request.source_revision_policy, request.flake_reference),
                )),
                Err(detail) => Self::effect_failed(nexus::EffectStage::FlakeAuth, detail),
            },
            Err(detail) => Self::effect_failed(nexus::EffectStage::FlakeAuth, detail),
        }
    }

    async fn run_horizon_materialization(
        &self,
        command: nexus::HorizonMaterializationCommand,
    ) -> nexus::EffectResult {
        let materialization =
            HorizonMaterialization::new(self.configuration.as_ref().clone(), command);
        match materialization.run().await {
            Ok(inputs) => nexus::EffectResult::HorizonMaterialized(inputs),
            Err(detail) => Self::effect_failed(nexus::EffectStage::MaterializeHorizon, detail),
        }
    }

    async fn run_nix_eval(&self, command: nexus::NixEvalCommand) -> nexus::EffectResult {
        let attribute = format!(
            "{}#{}",
            command.flake_reference.payload(),
            command.deployment_output_selector.payload().payload(),
        );
        let refresh = EvalRefresh::for_source(
            command.source_revision_record.source_revision_policy,
            command.flake_reference.payload(),
        );
        match NixCommand::eval_drv_path(
            &attribute,
            &command.flake_input_override_vector,
            &command.build_target,
            refresh,
        )
        .run(self.configuration.effect_execution())
        .await
        {
            Ok(output) => {
                let closure_path = NixCommand::first_line(&output);
                if !canonical_nix_store_root(&closure_path) {
                    return Self::effect_failed(
                        nexus::EffectStage::Eval,
                        "nix eval returned a noncanonical closure path".to_string(),
                    );
                }
                nexus::EffectResult::ClosureEvaluated(nexus::EvaluatedClosure {
                    generation_identifier: command.generation_identifier,
                    closure_path: ordinary::ClosurePath::new(closure_path),
                })
            }
            Err(detail) => Self::effect_failed(nexus::EffectStage::Eval, detail),
        }
    }

    async fn run_nix_build(&self, command: nexus::NixBuildCommand) -> nexus::EffectResult {
        // The default owner transport realizes in the authenticated local Lojix
        // context. An explicit builder still uses the local Nix client and
        // imports its result locally; no deploy stage builds through an ssh-ng
        // target store. The following copy stage transports this exact output.
        let invocation = match &command.build_target {
            nexus::BuildTarget::Local => NixCommand::build_closure(
                command.closure_path.payload(),
                &command.extra_substituter_vector,
            ),
            nexus::BuildTarget::Remote(builder_spec) => NixCommand::build_closure_remote(
                command.closure_path.payload(),
                builder_spec.payload(),
                &command.extra_substituter_vector,
            ),
        };
        match invocation.run(self.configuration.effect_execution()).await {
            Ok(output) => {
                let closure_path =
                    NixCommand::first_line_or(&output, command.closure_path.payload());
                if !canonical_nix_store_root(&closure_path) {
                    return Self::effect_failed(
                        nexus::EffectStage::Build,
                        "nix build returned a noncanonical closure path".to_string(),
                    );
                }
                nexus::EffectResult::ClosureBuilt(nexus::BuiltClosure {
                    generation_identifier: command.generation_identifier,
                    closure_path: ordinary::ClosurePath::new(closure_path),
                })
            }
            Err(detail) => Self::effect_failed(nexus::EffectStage::Build, detail),
        }
    }

    async fn run_copy_closure(&self, command: nexus::CopyClosureCommand) -> nexus::EffectResult {
        let copied = nexus::EffectResult::ClosureCopied(nexus::CopiedClosure {
            generation_identifier: command.generation_identifier.clone(),
            node_name: command.node_name.clone(),
            closure_path: command.closure_path.clone(),
        });
        let copy = match ClosureCopy::from_command(&command) {
            Ok(copy) => copy,
            Err(detail) => return Self::effect_failed(nexus::EffectStage::CopyClosure, detail),
        };
        match copy.run(self.configuration.effect_execution()).await {
            Ok(()) => copied,
            Err(detail) => Self::effect_failed(nexus::EffectStage::CopyClosure, detail),
        }
    }

    async fn run_activate_generation(
        &self,
        command: nexus::ActivateGenerationCommand,
    ) -> nexus::EffectResult {
        let slot = Self::activation_slot(&command.activation_effect);
        let activation =
            match Activation::from_command(&command, Some(self.configuration.daemon_host())) {
                Ok(activation) => activation,
                Err(detail) => return Self::effect_failed(nexus::EffectStage::Activate, detail),
            };
        match activation.run(self.configuration.effect_execution()).await {
            Ok(()) => nexus::EffectResult::GenerationActivated(nexus::ActivatedGeneration {
                generation_identifier: command.generation_identifier,
                node_name: command.node_name,
                generation_slot: slot,
            }),
            Err(detail) => Self::effect_failed(nexus::EffectStage::Activate, detail),
        }
    }

    fn activation_slot(activation_effect: &ordinary::ActivationEffect) -> ordinary::GenerationSlot {
        match activation_effect {
            ordinary::ActivationEffect::LiveActivation => ordinary::GenerationSlot::Current,
            ordinary::ActivationEffect::BootProfile => ordinary::GenerationSlot::BootPending,
            ordinary::ActivationEffect::TestActivation => ordinary::GenerationSlot::Recent,
            ordinary::ActivationEffect::BootOnceProfile => ordinary::GenerationSlot::BootPending,
            ordinary::ActivationEffect::ProfileOnly => ordinary::GenerationSlot::Current,
        }
    }

    async fn run_path_info_gc(&self, command: nexus::PathInfoGcCommand) -> nexus::EffectResult {
        match NixCommand::collect_garbage(command.node_name.payload())
            .run(self.configuration.effect_execution())
            .await
        {
            Ok(output) => nexus::EffectResult::PathsCollected(nexus::GarbageCollected {
                cluster_name: command.cluster_name,
                node_name: command.node_name,
                integer: NixCommand::count_lines(&output),
            }),
            Err(detail) => Self::effect_failed(nexus::EffectStage::Gc, detail),
        }
    }

    /// The real hermetic check effect builds the exact profile selector. Exit
    /// 0 plus an out-path produces `HermeticCheckBuilt`; a non-zero exit
    /// produces `EffectFailed(HermeticCheck)`. The selected check owns its
    /// sandboxed VM, so this is a pure build with zero host effect. The outcome
    /// is the actual Nix build result.
    async fn run_hermetic_check(
        &self,
        command: nexus::HermeticCheckCommand,
    ) -> nexus::EffectResult {
        let cluster_name = command.cluster_name.clone();
        let node_name = command.node_name.clone();
        match HermeticCheck::new(command)
            .run(self.configuration.effect_execution())
            .await
        {
            Ok(closure_path) => nexus::EffectResult::HermeticCheckBuilt(nexus::CheckBuilt {
                cluster_name,
                node_name,
                closure_path,
            }),
            Err(detail) => Self::effect_failed(nexus::EffectStage::HermeticCheck, detail),
        }
    }

    /// The live bring-up effect. BUILT
    /// here, NOT run live (the first Prometheus cycle is psyche-gated): the
    /// host-untouched user-namespace bring-up command is constructed
    /// (`ssh <host-fqdn>` + `systemd-run --user` + `unshare -rn` + `nsenter`)
    /// and would, on a live run, start the generated microVM runner + additive
    /// tap inside a private user network namespace on the resolved vmhost. The
    /// gated build path returns `TestVmBroughtUp` so the bracket is provably
    /// constructed end-to-end without touching a real host.
    async fn run_bring_up_test_vm(
        &self,
        command: nexus::BringUpTestVmCommand,
    ) -> nexus::EffectResult {
        let bring_up = match LiveTestVm::from_bring_up(&command) {
            Ok(bring_up) => bring_up,
            Err(detail) => return Self::effect_failed(nexus::EffectStage::BringUpTestVm, detail),
        };
        // The invocation is CONSTRUCTED (the host-untouched user-namespace
        // command) but not executed — a live run is gated. Constructing it
        // proves the command shape; `invocation()` is the on-host effect a live
        // run would `.run().await`.
        let _invocation = bring_up.bring_up_invocation();
        nexus::EffectResult::TestVmStarted(nexus::TestVmBroughtUp {
            cluster_name: command.cluster_name,
            node: command.node,
            host: command.host,
        })
    }

    /// The LIVE teardown effect (Unit 2b). BUILT here, NOT run live: constructs
    /// the `systemctl --user stop` command that, on a live run, stops the user
    /// units so the tap + route vanish with the namespace (host netns
    /// byte-identical). Returns `TestVmTornDown` for the gated build path.
    async fn run_tear_down_test_vm(
        &self,
        command: nexus::TearDownTestVmCommand,
    ) -> nexus::EffectResult {
        let tear_down = match LiveTestVm::from_tear_down(&command) {
            Ok(tear_down) => tear_down,
            Err(detail) => return Self::effect_failed(nexus::EffectStage::TearDownTestVm, detail),
        };
        let _invocation = tear_down.tear_down_invocation();
        nexus::EffectResult::TestVmStopped(nexus::TestVmTornDown {
            cluster_name: command.cluster_name,
            node: command.node,
            host: command.host,
        })
    }

    fn effect_failed(stage: nexus::EffectStage, detail: String) -> nexus::EffectResult {
        nexus::EffectResult::EffectFailed(nexus::EffectFailure {
            effect_stage: stage,
            string: detail,
        })
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

impl HorizonMaterialization {
    fn new(
        configuration: RuntimeConfiguration,
        command: nexus::HorizonMaterializationCommand,
    ) -> Self {
        Self {
            configuration,
            command,
        }
    }

    async fn run(&self) -> std::result::Result<nexus::MaterializedInputs, String> {
        self.run_inner().await.map_err(|error| error.to_string())
    }

    async fn run_inner(&self) -> Result<nexus::MaterializedInputs> {
        let proposal = ProjectableProposal::from(
            ProposalFile::available(&self.command.proposal_source)
                .ok_or_else(|| Error::Invariant("proposal source is unavailable".to_string()))?
                .load()?,
        );
        let viewpoint = HorizonViewpoint::from_command(&self.command)?;
        let horizon = proposal.project(&viewpoint)?;
        let root = MaterializationRoot::new(self.configuration.materialization_root(&self.command));
        root.prepare()?;
        let secrets_source =
            ClusterSecretsDirectory::from_proposal_source(&self.command.proposal_source);
        MaterializedInputSet::new(
            root,
            horizon,
            self.command.materialization_shape.clone(),
            secrets_source,
        )
        .write(self.configuration.effect_execution())
        .await
    }
}

/// The cluster proposal file path carried by `signal-lojix::ProposalSource`.
#[derive(Debug, Clone)]
struct ProposalFile {
    path: PathBuf,
}

impl ProposalFile {
    /// The proposal source is a privileged local configuration input.  Its
    /// path must name the canonical regular proposal artifact directly; redirects,
    /// traversal, controls, and credential-shaped locations are not admitted
    /// into either Horizon projection or Nix materialization.
    fn checked(source: &ordinary::ProposalSource) -> Option<Self> {
        let raw = source.payload();
        if raw.is_empty() || raw.chars().any(char::is_control) || credential_like(raw) {
            return None;
        }
        let path = PathBuf::from(raw);
        if !path.is_absolute()
            || path.file_name().and_then(|name| name.to_str())
                != Some(CANONICAL_PROPOSAL_ARTIFACT)
            || path.components().any(|component| {
                !matches!(
                    component,
                    std::path::Component::RootDir | std::path::Component::Normal(_)
                )
            })
        {
            return None;
        }
        let mut prefix = PathBuf::from("/");
        for component in path.components() {
            let std::path::Component::Normal(part) = component else {
                continue;
            };
            prefix.push(part);
            let metadata = fs::symlink_metadata(&prefix).ok()?;
            if metadata.file_type().is_symlink() {
                return None;
            }
        }
        let metadata = fs::symlink_metadata(&path).ok()?;
        metadata.file_type().is_file().then_some(Self { path })
    }

    /// Prove that the source is both safe to address and parsable as the
    /// actual proposal shape before a deploy is admitted or an effect starts.
    fn available(source: &ordinary::ProposalSource) -> Option<Self> {
        let proposal = Self::checked(source)?;
        proposal.load().ok().map(|_| proposal)
    }

    fn load(&self) -> Result<ClusterProposal> {
        let text = fs::read_to_string(&self.path)
            .map_err(|_| Error::Invariant("proposal source is unavailable".to_string()))?;
        Text::<ClusterProposal>::from(text.as_str())
            .embody()
            .map_err(|_| Error::Invariant("proposal source is not valid Datomic".to_string()))
    }
}

/// Typed Horizon viewpoint derived from the deploy command.
#[derive(Debug, Clone)]
struct HorizonViewpoint {
    cluster: HorizonClusterName,
    node: HorizonNodeName,
}

impl HorizonViewpoint {
    fn from_command(command: &nexus::HorizonMaterializationCommand) -> Result<Self> {
        Ok(Self {
            cluster: HorizonClusterName::try_new(command.cluster_name.payload().clone())?,
            node: HorizonNodeName::try_new(command.node_name.payload().clone())?,
        })
    }

    fn as_horizon_viewpoint(&self) -> Viewpoint {
        Viewpoint {
            cluster: self.cluster.clone(),
            node: self.node.clone(),
        }
    }
}

/// Projection model: today's production Horizon shape still has separate
/// pan-Horizon and cluster proposals to match the old deploy stack.
#[derive(Debug, Clone)]
struct ProjectableProposal {
    cluster: ClusterProposal,
}

impl ProjectableProposal {
    fn project(&self, viewpoint: &HorizonViewpoint) -> Result<Horizon> {
        Ok(self.cluster.project(&viewpoint.as_horizon_viewpoint())?)
    }
}

impl From<ClusterProposal> for ProjectableProposal {
    fn from(cluster: ClusterProposal) -> Self {
        Self { cluster }
    }
}

/// Root directory for one materialization result.
#[derive(Debug, Clone)]
struct MaterializationRoot {
    path: PathBuf,
}

impl MaterializationRoot {
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

impl MaterializedInputSet {
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
                .write_system(&self.horizon.node.system)?
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

impl GeneratedInputDirectory {
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

    fn write_system(&self, system: &horizon_lib::species::System) -> Result<Self> {
        self.prepare()?;
        fs::write(
            self.path.join("flake.nix"),
            format!(
                "{{ outputs = _: {{ system = \"{}\"; }}; }}\n",
                NixSystemName::from_horizon_system(system).as_str()
            ),
        )?;
        Ok(self.clone())
    }

    fn write_deployment(&self, deployment: &DeploymentInput) -> Result<Self> {
        self.prepare()?;
        fs::write(self.path.join("flake.nix"), deployment.flake_text())?;
        Ok(self.clone())
    }

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

    /// Remove the generated directory (ignoring an absent one) and recreate it
    /// empty, so a regenerated `secrets` input contains exactly the current
    /// files with no stale leftovers from a prior deploy.
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
    ) -> Result<nexus::FlakeInputOverride> {
        let hash = NarHash::from_path(&self.path, execution).await?;
        Ok(nexus::FlakeInputOverride {
            string: name.as_str().to_string(),
            flake_input_reference: nexus::FlakeInputReference {
                url: format!("path:{}", self.path.display()),
                nix_archive_hash: hash.as_url_query_value(),
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

impl GeneratedInputName {
    fn as_str(self) -> &'static str {
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

impl DeploymentInput {
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

/// The cluster repository's `secrets/` directory — the sibling of the deploy
/// datom source. Each sops-encrypted file there becomes one
/// `inputs.secrets.sopsFiles.<stem>` entry the daemon provisions per deploy,
/// where `<stem>` is the `.sops` filename stem VERBATIM (the file is named with
/// its exact consumer attribute name; no case transform), overriding CriomOS's
/// `stubs/no-secrets` stub. The directory may be absent (a bare bootstrap datom
/// with no cluster secrets), in which case the generated `secrets` input still
/// exposes an empty `sopsFiles = { }`.
#[derive(Debug, Clone)]
struct ClusterSecretsDirectory {
    path: PathBuf,
}

impl ClusterSecretsDirectory {
    /// `<source-parent>/secrets` — the datom source's sibling secrets directory.
    fn from_proposal_source(source: &ordinary::ProposalSource) -> Self {
        let source_path = PathBuf::from(source.payload());
        let parent = source_path
            .parent()
            .map(Path::to_path_buf)
            .unwrap_or_default();
        Self {
            path: parent.join("secrets"),
        }
    }

    /// The `*.sops` files in this directory, sorted by file name for a stable
    /// generated flake. An absent directory yields an empty list (bootstrap
    /// sources carry no cluster secrets); other read failures propagate.
    fn secret_files(&self) -> Result<Vec<ClusterSecretFile>> {
        if !self.path.is_dir() {
            return Ok(Vec::new());
        }
        let mut files = Vec::new();
        for entry in fs::read_dir(&self.path)? {
            let path = entry?.path();
            if path.extension().and_then(|extension| extension.to_str()) == Some("sops") {
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

impl ClusterSecretFile {
    fn new(path: PathBuf) -> Self {
        Self { path }
    }

    /// The full path, used as the stable sort key for a deterministic generated
    /// flake. Within one secrets directory it orders identically to the file
    /// name, and unlike `file_name` it is infallible so the sort comparator
    /// stays total even for a (later-rejected) non-UTF-8 name.
    fn sort_key(&self) -> &Path {
        &self.path
    }

    /// The bare file name (`fixtureCamelCase.sops`) used both for the copy
    /// destination and the generated `./<file>.sops` Nix path. A non-UTF-8 file
    /// name is a real error (it cannot name a Nix path), not an empty string, so
    /// it returns a typed `Error::SecretFileNameNotUtf8` per the typed-error
    /// discipline.
    fn file_name(&self) -> Result<String> {
        self.path
            .file_name()
            .and_then(|name| name.to_str())
            .map(str::to_string)
            .ok_or_else(|| crate::Error::SecretFileNameNotUtf8(self.path.clone()))
    }

    /// The CriomOS `sopsFiles` attribute name: the `.sops` filename stem
    /// VERBATIM (only the `.sops` suffix stripped), with NO case transform. The
    /// coordinated design renames each cluster secret file to its exact
    /// camelCase consumer name (`fixtureCamelCase.sops`), so the attribute
    /// name is the stem as written — no hidden, lossy kebab-to-camel coupling. A
    /// non-UTF-8 stem returns a typed `Error::SecretFileNameNotUtf8`.
    fn attribute_name(&self) -> Result<String> {
        self.path
            .file_stem()
            .and_then(|stem| stem.to_str())
            .map(str::to_string)
            .ok_or_else(|| crate::Error::SecretFileNameNotUtf8(self.path.clone()))
    }

    /// Copy the opaque ciphertext into the generated input directory so the
    /// flake is self-contained (its narHash covers the file). The contents are
    /// never read into the daemon.
    fn copy_into(&self, directory: &Path) -> Result<()> {
        fs::copy(&self.path, directory.join(self.file_name()?))?;
        Ok(())
    }
}

/// Nix platform string derived from Horizon's typed system.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct NixSystemName(&'static str);

impl NixSystemName {
    fn from_horizon_system(system: &horizon_lib::species::System) -> Self {
        match system {
            horizon_lib::species::System::X86_64Linux => Self("x86_64-linux"),
            horizon_lib::species::System::Aarch64Linux => Self("aarch64-linux"),
        }
    }

    fn as_str(self) -> &'static str {
        self.0
    }
}

/// SRI NAR hash for a generated input directory.
#[derive(Debug, Clone, PartialEq, Eq)]
struct NarHash(String);

impl NarHash {
    async fn from_path(path: &Path, execution: &EffectExecution) -> Result<Self> {
        let output = NixCommand::hash_path(path)
            .run(execution)
            .await
            .map_err(|detail| {
                std::io::Error::other(format!("failed to hash generated input: {detail}"))
            })?;
        Ok(Self(NixCommand::first_line(&output)))
    }

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

/// Exact request-supplied deployment transport. The two strings have distinct
/// consumers: Nix receives `nix_store_uri` and SSH receives
/// `ssh_destination`. They are validated as command arguments but never
/// normalized, rewritten, or derived from cluster/node identity.
#[derive(Debug, Clone, PartialEq, Eq)]
struct SshTarget {
    nix_store_uri: String,
    ssh_destination: String,
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

impl SshTarget {
    fn from_transport(transport: &nexus::DeploymentTransport) -> std::result::Result<Self, String> {
        let nix_store_uri = transport.nix_store_uri.payload().clone();
        let ssh_destination = transport.ssh_destination.payload().clone();
        Self::validate_nix_store_uri(&nix_store_uri)?;
        Self::validate_ssh_destination(&ssh_destination)?;
        Ok(Self {
            nix_store_uri,
            ssh_destination,
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

    /// Classify the explicit SSH login for a Home Manager target. Only the
    /// literal `root` login carries mediation authority; Lojix never promotes
    /// another login or derives a root route from node or user identity.
    fn user_environment_activation_authority(
        &self,
        user: &HorizonUserName,
    ) -> RemoteUserActivationAuthority {
        let (login, _) = self
            .ssh_destination
            .split_once('@')
            .expect("SshTarget retains a validated SSH destination");
        if login == user.as_str() {
            RemoteUserActivationAuthority::MatchedUser
        } else if login == "root" {
            RemoteUserActivationAuthority::RootMediated
        } else {
            RemoteUserActivationAuthority::UnprivilegedMismatch
        }
    }

    /// `ssh -o BatchMode=yes <user>@<domain> <remote_command>`.
    fn remote_invocation(&self, remote_command: ShellCommand) -> NixCommand {
        NixCommand::new(
            "ssh",
            vec![
                "-o".to_string(),
                "BatchMode=yes".to_string(),
                self.ssh_destination.clone(),
                remote_command.into_text(),
            ],
        )
    }
}

/// A pre-quoted remote shell command body — the single string ssh runs on the
/// target.
#[derive(Debug, Clone, PartialEq, Eq)]
struct ShellCommand(String);

impl ShellCommand {
    fn from_raw(script: impl Into<String>) -> Self {
        Self(script.into())
    }

    fn into_text(self) -> String {
        self.0
    }
}

/// A single argument rendered safe for a remote `/bin/sh -c` body. Ported from
/// shell argument rendering: bare when the text is wholly
/// shell-safe, single-quoted (with `'\''` escaping) otherwise.
#[derive(Debug, Clone, PartialEq, Eq)]
struct ShellArgument {
    text: String,
}

impl ShellArgument {
    fn new(text: impl Into<String>) -> Self {
        Self { text: text.into() }
    }

    fn to_command_text(&self) -> String {
        let safe = !self.text.is_empty()
            && self.text.bytes().all(|byte| {
                matches!(
                    byte,
                    b'a'..=b'z'
                        | b'A'..=b'Z'
                        | b'0'..=b'9'
                        | b'-'
                        | b'_'
                        | b'.'
                        | b'/'
                        | b'='
                        | b':'
                        | b'#'
                        | b'+'
                        | b','
                )
            });
        if safe {
            return self.text.clone();
        }
        format!("'{}'", self.text.replace('\'', "'\\''"))
    }
}

/// Move the locally realized immutable closure from the dispatcher store to
/// the activation target. Always passes `--substitute-on-destination` so the
/// target pulls signed paths from the cluster cache when available; unsigned
/// daemon-to-daemon transfer is rejected under `require-sigs` (risk R6).
#[derive(Debug, Clone, PartialEq, Eq)]
struct ClosureCopy {
    store_path: String,
    target: SshTarget,
}

impl ClosureCopy {
    fn from_command(command: &nexus::CopyClosureCommand) -> std::result::Result<Self, String> {
        let target = SshTarget::from_transport(&command.deployment_transport)?;
        Ok(Self {
            store_path: command.closure_path.payload().clone(),
            target,
        })
    }

    /// Copy is idempotent: if the closure already exists on the target, Nix
    /// exits successfully without changing the activation state.
    fn invocation(&self) -> NixCommand {
        let arguments: Vec<String> = vec![
            "copy".to_string(),
            "--substitute-on-destination".to_string(),
            "--to".to_string(),
            self.target.nix_store_uri.clone(),
            self.store_path.clone(),
        ];
        NixCommand::new("nix", arguments)
    }

    async fn run(&self, execution: &EffectExecution) -> std::result::Result<(), String> {
        self.invocation().run(execution).await.map(|_| ())
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

impl Activation {
    /// `daemon_host` is the node the dispatching daemon runs on, so a host
    /// activation can detect a self-targeting deploy and route around the
    /// self-Switch deadlock; `None` when no daemon host context is available.
    fn from_command(
        command: &nexus::ActivateGenerationCommand,
        daemon_host: Option<&ordinary::NodeName>,
    ) -> std::result::Result<Self, String> {
        let target = SshTarget::from_transport(&command.deployment_transport)?;
        let store_path = command.closure_path.payload().clone();
        match (&command.activation_backend, &command.activation_profile) {
            (
                nexus::ActivationBackend::NixosSystemdBootV1,
                nexus::ActivationProfile::Host(action),
            ) => Ok(Self::Host(HostActivation {
                deployment_identifier: command.deployment_identifier.clone(),
                target,
                node_name: command.node_name.clone(),
                daemon_host: daemon_host.cloned(),
                store_path,
                action: *action,
            })),
            (
                nexus::ActivationBackend::HomeManagerNixProfileV1,
                nexus::ActivationProfile::UserEnvironment(profile),
            ) => {
                let user = HorizonUserName::try_new(profile.user_name.payload().clone())
                    .map_err(|error| format!("invalid user name for home activation: {error}"))?;
                Ok(Self::UserEnvironment(UserEnvironmentActivation {
                    node_name: command.node_name.clone(),
                    authority: target.user_environment_activation_authority(&user),
                    target,
                    user,
                    store_path,
                    mode: profile.user_environment_action,
                }))
            }
            _ => Err(
                "activation backend does not support the requested activation profile".to_string(),
            ),
        }
    }

    async fn run(&self, execution: &EffectExecution) -> std::result::Result<(), String> {
        match self {
            Self::Host(activation) => activation.run(execution).await,
            Self::UserEnvironment(activation) => activation.run(execution).await,
        }
    }
}

/// Host activation on the target.
///
/// `Boot`/`Switch`/`Test`: one ssh call running `switch-to-configuration
/// <action>` directly (Boot/Switch first set the system profile). `BootOnce`:
/// one ssh call wrapping the boot-once script in `systemd-run --unit=<name>
/// --collect --wait --service-type=oneshot /bin/sh -c '…'` — owned by PID 1,
/// not the dispatcher's ssh, so a network blip that kills the ssh leaves the
/// unit running on the target to completion.
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
    store_path: String,
    action: ordinary::HostDeployAction,
}

impl HostActivation {
    /// Invocation for the simple Boot/Switch/Test path. `None` for the
    /// non-simple actions (BootOnce uses `systemd_run_invocation`; Eval/Build
    /// do not activate).
    fn ssh_invocation(&self) -> Option<NixCommand> {
        let action_word = match self.action {
            ordinary::HostDeployAction::SetBootProfile => "boot",
            ordinary::HostDeployAction::ActivateNow => "switch",
            ordinary::HostDeployAction::TestActivation => "test",
            ordinary::HostDeployAction::ScheduleBootOnce
            | ordinary::HostDeployAction::Evaluate
            | ordinary::HostDeployAction::Realize => return None,
        };
        let store = &self.store_path;
        let remote_command = if matches!(self.action, ordinary::HostDeployAction::TestActivation) {
            format!("{store}/bin/switch-to-configuration {action_word}")
        } else {
            format!(
                "nix-env -p /nix/var/nix/profiles/system --set {store} \
                 && {store}/bin/switch-to-configuration {action_word}"
            )
        };
        Some(
            self.target
                .remote_invocation(ShellCommand::from_raw(remote_command)),
        )
    }

    /// The deterministic BootOnce transient unit name for this deploy:
    /// `lojix-boot-once-deploy-<deployment-identifier>`, the same string the
    /// durable resume cursor (`DeployJob::boot_once_unit`) persists at submit.
    /// Deriving both from the deployment identifier (not the old time + pid
    /// suffix) is what lets a daemon that crashes inside the BootOnce window
    /// recompute the running unit's name on restart and reconcile by polling
    /// `journalctl -u <unit>` rather than re-activating (report 150). One
    /// deploy → one identifier → one unit, so concurrent deploys still don't
    /// collide.
    fn unit_name(&self) -> String {
        self.deployment_identifier.boot_once_unit_name()
    }

    /// The bash script that runs inside the transient unit on the target. `OLD`
    /// (the rollback target) is read from `bootctl status`'s `Current Entry`.
    /// After the candidate has generated its boot configuration, `NEW` is read
    /// from its generated `loader.conf`, which is the only authority that names
    /// the hash-named entry. Reboot 1 lands NEW; reboot 2+ returns to OLD.
    fn boot_once_script(&self) -> String {
        let store = &self.store_path;
        format!(
            "export PATH=/run/current-system/sw/bin:/run/wrappers/bin:$PATH\n\
             set -eu\n\
             CLOSURE='{store}'\n\
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
        )
    }

    /// BootOnce ssh call: wraps the boot-once script in `systemd-run --wait`.
    /// ssh holds open as a live stdout/stderr channel; if it dies the unit runs
    /// to completion regardless (`detached_invocation()`).
    fn systemd_run_invocation(&self, unit_name: &str) -> NixCommand {
        self.detached_invocation(unit_name, self.boot_once_script())
    }

    /// Wrap an arbitrary activation `script` in a PID-1-owned transient unit
    /// (`systemd-run --service-type=oneshot --wait`). The unit is owned by
    /// systemd, not the dispatcher's ssh, so a restart of the daemon mid-script
    /// (a self-Switch restarting `switch-to-configuration switch`) or a network
    /// blip that kills the ssh leaves the unit running to completion on the
    /// target. Shared by BootOnce and the deadlock-free self-Switch shape.
    fn detached_invocation(&self, unit_name: &str, script: String) -> NixCommand {
        let remote_command = format!(
            "systemd-run \
             --unit={unit_name} \
             --collect \
             --wait \
             --service-type=oneshot \
             /bin/sh -c {script}",
            script = ShellArgument::new(script).to_command_text(),
        );
        self.target
            .remote_invocation(ShellCommand::from_raw(remote_command))
    }

    /// Whether this activation must run in the detached PID-1-owned shape rather
    /// than a foreground ssh: a `Switch` whose target node IS the dispatching
    /// daemon's own host. `switch-to-configuration switch` restarts the daemon,
    /// which would kill a foreground ssh and deadlock the deploy; the detached
    /// transient unit survives the restart (report 150 self-Switch). Always
    /// false when the daemon host is unknown or the target is a different node.
    fn runs_detached_self_switch(&self) -> bool {
        matches!(self.action, ordinary::HostDeployAction::ActivateNow)
            && self
                .daemon_host
                .as_ref()
                .is_some_and(|host| host.payload() == self.node_name.payload())
    }

    /// The deterministic transient unit name for a detached self-Switch:
    /// `lojix-self-switch-deploy-<deployment-identifier>`. Distinct from the
    /// BootOnce unit name (a self-Switch is `switch-to-configuration switch`,
    /// not a boot-once entry), but derived from the same deployment identifier so
    /// it is one deploy → one unit.
    fn self_switch_unit_name(&self) -> String {
        format!(
            "lojix-self-switch-deploy-{}",
            self.deployment_identifier.payload()
        )
    }

    /// The activation script for a detached self-Switch: set the system profile,
    /// `switch-to-configuration switch` — the same Switch semantics as the
    /// foreground path's `ssh_invocation`, NOT a boot-once entry — then clears
    /// both EFI overrides so declarative `loader.conf` is the sole authority.
    /// The whole
    /// activation runs inside the transient unit, so the daemon restart `switch`
    /// triggers cannot kill it mid-flight; the post-switch reconcile rides along
    /// in the same PID-1-owned unit rather than a (now-dead) foreground ssh.
    fn self_switch_script(&self) -> String {
        let store = &self.store_path;
        format!(
            "export PATH=/run/current-system/sw/bin:/run/wrappers/bin:$PATH\n\
             set -eu\n\
             nix-env -p /nix/var/nix/profiles/system --set {store}\n\
             {store}/bin/switch-to-configuration switch\n\
             bootctl set-default ''\n\
             bootctl set-oneshot ''\n"
        )
    }

    /// Whether this action reconciles EFI bootloader vars after activation.
    /// `Boot`/`Switch` write the declarative `loader.conf` default. Reconcile
    /// removes both EFI overrides so they cannot compete. `Test` is
    /// non-persistent; `BootOnce` is its own thing
    /// (`requires_efi_reconcile()`).
    fn requires_efi_reconcile(&self) -> bool {
        matches!(
            self.action,
            ordinary::HostDeployAction::SetBootProfile | ordinary::HostDeployAction::ActivateNow
        )
    }

    /// Clear EFI `LoaderEntryDefault`; `loader.conf` owns persistent boot.
    fn step_clear_efi_default_invocation(&self) -> NixCommand {
        self.target
            .remote_invocation(ShellCommand::from_raw("bootctl set-default ''"))
    }

    /// `bootctl set-oneshot ''` — clears any pending EFI one-shot from a prior
    /// BootOnce so it does not hijack the next reboot.
    fn step_clear_efi_oneshot_invocation(&self) -> NixCommand {
        self.target
            .remote_invocation(ShellCommand::from_raw("bootctl set-oneshot ''"))
    }

    async fn run(&self, execution: &EffectExecution) -> std::result::Result<(), String> {
        if self.runs_detached_self_switch() {
            return self.run_self_switch(execution).await;
        }
        match self.action {
            ordinary::HostDeployAction::ScheduleBootOnce => self.run_boot_once(execution).await,
            _ => self.run_simple(execution).await,
        }
    }

    /// Deadlock-free self-Switch: run the full Switch activation inside a
    /// PID-1-owned transient unit (the BootOnce mechanism, carrying Switch
    /// semantics) so `switch-to-configuration switch` restarting the dispatching
    /// daemon does not kill the activation's foreground ssh (report 150).
    async fn run_self_switch(
        &self,
        execution: &EffectExecution,
    ) -> std::result::Result<(), String> {
        let unit_name = self.self_switch_unit_name();
        self.detached_invocation(&unit_name, self.self_switch_script())
            .run(execution)
            .await
            .map(|_| ())
    }

    async fn run_simple(&self, execution: &EffectExecution) -> std::result::Result<(), String> {
        match self.ssh_invocation() {
            Some(invocation) => invocation.run(execution).await.map(|_| ())?,
            None => {
                return Err(format!("no simple activation for action {:?}", self.action));
            }
        }
        if self.requires_efi_reconcile() {
            self.reconcile_efi(execution).await?;
        }
        Ok(())
    }

    async fn reconcile_efi(&self, execution: &EffectExecution) -> std::result::Result<(), String> {
        self.step_clear_efi_default_invocation()
            .run(execution)
            .await?;
        self.step_clear_efi_oneshot_invocation()
            .run(execution)
            .await?;
        Ok(())
    }

    async fn run_boot_once(&self, execution: &EffectExecution) -> std::result::Result<(), String> {
        let unit_name = self.unit_name();
        self.systemd_run_invocation(&unit_name)
            .run(execution)
            .await
            .map(|_| ())
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
    store_path: String,
    mode: meta::UserEnvironmentAction,
}

impl UserEnvironmentActivation {
    fn local_profile_invocation(&self, home: &Path) -> NixCommand {
        NixCommand::new(
            "nix-env",
            vec![
                "-p".to_string(),
                home.join(".local/state/nix/profiles/home-manager")
                    .display()
                    .to_string(),
                "--set".to_string(),
                self.store_path.clone(),
            ],
        )
    }

    fn local_activate_invocation(&self) -> NixCommand {
        NixCommand::new(format!("{}/activate", self.store_path), Vec::new())
    }

    fn remote_profile_invocation(&self) -> std::result::Result<NixCommand, String> {
        self.remote_invocation(ShellCommand::from_raw(format!(
            "nix-env -p \"$HOME/.local/state/nix/profiles/home-manager\" --set {}",
            ShellArgument::new(self.store_path.clone()).to_command_text(),
        )))
    }

    fn remote_activate_invocation(&self) -> std::result::Result<NixCommand, String> {
        self.remote_invocation(ShellCommand::from_raw(
            ShellArgument::new(format!("{}/activate", self.store_path)).to_command_text(),
        ))
    }

    /// Build the remote command from the authority carried by the explicit SSH
    /// destination. This is the effect-side fail-closed guard for recovered
    /// jobs; fresh submissions reject this same mismatch before any effect.
    fn remote_invocation(&self, command: ShellCommand) -> std::result::Result<NixCommand, String> {
        match self.authority {
            RemoteUserActivationAuthority::MatchedUser => {
                Ok(self.target.remote_invocation(command))
            }
            RemoteUserActivationAuthority::RootMediated => {
                Ok(self.root_mediated_invocation(command))
            }
            RemoteUserActivationAuthority::UnprivilegedMismatch => Err(
                "remote Home Manager activation login differs from the target user and is not root"
                    .to_string(),
            ),
        }
    }

    /// The explicit deployment SSH identity is root. Drop privilege to the
    /// target account for the profile and activation commands, so a user profile
    /// deploy works even when that account has no SSH login while preserving the
    /// profile's user-owned state.
    ///
    /// Drop through a login (`runuser --login`), not a bare privilege drop. The
    /// root SSH session's environment — `XDG_RUNTIME_DIR` and
    /// `DBUS_SESSION_BUS_ADDRESS` (`/run/user/0`), `NIX_PROFILES`,
    /// `XDG_DATA_DIRS`, `XDG_CONFIG_DIRS`, … — otherwise survives into the target
    /// context and points Home Manager's activation at root-owned runtime and
    /// profile paths; its `mkdir`, `dconf`, and systemd-reload steps then fail
    /// with permission errors. A login rebuilds the environment from the target
    /// account: correct `HOME`, `USER`, `LOGNAME`, and the target's own profile
    /// and runtime paths, so activation runs as a clean session of that user
    /// (its systemd reload reaches the target's live `/run/user/<uid>` session).
    /// The login also resolves the account's home natively, obviating a manual
    /// `getent` lookup.
    fn root_mediated_invocation(&self, command: ShellCommand) -> NixCommand {
        let user = ShellArgument::new(self.user.as_str()).to_command_text();
        let command = ShellArgument::new(command.into_text()).to_command_text();
        self.target
            .remote_invocation(ShellCommand::from_raw(format!(
                "runuser --login --command {command} {user}",
            )))
    }

    async fn run(&self, execution: &EffectExecution) -> std::result::Result<(), String> {
        match self.mode {
            meta::UserEnvironmentAction::Realize => Ok(()),
            meta::UserEnvironmentAction::SetProfile => self.run_profile(execution).await,
            meta::UserEnvironmentAction::ActivateNow => {
                self.run_profile(execution).await?;
                self.run_activate(execution).await
            }
        }
    }

    async fn run_profile(&self, execution: &EffectExecution) -> std::result::Result<(), String> {
        if !self.is_local_context(execution).await {
            return self
                .remote_profile_invocation()?
                .run(execution)
                .await
                .map(|_| ());
        }
        let home = std::env::var("HOME")
            .map_err(|_| "HOME is unset for local home activation".to_string())?;
        self.local_profile_invocation(Path::new(&home))
            .run(execution)
            .await
            .map(|_| ())
    }

    async fn run_activate(&self, execution: &EffectExecution) -> std::result::Result<(), String> {
        if !self.is_local_context(execution).await {
            return self
                .remote_activate_invocation()?
                .run(execution)
                .await
                .map(|_| ());
        }
        self.local_activate_invocation()
            .run(execution)
            .await
            .map(|_| ())
    }

    /// The local fast-path predicate: the dispatcher is already the requested
    /// user on the target node, so activation runs locally without ssh.
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
        let output = NixCommand::new("hostname", vec!["-s".to_string()])
            .run(execution)
            .await
            .ok()?;
        Some(output.trim().to_string())
    }
}

/// A typed `nix` / `nix-store` invocation. Holds the program name and its
/// argument vector so the same value can be inspected before it runs; `run`
/// spawns it via `tokio::process::Command` and returns captured stdout or a
/// failure detail string. Constructors model the target-side Nix invocations.
#[derive(Debug, Clone)]
struct NixCommand {
    program: String,
    arguments: Vec<String>,
}

impl NixCommand {
    fn new(program: impl Into<String>, arguments: Vec<String>) -> Self {
        Self {
            program: program.into(),
            arguments,
        }
    }

    fn flake_metadata(flake: &str) -> Self {
        Self::new(
            "nix",
            vec![
                "flake".to_string(),
                "metadata".to_string(),
                "--json".to_string(),
                flake.to_string(),
            ],
        )
    }

    /// Resolve the toplevel `.drvPath` before building in the daemon's local
    /// Nix context. The target parameter is retained in the generated command
    /// contract, but it deliberately contributes no `--store` redirect: an
    /// ssh-ng evaluation can deadlock in remote transport. The exact built
    /// output is copied to the target in the next stage instead.
    fn eval_drv_path(
        attribute: &str,
        overrides: &[nexus::FlakeInputOverride],
        target: &nexus::BuildTarget,
        refresh: EvalRefresh,
    ) -> Self {
        // `--refresh` is conditional (bead primary-8sv6): an immutable pin
        // evaluates deterministically, so the eval cache is authoritative and
        // the flag only forces a redundant full re-eval; a mutable ref keeps it.
        let mut arguments = vec!["eval".to_string()];
        if refresh.adds_refresh_flag() {
            arguments.push("--refresh".to_string());
        }
        arguments.push("--raw".to_string());
        let _ = target;
        arguments.extend(Self::override_input_options(overrides));
        arguments.push(format!("{attribute}.drvPath"));
        Self::new("nix", arguments)
    }

    fn hash_path(path: &Path) -> Self {
        Self::new(
            "nix",
            vec![
                "hash".to_string(),
                "path".to_string(),
                "--type".to_string(),
                "sha256".to_string(),
                "--sri".to_string(),
                path.display().to_string(),
            ],
        )
    }

    fn build_closure(closure_path: &str, substituters: &[nexus::ExtraSubstituter]) -> Self {
        let mut arguments = vec![
            "build".to_string(),
            "--no-link".to_string(),
            "--print-out-paths".to_string(),
            Self::output_installable(closure_path),
        ];
        arguments.extend(Self::substituter_options(substituters));
        Self::new("nix", arguments)
    }

    /// `nix build <installable> --no-link --print-out-paths` for a hermetic
    /// check. The exact request/configuration-owned output selector is passed
    /// verbatim — no derived attribute, `^*` output selector, or `.drvPath`
    /// indirection. Exit status is pass/fail; the printed line is the realised
    /// check out-path.
    fn build_check(installable: &str) -> Self {
        Self::new(
            "nix",
            vec![
                "build".to_string(),
                "--no-link".to_string(),
                "--print-out-paths".to_string(),
                installable.to_string(),
            ],
        )
    }

    fn build_closure_remote(
        closure_path: &str,
        builder_spec: &str,
        substituters: &[nexus::ExtraSubstituter],
    ) -> Self {
        let mut arguments = vec![
            "build".to_string(),
            "--no-link".to_string(),
            "--print-out-paths".to_string(),
            "--option".to_string(),
            "max-jobs".to_string(),
            "0".to_string(),
            "--builders".to_string(),
            builder_spec.to_string(),
            Self::output_installable(closure_path),
        ];
        arguments.extend(Self::substituter_options(substituters));
        Self::new("nix", arguments)
    }

    /// Select the derivation's *outputs* with the `^*` installable suffix so
    /// `nix build ... --print-out-paths` returns the realised output store
    /// path. The closure path threaded in is a `.drv` path (from
    /// `eval_drv_path`'s `.drvPath`); on nix 2.4+ a bare `.drv` installable
    /// makes `--print-out-paths` print the `.drv` path itself, NOT the built
    /// output — the daemon would then copy and activate the `.drv` and
    /// activation could never succeed (Unit C live e2e on Prometheus). The
    /// `^*` selector resolves the derivation to all its outputs.
    fn output_installable(closure_path: &str) -> String {
        format!("{closure_path}^*")
    }

    /// The `--option extra-substituters / extra-trusted-public-keys` arguments
    /// for the deploy's extra substituters (audit C2 — `NixBuildCommand`
    /// carries them but the build previously ignored them, so it could not pull
    /// from the configured cache). Empty when there are none.
    fn substituter_options(substituters: &[nexus::ExtraSubstituter]) -> Vec<String> {
        if substituters.is_empty() {
            return Vec::new();
        }
        let urls = substituters
            .iter()
            .map(|substituter| substituter.url.clone())
            .collect::<Vec<_>>()
            .join(" ");
        let public_keys = substituters
            .iter()
            .map(|substituter| substituter.public_key.clone())
            .collect::<Vec<_>>()
            .join(" ");
        vec![
            "--option".to_string(),
            "extra-substituters".to_string(),
            urls,
            "--option".to_string(),
            "extra-trusted-public-keys".to_string(),
            public_keys,
        ]
    }

    fn override_input_options(overrides: &[nexus::FlakeInputOverride]) -> Vec<String> {
        let mut arguments = Vec::new();
        for override_input in overrides {
            arguments.push("--override-input".to_string());
            arguments.push(override_input.string.clone());
            arguments.push(format!(
                "{}?narHash={}",
                override_input.flake_input_reference.url,
                override_input.flake_input_reference.nix_archive_hash
            ));
        }
        arguments
    }

    fn collect_garbage(node_name: &str) -> Self {
        Self::new(
            "ssh",
            vec![node_name.to_string(), "nix-store --gc".to_string()],
        )
    }

    /// Run the command to its own reported completion. Effect completion is
    /// owned by the command's exit status; elapsed wall time never converts an
    /// active Nix, SSH, or activation effect into a deployment failure.
    async fn run(&self, execution: &EffectExecution) -> std::result::Result<String, String> {
        let mut command = Command::new(execution.program(&self.program));
        command
            .args(&self.arguments)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        let mut child = command
            .spawn()
            .map_err(|error| format!("failed to spawn session for {}: {error}", self.program))?;
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| format!("spawned {} without a stdout pipe", self.program))?;
        let stderr = child
            .stderr
            .take()
            .ok_or_else(|| format!("spawned {} without a stderr pipe", self.program))?;
        let stdout_task = tokio::spawn(async move {
            let mut bytes = Vec::new();
            let mut stdout = stdout;
            stdout.read_to_end(&mut bytes).await.map(|_| bytes)
        });
        let stderr_task = tokio::spawn(async move {
            let mut bytes = Vec::new();
            let mut stderr = stderr;
            stderr.read_to_end(&mut bytes).await.map(|_| bytes)
        });

        let outcome = match child.wait().await {
            Ok(status) => Ok(status),
            Err(error) => Err(format!(
                "failed while waiting for {}: {error}",
                self.program
            )),
        };
        let stdout = stdout_task
            .await
            .map_err(|error| format!("failed to join {} stdout reader: {error}", self.program))?
            .map_err(|error| format!("failed to read {} stdout: {error}", self.program))?;
        let stderr = stderr_task
            .await
            .map_err(|error| format!("failed to join {} stderr reader: {error}", self.program))?
            .map_err(|error| format!("failed to read {} stderr: {error}", self.program))?;
        let status = outcome?;
        if status.success() {
            Ok(String::from_utf8_lossy(&stdout).into_owned())
        } else {
            Err(format!(
                "{} {} exited with {}: {}",
                self.program,
                self.arguments.join(" "),
                status,
                String::from_utf8_lossy(&stderr).trim()
            ))
        }
    }

    fn first_line(output: &str) -> String {
        output.lines().next().unwrap_or("").trim().to_string()
    }

    fn first_line_or(output: &str, fallback: &str) -> String {
        let line = Self::first_line(output);
        if line.is_empty() {
            fallback.to_string()
        } else {
            line
        }
    }

    fn count_lines(output: &str) -> u64 {
        output
            .lines()
            .filter(|line| !line.trim().is_empty())
            .count() as u64
    }

    /// The program name. Test-only inspection of the constructed invocation
    /// (the on-node execution is proven at S5).
    #[cfg(test)]
    fn program(&self) -> &str {
        &self.program
    }

    /// The arguments joined by single spaces — a flat view of the constructed
    /// argv for inspection (the remote-command body is the final argument).
    /// Test-only.
    #[cfg(test)]
    fn joined_arguments(&self) -> String {
        self.arguments.join(" ")
    }
}

impl nexus::NexusEngine for SchemaRuntime {
    async fn apply_sema_write(
        &mut self,
        _origin_route: nexus::OriginRoute,
        input: sema::SemaWriteInput,
    ) -> sema::SemaWriteOutput {
        self.apply_sema(input)
    }

    async fn observe_sema_read(
        &mut self,
        _origin_route: nexus::OriginRoute,
        input: sema::SemaReadInput,
    ) -> sema::SemaReadOutput {
        self.observe_sema(input)
    }

    async fn run_effect(&mut self, input: nexus::EffectCommand) -> nexus::EffectResult {
        match input {
            nexus::EffectCommand::ResolveFlakeAuth(request) => {
                self.resolve_flake_auth(request).await
            }
            nexus::EffectCommand::MaterializeHorizon(command) => {
                self.run_horizon_materialization(command).await
            }
            nexus::EffectCommand::NixEval(command) => self.run_nix_eval(command).await,
            nexus::EffectCommand::NixBuild(command) => self.run_nix_build(command).await,
            nexus::EffectCommand::CopyClosure(command) => self.run_copy_closure(command).await,
            nexus::EffectCommand::ActivateGeneration(command) => {
                self.run_activate_generation(command).await
            }
            nexus::EffectCommand::PathInfoGc(command) => self.run_path_info_gc(command).await,
            nexus::EffectCommand::HermeticCheck(command) => self.run_hermetic_check(command).await,
            nexus::EffectCommand::BringUpTestVm(command) => {
                self.run_bring_up_test_vm(command).await
            }
            nexus::EffectCommand::TearDownTestVm(command) => {
                self.run_tear_down_test_vm(command).await
            }
        }
    }

    fn budget_exhausted_reply(
        &self,
        _exhausted: triad_runtime::ContinuationExhausted,
    ) -> nexus::SignalOutput {
        nexus::SignalOutput::MetaOutput(meta::Output::DeployRejected(meta::DeployRejected::new(
            self.deploy_rejection(meta::DeployRejectionReason::DeploymentInFlight),
        )))
    }

    fn decide(
        &mut self,
        input: nexus::nexus::Nexus<nexus::nexus::Work>,
    ) -> nexus::nexus::Nexus<nexus::nexus::Action> {
        let origin_route = input.origin_route();
        let action = match input.into_root() {
            nexus::NexusWork::SignalArrived(input) => self.decide_signal_arrival(input),
            nexus::NexusWork::SemaReadCompleted(output) => self.decide_read_completion(output),
            nexus::NexusWork::SemaWriteCompleted(output) => self.decide_write_completion(output),
            nexus::NexusWork::EffectCompleted(result) => self.decide_effect_completion(result),
        };
        action.with_origin_route(origin_route)
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

    const STORE: &str = "/nix/store/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-toplevel";

    fn host_submission(action: ordinary::HostDeployAction) -> sema::DeploySubmission {
        sema::DeploySubmission::Host(meta::HostDeployment {
            cluster_name: ordinary::ClusterName::new("alpha"),
            node_name: ordinary::NodeName::new("node-1"),
            host_composition: ordinary::HostComposition::BaseHost,
            proposal_source: ordinary::ProposalSource::new("/dev/null"),
            flake_reference: ordinary::FlakeReference::new("github:owner/repo"),
            deployment_transport: fixture_transport(),
            deployment_input_mode: sema::DeploymentInputMode::Direct,
            deployment_output_selector: fixture_output_selector(),
            activation_backend: sema::ActivationBackend::NixosSystemdBootV1,
            host_deploy_action: action,
            source_revision_policy: meta::SourceRevisionPolicy::ResolveAndRecord,
            optional_nix_builder_spec: None,
            extra_substituter_vector: Vec::new(),
        })
    }

    fn host_pipeline(action: ordinary::HostDeployAction) -> DeployPipeline {
        DeployPipeline::from_submission(
            ordinary::DeploymentIdentifier::new(1),
            ordinary::GenerationIdentifier::new(1),
            SchemaRuntime::marker(0),
            host_submission(action),
        )
    }

    fn user_submission() -> sema::DeploySubmission {
        sema::DeploySubmission::UserEnvironment(meta::UserEnvironmentDeployment {
            cluster_name: ordinary::ClusterName::new("fixture-cluster"),
            node_name: ordinary::NodeName::new("fixture-daemon"),
            user_name: ordinary::UserName::new("fixture-user"),
            proposal_source: ordinary::ProposalSource::new("/dev/null"),
            flake_reference: ordinary::FlakeReference::new("github:owner/repo"),
            deployment_transport: fixture_transport(),
            deployment_input_mode: sema::DeploymentInputMode::Direct,
            deployment_output_selector: fixture_output_selector(),
            activation_backend: sema::ActivationBackend::HomeManagerNixProfileV1,
            user_environment_action: meta::UserEnvironmentAction::ActivateNow,
            source_revision_policy: meta::SourceRevisionPolicy::ResolveAndRecord,
            optional_nix_builder_spec: None,
            extra_substituter_vector: Vec::new(),
        })
    }

    fn source_revision(policy: ordinary::SourceRevisionPolicy) -> ordinary::SourceRevisionRecord {
        ordinary::SourceRevisionRecord {
            source_revision_policy: policy,
            requested_ref: ordinary::FlakeReference::new("github:owner/repo/main"),
            resolved_ref: ordinary::FlakeReference::new(
                "github:owner/repo?rev=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            ),
            string: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_string(),
        }
    }

    fn resolved_flake(policy: ordinary::SourceRevisionPolicy) -> nexus::ResolvedFlake {
        nexus::ResolvedFlake::new(source_revision(policy))
    }

    fn cluster() -> ordinary::ClusterName {
        ordinary::ClusterName::new("alpha")
    }

    fn node() -> ordinary::NodeName {
        ordinary::NodeName::new("node-1")
    }

    fn fixture_transport() -> sema::DeploymentTransport {
        sema::DeploymentTransport {
            nix_store_uri: sema::NixStoreUri::new("ssh-ng://fixture-copy.invalid"),
            ssh_destination: sema::SshDestination::new("fixture-login@fixture-activate.invalid"),
        }
    }

    fn fixture_output_selector() -> sema::DeploymentOutputSelector {
        sema::DeploymentOutputSelector::new(sema::FlakeAttribute::new("checks.fixture-a"))
    }

    fn fixture_test_profile() -> nexus::TestExecutionProfile {
        nexus::TestExecutionProfile {
            test_mode: ordinary::TestMode::Hermetic,
            nix_system: nexus::NixSystem::new("x86_64-linux"),
            deployment_output_selector: nexus::DeploymentOutputSelector::new(
                nexus::FlakeAttribute::new("checks.fixture-a"),
            ),
            optional_deployment_transport: None,
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
        let built = ordinary::ClosurePath::new(STORE);
        assert!(engine.set_closure_path(built.clone()));
        engine.set_stage(DeployStage::BuildingRecorded);

        match engine.advance_after_phase() {
            nexus::NexusAction::CommandEffect(nexus::EffectCommand::ActivateGeneration(
                command,
            )) => {
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
            sema::SemaWriteOutput::DeploySubmitted(_)
        ));
        engine.set_stage(DeployStage::BuildingRecorded);

        match engine.advance_after_phase() {
            nexus::NexusAction::ReplyToSignal(nexus::SignalOutput::MetaOutput(
                meta::Output::DeployTerminal(_record),
            )) => {}
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
        pipeline.closure_path = Some(ordinary::ClosurePath::new(STORE));
        let commit = pipeline.activation_commit().expect("commit with closure");
        assert_eq!(commit.closure_path.payload(), STORE);
    }

    #[test]
    fn activation_commit_persists_computed_boot_profile_slot() {
        let mut pipeline = host_pipeline(ordinary::HostDeployAction::SetBootProfile);
        pipeline.closure_path = Some(ordinary::ClosurePath::new(STORE));
        pipeline.source_revision = Some(source_revision(
            ordinary::SourceRevisionPolicy::ResolveAndRecord,
        ));
        pipeline.activation_slot = Some(ordinary::GenerationSlot::BootPending);
        let commit = pipeline.activation_commit().expect("commit");
        assert_eq!(
            commit.generation_slot,
            ordinary::GenerationSlot::BootPending
        );
    }

    #[test]
    fn activation_commit_persists_computed_boot_once_slot() {
        let mut pipeline = host_pipeline(ordinary::HostDeployAction::ScheduleBootOnce);
        pipeline.closure_path = Some(ordinary::ClosurePath::new(STORE));
        pipeline.source_revision = Some(source_revision(
            ordinary::SourceRevisionPolicy::ResolveAndRecord,
        ));
        pipeline.activation_slot = Some(ordinary::GenerationSlot::BootPending);
        let commit = pipeline.activation_commit().expect("commit");
        assert_eq!(
            commit.generation_slot,
            ordinary::GenerationSlot::BootPending
        );
    }

    #[test]
    fn activation_commit_persists_computed_test_activation_slot() {
        let mut pipeline = host_pipeline(ordinary::HostDeployAction::TestActivation);
        pipeline.closure_path = Some(ordinary::ClosurePath::new(STORE));
        pipeline.source_revision = Some(source_revision(
            ordinary::SourceRevisionPolicy::ResolveAndRecord,
        ));
        pipeline.activation_slot = Some(ordinary::GenerationSlot::Recent);
        let commit = pipeline.activation_commit().expect("commit");
        assert_eq!(commit.generation_slot, ordinary::GenerationSlot::Recent);
    }

    // ---- Step 2: the reject-guard opens the activating actions ----

    fn require_immutable_request(flake: &str) -> meta::DeployRequest {
        deployment_request(meta::SourceRevisionPolicy::RequireImmutable, flake)
    }

    fn deployment_request(policy: meta::SourceRevisionPolicy, flake: &str) -> meta::DeployRequest {
        meta::DeployRequest::Host(meta::HostDeployment {
            cluster_name: cluster(),
            node_name: node(),
            host_composition: ordinary::HostComposition::BaseHost,
            proposal_source: ordinary::ProposalSource::new("/dev/null"),
            flake_reference: ordinary::FlakeReference::new(flake),
            deployment_transport: fixture_transport(),
            deployment_input_mode: sema::DeploymentInputMode::Direct,
            deployment_output_selector: fixture_output_selector(),
            activation_backend: sema::ActivationBackend::NixosSystemdBootV1,
            host_deploy_action: ordinary::HostDeployAction::Evaluate,
            source_revision_policy: policy,
            optional_nix_builder_spec: None,
            extra_substituter_vector: Vec::new(),
        })
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
                SchemaRuntime::source_revision_policy_rejection(&deployment_request(policy, flake));
            assert_eq!(rejected.is_none(), accepted, "{policy:?}: {flake}");
        }
    }

    #[test]
    fn admission_rejects_invalid_request_owned_routing_before_snapshot_persistence() {
        let mut request = deployment_request(
            meta::SourceRevisionPolicy::ResolveAndRecord,
            "github:owner/repo",
        );
        let meta::DeployRequest::Host(deployment) = &mut request else {
            unreachable!("fixture is a host deploy")
        };
        deployment.deployment_transport.ssh_destination = sema::SshDestination::new("root");
        assert_eq!(
            SchemaRuntime::deployment_routing_rejection(&request),
            Some(meta::DeployRejectionReason::InvalidDeploymentRouting)
        );
        let mut runtime = SchemaRuntime::new();
        let outcome = runtime.submit_deploy(match request {
            meta::DeployRequest::Host(deployment) => sema::DeploySubmission::Host(deployment),
            meta::DeployRequest::UserEnvironment(deployment) => {
                sema::DeploySubmission::UserEnvironment(deployment)
            }
        });
        let DeploySubmissionOutcome::Rejected(rejected) = outcome else {
            panic!("invalid routing must reject")
        };
        assert!(matches!(
            rejected.into_payload().optional_deployment_terminal,
            Some(sema::DeploymentTerminal::Rejected(
                sema::DeploymentTerminalReason::InvalidDeploymentRouting
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
                    SchemaRuntime::source_revision_policy_rejection(&deployment_request(
                        policy, flake,
                    )),
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
            SchemaRuntime::source_revision_policy_rejection(&request),
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
                SchemaRuntime::source_revision_policy_rejection(&request),
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
                SchemaRuntime::source_revision_policy_rejection(&request),
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
        assert!(SchemaRuntime::source_revision_policy_rejection(&request).is_none());
    }

    #[test]
    fn proposal_source_is_the_safe_canonical_artifact_before_deploy_admission() {
        use std::os::unix::fs::{PermissionsExt, symlink};

        let directory = tempfile::tempdir().expect("temporary proposal directory");
        let malformed = directory.path().join("malformed.datom");
        fs::write(&malformed, "not a cluster proposal").expect("write malformed proposal");
        let unreadable = directory.path().join("unreadable.datom");
        fs::write(&unreadable, "not used").expect("write unreadable proposal");
        let mut permissions = fs::metadata(&unreadable)
            .expect("unreadable proposal metadata")
            .permissions();
        permissions.set_mode(0o000);
        fs::set_permissions(&unreadable, permissions).expect("make proposal unreadable");
        let symlink_path = directory.path().join("linked.datom");
        symlink(&malformed, &symlink_path).expect("create proposal symlink");

        for source in [
            directory.path().join("missing.datom"),
            directory.path().join("proposal.nota"),
            directory.path().join("proposal.dotos"),
            directory.path().join("proposal.datomic"),
            malformed,
            unreadable,
            symlink_path,
            directory.path().join("private-secret.datom"),
            directory
                .path()
                .join("nested")
                .join("..")
                .join("proposal.datom"),
        ] {
            let source = ordinary::ProposalSource::new(source.to_string_lossy().to_string());
            assert!(
                ProposalFile::available(&source).is_none(),
                "unsafe or unavailable proposal source must fail admission"
            );
        }
        let control = ordinary::ProposalSource::new("/tmp/proposal\n.datom");
        assert!(ProposalFile::available(&control).is_none());
    }

    #[test]
    fn unavailable_proposal_source_returns_a_safe_correlated_rejection_before_effects() {
        let mut engine = SchemaRuntime::new();
        let request = deployment_request(
            meta::SourceRevisionPolicy::ResolveAndRecord,
            "github:owner/repo",
        );
        match engine.submit_deploy(request) {
            DeploySubmissionOutcome::Rejected(record) => {
                assert!(matches!(
                    record.into_payload().optional_deployment_terminal,
                    Some(sema::DeploymentTerminal::Rejected(
                        sema::DeploymentTerminalReason::ProposalSourceUnreachable
                    ))
                ));
            }
            other => panic!("unavailable proposal must reject before effects, got {other:?}"),
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
            sema::DeployResumeStage::RecordBuilding,
        ));
        let pipeline = engine.active_deploy.as_ref().expect("pipeline");
        assert_eq!(
            pipeline.flake,
            source_revision(ordinary::SourceRevisionPolicy::ResolveAndRecord).resolved_ref
        );
        assert_eq!(
            pipeline
                .source_revision
                .as_ref()
                .expect("source revision")
                .requested_ref,
            ordinary::FlakeReference::new("github:owner/repo/main")
        );
        let command = pipeline.nix_eval_command();
        assert_eq!(
            command.source_revision_record.string,
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        );
        assert_eq!(
            command.flake_reference,
            command.source_revision_record.resolved_ref
        );
    }

    #[test]
    fn unproven_resolver_metadata_terminalizes_before_any_later_effect() {
        let mut engine = SchemaRuntime::new();
        assert!(matches!(
            engine.record_deploy_submitted(host_submission(ordinary::HostDeployAction::Evaluate)),
            sema::SemaWriteOutput::DeploySubmitted(_)
        ));
        let mut source = source_revision(ordinary::SourceRevisionPolicy::ResolveAndRecord);
        source.string = "sha256-unproven-nar-hash".to_string();
        match engine.decide_effect_completion(nexus::EffectResult::FlakeResolved(
            nexus::ResolvedFlake::new(source),
        )) {
            nexus::NexusAction::ReplyToSignal(nexus::SignalOutput::MetaOutput(
                meta::Output::DeployTerminal(record),
            )) => {
                assert!(matches!(
                    record.optional_deployment_terminal,
                    Some(sema::DeploymentTerminal::Failed(sema::DeploymentFailure {
                        deployment_failure_stage: sema::DeploymentFailureStage::FlakeAuth,
                        ..
                    }))
                ));
            }
            other => panic!("unproven metadata must terminalize, got {other:?}"),
        }
        assert!(engine.active_deploy.is_none());
    }

    #[test]
    fn injected_closure_effects_terminalize_before_build_copy_or_activation() {
        let unsafe_path = ordinary::ClosurePath::new(
            "/nix/store/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-private-secret",
        );

        let mut evaluated = SchemaRuntime::new();
        assert!(matches!(
            evaluated.record_deploy_submitted(host_submission(ordinary::HostDeployAction::Realize)),
            sema::SemaWriteOutput::DeploySubmitted(_)
        ));
        match evaluated.decide_effect_completion(nexus::EffectResult::ClosureEvaluated(
            nexus::EvaluatedClosure {
                generation_identifier: ordinary::GenerationIdentifier::new(1),
                closure_path: unsafe_path.clone(),
            },
        )) {
            nexus::NexusAction::ReplyToSignal(nexus::SignalOutput::MetaOutput(
                meta::Output::DeployTerminal(record),
            )) => assert!(matches!(
                record.optional_deployment_terminal,
                Some(sema::DeploymentTerminal::Failed(sema::DeploymentFailure {
                    deployment_failure_stage: sema::DeploymentFailureStage::Eval,
                    ..
                }))
            )),
            other => panic!("unsafe evaluated closure must terminalize, got {other:?}"),
        }
        assert!(evaluated.active_deploy.is_none());

        let mut built = SchemaRuntime::new();
        assert!(matches!(
            built.record_deploy_submitted(host_submission(ordinary::HostDeployAction::ActivateNow)),
            sema::SemaWriteOutput::DeploySubmitted(_)
        ));
        match built.decide_effect_completion(nexus::EffectResult::ClosureBuilt(
            nexus::BuiltClosure {
                generation_identifier: ordinary::GenerationIdentifier::new(1),
                closure_path: unsafe_path,
            },
        )) {
            nexus::NexusAction::ReplyToSignal(nexus::SignalOutput::MetaOutput(
                meta::Output::DeployTerminal(record),
            )) => assert!(matches!(
                record.optional_deployment_terminal,
                Some(sema::DeploymentTerminal::Failed(sema::DeploymentFailure {
                    deployment_failure_stage: sema::DeploymentFailureStage::Build,
                    ..
                }))
            )),
            other => panic!("unsafe built closure must terminalize, got {other:?}"),
        }
        assert!(built.active_deploy.is_none());
    }

    #[test]
    fn phase_projection_never_labels_nix_metadata_as_an_immutable_revision() {
        let mut pipeline = host_pipeline(ordinary::HostDeployAction::Evaluate);
        let mut source = source_revision(ordinary::SourceRevisionPolicy::ResolveAndRecord);
        source.string = "sha256-unproven-nar-hash".to_string();
        pipeline.source_revision = Some(source);
        assert!(
            pipeline
                .phase_event(
                    ordinary::DeploymentPhase::Building,
                    ordinary::EventLogPosition::new(7),
                    None,
                )
                .optional_immutable_revision
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
        let store = Arc::new(Store::open(directory.path().join("lojix.sema")).expect("open store"));
        let engine = SchemaRuntime::with_store_and_configuration(store, configuration.clone());
        let mut pipeline = host_pipeline(ordinary::HostDeployAction::Evaluate);
        pipeline.source_revision = Some(source_revision(
            ordinary::SourceRevisionPolicy::ResolveAndRecord,
        ));
        let eval = pipeline.nix_eval_command();
        let build = nexus::NixBuildCommand {
            generation_identifier: ordinary::GenerationIdentifier::new(1),
            closure_path: ordinary::ClosurePath::new(STORE),
            build_target: nexus::BuildTarget::Local,
            extra_substituter_vector: Vec::new(),
        };
        let hermetic = nexus::HermeticCheckCommand {
            cluster_name: cluster(),
            node_name: node(),
            flake_reference: ordinary::FlakeReference::new("github:owner/repo"),
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
                nexus::EffectResult::EffectFailed(failure) => {
                    assert_eq!(failure.effect_stage, stage);
                    assert!(!failure.string.contains(unsafe_output));
                }
                other => panic!("unsafe output must fail at {stage:?}, got {other:?}"),
            }
        }
    }

    #[test]
    fn crash_after_atomic_resolved_source_commit_reopens_at_materialization_without_resolving_again()
     {
        let directory = tempfile::tempdir().expect("temporary store directory");
        let path = directory.path().join("lojix.sema");
        let store = Arc::new(Store::open(&path).expect("open store"));
        let mut before_crash = SchemaRuntime::with_store(store.clone());
        let accepted = match before_crash
            .record_deploy_submitted(host_submission(ordinary::HostDeployAction::Evaluate))
        {
            sema::SemaWriteOutput::DeploySubmitted(accepted) => accepted,
            other => panic!("expected accepted deploy, got {other:?}"),
        };
        before_crash.set_resolved_flake(
            resolved_flake(ordinary::SourceRevisionPolicy::ResolveAndRecord),
            sema::DeployResumeStage::MaterializeHorizon,
        );
        before_crash.set_input_overrides(vec![nexus::FlakeInputOverride {
            string: "criomos".to_string(),
            flake_input_reference: nexus::FlakeInputReference {
                url: "path:/var/lib/lojix/inputs/criomos".to_string(),
                nix_archive_hash: "sha256-immutable-horizon-snapshot".to_string(),
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
            sema::DeployResumeStage::RecordBuilding
        );
        assert_eq!(
            persisted.optional_flake_reference,
            Some(source_revision(ordinary::SourceRevisionPolicy::ResolveAndRecord).resolved_ref)
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
        assert_eq!(pipeline.accepted_marker, accepted.state_marker);
        assert_eq!(
            pipeline.resume_stage,
            sema::DeployResumeStage::RecordBuilding
        );
        assert_eq!(
            pipeline.flake,
            source_revision(ordinary::SourceRevisionPolicy::ResolveAndRecord).resolved_ref
        );
        assert_eq!(
            pipeline
                .source_revision
                .as_ref()
                .expect("exact durable source revision")
                .string,
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        );
        assert_eq!(pipeline.input_overrides.len(), 1);
        assert_eq!(pipeline.input_overrides[0].string, "criomos");
        assert_eq!(
            pipeline.input_overrides[0]
                .flake_input_reference
                .nix_archive_hash,
            "sha256-immutable-horizon-snapshot"
        );
    }

    #[test]
    fn restart_restores_or_recovers_the_exact_phase_receipt_for_each_resume_continuation() {
        let directory = tempfile::tempdir().expect("temporary store directory");
        let path = directory.path().join("lojix.sema");
        let store = Arc::new(Store::open(&path).expect("open store"));
        let mut engine = SchemaRuntime::with_store(store.clone());
        assert!(matches!(
            engine
                .record_deploy_submitted(host_submission(ordinary::HostDeployAction::ActivateNow)),
            sema::SemaWriteOutput::DeploySubmitted(_)
        ));
        engine.set_resolved_flake(
            resolved_flake(ordinary::SourceRevisionPolicy::ResolveAndRecord),
            sema::DeployResumeStage::RecordBuilding,
        );
        assert!(engine.set_closure_path(ordinary::ClosurePath::new(STORE)));
        engine.set_activation_slot(ordinary::GenerationSlot::Current);

        for (phase, resume_stage) in [
            (
                ordinary::DeploymentPhase::Building,
                sema::DeployResumeStage::NixEval,
            ),
            (
                ordinary::DeploymentPhase::Copying,
                sema::DeployResumeStage::ActivateGeneration,
            ),
            (
                ordinary::DeploymentPhase::Activated,
                sema::DeployResumeStage::RecordGenerationActivated,
            ),
        ] {
            let position = store
                .allocate_event_log_position()
                .expect("reserve event position");
            let event = {
                let pipeline = engine.active_deploy.as_mut().expect("pipeline");
                pipeline.resume_stage = resume_stage;
                pipeline.phase_event(phase, ordinary::EventLogPosition::new(position), None)
            };
            let receipt = match engine.record_phase_transition(event) {
                sema::SemaWriteOutput::PhaseRecorded(receipt) => receipt,
                other => panic!("expected phase receipt, got {other:?}"),
            };
            let mut job = store
                .deploy_jobs()
                .expect("job row")
                .into_iter()
                .next()
                .expect("one job");
            assert_eq!(job.optional_phase_receipt, Some(receipt.clone()));

            // Simulate a crash after public event append but before the
            // receipt-side job rewrite. Reopen reconstructs the SAME receipt
            // from the durable event, not a synthetic value.
            job.optional_phase_receipt = None;
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
            sema::SemaWriteOutput::DeploySubmitted(_)
        ));
        let mut pipeline = engine.active_deploy.clone().expect("accepted pipeline");
        let source_revision = source_revision(ordinary::SourceRevisionPolicy::ResolveAndRecord);
        pipeline.source_revision = Some(source_revision.clone());
        pipeline.activation_slot = Some(ordinary::GenerationSlot::Current);
        pipeline.closure_path = Some(ordinary::ClosurePath::new(STORE));
        engine.active_deploy = Some(pipeline.clone());

        let event = pipeline.phase_event(
            ordinary::DeploymentPhase::Building,
            ordinary::EventLogPosition::new(1),
            None,
        );
        assert_eq!(event.deployment_identifier, pipeline.deployment_identifier);
        assert!(matches!(
            engine.record_phase_transition(event),
            sema::SemaWriteOutput::PhaseRecorded(_)
        ));
        match engine.read_event_log(ordinary::EventLogRange {
            from: ordinary::EventLogPosition::new(1),
            until: ordinary::EventLogPosition::new(2),
        }) {
            sema::SemaReadOutput::EventLogRead(page) => assert_eq!(
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
            sema::SemaWriteOutput::GenerationActivated(_)
        ));
        match engine.query_generations(ordinary::Selection::ByNode(ordinary::NodeSelector {
            cluster_name: cluster(),
            node_name: node(),
            optional_generation_artifact: None,
        })) {
            sema::SemaReadOutput::GenerationsQueried(listing) => assert_eq!(
                listing
                    .generation_vector
                    .first()
                    .expect("generation")
                    .closure_path,
                ordinary::ClosurePath::new(STORE)
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
            let request = meta::DeployRequest::Host(meta::HostDeployment {
                cluster_name: cluster(),
                node_name: node(),
                host_composition: ordinary::HostComposition::BaseHost,
                proposal_source: ordinary::ProposalSource::new("/dev/null"),
                flake_reference: ordinary::FlakeReference::new("github:owner/repo"),
                deployment_transport: fixture_transport(),
                deployment_input_mode: sema::DeploymentInputMode::Direct,
                deployment_output_selector: fixture_output_selector(),
                activation_backend: sema::ActivationBackend::NixosSystemdBootV1,
                host_deploy_action: action,
                source_revision_policy: meta::SourceRevisionPolicy::ResolveAndRecord,
                optional_nix_builder_spec: None,
                extra_substituter_vector: Vec::new(),
            });
            assert!(
                SchemaRuntime::unsupported_deploy_reason(&request).is_none(),
                "Host {action:?} should be supported"
            );
        }
        for mode in [
            meta::UserEnvironmentAction::Realize,
            meta::UserEnvironmentAction::SetProfile,
            meta::UserEnvironmentAction::ActivateNow,
        ] {
            let request = meta::DeployRequest::UserEnvironment(meta::UserEnvironmentDeployment {
                cluster_name: cluster(),
                node_name: node(),
                user_name: ordinary::UserName::new("li"),
                proposal_source: ordinary::ProposalSource::new("/dev/null"),
                flake_reference: ordinary::FlakeReference::new("github:owner/repo"),
                deployment_transport: fixture_transport(),
                deployment_input_mode: sema::DeploymentInputMode::Direct,
                deployment_output_selector: fixture_output_selector(),
                activation_backend: sema::ActivationBackend::HomeManagerNixProfileV1,
                user_environment_action: mode,
                source_revision_policy: meta::SourceRevisionPolicy::ResolveAndRecord,
                optional_nix_builder_spec: None,
                extra_substituter_vector: Vec::new(),
            });
            assert!(
                SchemaRuntime::unsupported_deploy_reason(&request).is_none(),
                "User environment {mode:?} should be supported"
            );
        }
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
        let invocation = NixCommand::build_closure(DERIVATION, &[]);
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
        let invocation = NixCommand::build_closure_remote(DERIVATION, builder_spec, &[]);
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
            nix_store_uri: nexus::NixStoreUri::new("ssh-ng://fixture-copy.invalid"),
            ssh_destination: nexus::SshDestination::new("fixture-login@fixture-activate.invalid"),
        }
    }

    fn copy_command() -> nexus::CopyClosureCommand {
        nexus::CopyClosureCommand {
            generation_identifier: ordinary::GenerationIdentifier::new(1),
            node_name: node(),
            deployment_transport: nexus_fixture_transport(),
            closure_path: ordinary::ClosurePath::new(STORE),
        }
    }

    // ---- Step 3: copy argv — always --substitute-on-destination, target only ----

    #[test]
    fn copy_from_dispatcher_uses_to_only_with_substitute() {
        let copy = ClosureCopy::from_command(&copy_command())
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
    fn copy_uses_exact_nix_store_uri_without_rewriting_it() {
        let mut command = copy_command();
        command.deployment_transport.nix_store_uri =
            nexus::NixStoreUri::new("ssh-ng://other-copy.invalid:2222?compress=true");
        let copy = ClosureCopy::from_command(&command).expect("copy transport");
        let invocation = copy.invocation();
        let argv = invocation.joined_arguments();
        assert!(argv.contains("--substitute-on-destination"), "{argv}");
        assert!(!argv.contains("--from"), "{argv}");
        assert!(
            argv.contains("--to ssh-ng://other-copy.invalid:2222?compress=true"),
            "{argv}"
        );
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
            closure_path: ordinary::ClosurePath::new(STORE),
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
            activation.runs_detached_self_switch(),
            "self-host Switch must take the detached shape"
        );
        let invocation = activation.detached_invocation(
            &activation.self_switch_unit_name(),
            activation.self_switch_script(),
        );
        assert_eq!(invocation.program(), "ssh");
        let argv = invocation.joined_arguments();
        assert!(argv.contains("systemd-run"), "PID-1-owned unit: {argv}");
        assert!(argv.contains("--service-type=oneshot"), "{argv}");
        assert!(argv.contains("--wait"), "{argv}");
        assert!(
            argv.contains("--unit=lojix-self-switch-deploy-7"),
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

    fn interrupted_self_switch_job(closure: Option<&str>) -> sema::DeployJob {
        sema::DeployJob {
            deployment_identifier: ordinary::DeploymentIdentifier::new(72),
            generation_identifier: ordinary::GenerationIdentifier::new(72),
            cluster_name: ordinary::ClusterName::new("fixture-cluster"),
            node_name: ordinary::NodeName::new("fixture-daemon"),
            deploy_job_phase: sema::DeployJobPhase::Activating,
            optional_closure_path: closure.map(ordinary::ClosurePath::new),
            source_revision_policy: ordinary::SourceRevisionPolicy::RequireImmutable,
            flake_reference: ordinary::FlakeReference::new(
                "github:LiGoldragon/CriomOS?rev=0123456789abcdef0123456789abcdef01234567",
            ),
            optional_flake_reference: Some(ordinary::FlakeReference::new(
                "github:LiGoldragon/CriomOS?rev=0123456789abcdef0123456789abcdef01234567",
            )),
            resolved_revision: Some("0123456789abcdef0123456789abcdef01234567".to_string()),
            deployment_transport: fixture_transport(),
            deployment_input_mode: sema::DeploymentInputMode::Direct,
            deployment_output_selector: fixture_output_selector(),
            activation_backend: sema::ActivationBackend::NixosSystemdBootV1,
            optional_nix_builder_spec: None,
            boot_once_unit: None,
            optional_generation_slot: None,
            persisted_flake_input_override_vector: Vec::new(),
            deploy_resume_stage: sema::DeployResumeStage::ResolveFlakeAuth,
            optional_phase_receipt: None,
            optional_deploy_submission: Some(host_submission(
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
        assert_eq!(
            generation.generation_artifact,
            ordinary::GenerationArtifact::CompleteHost
        );
        assert_eq!(
            generation.closure_path.payload(),
            "/nix/store/aaaaaaaa-system"
        );
        assert_eq!(
            generation.source_revision_record.string,
            "0123456789abcdef0123456789abcdef01234567"
        );
        assert_eq!(root.generation_slot, ordinary::GenerationSlot::Current);
        assert_eq!(root.closure_path.payload(), "/nix/store/aaaaaaaa-system");
        assert_eq!(root.optional_pin_label, None);
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
        job.optional_deploy_submission =
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
        job.optional_deploy_submission = Some(user_submission());
        assert!(
            job.self_switch_activation_record(Some("/nix/store/aaaaaaaa-system"))
                .is_none()
        );
    }

    #[test]
    fn matching_profile_without_the_systemd_boot_backend_is_not_recovered() {
        let mut job = interrupted_self_switch_job(Some("/nix/store/aaaaaaaa-system"));
        job.activation_backend = sema::ActivationBackend::HomeManagerNixProfileV1;
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
            Some(ordinary::NodeName::new("some-other-node")),
        );
        assert!(!foreign.runs_detached_self_switch());
        let no_context = host_activation(ordinary::HostDeployAction::ActivateNow);
        assert!(!no_context.runs_detached_self_switch());
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
        assert!(!boot.runs_detached_self_switch());
        let boot_once =
            host_activation_on_host(ordinary::HostDeployAction::ScheduleBootOnce, Some(node()));
        assert!(!boot_once.runs_detached_self_switch());
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

    // ---- local owner transport: selection of the build context ----

    #[test]
    fn build_on_a_different_target_stays_in_the_local_owner_context() {
        // Remote activation targets now evaluate/build locally and copy their
        // exact immutable output afterwards; no eval or build targets ssh-ng.
        let pipeline = host_pipeline(ordinary::HostDeployAction::ScheduleBootOnce);
        assert!(matches!(pipeline.build_target(), nexus::BuildTarget::Local));
    }

    #[test]
    fn build_on_the_daemon_host_stays_local() {
        // Deploying the daemon's own host also stays local. The same transport
        // invariant applies to every target, including the daemon host.
        let pipeline = host_pipeline(ordinary::HostDeployAction::ScheduleBootOnce);
        assert!(matches!(pipeline.build_target(), nexus::BuildTarget::Local));
    }

    #[test]
    fn explicit_builder_override_wins_over_the_default_local_builder() {
        // An operator-named builder still dispatches to that Nix builder machine
        // through the local Nix client; only the default has no named builder.
        let mut pipeline = host_pipeline(ordinary::HostDeployAction::ScheduleBootOnce);
        pipeline.builder = Some(sema::NixBuilderSpec::new(
            "ssh-ng://fixture-builder.invalid x86_64-linux - 4 2 k1",
        ));
        assert!(matches!(
            pipeline.build_target(),
            nexus::BuildTarget::Remote(_)
        ));
    }

    #[test]
    fn build_closure_stays_local_and_selects_the_realised_output() {
        let invocation = NixCommand::build_closure(DERIVATION, &[]);
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
    fn materialized_horizon_json_preserves_empty_service_vectors() {
        use std::collections::BTreeMap;

        use horizon_lib::address::{YggAddress, YggSubnet};
        use horizon_lib::domain::DomainConfiguration;
        use horizon_lib::io::Io;
        use horizon_lib::machine::Machine;
        use horizon_lib::magnitude::Magnitude;
        use horizon_lib::name::{ClusterName, NodeName};
        use horizon_lib::proposal::{ClusterTrust, NodeProposal, NodePubKeys, YggPubKeyEntry};
        use horizon_lib::pub_key::{NixPubKey, SshPubKey, YggPubKey};
        use horizon_lib::species::{Arch, Bootloader, Keyboard, MachineSpecies, NodeSpecies};

        let node = |services| NodeProposal {
            species: NodeSpecies::EdgeTesting,
            size: Magnitude::Large,
            trust: Magnitude::Max,
            machine: Machine {
                species: MachineSpecies::Metal,
                arch: Some(Arch::X86_64),
                cores: 4,
                model: None,
                mother_board: None,
                super_node: None,
                super_user: None,
                chip_gen: None,
                ram_gb: None,
                disk_gb: None,
                location: None,
                super_nodes: Vec::new(),
            },
            io: Io {
                keyboard: Keyboard::Qwerty,
                bootloader: Bootloader::Uefi,
                disks: BTreeMap::new(),
                swap_devices: Vec::new(),
                compressed_swap: None,
            },
            pub_keys: NodePubKeys {
                ssh: SshPubKey::try_new("AAA=").expect("valid SSH public key"),
                nix: Some(
                    NixPubKey::try_new("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA")
                        .expect("valid Nix public key"),
                ),
                yggdrasil: Some(YggPubKeyEntry {
                    pub_key: YggPubKey::try_new("a".repeat(64)).expect("valid Yggdrasil key"),
                    address: YggAddress::try_new("200::1").expect("valid Yggdrasil address"),
                    subnet: YggSubnet::try_new("300:ca41:6b12:fba")
                        .expect("valid Yggdrasil subnet"),
                }),
            },
            link_local_ips: Vec::new(),
            node_ip: None,
            wireguard_pub_key: None,
            nordvpn: false,
            wifi_cert: false,
            wireguard_untrusted_proxies: Vec::new(),
            wants_printing: false,
            wants_hw_video_accel: false,
            router_interfaces: None,
            online: None,
            services,
        };
        let mut nodes = BTreeMap::new();
        nodes.insert(
            NodeName::try_new("edge").expect("edge node name"),
            node(vec![]),
        );
        nodes.insert(
            NodeName::try_new("worker").expect("worker node name"),
            node(vec![]),
        );
        let proposal = ClusterProposal {
            nodes,
            users: BTreeMap::new(),
            domains: BTreeMap::new(),
            trust: ClusterTrust {
                cluster: Magnitude::Max,
                clusters: BTreeMap::new(),
                nodes: BTreeMap::new(),
                users: BTreeMap::new(),
            },
            domain_configuration: DomainConfiguration::default(),
        };
        let horizon = proposal
            .project(&Viewpoint {
                cluster: ClusterName::try_new("test-cluster").expect("cluster name"),
                node: NodeName::try_new("edge").expect("edge node name"),
            })
            .expect("project empty service vectors");
        let directory = tempfile::tempdir().expect("materialization directory");
        GeneratedInputDirectory::new(directory.path().join("horizon"))
            .write_horizon(&horizon)
            .expect("write horizon input");
        let horizon_json: serde_json::Value = serde_json::from_slice(
            &fs::read(directory.path().join("horizon/horizon.json")).expect("read horizon JSON"),
        )
        .expect("parse materialized horizon JSON");

        assert_eq!(horizon_json["node"]["services"], serde_json::json!([]));
        assert_eq!(
            horizon_json["exNodes"]["worker"]["services"],
            serde_json::json!([])
        );
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
            nexus::UserEnvironmentMaterialization::new(ordinary::UserName::new("li")),
        );
        assert!(DeploymentInput::from_shape(&user).is_none());
    }

    #[test]
    fn local_eval_never_redirects_over_ssh_ng() {
        let store = nexus::BuildTarget::Local;
        let invocation =
            NixCommand::eval_drv_path(".#toplevel", &[], &store, EvalRefresh::ForceRefresh);
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
        let store = nexus::BuildTarget::Local;
        let invocation =
            NixCommand::eval_drv_path(".#toplevel", &[], &store, EvalRefresh::TrustImmutablePin);
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
            EvalRefresh::for_source(ordinary::SourceRevisionPolicy::RequireImmutable, pinned),
            EvalRefresh::TrustImmutablePin
        );
        assert_eq!(
            EvalRefresh::for_source(ordinary::SourceRevisionPolicy::RequireImmutable, mutable),
            EvalRefresh::ForceRefresh
        );
        assert_eq!(
            EvalRefresh::for_source(ordinary::SourceRevisionPolicy::ResolveAndRecord, pinned),
            EvalRefresh::ForceRefresh
        );
    }

    #[test]
    fn local_eval_reads_the_daemon_host_store_with_no_redirect() {
        // A daemon-host target (`Local`) keeps the host-local eval — its store
        // already holds everything the config references, and a store redirect
        // would be wrong. No `--store` / `--eval-store` flags are added.
        let invocation = NixCommand::eval_drv_path(
            ".#toplevel",
            &[],
            &nexus::BuildTarget::Local,
            EvalRefresh::ForceRefresh,
        );
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
        let target = nexus::BuildTarget::Remote(nexus::NixBuilderSpec::new(
            "ssh-ng://fixture-builder.invalid x86_64-linux - 4 2 k1",
        ));
        let invocation =
            NixCommand::eval_drv_path(".#toplevel", &[], &target, EvalRefresh::ForceRefresh);
        let argv = invocation.joined_arguments();
        assert!(
            !argv.contains("--store"),
            "remote eval adds no store redirect: {argv}"
        );
        assert!(
            !argv.contains("--eval-store"),
            "remote eval adds no eval-store: {argv}"
        );
        assert!(argv.ends_with(".#toplevel.drvPath"), "{argv}");
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
        assert_eq!(activation.boot_once_script(), expected);
    }

    // ---- Step 3: user-environment activation argv ----

    fn user_environment_activation(
        mode: meta::UserEnvironmentAction,
        ssh_destination: &str,
    ) -> UserEnvironmentActivation {
        let profile =
            nexus::ActivationProfile::UserEnvironment(nexus::UserEnvironmentActivationProfile {
                user_environment_action: mode,
                user_name: ordinary::UserName::new("li"),
            });
        let mut command = activate_command(profile, ordinary::ActivationEffect::LiveActivation);
        command.deployment_transport.ssh_destination = nexus::SshDestination::new(ssh_destination);
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
    fn secrets_directory_is_the_datom_source_sibling() {
        let source = ordinary::ProposalSource::new("/fixture/cluster/proposal.datom".to_string());
        let directory = ClusterSecretsDirectory::from_proposal_source(&source);
        assert_eq!(directory.path, PathBuf::from("/fixture/cluster/secrets"));
    }

    #[test]
    fn absent_secrets_directory_yields_no_files() {
        let source = ordinary::ProposalSource::new(
            "/nonexistent/path/that/has/no/secrets/proposal.datom".to_string(),
        );
        let directory = ClusterSecretsDirectory::from_proposal_source(&source);
        assert!(
            directory
                .secret_files()
                .expect("absent secrets dir is empty, not an error")
                .is_empty()
        );
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
        let source = ordinary::ProposalSource::new(
            source_directory
                .join("proposal.datom")
                .to_string_lossy()
                .to_string(),
        );
        let cluster = ClusterSecretsDirectory::from_proposal_source(&source);
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
        let source = ordinary::ProposalSource::new(
            source_directory
                .join("proposal.datom")
                .to_string_lossy()
                .to_string(),
        );
        let cluster = ClusterSecretsDirectory::from_proposal_source(&source);
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
        let source =
            ordinary::ProposalSource::new("/nonexistent/bootstrap/proposal.datom".to_string());
        let cluster = ClusterSecretsDirectory::from_proposal_source(&source);
        GeneratedInputDirectory::new(generated.clone())
            .write_secrets(&cluster)
            .expect("write empty secrets input");
        let flake = fs::read_to_string(generated.join("flake.nix")).expect("read flake");
        assert!(flake.contains("sopsFiles = {"), "{flake}");
        // no entries between the braces
        assert!(!flake.contains(" = ./"), "no entries: {flake}");
        let _ = fs::remove_dir_all(&generated);
    }
}
