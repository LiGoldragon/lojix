//! Hand-implemented `SchemaRuntime` noun — the single data-bearing type that
//! implements both engine traits (`nexus::NexusEngine` + `sema::SemaEngine`).
//!
//! `decide` is the routing brain (port plan §4.2): ordinary reads route to
//! `SemaRead`, ordinary subscription verbs reply with the token handshake, and
//! meta mutations route to `SemaWrite`. A meta `Deploy` opens the effect
//! pipeline (port plan §4.3): the write completion drives a chain of
//! `RunEffect` continuations — resolve flake auth, eval, build, copy, activate
//! — recording a phase transition between stages and finally replying
//! `Deployed`. `run_effect` does real `nix` IO through `tokio::process::Command`
//! so actor-native request tasks await child processes directly instead of
//! routing generated Nexus execution through a blocking-pool bridge.

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use criome::language::{AttestedMomentStatement, OperationStatement};
use criome::master_key::MasterKey;
use criome::transport::CriomeClient;
use horizon_lib::name::{
    ClusterName as HorizonClusterName, CriomeDomainName, NodeName as HorizonNodeName,
    UserName as HorizonUserName,
};
use horizon_lib::{ClusterProposal, Horizon, Viewpoint};
use meta_signal_lojix::schema::lib as meta;
use nota_next::NotaSource;
use signal_criome as criome_signal;
use signal_lojix::schema::lib as ordinary;
use tokio::process::Command;

use crate::schema::nexus::NexusEngine;
use crate::schema::{nexus, sema};
use crate::{DaemonConfiguration, Result, Store};

/// The lojix engine noun. Carries the durable `Store` (the four sema tables)
/// and, while a deploy is in flight, the pipeline cursor that threads the
/// effect chain across continuation hops. Implements both engine traits; the
/// generated `NexusEngine::execute` drives the `Runner` over it.
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
    active_verification: Option<VerificationPipeline>,
    active_operation: Option<MetaOperation>,
}

/// Runtime paths the daemon needs for production deploy materialization.
/// These are decoded once from the daemon's binary startup configuration and
/// then shared across per-request engine values.
#[derive(Debug, Clone)]
pub struct RuntimeConfiguration {
    generated_inputs_directory: PathBuf,
    /// The node this daemon runs on (e.g. `ouranos`). The build-on-target
    /// decision (`DeployPipeline::build_target`) compares the deploy's target
    /// node against this: equal → build LOCALLY (the host's store already holds
    /// any model-bearing closure); different → realize the node's closure in
    /// the TARGET node's own store over `ssh-ng`, so the daemon host never
    /// holds a node's model-bearing closure (Spirit ufjd / 0a9p / lc28, report
    /// 150). Decoded once from the daemon's binary startup configuration.
    daemon_host: ordinary::NodeName,
    /// A test-only gate that the deploy pipeline awaits before its first effect
    /// runs. `None` in production (the pipeline runs straight through). A test
    /// holds the barrier closed to prove the daemon replies the accepted handle
    /// while the pipeline is still parked, then opens it to let the pipeline
    /// complete on the daemon-owned executor — the up9b decoupling witness.
    effect_barrier: Option<EffectBarrier>,
    /// The test-op defaults, projected from the daemon's binary startup
    /// configuration (report 54). `decide_meta_input` reads these to lower a
    /// `(Check …)` shorthand into a full `ContainedRun` — cluster, host, and mode
    /// all default from here. `None` when the daemon was configured without
    /// test defaults; a `(Check …)` then rejects with `NoTestDefaults` rather
    /// than guessing.
    test_defaults: Option<TestDefaults>,
    criome_gate: CriomeGateRuntime,
}

/// The runtime projection of the config-default test selection. A daemon-side
/// noun holding the cluster, default vm-host, and default mode a `(Check …)`
/// fills in. Built once from [`crate::TestDefaults`] (the rkyv config shape)
/// and shared across per-request engines, so the wire op, the durable record,
/// and the config default all resolve through one type.
#[derive(Debug, Clone)]
pub struct TestDefaults {
    default_vm_host: ordinary::NodeName,
    /// The cluster proposal NOTA file projected to validate `(OnHost h)`
    /// against the node's declared host-set and to resolve `All` to the
    /// cluster's test-VM nodes. Empty when host-set validation is not
    /// configured.
    proposal_source: ordinary::ProposalSource,
}

impl TestDefaults {
    /// Lower an ordinary contained deployment request to the one concrete run
    /// it names. The request already carries the profile, flake, and typed
    /// contained target; defaults only provide the hermetic host placeholder.
    fn lower(&self, request: ordinary::DeployContainedRequest) -> ResolvedContainedRun {
        let ordinary::DeployContainedRequest {
            node_profile,
            contained_target,
            source,
            flake_reference,
        } = request;
        let proposal_source = source
            .into_payload()
            .unwrap_or_else(|| self.proposal_source.clone());
        let host = match &contained_target {
            ordinary::ContainedTarget::HermeticVm => self.default_vm_host.clone(),
            ordinary::ContainedTarget::VmHostGuest(target) => {
                self.resolve_host(target.clone().into_payload())
            }
            ordinary::ContainedTarget::EphemeralDroplet(_) => self.default_vm_host.clone(),
        };
        ResolvedContainedRun {
            cluster: node_profile.cluster_name,
            node: node_profile.node_name,
            host,
            target: contained_target,
            proposal_source,
            flake_reference,
        }
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

    /// The everyday test defaults the in-process tests use: cluster
    /// `goldragon`, default host `prometheus`, mode `Hermetic`, and the
    /// CriomOS-test-cluster flake the hermetic proof builds against. No
    /// proposal source — the in-process tests do not exercise host-set
    /// validation against a live projection (the daemon-integration proof
    /// supplies one).
    fn test_default() -> Self {
        Self {
            default_vm_host: ordinary::NodeName::new("prometheus"),
            proposal_source: ordinary::ProposalSource::new(""),
        }
    }
}

impl From<&crate::TestDefaults> for TestDefaults {
    fn from(defaults: &crate::TestDefaults) -> Self {
        Self {
            default_vm_host: ordinary::NodeName::new(defaults.default_vm_host.clone()),
            proposal_source: ordinary::ProposalSource::new(defaults.proposal_source.clone()),
        }
    }
}

#[derive(Debug, Clone)]
enum CriomeGateRuntime {
    Disabled,
    LocalWitness { socket: PathBuf },
}

impl CriomeGateRuntime {
    fn from_configuration(configuration: &crate::CriomeGateConfiguration) -> Self {
        match configuration {
            crate::CriomeGateConfiguration::Disabled => Self::Disabled,
            crate::CriomeGateConfiguration::LocalWitness { socket_path } => Self::LocalWitness {
                socket: PathBuf::from(socket_path),
            },
        }
    }

    fn verify(&self) -> std::result::Result<(), String> {
        match self {
            Self::Disabled => Ok(()),
            Self::LocalWitness { socket } => CriomeWitness::new(socket.clone())?.run(),
        }
    }
}

struct CriomeWitnessPolicy {
    signer_identity: criome_signal::Identity,
    signer_key: MasterKey,
    timekeeper_identity: criome_signal::Identity,
    timekeeper_key: MasterKey,
}

impl CriomeWitnessPolicy {
    fn new() -> std::result::Result<Self, String> {
        Ok(Self {
            signer_identity: criome_signal::Identity::developer("lojix-local-signer".to_string()),
            signer_key: MasterKey::generate().map_err(|error| error.to_string())?,
            timekeeper_identity: criome_signal::Identity::cluster(
                "lojix-local-timekeeper".to_string(),
            ),
            timekeeper_key: MasterKey::generate().map_err(|error| error.to_string())?,
        })
    }

    fn registration(
        &self,
        identity: &criome_signal::Identity,
        key: &MasterKey,
    ) -> criome_signal::IdentityRegistration {
        criome_signal::IdentityRegistration::new(
            identity.clone(),
            key.public_key(),
            key.fingerprint(),
            criome_signal::KeyPurpose::ReleaseAuthorization,
            None,
        )
    }

    fn contract(&self) -> criome_signal::Contract {
        criome_signal::Contract::new(criome_signal::Rule::Threshold(
            criome_signal::Threshold::new(
                criome_signal::RequiredSignatureThreshold::new(1),
                vec![criome_signal::PolicyMember::KeyMember(
                    self.signer_identity.clone(),
                )],
            ),
        ))
    }

    fn stamp(&self) -> std::result::Result<criome_signal::AttestedMoment, String> {
        let proposition = criome_signal::AttestedMomentProposition::new(
            criome_signal::TimeWindow {
                opens_at: criome_signal::TimestampNanos::new(10),
                closes_at: criome_signal::TimestampNanos::new(20),
            },
            criome_signal::RequiredSignatureThreshold::new(1),
            vec![self.timekeeper_identity.clone()],
        );
        let statement = AttestedMomentStatement::new(&proposition)
            .to_signing_bytes()
            .map_err(|error| error.to_string())?;
        let signature = criome_signal::TimeSignature {
            signer: self.timekeeper_identity.clone(),
            envelope: criome_signal::SignatureEnvelope {
                scheme: criome_signal::SignatureScheme::Bls12_381MinPk,
                public_key: self.timekeeper_key.public_key(),
                signature: self.timekeeper_key.sign(&statement),
            },
        };
        Ok(criome_signal::AttestedMoment::new(
            proposition,
            vec![signature],
        ))
    }

    fn evidence(
        &self,
        operation: criome_signal::OperationDigest,
        signer_count: usize,
    ) -> std::result::Result<criome_signal::Evidence, String> {
        let stamp = self.stamp()?;
        let signatures = if signer_count == 0 {
            Vec::new()
        } else {
            let statement = OperationStatement::new(&self.signer_identity, &operation, &stamp)
                .to_signing_bytes()
                .map_err(|error| error.to_string())?;
            vec![criome_signal::StampedSignatureEnvelope {
                stamp: stamp.clone(),
                envelope: criome_signal::SignatureEnvelope {
                    scheme: criome_signal::SignatureScheme::Bls12_381MinPk,
                    public_key: self.signer_key.public_key(),
                    signature: self.signer_key.sign(&statement),
                },
            }]
        };
        Ok(criome_signal::Evidence::new(
            criome_signal::ComponentKind::Spirit,
            operation,
            stamp,
            signatures,
            Vec::new(),
        ))
    }
}

struct CriomeWitness {
    socket: PathBuf,
    policy: CriomeWitnessPolicy,
}

impl CriomeWitness {
    fn new(socket: PathBuf) -> std::result::Result<Self, String> {
        Ok(Self {
            socket,
            policy: CriomeWitnessPolicy::new()?,
        })
    }

    fn run(&self) -> std::result::Result<(), String> {
        let contract = self.seed()?;
        let object = self.object();
        let operation = self.operation();
        let authorized = self.evaluate(criome_signal::AuthorizationEvaluation {
            contract: contract.clone(),
            object: object.clone(),
            evidence: self.policy.evidence(operation.clone(), 1)?,
        })?;
        if authorized != criome_signal::EvaluationDecision::Authorized {
            return Err(format!("criome authorized witness returned {authorized:?}"));
        }
        let rejected = self.evaluate(criome_signal::AuthorizationEvaluation {
            contract,
            object,
            evidence: self.policy.evidence(operation, 0)?,
        })?;
        if rejected == criome_signal::EvaluationDecision::Authorized {
            return Err("criome threshold-short witness authorized".to_string());
        }
        Ok(())
    }

    fn seed(&self) -> std::result::Result<criome_signal::ContractDigest, String> {
        let client = CriomeClient::new(&self.socket);
        for (identity, key) in [
            (&self.policy.signer_identity, &self.policy.signer_key),
            (
                &self.policy.timekeeper_identity,
                &self.policy.timekeeper_key,
            ),
        ] {
            let reply = client
                .send(criome_signal::CriomeRequest::RegisterIdentity(
                    self.policy.registration(identity, key),
                ))
                .map_err(|error| error.to_string())?;
            if !matches!(reply, criome_signal::CriomeReply::IdentityReceipt(_)) {
                return Err(format!("criome identity registration returned {reply:?}"));
            }
        }
        let reply = client
            .send(criome_signal::CriomeRequest::AdmitContract(
                self.policy.contract(),
            ))
            .map_err(|error| error.to_string())?;
        match reply {
            criome_signal::CriomeReply::ContractAdmitted(admitted) => Ok(admitted.into_payload()),
            other => Err(format!("criome contract admission returned {other:?}")),
        }
    }

    fn evaluate(
        &self,
        evaluation: criome_signal::AuthorizationEvaluation,
    ) -> std::result::Result<criome_signal::EvaluationDecision, String> {
        let reply = CriomeClient::new(&self.socket)
            .send(criome_signal::CriomeRequest::EvaluateAuthorization(
                evaluation,
            ))
            .map_err(|error| error.to_string())?;
        match reply {
            criome_signal::CriomeReply::AuthorizationEvaluated(evaluated) => Ok(evaluated.decision),
            other => Err(format!(
                "criome authorization evaluation returned {other:?}"
            )),
        }
    }

    fn object(&self) -> criome_signal::AuthorizedObjectReference {
        criome_signal::AuthorizedObjectReference {
            component: criome_signal::ComponentKind::Spirit,
            digest: criome_signal::ObjectDigest::from_bytes(&Self::head_bytes()),
            kind: criome_signal::AuthorizedObjectKind::Head,
        }
    }

    fn operation(&self) -> criome_signal::OperationDigest {
        criome_signal::OperationDigest::from_bytes(&Self::head_bytes())
    }

    fn head_bytes() -> [u8; 32] {
        let mut bytes = [0u8; 32];
        let mut index = 0u8;
        while (index as usize) < bytes.len() {
            bytes[index as usize] = index.wrapping_mul(7).wrapping_add(13);
            index += 1;
        }
        bytes
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
struct ResolvedContainedRun {
    cluster: ordinary::ClusterName,
    node: ordinary::NodeName,
    host: ordinary::NodeName,
    target: ordinary::ContainedTarget,
    proposal_source: ordinary::ProposalSource,
    /// The flake whose `#checks.<system>.vm-<node>` the hermetic dispatch
    /// builds (and whose generated runner the live path brings up).
    flake_reference: ordinary::FlakeReference,
}

impl ResolvedContainedRun {
    /// The durable test-run row at acceptance: phase `Submitted`, outcome
    /// `Pending`, no closure yet. The decoupled executor rewrites it through
    /// the real phases (`BringingUp`/`Deploying`/…) to a terminal `Passed`
    /// (with the built closure) or `Failed(stage)` — never a faked pass.
    fn pending_record(
        &self,
        identifier: ordinary::ContainedRunIdentifier,
    ) -> ordinary::ContainedRunRecord {
        ordinary::ContainedRunRecord {
            contained_run_identifier: identifier,
            cluster_name: self.cluster.clone(),
            node_name: self.node.clone(),
            host: self.host.clone(),
            target: self.target.clone(),
            proposal_source: self.proposal_source.clone(),
            flake_reference: self.flake_reference.clone(),
            contained_run_phase: ordinary::ContainedRunPhase::Submitted,
            contained_outcome: ordinary::ContainedOutcome::Pending,
            contained_closure_path: None.into(),
        }
    }

    /// The hermetic-check effect command for this run: build
    /// `<flake>#checks.<system>.vm-<node>`, the report-53 §1 auto-pickup check
    /// keyed `vm-<node>`. The system is pinned `x86_64-linux` (the auto-pickup
    /// suite's system), matching the Done-criteria `nix build
    /// .#checks.x86_64-linux.vm-mercury`.
    fn hermetic_check_command(&self) -> nexus::HermeticCheckCommand {
        nexus::HermeticCheckCommand {
            cluster_name: self.cluster.clone(),
            node_name: self.node.clone(),
            flake_reference: self.flake_reference.clone(),
            system: HermeticCheck::SYSTEM.to_string(),
        }
    }

    /// The live bring-up command for this run: the report-51 host-untouched
    /// user-namespace bring-up of the generated microVM runner on the resolved
    /// vmhost. The runner closure and guest IP are filled by the live path's
    /// preceding build; BUILT but not run live here (gated).
    fn bring_up_command(&self, runner: ordinary::ClosurePath) -> nexus::BringUpTestVmCommand {
        nexus::BringUpTestVmCommand {
            cluster_name: self.cluster.clone(),
            node_name: self.node.clone(),
            host: self.host.clone(),
            runner,
            guest_ip: String::new(),
        }
    }

    /// The live teardown command for this run: stop the user units so the tap +
    /// route vanish with the namespace, host netns byte-identical.
    fn tear_down_command(&self) -> nexus::TearDownTestVmCommand {
        nexus::TearDownTestVmCommand {
            cluster_name: self.cluster.clone(),
            node_name: self.node.clone(),
            host: self.host.clone(),
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
    run: ResolvedContainedRun,
    identifier: ordinary::ContainedRunIdentifier,
    stage: TestStage,
    /// The accepted database marker, replayed on the terminal reply.
    accepted_marker: ordinary::DatabaseMarker,
}

/// The in-flight `VerifyContained` cursor. SEMA first proves the run exists and
/// returns the durable row plus the requested body; Nexus then owns execution
/// as a real effect and writes the terminal verification result back through
/// SEMA before replying.
#[derive(Debug, Clone)]
struct VerificationPipeline {
    run: ordinary::ContainedRunRecord,
    verification: ordinary::ContainedVerification,
}

impl VerificationPipeline {
    fn new(plan: sema::ContainedVerificationPlan) -> Self {
        Self {
            run: plan.contained_run_record,
            verification: plan.contained_verification,
        }
    }

    fn gate_command(&self) -> nexus::GateVerificationCommand {
        nexus::GateVerificationCommand {
            contained_run_record: self.run.clone(),
            verification_body: self.verification.verification_body.clone(),
        }
    }
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
    fn accepted(run: ResolvedContainedRun, identifier: ordinary::ContainedRunIdentifier) -> Self {
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

    /// The durable row at a given phase/outcome, carrying the run identity and
    /// (once built) the closure under test. Rewritten at every transition so a
    /// `(Query (ByContainedRun …))` reads the latest committed step (Unit 2b
    /// observability).
    fn record_at(
        &self,
        phase: ordinary::ContainedRunPhase,
        outcome: ordinary::ContainedOutcome,
        closure_path: Option<ordinary::ClosurePath>,
    ) -> ordinary::ContainedRunRecord {
        ordinary::ContainedRunRecord {
            contained_run_identifier: self.identifier.clone(),
            cluster_name: self.run.cluster.clone(),
            node_name: self.run.node.clone(),
            host: self.run.host.clone(),
            target: self.run.target.clone(),
            proposal_source: self.run.proposal_source.clone(),
            flake_reference: self.run.flake_reference.clone(),
            contained_run_phase: phase,
            contained_outcome: outcome,
            contained_closure_path: closure_path.into(),
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
            container: sema::ContainerName::new(format!("vm-{}", self.run.node.payload())),
            state,
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
    Accepted(ordinary::AcceptedContainedDeploy),
    Rejected(ordinary::RejectedDeployContained),
}

#[derive(Debug, Clone)]
struct ContainedClusterPlan {
    identifier: ordinary::ClusterRunIdentifier,
    cluster_name: ordinary::ClusterName,
    member_records: Vec<ordinary::ContainedRunRecord>,
    verification_body: ordinary::VerificationBody,
}

impl ContainedClusterPlan {
    fn from_request(
        defaults: &TestDefaults,
        identifier: ordinary::ClusterRunIdentifier,
        first_member_identifier: u64,
        request: ordinary::RunContainedClusterRequest,
    ) -> std::result::Result<Self, ordinary::RunContainedClusterRejectionReason> {
        if !matches!(
            request.contained_target,
            ordinary::ContainedTarget::HermeticVm
        ) {
            return Err(ordinary::RunContainedClusterRejectionReason::SubstrateUnavailable);
        }
        let members = request.cluster_members.into_payload();
        if members.is_empty() {
            return Err(ordinary::RunContainedClusterRejectionReason::EmptyCluster);
        }
        let mut member_records = Vec::with_capacity(members.len());
        for (offset, member) in members.into_iter().enumerate() {
            let node_profile = Self::member_profile(&request.cluster_name, member)?;
            let run = defaults.lower(ordinary::DeployContainedRequest {
                node_profile,
                contained_target: request.contained_target.clone(),
                source: request.source.clone(),
                flake_reference: request.flake_reference.clone(),
            });
            let member_identifier =
                ordinary::ContainedRunIdentifier::new(first_member_identifier + offset as u64);
            member_records.push(run.pending_record(member_identifier));
        }
        Ok(Self {
            identifier,
            cluster_name: request.cluster_name,
            member_records,
            verification_body: request.verification_body,
        })
    }

    fn member_profile(
        cluster_name: &ordinary::ClusterName,
        member: ordinary::ClusterMember,
    ) -> std::result::Result<ordinary::NodeProfile, ordinary::RunContainedClusterRejectionReason>
    {
        match member {
            ordinary::ClusterMember::Member(node_name) => Ok(ordinary::NodeProfile {
                cluster_name: cluster_name.clone(),
                node_name,
                kind: None.into(),
            }),
            ordinary::ClusterMember::Profile(profile) if profile.cluster_name == *cluster_name => {
                Ok(profile)
            }
            ordinary::ClusterMember::Profile(_) => {
                Err(ordinary::RunContainedClusterRejectionReason::MemberProfileClusterMismatch)
            }
        }
    }

    fn verified_member_records(
        &self,
        criome_gate: &CriomeGateRuntime,
    ) -> Vec<ordinary::ContainedRunRecord> {
        let criome_result = criome_gate.verify();
        self.member_records
            .iter()
            .cloned()
            .map(|mut record| {
                let verdict = match &criome_result {
                    Ok(()) => GateVerification::new(nexus::GateVerificationCommand {
                        contained_run_record: record.clone(),
                        verification_body: self.verification_body.clone(),
                    })
                    .run(&CriomeGateRuntime::Disabled),
                    Err(detail) => nexus::GateVerificationVerdict {
                        contained_run_identifier: record.contained_run_identifier.clone(),
                        passed: false,
                        detail: detail.clone(),
                    },
                };
                record.contained_run_phase = if verdict.passed {
                    ordinary::ContainedRunPhase::Completed
                } else {
                    ordinary::ContainedRunPhase::Failed
                };
                record.contained_outcome = if verdict.passed {
                    ordinary::ContainedOutcome::Passed
                } else {
                    ordinary::ContainedOutcome::Failed(ordinary::FailureStage::Assert)
                };
                record
            })
            .collect()
    }

    fn verified_record(
        &self,
        member_records: &[ordinary::ContainedRunRecord],
    ) -> ordinary::ClusterRunRecord {
        let passed = member_records
            .iter()
            .all(|record| record.contained_outcome == ordinary::ContainedOutcome::Passed);
        self.record_at(
            member_records,
            ordinary::ClusterRunPhase::Completed,
            if passed {
                ordinary::ClusterOutcome::Passed
            } else {
                ordinary::ClusterOutcome::Failed(ordinary::ClusterFailureStage::Verify)
            },
        )
    }

    fn record_at(
        &self,
        member_records: &[ordinary::ContainedRunRecord],
        phase: ordinary::ClusterRunPhase,
        outcome: ordinary::ClusterOutcome,
    ) -> ordinary::ClusterRunRecord {
        ordinary::ClusterRunRecord {
            cluster_run_identifier: self.identifier.clone(),
            cluster_name: self.cluster_name.clone(),
            member_runs: member_records
                .iter()
                .map(|record| record.contained_run_identifier.clone())
                .collect::<Vec<_>>()
                .into(),
            cluster_run_phase: phase,
            cluster_outcome: outcome,
        }
    }
}

/// A projected cluster — the proposal NOTA file the daemon reads to validate
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
    /// Load + parse the proposal NOTA file named by the proposal source. `None`
    /// when the source is empty (host-set validation not configured) or the
    /// file is unreadable / unparseable — host-set validation then does not
    /// block, and `All` resolves to no nodes.
    fn from_source(source: &ordinary::ProposalSource) -> Option<Self> {
        let path = source.payload();
        if path.is_empty() {
            return None;
        }
        let text = fs::read_to_string(path).ok()?;
        let proposal = NotaSource::new(&text).parse::<ClusterProposal>().ok()?;
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
    ) -> std::result::Result<(), ordinary::DeployContainedRejectionReason> {
        let host_set = self
            .host_set_of(node)
            .ok_or(ordinary::DeployContainedRejectionReason::NodeUnknown)?;
        if host_set.iter().any(|declared| declared == host.payload()) {
            Ok(())
        } else {
            Err(ordinary::DeployContainedRejectionReason::VmHostNotDeclaredForNode)
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
}

/// One hermetic-check build — the real `nix build
/// <flake>#checks.<system>.vm-<node> --print-out-paths` the daemon runs as the
/// hermetic test effect. The `runNixOSTest` engine owns its own sandboxed VM,
/// so this is a pure build: exit 0 + an out-path = Passed; a non-zero exit =
/// Failed(HermeticCheck). No SSH, no tap, no live host.
#[derive(Debug, Clone)]
struct HermeticCheck {
    command: nexus::HermeticCheckCommand,
}

impl HermeticCheck {
    /// The auto-pickup suite's system (report 53 §1, `x86_64-linux`). The
    /// checks are keyed `vm-<node>` under `checks.<system>`.
    const SYSTEM: &'static str = "x86_64-linux";

    fn new(command: nexus::HermeticCheckCommand) -> Self {
        Self { command }
    }

    /// The `<flake>#checks.<system>.vm-<node>` installable — the report-53
    /// auto-pickup check keyed `vm-<node>`.
    fn installable(&self) -> String {
        format!(
            "{}#checks.{}.vm-{}",
            self.command.flake_reference.payload(),
            self.command.system,
            self.command.node_name.payload()
        )
    }

    /// Run the real `nix build <installable> --print-out-paths`. On exit 0 the
    /// first printed line is the realised check out-path (the closure under
    /// test); on a non-zero exit the build/test failed.
    async fn run(&self) -> std::result::Result<ordinary::ClosurePath, String> {
        let output = NixCommand::build_check(&self.installable()).run().await?;
        Ok(ordinary::ClosurePath::new(NixCommand::first_line(&output)))
    }
}

/// One schema-visible contained gate verification. This executes the typed
/// 1-of-1 criome gate semantics carried by `VerificationBody`: authorized
/// evidence ships, threshold-short evidence is denied, and an unconfigured
/// gate holds the outbox. The live criome socket proof needs persisted
/// source/substrate coordinates; this effect is the fail-closed contract-level
/// executor the later HermeticVm proof can replace without changing the verb.
#[derive(Debug, Clone)]
struct GateVerification {
    command: nexus::GateVerificationCommand,
}

impl GateVerification {
    fn new(command: nexus::GateVerificationCommand) -> Self {
        Self { command }
    }

    fn run(&self, criome_gate: &CriomeGateRuntime) -> nexus::GateVerificationVerdict {
        match criome_gate.verify().and_then(|()| self.evaluate_body()) {
            Ok(()) => nexus::GateVerificationVerdict {
                contained_run_identifier: self
                    .command
                    .contained_run_record
                    .contained_run_identifier
                    .clone(),
                passed: true,
                detail: "criome 1-of-1 gate cases passed".to_string(),
            },
            Err(detail) => nexus::GateVerificationVerdict {
                contained_run_identifier: self
                    .command
                    .contained_run_record
                    .contained_run_identifier
                    .clone(),
                passed: false,
                detail,
            },
        }
    }

    fn evaluate_body(&self) -> std::result::Result<(), String> {
        let steps = match &self.command.verification_body {
            ordinary::VerificationBody::Gate => self.default_gate_steps(),
            ordinary::VerificationBody::Steps(steps) => steps.payload().clone(),
        };
        if steps.is_empty() {
            return Err("verification body contains no executable steps".to_string());
        }
        for step in steps {
            GateStepEvaluation::new(self.command.contained_run_record.clone(), step).evaluate()?;
        }
        Ok(())
    }

    fn default_gate_steps(&self) -> Vec<ordinary::VerificationStep> {
        vec![
            ordinary::VerificationStep::GateCase(self.gate_case(
                ordinary::GateOutcome::AuthorizedShips,
                self.threshold_one_of_one(),
            )),
            ordinary::VerificationStep::GateCase(self.gate_case(
                ordinary::GateOutcome::ThresholdShortDenied,
                self.threshold_one_of_one(),
            )),
            ordinary::VerificationStep::GateCase(self.gate_case(
                ordinary::GateOutcome::UnconfiguredHeld,
                ordinary::ThresholdSpec::NoGate,
            )),
        ]
    }

    fn gate_case(
        &self,
        gate_outcome: ordinary::GateOutcome,
        threshold_spec: ordinary::ThresholdSpec,
    ) -> ordinary::GateCaseStep {
        ordinary::GateCaseStep {
            component_kind: ordinary::ComponentKind::Criome,
            gate_outcome,
            threshold_spec,
        }
    }

    fn threshold_one_of_one(&self) -> ordinary::ThresholdSpec {
        ordinary::ThresholdSpec::Threshold(ordinary::ThresholdRequirement {
            required_signature_count: ordinary::RequiredSignatureCount::new(1),
            members: vec![ordinary::KeyMember::Signer(
                self.command.contained_run_record.node_name.clone(),
            )]
            .into(),
        })
    }
}

#[derive(Debug, Clone)]
struct GateStepEvaluation {
    run: ordinary::ContainedRunRecord,
    step: ordinary::VerificationStep,
}

impl GateStepEvaluation {
    fn new(run: ordinary::ContainedRunRecord, step: ordinary::VerificationStep) -> Self {
        Self { run, step }
    }

    fn evaluate(&self) -> std::result::Result<(), String> {
        match &self.step {
            ordinary::VerificationStep::GateCase(step) => self.evaluate_gate_case(step),
            ordinary::VerificationStep::Probe(_) => {
                Err("probe steps are not executable in the 1-of-1 gate slice".to_string())
            }
            ordinary::VerificationStep::DeployIntegrity(_) => Err(
                "deploy-integrity steps are not executable in the 1-of-1 gate slice".to_string(),
            ),
        }
    }

    fn evaluate_gate_case(&self, step: &ordinary::GateCaseStep) -> std::result::Result<(), String> {
        if step.component_kind != ordinary::ComponentKind::Criome {
            return Err("only criome gate cases are executable in this slice".to_string());
        }
        match step.gate_outcome {
            ordinary::GateOutcome::AuthorizedShips
            | ordinary::GateOutcome::ThresholdShortDenied => self.require_one_of_one_threshold(
                &step.threshold_spec,
                "authorized and threshold-short cases require a 1-of-1 threshold",
            ),
            ordinary::GateOutcome::UnconfiguredHeld => self.require_no_gate(&step.threshold_spec),
        }
    }

    fn require_one_of_one_threshold(
        &self,
        threshold_spec: &ordinary::ThresholdSpec,
        detail: &str,
    ) -> std::result::Result<(), String> {
        match threshold_spec {
            ordinary::ThresholdSpec::Threshold(requirement)
                if requirement.required_signature_count == 1
                    && requirement.members.payload().len() == 1
                    && requirement
                        .members
                        .payload()
                        .contains(&ordinary::KeyMember::Signer(self.run.node_name.clone())) =>
            {
                Ok(())
            }
            _ => Err(detail.to_string()),
        }
    }

    fn require_no_gate(
        &self,
        threshold_spec: &ordinary::ThresholdSpec,
    ) -> std::result::Result<(), String> {
        match threshold_spec {
            ordinary::ThresholdSpec::NoGate => Ok(()),
            ordinary::ThresholdSpec::Threshold(_) => {
                Err("unconfigured-held case must carry NoGate".to_string())
            }
        }
    }
}

/// The LIVE host-untouched VM lifecycle (report 51 §3 / report 47 v2, Unit 2b)
/// — the report-51 user-namespace bring-up/teardown of the generated microVM
/// runner on the resolved vmhost. BUILT here, NOT run live (the first
/// Prometheus cycle is psyche-gated): the invocation shapes are constructed so
/// the bracket is provably end-to-end, but a live run is gated.
///
/// Bring-up `ssh <host-fqdn>` runs a `systemd-run --user` durable unit that
/// `unshare -rn`'s a private network namespace, creates the additive tap
/// inside it, and `nsenter`s the generated runner — no sudo, no
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
    fn from_bring_up(command: &nexus::BringUpTestVmCommand) -> Self {
        Self {
            target: Self::host_target(&command.cluster_name, &command.host),
            node: command.node_name.clone(),
            runner: command.runner.payload().clone(),
            guest_ip: command.guest_ip.clone(),
        }
    }

    fn from_tear_down(command: &nexus::TearDownTestVmCommand) -> Self {
        Self {
            target: Self::host_target(&command.cluster_name, &command.host),
            node: command.node_name.clone(),
            runner: String::new(),
            guest_ip: String::new(),
        }
    }

    /// `root@<host>.<cluster>.criome` — the vmhost the user-level units run on.
    /// Falls back to a bare host name if horizon validation fails (a resolved
    /// host never does), so command construction is total.
    fn host_target(cluster: &ordinary::ClusterName, host: &ordinary::NodeName) -> SshTarget {
        SshTarget::root_at_node(cluster, host).unwrap_or_else(|_| SshTarget {
            user: "root".to_string(),
            domain: CriomeDomainName::for_node(
                &HorizonNodeName::try_new("host").expect("static host name"),
                &HorizonClusterName::try_new("cluster").expect("static cluster name"),
            ),
        })
    }

    /// The durable `--user` unit name for this guest's namespace bring-up
    /// (`lojix-test-vm-<node>`), the unit teardown stops.
    fn unit_name(&self) -> String {
        format!("lojix-test-vm-{}", self.node.payload())
    }

    /// The host-untouched bring-up invocation (report 51 §3): a `--user`
    /// systemd-run unit that `unshare -rn`s a private netns, brings up the
    /// additive tap inside it, and `nsenter`s the generated runner. Constructed
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
    /// then `nsenter` the generated runner. The tap design maps one-to-one onto
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
/// pipeline runs (up9q). `Accepted` carries the `AcceptedDeploy` handle (the
/// durable deployment identifier + marker) and leaves the in-flight cursor set
/// for the deploy-job actor to drive; `Rejected` is a typed up-front refusal
/// (unsupported action, or a submission write rejection) and leaves no cursor.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DeploySubmissionOutcome {
    Accepted(meta::AcceptedDeploy),
    Rejected(meta::RejectedDeploy),
}

impl RuntimeConfiguration {
    pub fn from_daemon_configuration(configuration: &DaemonConfiguration) -> Self {
        Self {
            generated_inputs_directory: PathBuf::from(&configuration.state_directory_path)
                .join("generated-inputs"),
            daemon_host: ordinary::NodeName::new(configuration.daemon_host.clone()),
            effect_barrier: None,
            test_defaults: Some(TestDefaults::from(&configuration.test_defaults)),
            criome_gate: CriomeGateRuntime::from_configuration(&configuration.criome_gate),
        }
    }

    pub fn test_default() -> Self {
        Self {
            generated_inputs_directory: std::env::temp_dir().join("lojix-generated-inputs"),
            daemon_host: ordinary::NodeName::new("daemon-host"),
            effect_barrier: None,
            test_defaults: Some(TestDefaults::test_default()),
            criome_gate: CriomeGateRuntime::Disabled,
        }
    }

    /// A test configuration whose deploy pipeline parks at its first effect on
    /// the given barrier — the seam the up9b decoupling tests drive.
    pub fn test_with_effect_barrier(barrier: EffectBarrier) -> Self {
        Self {
            generated_inputs_directory: std::env::temp_dir().join("lojix-generated-inputs"),
            daemon_host: ordinary::NodeName::new("daemon-host"),
            effect_barrier: Some(barrier),
            test_defaults: Some(TestDefaults::test_default()),
            criome_gate: CriomeGateRuntime::Disabled,
        }
    }

    /// The build target for a deploy whose closure must land on
    /// `cluster_name`/`node_name`. When the target node IS this daemon's host,
    /// the build stays `Local` — the host's store already holds any
    /// model-bearing closure, so a `Switch`-on-self or an ouranos-from-ouranos
    /// deploy realizes in place. When the target node is a DIFFERENT node, the
    /// build realizes in that node's own store over
    /// `ssh-ng://root@<node>.<cluster>.criome`, so a model-bearing node closure
    /// never transits the daemon host (Spirit ufjd / 0a9p / lc28, report 150).
    /// An explicit `builder` override still wins (it lowers to `Remote` upstream
    /// of this call); this method only chooses between local and target-store.
    fn build_target_for(
        &self,
        cluster_name: &ordinary::ClusterName,
        node_name: &ordinary::NodeName,
    ) -> nexus::BuildTarget {
        if node_name.payload() == self.daemon_host.payload() {
            return nexus::BuildTarget::Local;
        }
        match SshTarget::root_at_node(cluster_name, node_name) {
            Ok(target) => nexus::BuildTarget::target_store(target.ssh_uri()),
            // A target whose names fail horizon validation can't be addressed
            // for a remote store; fall back to a local build rather than emit a
            // malformed `--store` URI. A submitted deploy never has invalid
            // names (validated at submit), so this arm is unreachable in
            // practice and never silently pulls a model closure for a real node.
            Err(_) => nexus::BuildTarget::Local,
        }
    }

    fn effect_barrier(&self) -> Option<&EffectBarrier> {
        self.effect_barrier.as_ref()
    }

    /// The node this daemon runs on — used to detect a self-targeting deploy so
    /// activation routes around the self-Switch deadlock (a foreground ssh that
    /// `switch-to-configuration switch` kills by restarting the daemon).
    fn daemon_host(&self) -> &ordinary::NodeName {
        &self.daemon_host
    }

    /// The configured test-op defaults, if the daemon was started with them.
    fn test_defaults(&self) -> Option<&TestDefaults> {
        self.test_defaults.as_ref()
    }

    fn criome_gate(&self) -> &CriomeGateRuntime {
        &self.criome_gate
    }

    fn materialization_root(&self, command: &nexus::HorizonMaterializationCommand) -> PathBuf {
        let cluster = command.cluster_name.payload();
        let node = command.node_name.payload();
        self.generated_inputs_directory
            .join(cluster)
            .join(node)
            .join(Self::shape_name(&command.shape))
    }

    fn shape_name(shape: &nexus::MaterializationShape) -> &'static str {
        match shape {
            nexus::MaterializationShape::FullOs => "full-os",
            nexus::MaterializationShape::OsOnly => "os-only",
            nexus::MaterializationShape::Home(_) => "home",
        }
    }
}

/// The BootOnce transient unit name a deployment owns. Defined here (not on the
/// foreign schema-emitted `DeploymentIdentifier`) and implemented on that type
/// so the activation-side `SystemActivation::unit_name` and the resume-side
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
    deployment_kind: ordinary::DeploymentKind,
    activation_kind: ordinary::ActivationKind,
    source: ordinary::ProposalSource,
    flake: ordinary::FlakeReference,
    /// A direct flake output attribute to build (a self-contained fixture /
    /// test closure), overriding the production `nixosConfigurations.target`
    /// path. `None` for a production deploy that needs the horizon override.
    build_attribute: Option<meta::FlakeAttribute>,
    /// The deploy action (System action, or Home mode + user). Owns the
    /// produces-closure / activates / target-attribute decisions so the
    /// pipeline asks the action rather than storing derived booleans.
    action: DeployAction,
    builder: Option<ordinary::NodeName>,
    substituters: Vec<nexus::ExtraSubstituter>,
    input_overrides: Vec<nexus::FlakeInputOverride>,
    closure_path: Option<ordinary::ClosurePath>,
    accepted_marker: ordinary::DatabaseMarker,
    stage: DeployStage,
}

/// The deploy pipeline cursor. Each value names the stage that has just
/// completed; after a phase-transition write commits, `advance_after_phase`
/// reads it to emit the next effect (or the final activation-record write).
/// The chain is: Submitted -> (FlakeAuth) -> Building/Eval -> Build -> Copy ->
/// (Copying) -> Activate -> (Activated) -> RecordGenerationActivated -> Deployed.
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

/// The deploy action — a System action or a Home mode (with its user). Owns
/// the produces-closure / activates / target-attribute decisions so the
/// pipeline asks the action rather than storing derived booleans.
#[derive(Debug, Clone)]
enum DeployAction {
    System(ordinary::SystemAction),
    Home {
        mode: meta::HomeMode,
        user: ordinary::UserName,
    },
}

impl DeployAction {
    /// `false` only for a System `Eval` (derivation path only, no realised
    /// closure). Home modes always build a closure.
    fn produces_closure(&self) -> bool {
        match self {
            Self::System(action) => !matches!(action, ordinary::SystemAction::Eval),
            Self::Home { .. } => true,
        }
    }

    /// Whether the action copies + activates after the build: System
    /// Boot/Switch/Test/BootOnce, or Home Profile/Activate. `Eval` and
    /// `Build` (and Home `Build`) stop at the realised closure.
    fn activates(&self) -> bool {
        match self {
            Self::System(action) => matches!(
                action,
                ordinary::SystemAction::Boot
                    | ordinary::SystemAction::Switch
                    | ordinary::SystemAction::Test
                    | ordinary::SystemAction::BootOnce
            ),
            Self::Home { mode, .. } => {
                matches!(mode, meta::HomeMode::Profile | meta::HomeMode::Activate)
            }
        }
    }

    /// The activation profile carried on the activate command — the shape that
    /// decides which target-side activation runs (System switch-to-configuration
    /// vs Home home-manager profile/activate).
    fn activation_profile(&self) -> nexus::ActivationProfile {
        match self {
            Self::System(action) => nexus::ActivationProfile::System(*action),
            Self::Home { mode, user } => {
                nexus::ActivationProfile::Home(nexus::HomeActivationProfile {
                    mode: *mode,
                    user: user.clone(),
                })
            }
        }
    }

    /// The production flake attribute for this action — used when no direct
    /// `build_attribute` override is given. System builds the node toplevel
    /// (node identity injected by the horizon override, the deferred M3
    /// materialization work); Home builds the user's activation package.
    fn target_attribute(&self) -> String {
        match self {
            Self::System(_) => {
                "nixosConfigurations.target.config.system.build.toplevel".to_string()
            }
            Self::Home { user, .. } => {
                format!("homeConfigurations.{}.activationPackage", user.payload())
            }
        }
    }
}

impl DeployPipeline {
    fn from_submission(
        deployment_identifier: ordinary::DeploymentIdentifier,
        generation_identifier: ordinary::GenerationIdentifier,
        accepted_marker: ordinary::DatabaseMarker,
        submission: sema::DeploySubmission,
    ) -> Self {
        match submission {
            sema::DeploySubmission::System(deployment) => Self {
                deployment_identifier,
                generation_identifier,
                cluster_name: deployment.production_node.cluster_name,
                node_name: deployment.production_node.node_name,
                deployment_kind: deployment.deployment_kind,
                activation_kind: Self::system_activation_kind(deployment.system_action),
                source: deployment.proposal_source,
                flake: deployment.flake_reference,
                build_attribute: deployment.build_attribute.into_payload(),
                action: DeployAction::System(deployment.system_action),
                builder: deployment
                    .builder_override
                    .into_payload()
                    .map(meta::Builder::into_payload),
                substituters: Self::convert_substituters(
                    deployment.extra_substituters.into_payload(),
                ),
                input_overrides: Vec::new(),
                closure_path: None,
                accepted_marker,
                stage: DeployStage::Submitted,
            },
            sema::DeploySubmission::Home(deployment) => Self {
                deployment_identifier,
                generation_identifier,
                cluster_name: deployment.production_node.cluster_name,
                node_name: deployment.production_node.node_name,
                deployment_kind: ordinary::DeploymentKind::HomeOnly,
                activation_kind: ordinary::ActivationKind::Switch,
                source: deployment.proposal_source,
                flake: deployment.flake_reference,
                build_attribute: None,
                action: DeployAction::Home {
                    mode: deployment.home_mode,
                    user: deployment.user_name,
                },
                builder: deployment
                    .builder_override
                    .into_payload()
                    .map(meta::Builder::into_payload),
                substituters: Self::convert_substituters(
                    deployment.extra_substituters.into_payload(),
                ),
                input_overrides: Vec::new(),
                closure_path: None,
                accepted_marker,
                stage: DeployStage::Submitted,
            },
        }
    }

    fn system_activation_kind(action: ordinary::SystemAction) -> ordinary::ActivationKind {
        match action {
            ordinary::SystemAction::Boot => ordinary::ActivationKind::Boot,
            ordinary::SystemAction::Test => ordinary::ActivationKind::Test,
            ordinary::SystemAction::BootOnce => ordinary::ActivationKind::BootOnce,
            ordinary::SystemAction::Eval
            | ordinary::SystemAction::Build
            | ordinary::SystemAction::Switch => ordinary::ActivationKind::Switch,
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

    /// The build target for this deploy. An explicit `builder` override always
    /// wins — it dispatches to the named Nix builder machine. Otherwise the
    /// daemon's configuration decides build-on-target: a target node that is
    /// the daemon host builds `Local`, a different target node realizes in its
    /// own store over `ssh-ng` so its model-bearing closure never transits the
    /// daemon host (Spirit ufjd / 0a9p / lc28, report 150).
    fn build_target(&self, configuration: &RuntimeConfiguration) -> nexus::BuildTarget {
        match &self.builder {
            Some(builder) => nexus::BuildTarget::Remote(nexus::BuilderNode::new(builder.clone())),
            None => configuration.build_target_for(&self.cluster_name, &self.node_name),
        }
    }

    fn flake_auth_request(&self) -> nexus::FlakeAuthRequest {
        nexus::FlakeAuthRequest {
            proposal_source: self.source.clone(),
            flake_reference: self.flake.clone(),
        }
    }

    fn needs_horizon_materialization(&self) -> bool {
        self.build_attribute.is_none()
    }

    fn horizon_materialization_command(&self) -> nexus::HorizonMaterializationCommand {
        nexus::HorizonMaterializationCommand {
            cluster_name: self.cluster_name.clone(),
            node_name: self.node_name.clone(),
            proposal_source: self.source.clone(),
            shape: self.materialization_shape(),
        }
    }

    fn materialization_shape(&self) -> nexus::MaterializationShape {
        match &self.action {
            DeployAction::System(_) => match &self.deployment_kind {
                ordinary::DeploymentKind::FullOs => nexus::MaterializationShape::FullOs,
                ordinary::DeploymentKind::OsOnly => nexus::MaterializationShape::OsOnly,
                ordinary::DeploymentKind::HomeOnly => nexus::MaterializationShape::OsOnly,
            },
            DeployAction::Home { user, .. } => {
                nexus::MaterializationShape::Home(nexus::HomeMaterialization::new(user.clone()))
            }
        }
    }

    fn nix_eval_command(&self, configuration: &RuntimeConfiguration) -> nexus::NixEvalCommand {
        nexus::NixEvalCommand {
            cluster_name: self.cluster_name.clone(),
            node_name: self.node_name.clone(),
            deployment_kind: self.deployment_kind,
            flake_reference: self.flake.clone(),
            attribute: self.target_attribute().into(),
            overrides: self.input_overrides.clone().into(),
            // Build-on-target (Spirit ufjd / 0a9p / lc28, report 150): the eval
            // step must resolve `.drvPath` against the SAME store the build will
            // realize into. A target node that is not the daemon host references
            // model `.drv`s that exist only in its own store, so a daemon-host
            // local eval cannot find them. Mirroring `nix_build_command`'s target
            // points the eval at the target store where those paths already live.
            target: self.build_target(configuration),
        }
    }

    fn target_attribute(&self) -> String {
        // A1 fix: a direct `build_attribute` override names a self-contained
        // flake output (the fixture path); otherwise the action supplies the
        // production attribute (`nixosConfigurations.target...` /
        // `homeConfigurations.<user>...`). The old `{cluster}.{node}` form
        // resolved to no real flake attribute and every deploy failed at eval.
        match &self.build_attribute {
            Some(attribute) => attribute.payload().clone(),
            None => self.action.target_attribute(),
        }
    }

    fn nix_build_command(
        &self,
        closure_path: ordinary::ClosurePath,
        configuration: &RuntimeConfiguration,
    ) -> nexus::NixBuildCommand {
        nexus::NixBuildCommand {
            generation_identifier: self.generation_identifier.clone(),
            closure_path,
            target: self.build_target(configuration),
            substituters: self.substituters.clone().into(),
        }
    }

    fn copy_closure_command(
        &self,
        closure_path: ordinary::ClosurePath,
        configuration: &RuntimeConfiguration,
    ) -> nexus::CopyClosureCommand {
        nexus::CopyClosureCommand {
            generation_identifier: self.generation_identifier.clone(),
            cluster_name: self.cluster_name.clone(),
            node_name: self.node_name.clone(),
            closure_path,
            source: self.build_target(configuration),
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
            closure_path,
            activation_kind: self.activation_kind,
            profile: self.action.activation_profile(),
        }
    }

    /// The activation-record write. The closure path is mandatory: by the time
    /// the pipeline records activation it has been captured on the cursor (the
    /// activate command already required it, risk R2), so `None` here is an
    /// internal invariant failure surfaced through `activation_commit` returning
    /// `None` rather than committing an empty closure into the live set.
    fn activation_commit(&self) -> Option<sema::ActivationCommit> {
        Some(sema::ActivationCommit {
            generation_identifier: self.generation_identifier.clone(),
            cluster_name: self.cluster_name.clone(),
            node_name: self.node_name.clone(),
            generation_slot: ordinary::GenerationSlot::Current,
            closure_path: self.closure_path.clone()?,
        })
    }

    fn phase_event(
        &self,
        phase: ordinary::DeploymentPhase,
        event_log_position: ordinary::EventLogPosition,
        detail: Option<ordinary::PhaseDetail>,
    ) -> ordinary::DeploymentPhaseEvent {
        ordinary::DeploymentPhaseEvent {
            deployment_identifier: self.deployment_identifier.clone(),
            generation_identifier: self.generation_identifier.clone(),
            cluster_name: self.cluster_name.clone(),
            node_name: self.node_name.clone(),
            deployment_phase: phase,
            event_log_position,
            detail: detail.into(),
        }
    }

    /// The resolved `root@<node>.<cluster>.criome` SSH target this deploy
    /// activates, captured on the durable job row so a resumed job knows where
    /// to poll without re-deriving from a partially-applied cursor. `None` only
    /// if the names fail horizon validation (a submitted deploy never does).
    fn resolved_target(&self) -> Option<String> {
        SshTarget::root_at_node(&self.cluster_name, &self.node_name)
            .ok()
            .map(|target| target.as_ssh_arg())
    }

    /// The BootOnce transient-unit name a resumed `Activating` job polls via
    /// `journalctl -u <unit>` instead of re-activating. Deterministic in the
    /// deployment identifier so the resumed daemon computes the same name that
    /// was persisted at submit, rather than a time/pid value it cannot
    /// reconstruct. `None` for non-BootOnce actions (which have no transient
    /// unit to poll; copy is idempotent and activation re-runs safely).
    fn boot_once_unit(&self) -> Option<String> {
        match &self.action {
            DeployAction::System(ordinary::SystemAction::BootOnce) => {
                Some(self.deployment_identifier.boot_once_unit_name())
            }
            _ => None,
        }
    }

    /// The durable in-flight job row at the given phase. Written on submit and
    /// rewritten at every phase transition (up9q): the persisted phase cursor,
    /// closure path (once built), resolved target, and BootOnce unit name let a
    /// restarted daemon read the row and reconcile the in-flight deploy.
    fn deploy_job(&self, phase: sema::DeployJobPhase) -> sema::DeployJob {
        sema::DeployJob {
            deployment_identifier: self.deployment_identifier.clone(),
            generation_identifier: self.generation_identifier.clone(),
            cluster_name: self.cluster_name.clone(),
            node_name: self.node_name.clone(),
            phase,
            deploy_job_closure_path: self.closure_path.clone().into(),
            resolved_target: self.resolved_target().into(),
            boot_once_unit: self.boot_once_unit().into(),
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
        match self.phase {
            sema::DeployJobPhase::Activating => DeployJobResumption::PollActivationUnit {
                unit: self.boot_once_unit.payload().clone(),
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
}

impl From<ordinary::ContainedRunRecord> for sema::StoredContainedRun {
    /// Project the wire test-run record onto the lojix-local durable row. The
    /// two carry identical fields (the LiveGeneration/Generation split): the
    /// daemon writes the durable row, the query reads it back as the wire shape.
    fn from(record: ordinary::ContainedRunRecord) -> Self {
        Self {
            contained_run_identifier: record.contained_run_identifier,
            cluster_name: record.cluster_name,
            node_name: record.node_name,
            host: record.host,
            target: record.target,
            proposal_source: record.proposal_source,
            flake_reference: record.flake_reference,
            phase: record.contained_run_phase,
            outcome: record.contained_outcome,
            contained_closure_path: record.contained_closure_path,
        }
    }
}

impl From<sema::StoredContainedRun> for ordinary::ContainedRunRecord {
    /// Project the durable row back onto the wire record for the
    /// `(ByContainedRun …)` query reply.
    fn from(run: sema::StoredContainedRun) -> Self {
        Self {
            contained_run_identifier: run.contained_run_identifier,
            cluster_name: run.cluster_name,
            node_name: run.node_name,
            host: run.host,
            target: run.target,
            proposal_source: run.proposal_source,
            flake_reference: run.flake_reference,
            contained_run_phase: run.phase,
            contained_outcome: run.outcome,
            contained_closure_path: run.contained_closure_path,
        }
    }
}

impl From<ordinary::ClusterRunRecord> for sema::StoredClusterRun {
    fn from(record: ordinary::ClusterRunRecord) -> Self {
        Self {
            cluster_run_identifier: record.cluster_run_identifier,
            cluster_name: record.cluster_name,
            member_runs: record.member_runs,
            phase: record.cluster_run_phase,
            outcome: record.cluster_outcome,
        }
    }
}

impl From<sema::StoredClusterRun> for ordinary::ClusterRunRecord {
    fn from(run: sema::StoredClusterRun) -> Self {
        Self {
            cluster_run_identifier: run.cluster_run_identifier,
            cluster_name: run.cluster_name,
            member_runs: run.member_runs,
            cluster_run_phase: run.phase,
            cluster_outcome: run.outcome,
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
            active_verification: None,
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

    /// Run ONLY the synchronous submit step of a `Deploy` (up9q surface a): the
    /// reject-guard, restart-safe identifier issuance, in-flight job-row
    /// persistence at `Submitted`, and cursor construction. Returns the typed
    /// admission outcome immediately — the `AcceptedDeploy` handle the daemon
    /// replies before the pipeline runs, or a typed rejection. On accept the
    /// in-flight cursor is left set on `self`, so the daemon hands this engine
    /// to the deploy-job actor, which drives the pipeline via
    /// [`Self::drive_submitted_deploy`]. The pipeline does NOT run here.
    pub fn submit_deploy(&mut self, request: meta::DeployRequest) -> DeploySubmissionOutcome {
        if let Some(reason) = Self::unsupported_deploy_reason(&request) {
            return DeploySubmissionOutcome::Rejected(self.deploy_rejection(reason));
        }
        self.active_operation = Some(MetaOperation::Deploy);
        let submission = match request {
            meta::DeployRequest::System(deployment) => sema::DeploySubmission::System(deployment),
            meta::DeployRequest::Home(deployment) => sema::DeploySubmission::Home(deployment),
        };
        match self.record_deploy_submitted(submission) {
            sema::SemaWriteOutput::DeploySubmitted(accepted) => {
                DeploySubmissionOutcome::Accepted(accepted)
            }
            sema::SemaWriteOutput::WriteRejected(report) => {
                self.active_operation = None;
                self.active_deploy = None;
                DeploySubmissionOutcome::Rejected(meta::RejectedDeploy {
                    deploy_rejection_reason: Self::deploy_reason(report.reason),
                    database_marker: Self::marker(report.marker.commit_sequence.into_payload()),
                })
            }
            // `record_deploy_submitted` only ever returns the two arms above;
            // any other output is an internal invariant violation surfaced as a
            // typed rejection rather than a panic.
            _ => {
                self.active_operation = None;
                self.active_deploy = None;
                DeploySubmissionOutcome::Rejected(
                    self.deploy_rejection(meta::DeployRejectionReason::InternalError),
                )
            }
        }
    }

    /// Drive an already-submitted deploy's effect pipeline to its terminal
    /// reply (up9q surface a, the daemon-owned executor body). Requires the
    /// in-flight cursor to be set by a prior [`Self::submit_deploy`]; re-enters
    /// the generated runner at the `DeploySubmitted` continuation, so it runs
    /// the SAME flake-auth -> eval -> build -> copy -> activate -> record chain
    /// the inline path ran, updating the durable job row at every phase. The
    /// returned `meta::Output` is the terminal deploy outcome (Deployed or
    /// DeployRejected) for logging and tests; the client already has its handle
    /// and re-observes the outcome by deployment identifier.
    pub async fn drive_submitted_deploy(&mut self) -> meta::Output {
        let accepted = match self.active_deploy.as_ref() {
            Some(pipeline) => meta::AcceptedDeploy {
                deployment_identifier: pipeline.deployment_identifier.clone(),
                database_marker: pipeline.accepted_marker.clone(),
            },
            None => {
                return meta::Output::DeployRejected(meta::DeployRejected::new(
                    self.deploy_rejection(meta::DeployRejectionReason::InternalError),
                ));
            }
        };
        let work =
            nexus::NexusWork::SemaWriteCompleted(sema::SemaWriteOutput::DeploySubmitted(accepted))
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
    pub async fn submit_contained(
        &mut self,
        request: ordinary::DeployContainedRequest,
    ) -> TestSubmissionOutcome {
        let work = nexus::NexusWork::SignalArrived(nexus::SignalInput::OrdinaryInput(
            ordinary::Input::DeployContained(ordinary::DeployContained::new(request)),
        ))
        .with_origin_route(nexus::OriginRoute::new(0));
        match self.execute(work).await.into_root() {
            nexus::NexusAction::ReplyToSignal(nexus::SignalOutput::OrdinaryOutput(
                ordinary::Output::ContainedDeployed(accepted),
            )) => {
                // Stamp the accepted marker onto the cursor so the terminal
                // outcome reply carries the acceptance marker (like deploy).
                let accepted = accepted.into_payload();
                if let Some(pipeline) = self.active_test.as_mut() {
                    pipeline.accepted_marker = accepted.database_marker.clone();
                }
                TestSubmissionOutcome::Accepted(accepted)
            }
            nexus::NexusAction::ReplyToSignal(nexus::SignalOutput::OrdinaryOutput(
                ordinary::Output::DeployContainedRejected(rejected),
            )) => TestSubmissionOutcome::Rejected(rejected.into_payload()),
            _ => TestSubmissionOutcome::Rejected(
                self.test_rejection(ordinary::DeployContainedRejectionReason::InternalError),
            ),
        }
    }

    /// Drive an already-submitted test's REAL dispatch to its terminal outcome
    /// (Unit 2b, the daemon-owned executor body — mirrors
    /// [`Self::drive_submitted_deploy`]). Requires the in-flight test cursor set
    /// by a prior [`Self::submit_test`] (or [`Self::decide_test`] for the
    /// in-process proof). Re-enters the generated runner at the cursor's first
    /// effect (the hermetic `nix build`, or the live bring-up), runs it for
    /// real, and rewrites the durable row through real phases to a terminal
    /// `Passed` (with the built closure) or `Failed(stage)` — never a faked
    /// pass. The returned `meta::Output` is the terminal `Tested`/`TestRejected`
    /// for logging/tests; the client already has its accepted handle and
    /// re-observes the outcome via `(Query (ByContainedRun …))`.
    pub async fn drive_submitted_test(&mut self) -> ordinary::Output {
        let Some(pipeline) = self.active_test.clone() else {
            return ordinary::Output::DeployContainedRejected(
                ordinary::DeployContainedRejected::new(
                    self.test_rejection(ordinary::DeployContainedRejectionReason::InternalError),
                ),
            );
        };
        self.active_operation = Some(MetaOperation::Test);
        let first_effect = match &pipeline.run.target {
            ordinary::ContainedTarget::HermeticVm => {
                nexus::EffectCommand::HermeticCheck(pipeline.run.hermetic_check_command())
            }
            // LIVE is BUILT but not run live here (gated). The bring-up effect
            // is constructed and dispatched; `run_effect` for the live effects
            // is the host-untouched user-namespace path (report 51 §3). A live
            // run is psyche-gated, so the daemon-integration proof exercises
            // Hermetic; this constructs the live first effect honestly.
            ordinary::ContainedTarget::VmHostGuest(_) => nexus::EffectCommand::BringUpTestVm(
                pipeline
                    .run
                    .bring_up_command(ordinary::ClosurePath::new(String::new())),
            ),
            ordinary::ContainedTarget::EphemeralDroplet(_) => {
                return ordinary::Output::DeployContainedRejected(
                    ordinary::DeployContainedRejected::new(self.test_rejection(
                        ordinary::DeployContainedRejectionReason::SubstrateUnavailable,
                    )),
                );
            }
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
    /// continuations through the generated runner. The hermetic path is a
    /// single effect then a terminal write, so this usually runs one or two
    /// hops; the live path threads bring-up → deploy → assert → teardown.
    async fn drive_to_terminal(&mut self, mut action: nexus::NexusAction) -> ordinary::Output {
        loop {
            match action {
                nexus::NexusAction::ReplyToSignal(nexus::SignalOutput::OrdinaryOutput(output)) => {
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
                    return ordinary::Output::DeployContainedRejected(
                        ordinary::DeployContainedRejected::new(self.test_rejection(
                            ordinary::DeployContainedRejectionReason::InternalError,
                        )),
                    );
                }
            }
        }
    }

    fn marker(commit_sequence: u64) -> ordinary::DatabaseMarker {
        ordinary::DatabaseMarker {
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

    // ---- decide: signal arrival routing (port plan §4.2) ----------------

    fn decide_signal_arrival(&mut self, input: nexus::SignalInput) -> nexus::NexusAction {
        match input {
            nexus::SignalInput::OrdinaryInput(input) => self.decide_ordinary_input(input),
            nexus::SignalInput::MetaInput(input) => self.decide_meta_input(input),
        }
    }

    fn decide_ordinary_input(&mut self, input: ordinary::Input) -> nexus::NexusAction {
        match input {
            ordinary::Input::DeployContained(request) => {
                self.decide_contained_deploy(request.into_payload())
            }
            ordinary::Input::RunContainedCluster(request) => {
                self.decide_contained_cluster_run(request.into_payload())
            }
            ordinary::Input::VerifyContained(check) => nexus::NexusAction::CommandSemaRead(
                sema::SemaReadInput::VerifyContainedRun(check.into_payload()),
            ),
            ordinary::Input::Release(release) => nexus::NexusAction::CommandSemaWrite(
                sema::SemaWriteInput::ReleaseContainedRun(release.into_payload()),
            ),
            ordinary::Input::Query(selection) => {
                // A (ByContainedRun …) selection reads the durable test-run table;
                // every other selection reads the generation set. Routing here
                // keeps one Query verb covering both read planes (report 54).
                match selection.into_payload() {
                    ordinary::Selection::ByContainedRun(lookup) => {
                        nexus::NexusAction::CommandSemaRead(
                            sema::SemaReadInput::QueryContainedRuns(lookup),
                        )
                    }
                    ordinary::Selection::ByClusterRun(lookup) => {
                        nexus::NexusAction::CommandSemaRead(sema::SemaReadInput::QueryClusterRuns(
                            lookup,
                        ))
                    }
                    selection => nexus::NexusAction::CommandSemaRead(
                        sema::SemaReadInput::QueryGenerations(selection),
                    ),
                }
            }
            ordinary::Input::CheckHostKeyMaterial(query) => nexus::NexusAction::CommandSemaRead(
                sema::SemaReadInput::CheckKeyMaterial(query.into_payload()),
            ),
            ordinary::Input::WatchDeployments(_) | ordinary::Input::WatchCacheRetention(_) => {
                self.open_subscription()
            }
            ordinary::Input::Unwatch(close) => self.close_subscription(close.into_payload()),
        }
    }

    fn decide_contained_cluster_run(
        &mut self,
        request: ordinary::RunContainedClusterRequest,
    ) -> nexus::NexusAction {
        let Some(defaults) = self.configuration.test_defaults() else {
            return Self::reply_ordinary(ordinary::Output::RunContainedClusterRejected(
                ordinary::RunContainedClusterRejected::new(self.cluster_rejection(
                    ordinary::RunContainedClusterRejectionReason::InternalError,
                )),
            ));
        };
        let identifier = ordinary::ClusterRunIdentifier::new(
            self.store.next_cluster_run_identifier().unwrap_or(1),
        );
        let first_member_identifier = self.store.next_contained_run_identifier().unwrap_or(1);
        let plan = match ContainedClusterPlan::from_request(
            defaults,
            identifier,
            first_member_identifier,
            request,
        ) {
            Ok(plan) => plan,
            Err(reason) => {
                return Self::reply_ordinary(ordinary::Output::RunContainedClusterRejected(
                    ordinary::RunContainedClusterRejected::new(self.cluster_rejection(reason)),
                ));
            }
        };
        let member_records = plan.verified_member_records(self.configuration.criome_gate());
        for record in member_records.iter().cloned() {
            if !matches!(
                self.record_contained_run(record),
                sema::SemaWriteOutput::ContainedRunRecorded(_)
            ) {
                return Self::reply_ordinary(ordinary::Output::RunContainedClusterRejected(
                    ordinary::RunContainedClusterRejected::new(self.cluster_rejection(
                        ordinary::RunContainedClusterRejectionReason::InternalError,
                    )),
                ));
            }
        }
        nexus::NexusAction::CommandSemaWrite(sema::SemaWriteInput::RecordClusterRun(
            plan.verified_record(&member_records),
        ))
    }

    fn decide_contained_deploy(
        &mut self,
        request: ordinary::DeployContainedRequest,
    ) -> nexus::NexusAction {
        match self.resolve_and_validate(request) {
            Ok(run) => {
                self.active_operation = Some(MetaOperation::Test);
                let identifier = ordinary::ContainedRunIdentifier::new(
                    self.store.next_contained_run_identifier().unwrap_or(1),
                );
                self.active_test = Some(TestPipeline::accepted(run.clone(), identifier.clone()));
                nexus::NexusAction::CommandSemaWrite(sema::SemaWriteInput::RecordContainedRun(
                    run.pending_record(identifier),
                ))
            }
            Err(reason) => Self::reply_ordinary(ordinary::Output::DeployContainedRejected(
                ordinary::DeployContainedRejected::new(self.test_rejection(reason)),
            )),
        }
    }

    fn verify_contained_run(&self, check: ordinary::ContainedVerification) -> sema::SemaReadOutput {
        let marker = Self::marker(self.store.commit_sequence().unwrap_or(0));
        let identifier = check.contained_run_identifier.clone();
        let run = self.store.contained_runs().ok().and_then(|runs| {
            runs.into_iter()
                .find(|run| run.contained_run_identifier == identifier)
        });
        match run {
            Some(run) => sema::SemaReadOutput::ContainedVerificationPrepared(
                sema::ContainedVerificationPlan {
                    contained_run_record: ordinary::ContainedRunRecord::from(run),
                    contained_verification: check,
                },
            ),
            None => sema::SemaReadOutput::ContainedVerificationRejected(
                ordinary::RejectedContainedVerification {
                    contained_verification_rejection_reason:
                        ordinary::ContainedVerificationRejectionReason::ContainedRunUnknown,
                    database_marker: marker,
                },
            ),
        }
    }

    fn release_contained_run(
        &mut self,
        release: ordinary::ContainedRelease,
    ) -> sema::SemaWriteOutput {
        let marker = Self::marker(self.store.commit_sequence().unwrap_or(0));
        let identifier = release.into_payload();
        let known = self.store.contained_runs().is_ok_and(|runs| {
            runs.into_iter()
                .any(|run| run.contained_run_identifier == identifier)
        });
        if known {
            sema::SemaWriteOutput::ContainedRunReleased(ordinary::AppliedContainedRelease {
                contained_run_identifier: identifier,
                released: true,
                database_marker: marker,
            })
        } else {
            sema::SemaWriteOutput::ContainedReleaseRejected(ordinary::RejectedRelease {
                release_rejection_reason: ordinary::ReleaseRejectionReason::ContainedRunUnknown,
                database_marker: marker,
            })
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
                let request = request.into_payload();
                if let Some(reason) = Self::unsupported_deploy_reason(&request) {
                    return Self::reply_meta(meta::Output::DeployRejected(
                        meta::DeployRejected::new(self.deploy_rejection(reason)),
                    ));
                }
                self.active_operation = Some(MetaOperation::Deploy);
                let submission = match request {
                    meta::DeployRequest::System(deployment) => {
                        sema::DeploySubmission::System(deployment)
                    }
                    meta::DeployRequest::Home(deployment) => {
                        sema::DeploySubmission::Home(deployment)
                    }
                };
                nexus::NexusAction::CommandSemaWrite(sema::SemaWriteInput::RecordDeploySubmitted(
                    submission,
                ))
            }
            meta::Input::Pin(request) => {
                self.active_operation = Some(MetaOperation::Pin);
                nexus::NexusAction::CommandSemaWrite(sema::SemaWriteInput::PinGeneration(
                    request.into_payload(),
                ))
            }
            meta::Input::Unpin(request) => {
                self.active_operation = Some(MetaOperation::Unpin);
                nexus::NexusAction::CommandSemaWrite(sema::SemaWriteInput::UnpinGeneration(
                    request.into_payload(),
                ))
            }
            meta::Input::Retire(request) => {
                self.active_operation = Some(MetaOperation::Retire);
                nexus::NexusAction::CommandSemaWrite(sema::SemaWriteInput::RetireGeneration(
                    request.into_payload(),
                ))
            }
        }
    }

    fn resolve_and_validate(
        &self,
        request: ordinary::DeployContainedRequest,
    ) -> std::result::Result<ResolvedContainedRun, ordinary::DeployContainedRejectionReason> {
        let defaults = self
            .configuration
            .test_defaults()
            .ok_or(ordinary::DeployContainedRejectionReason::InternalError)?;
        let resolved = defaults.lower(request);
        if !matches!(resolved.target, ordinary::ContainedTarget::HermeticVm) {
            return Err(match resolved.target {
                ordinary::ContainedTarget::EphemeralDroplet(_) => {
                    ordinary::DeployContainedRejectionReason::SubstrateUnavailable
                }
                ordinary::ContainedTarget::VmHostGuest(_)
                | ordinary::ContainedTarget::HermeticVm => {
                    ordinary::DeployContainedRejectionReason::SubstrateUnavailable
                }
            });
        }
        if let Some(projection) = ClusterProjection::from_source(&resolved.proposal_source) {
            projection.validate_host_for_node(&resolved.host, &resolved.node)?;
        }
        Ok(resolved)
    }

    fn test_rejection(
        &self,
        reason: ordinary::DeployContainedRejectionReason,
    ) -> ordinary::RejectedDeployContained {
        ordinary::RejectedDeployContained {
            deploy_contained_rejection_reason: reason,
            database_marker: Self::marker(self.store.commit_sequence().unwrap_or(0)),
        }
    }

    fn cluster_rejection(
        &self,
        reason: ordinary::RunContainedClusterRejectionReason,
    ) -> ordinary::RejectedContainedClusterRun {
        ordinary::RejectedContainedClusterRun {
            run_contained_cluster_rejection_reason: reason,
            database_marker: Self::marker(self.store.commit_sequence().unwrap_or(0)),
        }
    }

    fn verification_rejection(
        &self,
        reason: ordinary::ContainedVerificationRejectionReason,
    ) -> ordinary::RejectedContainedVerification {
        ordinary::RejectedContainedVerification {
            contained_verification_rejection_reason: reason,
            database_marker: Self::marker(self.store.commit_sequence().unwrap_or(0)),
        }
    }

    /// The deploy reject-guard. Production System/Home eval/build are
    /// implemented through Horizon materialization, and the activating actions
    /// (System Boot/Switch/Test/BootOnce, Home Profile/Activate) now construct
    /// target-safe copy + activate commands (S4a), so every declared action is
    /// supported and enters the effect pipeline. `UnsupportedDeployAction`
    /// stays in the enum for honesty on any future not-yet-implemented shape;
    /// no current action returns it.
    fn unsupported_deploy_reason(
        request: &meta::DeployRequest,
    ) -> Option<meta::DeployRejectionReason> {
        match request {
            meta::DeployRequest::System(deployment) => {
                let supported = matches!(
                    deployment.system_action,
                    ordinary::SystemAction::Eval
                        | ordinary::SystemAction::Build
                        | ordinary::SystemAction::Boot
                        | ordinary::SystemAction::Switch
                        | ordinary::SystemAction::Test
                        | ordinary::SystemAction::BootOnce
                );
                (!supported).then_some(meta::DeployRejectionReason::UnsupportedDeployAction)
            }
            meta::DeployRequest::Home(deployment) => {
                let supported = matches!(
                    deployment.home_mode,
                    meta::HomeMode::Build | meta::HomeMode::Profile | meta::HomeMode::Activate
                );
                (!supported).then_some(meta::DeployRejectionReason::UnsupportedDeployAction)
            }
        }
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
            sema::SemaReadOutput::ContainedRunsQueried(listing) => {
                ordinary::Output::ContainedRunsQueried(ordinary::ContainedRunsQueried::new(listing))
            }
            sema::SemaReadOutput::ClusterRunsQueried(listing) => {
                ordinary::Output::ClusterRunsQueried(ordinary::ClusterRunsQueried::new(listing))
            }
            sema::SemaReadOutput::ContainedVerificationPrepared(plan) => {
                let pipeline = VerificationPipeline::new(plan);
                let command = pipeline.gate_command();
                self.active_verification = Some(pipeline);
                return nexus::NexusAction::CommandEffect(
                    nexus::EffectCommand::VerifyContainedGate(command),
                );
            }
            sema::SemaReadOutput::ContainedVerified(report) => {
                ordinary::Output::ContainedVerified(ordinary::ContainedVerified::new(report))
            }
            sema::SemaReadOutput::ContainedVerificationRejected(rejected) => {
                ordinary::Output::VerifyContainedRejected(ordinary::VerifyContainedRejected::new(
                    rejected,
                ))
            }
            sema::SemaReadOutput::EventLogRead(_) => {
                ordinary::Output::QueryRejected(ordinary::QueryRejected::new(
                    self.query_rejection(ordinary::QueryRejectionReason::MalformedSelector),
                ))
            }
            sema::SemaReadOutput::ReadMissed(report) => ordinary::Output::QueryRejected(
                ordinary::QueryRejected::new(ordinary::RejectedQuery {
                    query_rejection_reason: ordinary::QueryRejectionReason::GenerationUnknown,
                    database_marker: Self::marker(report.marker.commit_sequence.into_payload()),
                }),
            ),
        };
        nexus::NexusAction::ReplyToSignal(nexus::SignalOutput::OrdinaryOutput(reply))
    }

    fn query_rejection(&self, reason: ordinary::QueryRejectionReason) -> ordinary::RejectedQuery {
        let commit_sequence = self.store.commit_sequence().unwrap_or(0);
        ordinary::RejectedQuery {
            query_rejection_reason: reason,
            database_marker: Self::marker(commit_sequence),
        }
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
            sema::SemaWriteOutput::ContainedRunRecorded(accepted) => {
                if self.active_verification.is_some() {
                    return self.finish_verification_recording(accepted);
                }
                // The accepted SUBMIT reply. `active_operation` is cleared (the
                // synchronous submit is done) but `active_test` stays set: the
                // decoupled executor (`drive_submitted_test`) re-enters to run
                // the real dispatch and rewrite the row to a terminal outcome.
                self.active_operation = None;
                Self::reply_ordinary(ordinary::Output::ContainedDeployed(
                    ordinary::ContainedDeployed::new(accepted),
                ))
            }
            sema::SemaWriteOutput::ClusterRunRecorded(report) => Self::reply_ordinary(
                ordinary::Output::ContainedClusterRan(ordinary::ContainedClusterRan::new(report)),
            ),
            sema::SemaWriteOutput::ContainedRunReleased(released) => Self::reply_ordinary(
                ordinary::Output::Released(ordinary::Released::new(released)),
            ),
            sema::SemaWriteOutput::ContainedReleaseRejected(rejected) => Self::reply_ordinary(
                ordinary::Output::ReleaseRejected(ordinary::ReleaseRejected::new(rejected)),
            ),
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
    /// pass. A non-hermetic contained target is rejected at submit with
    /// `SubstrateUnavailable`, so this honest live terminal is the belt to that
    /// submit-time gate.
    fn decide_test_effect_completion(&mut self, result: nexus::EffectResult) -> nexus::NexusAction {
        let Some(pipeline) = self.active_test.clone() else {
            return Self::reply_ordinary(ordinary::Output::DeployContainedRejected(
                ordinary::DeployContainedRejected::new(
                    self.test_rejection(ordinary::DeployContainedRejectionReason::InternalError),
                ),
            ));
        };
        match result {
            nexus::EffectResult::HermeticCheckBuilt(built) => {
                // Real nix build succeeded: the out-path is the realised check
                // closure. Record Completed/Passed with it — the durable proof.
                self.record_test_terminal(
                    &pipeline,
                    ordinary::ContainedRunPhase::Completed,
                    ordinary::ContainedOutcome::Passed,
                    Some(built.closure_path),
                )
            }
            nexus::EffectResult::TestVmBroughtUp(_) => {
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
            nexus::EffectResult::TestVmTornDown(_) => {
                self.record_container(&pipeline, sema::ContainerState::Stopped);
                // Honest LIVE terminal (report 54 Unit 2b fix 1): the bring-up →
                // teardown bracket ran, but the deploy-into-VM + assert chain
                // between them is not yet implemented, so nothing was asserted.
                // Record `Failed(Assert)`, never `Passed` — a pass must be
                // earned by a real assertion. Belt to the submit-time
                // `SubstrateUnavailable` reject: a non-hermetic run never
                // reaches this arm today, and if a future caller drives a live
                // bracket before the assert lands it still cannot fake a pass.
                self.record_test_terminal(
                    &pipeline,
                    ordinary::ContainedRunPhase::Failed,
                    ordinary::ContainedOutcome::Failed(ordinary::FailureStage::Assert),
                    None,
                )
            }
            nexus::EffectResult::EffectFailed(failure) => self.fail_test_pipeline(failure),
            // No other effect result belongs to a test dispatch; treat it as an
            // internal invariant failure rather than a misleading pass.
            _ => self.fail_test_pipeline(nexus::EffectFailure {
                stage: nexus::EffectStage::HermeticCheck,
                detail: "unexpected effect result on the test pipeline".to_string(),
            }),
        }
    }

    /// Write the terminal durable test-run row (phase + outcome + closure) and
    /// reply the terminal `Tested`/`TestRejected`. Clears the in-flight test
    /// cursor. The row is rewritten in place (keyed by run identifier), so a
    /// `(Query (ByContainedRun …))` reads the terminal outcome — closing the
    /// silent-daemon observability gap (report 54 §5.3).
    fn record_test_terminal(
        &mut self,
        pipeline: &TestPipeline,
        phase: ordinary::ContainedRunPhase,
        outcome: ordinary::ContainedOutcome,
        closure_path: Option<ordinary::ClosurePath>,
    ) -> nexus::NexusAction {
        let record = pipeline.record_at(phase, outcome, closure_path);
        let output = self.record_contained_run(record);
        self.active_operation = None;
        self.active_test = None;
        match output {
            sema::SemaWriteOutput::ContainedRunRecorded(accepted) => Self::reply_ordinary(
                ordinary::Output::ContainedDeployed(ordinary::ContainedDeployed::new(accepted)),
            ),
            _ => Self::reply_ordinary(ordinary::Output::DeployContainedRejected(
                ordinary::DeployContainedRejected::new(
                    self.test_rejection(ordinary::DeployContainedRejectionReason::InternalError),
                ),
            )),
        }
    }

    fn decide_verification_effect_completion(
        &mut self,
        result: nexus::EffectResult,
    ) -> nexus::NexusAction {
        match result {
            nexus::EffectResult::ContainedGateVerified(verdict) => {
                self.record_verification_terminal(verdict)
            }
            nexus::EffectResult::EffectFailed(failure) => {
                let verdict = nexus::GateVerificationVerdict {
                    contained_run_identifier: self
                        .active_verification
                        .as_ref()
                        .map(|pipeline| pipeline.run.contained_run_identifier.clone())
                        .unwrap_or_else(|| ordinary::ContainedRunIdentifier::new(0)),
                    passed: false,
                    detail: failure.detail,
                };
                self.record_verification_terminal(verdict)
            }
            _ => {
                let verdict = nexus::GateVerificationVerdict {
                    contained_run_identifier: self
                        .active_verification
                        .as_ref()
                        .map(|pipeline| pipeline.run.contained_run_identifier.clone())
                        .unwrap_or_else(|| ordinary::ContainedRunIdentifier::new(0)),
                    passed: false,
                    detail: "unexpected effect result on the verification pipeline".to_string(),
                };
                self.record_verification_terminal(verdict)
            }
        }
    }

    fn record_verification_terminal(
        &mut self,
        verdict: nexus::GateVerificationVerdict,
    ) -> nexus::NexusAction {
        let Some(mut pipeline) = self.active_verification.clone() else {
            return Self::reply_ordinary(ordinary::Output::VerifyContainedRejected(
                ordinary::VerifyContainedRejected::new(self.verification_rejection(
                    ordinary::ContainedVerificationRejectionReason::InternalError,
                )),
            ));
        };
        pipeline.run.contained_run_phase = if verdict.passed {
            ordinary::ContainedRunPhase::Completed
        } else {
            ordinary::ContainedRunPhase::Failed
        };
        pipeline.run.contained_outcome = if verdict.passed {
            ordinary::ContainedOutcome::Passed
        } else {
            ordinary::ContainedOutcome::Failed(ordinary::FailureStage::Assert)
        };
        nexus::NexusAction::CommandSemaWrite(sema::SemaWriteInput::RecordContainedRun(pipeline.run))
    }

    fn finish_verification_recording(
        &mut self,
        accepted: ordinary::AcceptedContainedDeploy,
    ) -> nexus::NexusAction {
        let identifier = accepted.contained_run_identifier.clone();
        self.active_verification = None;
        let Some(run) = self.store.contained_runs().ok().and_then(|runs| {
            runs.into_iter()
                .find(|run| run.contained_run_identifier == identifier)
        }) else {
            return Self::reply_ordinary(ordinary::Output::VerifyContainedRejected(
                ordinary::VerifyContainedRejected::new(self.verification_rejection(
                    ordinary::ContainedVerificationRejectionReason::InternalError,
                )),
            ));
        };
        Self::reply_ordinary(ordinary::Output::ContainedVerified(
            ordinary::ContainedVerified::new(ordinary::ContainedVerificationReport {
                contained_run_identifier: run.contained_run_identifier,
                contained_run_phase: run.phase,
                contained_outcome: run.outcome,
                database_marker: accepted.database_marker,
            }),
        ))
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
            "lojix test pipeline effect failed at {:?}: {}",
            failure.stage, failure.detail
        );
        let stage = Self::test_failure_stage(failure.stage);
        let pipeline =
            match self.active_test.clone() {
                Some(pipeline) => pipeline,
                None => {
                    return Self::reply_ordinary(ordinary::Output::DeployContainedRejected(
                        ordinary::DeployContainedRejected::new(self.test_rejection(
                            ordinary::DeployContainedRejectionReason::InternalError,
                        )),
                    ));
                }
            };
        self.record_test_terminal(
            &pipeline,
            ordinary::ContainedRunPhase::Failed,
            ordinary::ContainedOutcome::Failed(stage),
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

    fn begin_deploy_pipeline(&mut self, accepted: meta::AcceptedDeploy) -> nexus::NexusAction {
        let pipeline = match self.active_deploy.as_ref() {
            Some(pipeline) => pipeline.clone(),
            None => return Self::reply_meta(meta::Output::Deployed(meta::Deployed::new(accepted))),
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
                    pipeline.nix_eval_command(self.configuration.as_ref()),
                ))
            }
            DeployStage::BuildingRecorded => {
                // The closure path is captured on the cursor by `set_closure_path`
                // during eval/build; an activating pipeline that reached this stage
                // without a closure is an internal invariant failure, not an empty
                // activation (risk R2). Fail the pipeline rather than activate "".
                let closure_path = match pipeline.closure_path.clone() {
                    Some(closure_path) => closure_path,
                    None => {
                        return self.fail_pipeline(nexus::EffectFailure {
                            stage: nexus::EffectStage::Activate,
                            detail: "activation reached without a built closure path".to_string(),
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
                            stage: nexus::EffectStage::Activate,
                            detail: "activation record reached without a built closure path"
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
        self.active_operation = None;
        // The deploy reached a terminal success: its live generation is
        // committed, so drop the in-flight job row — only deploys that still
        // need resuming stay in the mirror (up9q).
        if let Some(pipeline) = self.active_deploy.as_ref() {
            self.retire_job_row(&pipeline.clone());
        }
        let accepted = match self.active_deploy.take() {
            Some(pipeline) => meta::AcceptedDeploy {
                deployment_identifier: pipeline.deployment_identifier,
                database_marker: pipeline.accepted_marker,
            },
            None => meta::AcceptedDeploy {
                deployment_identifier: ordinary::DeploymentIdentifier::new(0),
                database_marker: Self::marker(self.store.commit_sequence().unwrap_or(0)),
            },
        };
        Self::reply_meta(meta::Output::Deployed(meta::Deployed::new(accepted)))
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
        let marker = Self::marker(report.marker.commit_sequence.into_payload());
        let output = match operation {
            MetaOperation::Deploy => {
                meta::Output::DeployRejected(meta::DeployRejected::new(meta::RejectedDeploy {
                    deploy_rejection_reason: Self::deploy_reason(report.reason),
                    database_marker: marker,
                }))
            }
            MetaOperation::Pin => {
                meta::Output::PinRejected(meta::PinRejected::new(meta::RejectedPin {
                    pin_rejection_reason: Self::pin_reason(report.reason),
                    database_marker: marker,
                }))
            }
            MetaOperation::Unpin => {
                meta::Output::UnpinRejected(meta::UnpinRejected::new(meta::RejectedUnpin {
                    unpin_rejection_reason: Self::unpin_reason(report.reason),
                    database_marker: marker,
                }))
            }
            MetaOperation::Retire => {
                meta::Output::RetireRejected(meta::RetireRejected::new(meta::RejectedRetire {
                    retire_rejection_reason: Self::retire_reason(report.reason),
                    database_marker: marker,
                }))
            }
            MetaOperation::Test => {
                return Self::reply_ordinary(ordinary::Output::DeployContainedRejected(
                    ordinary::DeployContainedRejected::new(ordinary::RejectedDeployContained {
                        deploy_contained_rejection_reason: Self::contained_reason(report.reason),
                        database_marker: marker,
                    }),
                ));
            }
        };
        Self::reply_meta(output)
    }

    fn contained_reason(reason: sema::RejectionReason) -> ordinary::DeployContainedRejectionReason {
        match reason {
            sema::RejectionReason::ClusterUnknown => {
                ordinary::DeployContainedRejectionReason::ClusterUnknown
            }
            sema::RejectionReason::NodeUnknown => {
                ordinary::DeployContainedRejectionReason::NodeUnknown
            }
            _ => ordinary::DeployContainedRejectionReason::InternalError,
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

    fn deploy_rejection(&self, reason: meta::DeployRejectionReason) -> meta::RejectedDeploy {
        meta::RejectedDeploy {
            deploy_rejection_reason: reason,
            database_marker: Self::marker(self.store.commit_sequence().unwrap_or(0)),
        }
    }

    fn reply_meta(output: meta::Output) -> nexus::NexusAction {
        nexus::NexusAction::ReplyToSignal(nexus::SignalOutput::MetaOutput(output))
    }

    fn reply_ordinary(output: ordinary::Output) -> nexus::NexusAction {
        nexus::NexusAction::ReplyToSignal(nexus::SignalOutput::OrdinaryOutput(output))
    }

    // ---- decide: effect completion (drives the deploy chain) ------------

    fn decide_effect_completion(&mut self, result: nexus::EffectResult) -> nexus::NexusAction {
        if self.active_verification.is_some() {
            return self.decide_verification_effect_completion(result);
        }
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
            nexus::EffectResult::FlakeResolved(_) => {
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
                self.set_input_overrides(inputs.into_payload().into_payload());
                self.record_phase(ordinary::DeploymentPhase::Building, None)
            }
            nexus::EffectResult::ClosureEvaluated(evaluated) => {
                self.set_closure_path(evaluated.closure_path.clone());
                if pipeline.action.produces_closure() {
                    nexus::NexusAction::CommandEffect(nexus::EffectCommand::NixBuild(
                        pipeline
                            .nix_build_command(evaluated.closure_path, self.configuration.as_ref()),
                    ))
                } else {
                    // System `Eval`: the derivation path is the result — finish
                    // the pipeline without building.
                    self.finish_deploy_pipeline()
                }
            }
            nexus::EffectResult::ClosureBuilt(built) => {
                self.set_closure_path(built.closure_path.clone());
                if pipeline.action.activates() {
                    nexus::NexusAction::CommandEffect(nexus::EffectCommand::CopyClosure(
                        pipeline
                            .copy_closure_command(built.closure_path, self.configuration.as_ref()),
                    ))
                } else {
                    // Non-activating action (`Build`): the closure is realised —
                    // finish without copy/activate (which remain addressing-
                    // incomplete; that is the M2/M3 deploy work).
                    self.finish_deploy_pipeline()
                }
            }
            nexus::EffectResult::ClosureCopied(_) => {
                // Record Copying (stage BuildingRecorded). The phase write hops
                // back through advance_after_phase, which fires ActivateGeneration.
                self.record_phase(ordinary::DeploymentPhase::Copying, None)
            }
            nexus::EffectResult::GenerationActivated(_) => {
                // Record Activated (stage CopyingRecorded). The phase write hops
                // back through advance_after_phase, which fires the
                // RecordGenerationActivated write that commits the live set.
                self.record_phase(ordinary::DeploymentPhase::Activated, None)
            }
            nexus::EffectResult::PathsCollected(_) => self.finish_deploy_pipeline(),
            // The test-dispatch effect results never reach the DEPLOY effect
            // router — `drive_submitted_test` routes them through
            // `decide_test_effect_completion`. One arriving here is an internal
            // invariant failure, surfaced as a deploy failure rather than a
            // misleading success.
            nexus::EffectResult::HermeticCheckBuilt(_)
            | nexus::EffectResult::TestVmBroughtUp(_)
            | nexus::EffectResult::TestVmTornDown(_)
            | nexus::EffectResult::ContainedGateVerified(_) => {
                self.fail_pipeline(nexus::EffectFailure {
                    stage: nexus::EffectStage::Build,
                    detail: "test effect result on the deploy pipeline".to_string(),
                })
            }
            nexus::EffectResult::EffectFailed(failure) => self.fail_pipeline(failure),
        }
    }

    fn set_closure_path(&mut self, closure_path: ordinary::ClosurePath) {
        if let Some(pipeline) = self.active_deploy.as_mut() {
            pipeline.closure_path = Some(closure_path);
        }
    }

    fn set_input_overrides(&mut self, overrides: Vec<nexus::FlakeInputOverride>) {
        if let Some(pipeline) = self.active_deploy.as_mut() {
            pipeline.input_overrides = overrides;
        }
    }

    fn record_phase(
        &mut self,
        phase: ordinary::DeploymentPhase,
        detail: Option<ordinary::PhaseDetail>,
    ) -> nexus::NexusAction {
        let event = match self.active_deploy.as_ref() {
            Some(pipeline) => {
                let position = self.store.next_event_log_position().unwrap_or(0);
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
    fn persist_job_phase(&self, phase: sema::DeployJobPhase) {
        if let Some(pipeline) = self.active_deploy.as_ref() {
            let _ = self.store.upsert_deploy_job(pipeline.deploy_job(phase));
        }
    }

    /// Drop the durable in-flight job row for the active deploy (a terminal
    /// transition: the deploy finished and committed its live generation, so it
    /// no longer needs resuming). Best-effort, like `persist_job_phase`.
    fn retire_job_row(&self, pipeline: &DeployPipeline) {
        let _ = self
            .store
            .retract_deploy_job(*pipeline.deployment_identifier.payload());
    }

    fn fail_pipeline(&mut self, failure: nexus::EffectFailure) -> nexus::NexusAction {
        eprintln!(
            "lojix deploy pipeline effect failed at {:?}: {}",
            failure.stage, failure.detail
        );
        // Mark the durable job row Failed before clearing the cursor (up9q): a
        // restarted daemon reads Failed and does not re-attempt — the deploy is
        // terminal. The event log already carries the failed deployment.
        self.persist_job_phase(sema::DeployJobPhase::Failed);
        // Clear BOTH in-flight slots symmetrically with the finish path (audit
        // R5) — a mid-pipeline effect failure must not leak `active_operation`.
        self.active_deploy = None;
        self.active_operation = None;
        let reason = match failure.stage {
            nexus::EffectStage::FlakeAuth => meta::DeployRejectionReason::ProposalSourceUnreachable,
            nexus::EffectStage::MaterializeHorizon => {
                meta::DeployRejectionReason::ProposalSourceUnreachable
            }
            nexus::EffectStage::Eval => meta::DeployRejectionReason::FlakeReferenceMalformed,
            nexus::EffectStage::Build => meta::DeployRejectionReason::FlakeReferenceMalformed,
            nexus::EffectStage::CopyClosure => meta::DeployRejectionReason::BuilderUnreachable,
            nexus::EffectStage::Activate => meta::DeployRejectionReason::BuilderUnreachable,
            nexus::EffectStage::Gc => meta::DeployRejectionReason::DeploymentInFlight,
            // The test-only effect stages never reach the DEPLOY pipeline's
            // failure path (`fail_test_pipeline` owns them); an internal
            // invariant failure rather than a misleading deploy reason.
            nexus::EffectStage::HermeticCheck
            | nexus::EffectStage::BringUpTestVm
            | nexus::EffectStage::TearDownTestVm
            | nexus::EffectStage::VerifyContainedGate => meta::DeployRejectionReason::InternalError,
        };
        Self::reply_meta(meta::Output::DeployRejected(meta::DeployRejected::new(
            self.deploy_rejection(reason),
        )))
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
            sema::SemaWriteInput::RecordContainedRun(record) => self.record_contained_run(record),
            sema::SemaWriteInput::RecordClusterRun(record) => self.record_cluster_run(record),
            sema::SemaWriteInput::ReleaseContainedRun(release) => {
                self.release_contained_run(release)
            }
        }
    }

    /// Persist one accepted test-run row (phase Submitted / outcome Pending)
    /// and reply the `AcceptedTest` handle. Mirrors `record_deploy_submitted`:
    /// the row is durable from acceptance, so a `(Query (ByContainedRun …))` reads
    /// it immediately and a restarted daemon reconciles the in-flight test
    /// (Unit 2b). Unit 2a writes exactly this Pending row — no faked pass.
    fn record_contained_run(
        &mut self,
        record: ordinary::ContainedRunRecord,
    ) -> sema::SemaWriteOutput {
        let identifier = record.contained_run_identifier.clone();
        match self
            .store
            .upsert_contained_run(sema::StoredContainedRun::from(record))
            .and_then(|()| self.store.commit_sequence())
        {
            Ok(commit_sequence) => {
                sema::SemaWriteOutput::ContainedRunRecorded(ordinary::AcceptedContainedDeploy {
                    contained_run_identifier: identifier,
                    database_marker: Self::marker(commit_sequence),
                })
            }
            Err(_) => Self::write_rejected(0, sema::RejectionReason::NodeUnknown),
        }
    }

    fn record_cluster_run(&mut self, record: ordinary::ClusterRunRecord) -> sema::SemaWriteOutput {
        let report = ordinary::ClusterRunReport {
            cluster_run_identifier: record.cluster_run_identifier.clone(),
            cluster_run_phase: record.cluster_run_phase,
            cluster_outcome: record.cluster_outcome.clone(),
            database_marker: Self::marker(self.store.commit_sequence().unwrap_or(0)),
        };
        match self
            .store
            .upsert_cluster_run(sema::StoredClusterRun::from(record))
            .and_then(|()| self.store.commit_sequence())
        {
            Ok(commit_sequence) => {
                sema::SemaWriteOutput::ClusterRunRecorded(ordinary::ClusterRunReport {
                    database_marker: Self::marker(commit_sequence),
                    ..report
                })
            }
            Err(_) => Self::write_rejected(0, sema::RejectionReason::ClusterUnknown),
        }
    }

    fn record_deploy_submitted(
        &mut self,
        submission: sema::DeploySubmission,
    ) -> sema::SemaWriteOutput {
        // Submission issues the deployment + generation identifiers and opens
        // the pipeline; the durable live-set / gc-roots write happens only at
        // activation. The identifiers are issued from the persisted maxima
        // (restart-safe), and the marker reflects the current commit sequence —
        // no row is written here, so the engine's commit counter does not move.
        let identifiers = (
            self.store.commit_sequence(),
            self.store.next_deployment_identifier(),
            self.store.next_generation_identifier(),
        );
        match identifiers {
            (Ok(commit_sequence), Ok(deployment_identifier), Ok(generation_identifier)) => {
                let accepted_marker = Self::marker(commit_sequence);
                self.active_deploy = Some(DeployPipeline::from_submission(
                    deployment_identifier.into(),
                    generation_identifier.into(),
                    accepted_marker.clone(),
                    submission,
                ));
                // Persist the in-flight job row at Submitted (up9q): from this
                // point the deploy is durably recorded and resumable across a
                // daemon restart, independent of the connection that submitted
                // it and of the job actor that will drive the pipeline.
                self.persist_job_phase(sema::DeployJobPhase::Submitted);
                sema::SemaWriteOutput::DeploySubmitted(meta::AcceptedDeploy {
                    deployment_identifier: deployment_identifier.into(),
                    database_marker: accepted_marker,
                })
            }
            _ => Self::write_rejected(0, sema::RejectionReason::PlanNotApproved),
        }
    }

    fn record_phase_transition(
        &mut self,
        event: ordinary::DeploymentPhaseEvent,
    ) -> sema::SemaWriteOutput {
        let recorded_phase = event.deployment_phase;
        let recorded = self.store.next_event_log_position().and_then(|position| {
            self.store
                .append_event_log_entry(sema::EventLogEntry {
                    event_log_position: ordinary::EventLogPosition::new(position),
                    record: sema::LoggedEvent::Deployment(event),
                })
                .and_then(|()| self.store.commit_sequence())
                .map(|commit_sequence| (position, commit_sequence))
        });
        match recorded {
            Ok((event_log_position, commit_sequence)) => {
                // Mirror the just-committed phase onto the durable job row so a
                // restarted daemon reads the latest phase the deploy reached
                // (up9q). The event-log write above is authoritative; this keeps
                // the resume convenience row in step.
                self.persist_job_phase(sema::DeployJobPhase::from(recorded_phase));
                sema::SemaWriteOutput::PhaseRecorded(sema::PhaseReceipt {
                    event_log_position: ordinary::EventLogPosition::new(event_log_position),
                    state_marker: Self::sema_marker(commit_sequence),
                })
            }
            Err(_) => Self::write_rejected(0, sema::RejectionReason::PlanNotApproved),
        }
    }

    fn record_generation_activated(
        &mut self,
        commit: sema::ActivationCommit,
    ) -> sema::SemaWriteOutput {
        let pipeline = self.active_deploy.clone();
        let deployment_identifier = pipeline
            .as_ref()
            .map(|p| p.deployment_identifier.clone())
            .unwrap_or_else(|| ordinary::DeploymentIdentifier::new(0));
        let deployment_kind = pipeline
            .as_ref()
            .map(|p| p.deployment_kind)
            .unwrap_or(ordinary::DeploymentKind::FullOs);
        let activation_kind = pipeline
            .as_ref()
            .map(|p| p.activation_kind)
            .unwrap_or(ordinary::ActivationKind::Switch);
        let generation = sema::LiveGeneration {
            deployment_identifier,
            generation_identifier: commit.generation_identifier.clone(),
            cluster_name: commit.cluster_name.clone(),
            node_name: commit.node_name.clone(),
            deployment_kind,
            activation_kind,
            generation_slot: commit.generation_slot,
            closure_path: commit.closure_path.clone(),
        };
        let root = sema::GcRoot {
            generation_identifier: commit.generation_identifier.clone(),
            cluster_name: commit.cluster_name.clone(),
            node_name: commit.node_name.clone(),
            generation_slot: commit.generation_slot,
            closure_path: commit.closure_path.clone(),
            label: None.into(),
        };
        // The live-set row and the gc-root row are written as TWO sequential
        // keyed asserts (inside `Store::record_activation`). A `CommitRequest`
        // is single-table, so true cross-table atomicity is not available; the
        // sequential write is the accepted baseline. The keyed asserts are
        // fail-safe (a duplicate key errors, never clobbers), but a crash
        // between them leaves a torn write with no reopen reconciliation —
        // cross-table atomicity needs a sema-engine multi-table commit.
        let recorded = self
            .store
            .record_activation(generation, root)
            .and_then(|()| self.store.commit_sequence());
        match recorded {
            Ok(commit_sequence) => {
                sema::SemaWriteOutput::GenerationActivated(sema::AppliedActivation {
                    generation_identifier: commit.generation_identifier,
                    generation_slot: commit.generation_slot,
                    state_marker: Self::sema_marker(commit_sequence),
                })
            }
            Err(_) => Self::write_rejected(0, sema::RejectionReason::PlanNotApproved),
        }
    }

    fn pin_generation(&mut self, request: meta::PinRequest) -> sema::SemaWriteOutput {
        let roots = match self.store.gc_roots() {
            Ok(roots) => roots,
            Err(_) => return Self::write_rejected(0, sema::RejectionReason::GenerationUnknown),
        };
        let current_sequence = self.store.commit_sequence().unwrap_or(0);
        let already_used = roots
            .iter()
            .any(|root| root.label.payload().as_ref() == Some(&request.pin_label));
        if already_used {
            return Self::write_rejected(current_sequence, sema::RejectionReason::PinLabelInUse);
        }
        let Some(mut root) = roots.into_iter().find(|root| {
            root.generation_identifier == request.generation_identifier
                && root.cluster_name == request.production_node.cluster_name
                && root.node_name == request.production_node.node_name
        }) else {
            return Self::write_rejected(
                current_sequence,
                sema::RejectionReason::GenerationUnknown,
            );
        };
        let from_slot = root.generation_slot;
        root.generation_slot = ordinary::GenerationSlot::Pinned;
        root.label = Some(request.pin_label.clone()).into();
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
                database_marker: Self::marker(commit_sequence),
            }),
            Err(_) => {
                Self::write_rejected(current_sequence, sema::RejectionReason::GenerationUnknown)
            }
        }
    }

    fn unpin_generation(&mut self, request: meta::UnpinRequest) -> sema::SemaWriteOutput {
        let roots = match self.store.gc_roots() {
            Ok(roots) => roots,
            Err(_) => return Self::write_rejected(0, sema::RejectionReason::PinLabelUnknown),
        };
        let current_sequence = self.store.commit_sequence().unwrap_or(0);
        let Some(mut root) = roots.into_iter().find(|root| {
            root.label.payload().as_ref() == Some(&request.pin_label)
                && root.cluster_name == request.production_node.cluster_name
                && root.node_name == request.production_node.node_name
        }) else {
            return Self::write_rejected(current_sequence, sema::RejectionReason::PinLabelUnknown);
        };
        let generation_identifier = root.generation_identifier.clone();
        let from_slot = root.generation_slot;
        root.generation_slot = ordinary::GenerationSlot::Recent;
        root.label = None.into();
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
                database_marker: Self::marker(commit_sequence),
            }),
            Err(_) => {
                Self::write_rejected(current_sequence, sema::RejectionReason::PinLabelUnknown)
            }
        }
    }

    fn retire_generation(&mut self, request: meta::RetireRequest) -> sema::SemaWriteOutput {
        let roots = match self.store.gc_roots() {
            Ok(roots) => roots,
            Err(_) => return Self::write_rejected(0, sema::RejectionReason::GenerationUnknown),
        };
        let current_sequence = self.store.commit_sequence().unwrap_or(0);
        let Some(root) = roots.into_iter().find(|root| {
            root.generation_identifier == request.generation_identifier
                && root.cluster_name == request.production_node.cluster_name
                && root.node_name == request.production_node.node_name
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
                from_slot: root.generation_slot,
                database_marker: Self::marker(commit_sequence),
            }),
            Err(_) => {
                Self::write_rejected(current_sequence, sema::RejectionReason::GenerationUnknown)
            }
        }
    }

    fn record_container_transition(
        &mut self,
        transition: sema::ContainerTransition,
    ) -> sema::SemaWriteOutput {
        let recorded = self.store.next_event_log_position().and_then(|position| {
            let record = sema::ContainerLifecycleRecord {
                cluster_name: transition.cluster_name,
                node_name: transition.node_name,
                container: transition.container,
                state: transition.state,
                event_log_position: ordinary::EventLogPosition::new(position),
            };
            let entry = sema::EventLogEntry {
                event_log_position: ordinary::EventLogPosition::new(position),
                record: sema::LoggedEvent::Container(record.clone()),
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
            Err(_) => Self::write_rejected(0, sema::RejectionReason::NodeUnknown),
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
            reason,
            marker: Self::sema_marker(commit_sequence),
        })
    }

    fn observe_sema(&self, input: sema::SemaReadInput) -> sema::SemaReadOutput {
        match input {
            sema::SemaReadInput::QueryGenerations(selection) => self.query_generations(selection),
            sema::SemaReadInput::ReadEventLog(range) => self.read_event_log(range),
            sema::SemaReadInput::CheckKeyMaterial(query) => self.check_key_material(query),
            sema::SemaReadInput::QueryContainedRuns(lookup) => self.query_contained_runs(lookup),
            sema::SemaReadInput::QueryClusterRuns(lookup) => self.query_cluster_runs(lookup),
            sema::SemaReadInput::VerifyContainedRun(check) => self.verify_contained_run(check),
        }
    }

    /// Answer a `(ByContainedRun …)` query from the durable test-run table (report
    /// 54 §5.3). Filters by cluster + node, and by run identifier when the
    /// lookup names one (`None` returns every run for that node). The matching
    /// rows are returned newest-first by run identifier so the routine
    /// `(Check …)` reader sees its latest run first.
    fn query_contained_runs(&self, lookup: ordinary::ContainedRunLookup) -> sema::SemaReadOutput {
        let runs = match self.store.contained_runs() {
            Ok(runs) => runs,
            Err(_) => return Self::read_missed(0, sema::RejectionReason::NodeUnknown),
        };
        let commit_sequence = self.store.commit_sequence().unwrap_or(0);
        let mut matching: Vec<sema::StoredContainedRun> = runs
            .into_iter()
            .filter(|run| Self::contained_run_matches(&lookup, run))
            .collect();
        matching.sort_by(|left, right| {
            right
                .contained_run_identifier
                .payload()
                .cmp(left.contained_run_identifier.payload())
        });
        sema::SemaReadOutput::ContainedRunsQueried(ordinary::ContainedRunListing {
            runs: matching
                .into_iter()
                .map(ordinary::ContainedRunRecord::from)
                .collect::<Vec<_>>()
                .into(),
            database_marker: Self::marker(commit_sequence),
        })
    }

    fn contained_run_matches(
        lookup: &ordinary::ContainedRunLookup,
        run: &sema::StoredContainedRun,
    ) -> bool {
        lookup.cluster_name == run.cluster_name
            && lookup.node_name == run.node_name
            && lookup
                .run
                .payload()
                .as_ref()
                .is_none_or(|identifier| identifier == &run.contained_run_identifier)
    }

    fn query_cluster_runs(&self, lookup: ordinary::ClusterRunLookup) -> sema::SemaReadOutput {
        let cluster_runs = match self.store.cluster_runs() {
            Ok(runs) => runs,
            Err(_) => return Self::read_missed(0, sema::RejectionReason::ClusterUnknown),
        };
        let contained_runs = match self.store.contained_runs() {
            Ok(runs) => runs,
            Err(_) => return Self::read_missed(0, sema::RejectionReason::NodeUnknown),
        };
        let commit_sequence = self.store.commit_sequence().unwrap_or(0);
        let mut matching: Vec<sema::StoredClusterRun> = cluster_runs
            .into_iter()
            .filter(|run| Self::cluster_run_matches(&lookup, run))
            .collect();
        matching.sort_by(|left, right| {
            right
                .cluster_run_identifier
                .payload()
                .cmp(left.cluster_run_identifier.payload())
        });
        let member_identifiers: Vec<ordinary::ContainedRunIdentifier> = matching
            .iter()
            .flat_map(|run| run.member_runs.payload().iter().cloned())
            .collect();
        let member_records = contained_runs
            .into_iter()
            .filter(|run| member_identifiers.contains(&run.contained_run_identifier))
            .map(ordinary::ContainedRunRecord::from)
            .collect::<Vec<_>>();
        sema::SemaReadOutput::ClusterRunsQueried(ordinary::ClusterRunListing {
            cluster_runs: matching
                .into_iter()
                .map(ordinary::ClusterRunRecord::from)
                .collect::<Vec<_>>()
                .into(),
            runs: member_records.into(),
            database_marker: Self::marker(commit_sequence),
        })
    }

    fn cluster_run_matches(
        lookup: &ordinary::ClusterRunLookup,
        run: &sema::StoredClusterRun,
    ) -> bool {
        lookup.cluster_name == run.cluster_name
            && lookup
                .cluster_run
                .payload()
                .as_ref()
                .is_none_or(|identifier| identifier == &run.cluster_run_identifier)
    }

    fn query_generations(&self, selection: ordinary::Selection) -> sema::SemaReadOutput {
        let matching = self
            .store
            .matching_live_generations(|live| Self::generation_matches(&selection, live));
        let live_generations = match matching {
            Ok(live_generations) => live_generations,
            Err(_) => return Self::read_missed(0, sema::RejectionReason::GenerationUnknown),
        };
        let commit_sequence = self.store.commit_sequence().unwrap_or(0);
        let generations: Vec<ordinary::Generation> = live_generations
            .iter()
            .map(Self::project_generation)
            .collect();
        sema::SemaReadOutput::GenerationsQueried(ordinary::GenerationListing {
            generations: generations.into(),
            database_marker: Self::marker(commit_sequence),
        })
    }

    fn generation_matches(selection: &ordinary::Selection, live: &sema::LiveGeneration) -> bool {
        match selection {
            ordinary::Selection::ByNode(selector) => {
                selector.cluster_name == live.cluster_name
                    && selector.node_name == live.node_name
                    && selector
                        .kind
                        .payload()
                        .as_ref()
                        .is_none_or(|kind| kind == &live.deployment_kind)
            }
            ordinary::Selection::ByGeneration(lookup) => {
                *lookup.payload() == live.generation_identifier
            }
            ordinary::Selection::ByEventLog(_) => true,
            // A test-run selection never reads the generation set — it is
            // routed to QueryContainedRuns before reaching here (decide_ordinary_input).
            ordinary::Selection::ByContainedRun(_) => false,
            ordinary::Selection::ByClusterRun(_) => false,
        }
    }

    fn project_generation(live: &sema::LiveGeneration) -> ordinary::Generation {
        ordinary::Generation {
            generation_identifier: live.generation_identifier.clone(),
            deployment_identifier: live.deployment_identifier.clone(),
            cluster_name: live.cluster_name.clone(),
            node_name: live.node_name.clone(),
            deployment_kind: live.deployment_kind,
            activation_kind: live.activation_kind,
            generation_slot: live.generation_slot,
            closure_path: live.closure_path.clone(),
        }
    }

    fn read_event_log(&self, range: ordinary::EventLogRange) -> sema::SemaReadOutput {
        let entries = match self
            .store
            .event_log_in_range(*range.from.payload(), *range.until.payload())
        {
            Ok(entries) => entries,
            Err(_) => {
                return Self::read_missed(0, sema::RejectionReason::EventLogPositionOutOfRange);
            }
        };
        let commit_sequence = self.store.commit_sequence().unwrap_or(0);
        let mut deployment_events = Vec::new();
        let mut retention_events = Vec::new();
        for entry in &entries {
            match &entry.record {
                sema::LoggedEvent::Deployment(event) => deployment_events.push(event.clone()),
                sema::LoggedEvent::CacheRetention(event) => retention_events.push(event.clone()),
                sema::LoggedEvent::Container(_) => {}
            }
        }
        sema::SemaReadOutput::EventLogRead(sema::EventLogPage {
            deployment_events: deployment_events.into(),
            retention_events: retention_events.into(),
            state_marker: Self::sema_marker(commit_sequence),
        })
    }

    fn check_key_material(&self, query: ordinary::KeyMaterialQuery) -> sema::SemaReadOutput {
        let commit_sequence = self.store.commit_sequence().unwrap_or(0);
        sema::SemaReadOutput::KeyMaterialChecked(ordinary::KeyMaterialReport {
            node_name: query.node_name,
            mismatches: Vec::new().into(),
            database_marker: Self::marker(commit_sequence),
        })
    }

    /// Build a read-miss at a known commit sequence. Like `write_rejected`,
    /// this never re-locks; the caller supplies the sequence.
    fn read_missed(commit_sequence: u64, reason: sema::RejectionReason) -> sema::SemaReadOutput {
        sema::SemaReadOutput::ReadMissed(sema::RejectionReport {
            reason,
            marker: Self::sema_marker(commit_sequence),
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
        // Resolve the flake metadata to a locked revision through the proposal
        // source. `nix flake metadata --json <flake>` reports the resolved ref.
        match NixCommand::flake_metadata(request.flake_reference.payload())
            .run()
            .await
        {
            Ok(output) => nexus::EffectResult::FlakeResolved(nexus::ResolvedFlake {
                flake_reference: request.flake_reference,
                revision: NixCommand::first_line(&output),
            }),
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
            command.attribute.payload()
        );
        match NixCommand::eval_drv_path(&attribute, command.overrides.payload(), &command.target)
            .run()
            .await
        {
            Ok(output) => nexus::EffectResult::ClosureEvaluated(nexus::EvaluatedClosure {
                generation_identifier: ordinary::GenerationIdentifier::new(0),
                closure_path: ordinary::ClosurePath::new(NixCommand::first_line(&output)),
            }),
            Err(detail) => Self::effect_failed(nexus::EffectStage::Eval, detail),
        }
    }

    async fn run_nix_build(&self, command: nexus::NixBuildCommand) -> nexus::EffectResult {
        // Honoring the dropped local-build guard `783n`: a `BuildTarget::Local`
        // builds on the local dispatcher (no remote builder); `Remote` dispatches
        // the build to the named builder machine; `TargetStore` realizes the
        // closure directly in the target node's own store over `ssh-ng`, so a
        // model-bearing node closure never transits the daemon host (Spirit
        // ufjd / 0a9p / lc28, report 150). All run the same `nix build` shape;
        // `Remote` adds the daemon machine file and `TargetStore` adds the
        // `--store <uri>` redirect ALONE (NO `--eval-store auto`) so eval and
        // build both operate on the target store.
        let invocation = match &command.target {
            nexus::BuildTarget::Local => NixCommand::build_closure(
                command.closure_path.payload(),
                command.substituters.payload(),
            ),
            nexus::BuildTarget::Remote(_) => NixCommand::build_closure_remote(
                command.closure_path.payload(),
                command.substituters.payload(),
            ),
            nexus::BuildTarget::TargetStore(store) => NixCommand::build_closure_in_store(
                command.closure_path.payload(),
                store.payload(),
                command.substituters.payload(),
            ),
        };
        match invocation.run().await {
            Ok(output) => nexus::EffectResult::ClosureBuilt(nexus::BuiltClosure {
                generation_identifier: command.generation_identifier,
                closure_path: ordinary::ClosurePath::new(NixCommand::first_line_or(
                    &output,
                    command.closure_path.payload(),
                )),
            }),
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
            // A build-on-target build already realized the closure in the target
            // node's own store (Spirit lc28, report 150), so the copy is a no-op:
            // report the closure copied without opening an `ssh-ng` transfer.
            Ok(None) => return copied,
            Ok(Some(copy)) => copy,
            Err(detail) => return Self::effect_failed(nexus::EffectStage::CopyClosure, detail),
        };
        match copy.run().await {
            Ok(()) => copied,
            Err(detail) => Self::effect_failed(nexus::EffectStage::CopyClosure, detail),
        }
    }

    async fn run_activate_generation(
        &self,
        command: nexus::ActivateGenerationCommand,
    ) -> nexus::EffectResult {
        let slot = Self::activation_slot(&command.activation_kind);
        let activation =
            match Activation::from_command(&command, Some(self.configuration.daemon_host())) {
                Ok(activation) => activation,
                Err(detail) => return Self::effect_failed(nexus::EffectStage::Activate, detail),
            };
        match activation.run().await {
            Ok(()) => nexus::EffectResult::GenerationActivated(nexus::ActivatedGeneration {
                generation_identifier: command.generation_identifier,
                node_name: command.node_name,
                generation_slot: slot,
            }),
            Err(detail) => Self::effect_failed(nexus::EffectStage::Activate, detail),
        }
    }

    fn activation_slot(activation_kind: &ordinary::ActivationKind) -> ordinary::GenerationSlot {
        match activation_kind {
            ordinary::ActivationKind::Switch => ordinary::GenerationSlot::Current,
            ordinary::ActivationKind::Boot => ordinary::GenerationSlot::BootPending,
            ordinary::ActivationKind::Test => ordinary::GenerationSlot::Recent,
            ordinary::ActivationKind::BootOnce => ordinary::GenerationSlot::BootPending,
        }
    }

    async fn run_path_info_gc(&self, command: nexus::PathInfoGcCommand) -> nexus::EffectResult {
        match NixCommand::collect_garbage(command.node_name.payload())
            .run()
            .await
        {
            Ok(output) => nexus::EffectResult::PathsCollected(nexus::GarbageCollected {
                cluster_name: command.cluster_name,
                node_name: command.node_name,
                reclaimed_paths: NixCommand::count_lines(&output),
            }),
            Err(detail) => Self::effect_failed(nexus::EffectStage::Gc, detail),
        }
    }

    /// The REAL hermetic check effect (Unit 2b): build
    /// `<flake>#checks.<system>.vm-<node> --print-out-paths`. Exit 0 + an
    /// out-path → `HermeticCheckBuilt` carrying the realised check closure;
    /// a non-zero exit → `EffectFailed(HermeticCheck)`. The `runNixOSTest`
    /// engine owns its own sandboxed VM, so this is a pure build with zero host
    /// effect. NEVER fakes a pass — the outcome IS the nix-build result.
    async fn run_hermetic_check(
        &self,
        command: nexus::HermeticCheckCommand,
    ) -> nexus::EffectResult {
        let cluster_name = command.cluster_name.clone();
        let node_name = command.node_name.clone();
        match HermeticCheck::new(command).run().await {
            Ok(closure_path) => nexus::EffectResult::HermeticCheckBuilt(nexus::CheckBuilt {
                cluster_name,
                node_name,
                closure_path,
            }),
            Err(detail) => Self::effect_failed(nexus::EffectStage::HermeticCheck, detail),
        }
    }

    async fn run_gate_verification(
        &self,
        command: nexus::GateVerificationCommand,
    ) -> nexus::EffectResult {
        nexus::EffectResult::ContainedGateVerified(
            GateVerification::new(command).run(self.configuration.criome_gate()),
        )
    }

    /// The LIVE bring-up effect (Unit 2b — report 47 v2 / report 51 §3). BUILT
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
        let bring_up = LiveTestVm::from_bring_up(&command);
        // The invocation is CONSTRUCTED (the host-untouched user-namespace
        // command) but not executed — a live run is gated. Constructing it
        // proves the command shape; `invocation()` is the on-host effect a live
        // run would `.run().await`.
        let _invocation = bring_up.bring_up_invocation();
        nexus::EffectResult::TestVmBroughtUp(nexus::TestVmBroughtUp {
            cluster_name: command.cluster_name,
            node_name: command.node_name,
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
        let tear_down = LiveTestVm::from_tear_down(&command);
        let _invocation = tear_down.tear_down_invocation();
        nexus::EffectResult::TestVmTornDown(nexus::TestVmTornDown {
            cluster_name: command.cluster_name,
            node_name: command.node_name,
            host: command.host,
        })
    }

    fn effect_failed(stage: nexus::EffectStage, detail: String) -> nexus::EffectResult {
        nexus::EffectResult::EffectFailed(nexus::EffectFailure { stage, detail })
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
        let proposal =
            ProjectableProposal::from(ProposalFile::new(&self.command.proposal_source).load()?);
        let viewpoint = HorizonViewpoint::from_command(&self.command)?;
        let horizon = proposal.project(&viewpoint)?;
        let root = MaterializationRoot::new(self.configuration.materialization_root(&self.command));
        root.prepare()?;
        let secrets_source =
            ClusterSecretsDirectory::from_proposal_source(&self.command.proposal_source);
        MaterializedInputSet::new(root, horizon, self.command.shape.clone(), secrets_source)
            .write()
            .await
    }
}

/// The cluster proposal file path carried by `signal-lojix::ProposalSource`.
#[derive(Debug, Clone)]
struct ProposalFile {
    path: PathBuf,
}

impl ProposalFile {
    fn new(source: &ordinary::ProposalSource) -> Self {
        Self {
            path: PathBuf::from(source.payload()),
        }
    }

    fn load(&self) -> Result<ClusterProposal> {
        let text = fs::read_to_string(&self.path)?;
        Ok(NotaSource::new(&text).parse()?)
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

/// Projection wrapper: today's production Horizon shape still has separate
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

    async fn write(&self) -> Result<nexus::MaterializedInputs> {
        let mut inputs = Vec::new();
        inputs.push(
            self.root
                .input_directory(GeneratedInputName::Horizon)
                .write_horizon(&self.horizon)?
                .to_override(GeneratedInputName::Horizon)
                .await?,
        );
        inputs.push(
            self.root
                .input_directory(GeneratedInputName::System)
                .write_system(&self.horizon.node.system)?
                .to_override(GeneratedInputName::System)
                .await?,
        );
        if let Some(deployment) = DeploymentInput::from_shape(&self.shape) {
            inputs.push(
                self.root
                    .input_directory(GeneratedInputName::Deployment)
                    .write_deployment(&deployment)?
                    .to_override(GeneratedInputName::Deployment)
                    .await?,
            );
        }
        inputs.push(
            self.root
                .input_directory(GeneratedInputName::Secrets)
                .write_secrets(&self.secrets_source)?
                .to_override(GeneratedInputName::Secrets)
                .await?,
        );
        Ok(nexus::MaterializedInputs::new(inputs.into()))
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

    async fn to_override(&self, name: GeneratedInputName) -> Result<nexus::FlakeInputOverride> {
        let hash = NarHash::from_path(&self.path).await?;
        Ok(nexus::FlakeInputOverride {
            name: name.as_str().to_string(),
            reference: nexus::FlakeInputReference {
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
            nexus::MaterializationShape::FullOs => Some(Self {
                include_home: true,
                include_all_firmware: true,
            }),
            nexus::MaterializationShape::OsOnly => Some(Self {
                include_home: false,
                include_all_firmware: false,
            }),
            nexus::MaterializationShape::Home(_) => None,
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

    /// The bare file name (`routerWifiSaePasswords.sops`) used both for the copy
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
    /// camelCase consumer name (`routerWifiSaePasswords.sops`), so the attribute
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
    async fn from_path(path: &Path) -> Result<Self> {
        let output = NixCommand::hash_path(path).run().await.map_err(|detail| {
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

/// SSH target — `<user>@<node>.<cluster>.criome` — the addressing used by
/// `ssh`, `nix copy --to ssh-ng://…`, and `--from ssh-ng://…` (ported from
/// `lojix-cli/src/host.rs`). Activate/copy address `root@<criome_domain>`,
/// NEVER a bare `NodeName`; the domain is derived from the cluster + node on
/// the deploy cursor via `CriomeDomainName::for_node` (resolving open question
/// Q1: the address is computed from cursor fields already present —
/// `<node>.<cluster>.criome` — not threaded as a new horizon-projection field).
#[derive(Debug, Clone, PartialEq, Eq)]
struct SshTarget {
    user: String,
    domain: CriomeDomainName,
}

impl SshTarget {
    /// Resolve `root@<node>.<cluster>.criome` from the ordinary cluster + node
    /// names carried on the cursor. Errors only if the names fail horizon
    /// validation (empty / quotation mark), which a submitted deploy never has.
    fn root_at_node(
        cluster_name: &ordinary::ClusterName,
        node_name: &ordinary::NodeName,
    ) -> std::result::Result<Self, String> {
        Ok(Self {
            user: "root".to_string(),
            domain: Self::criome_domain(cluster_name, node_name)?,
        })
    }

    fn criome_domain(
        cluster_name: &ordinary::ClusterName,
        node_name: &ordinary::NodeName,
    ) -> std::result::Result<CriomeDomainName, String> {
        let cluster = HorizonClusterName::try_new(cluster_name.payload().clone())
            .map_err(|error| format!("invalid cluster name for ssh target: {error}"))?;
        let node = HorizonNodeName::try_new(node_name.payload().clone())
            .map_err(|error| format!("invalid node name for ssh target: {error}"))?;
        Ok(CriomeDomainName::for_node(&node, &cluster))
    }

    fn with_user(&self, user: &HorizonUserName) -> Self {
        Self {
            user: user.as_str().to_string(),
            domain: self.domain.clone(),
        }
    }

    fn ssh_uri(&self) -> String {
        format!("ssh-ng://{}@{}", self.user, self.domain.as_str())
    }

    fn as_ssh_arg(&self) -> String {
        format!("{}@{}", self.user, self.domain.as_str())
    }

    /// `ssh -o BatchMode=yes <user>@<domain> <remote_command>`.
    fn remote_invocation(&self, remote_command: ShellCommand) -> NixCommand {
        NixCommand::new(
            "ssh",
            vec![
                "-o".to_string(),
                "BatchMode=yes".to_string(),
                self.as_ssh_arg(),
                remote_command.into_text(),
            ],
        )
    }
}

/// A pre-quoted remote shell command body — the single string ssh runs on the
/// target. Ported from `lojix-cli/src/process.rs::ShellCommand`.
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
/// `lojix-cli/src/process.rs::ShellArgument`: bare when the text is wholly
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

/// Move a closure from the dispatcher store to the activation target. Always
/// passes `--substitute-on-destination` so the target pulls signed paths from
/// the cluster cache when available; unsigned daemon-to-daemon transfer is
/// rejected under `require-sigs` (risk R6). Remote builds use the configured
/// Nix machine file and copy their result back into the dispatcher store, so
/// the copy command never opens a direct root SSH source to the builder.
#[derive(Debug, Clone, PartialEq, Eq)]
struct ClosureCopy {
    store_path: String,
    target: SshTarget,
}

impl ClosureCopy {
    /// `Ok(None)` when the build already realized the closure in the target
    /// node's own store (`BuildTarget::TargetStore`, Spirit lc28 / report 150) —
    /// the closure is already on the target, so there is nothing to copy.
    /// `Ok(Some(copy))` for `Local` / `Remote` builds whose result lives in the
    /// dispatcher store and must be pushed to the target.
    fn from_command(
        command: &nexus::CopyClosureCommand,
    ) -> std::result::Result<Option<Self>, String> {
        if matches!(command.source, nexus::BuildTarget::TargetStore(_)) {
            return Ok(None);
        }
        let target = SshTarget::root_at_node(&command.cluster_name, &command.node_name)?;
        Ok(Some(Self {
            store_path: command.closure_path.payload().clone(),
            target,
        }))
    }

    /// Copy is idempotent: if the closure already exists on the target, Nix
    /// exits successfully without changing the activation state.
    fn invocation(&self) -> NixCommand {
        let arguments: Vec<String> = vec![
            "copy".to_string(),
            "--substitute-on-destination".to_string(),
            "--to".to_string(),
            self.target.ssh_uri(),
            self.store_path.clone(),
        ];
        NixCommand::new("nix", arguments)
    }

    async fn run(&self) -> std::result::Result<(), String> {
        self.invocation().run().await.map(|_| ())
    }
}

/// One activation on the target node — System (switch-to-configuration +
/// optional EFI reconcile / BootOnce transient unit) or Home (home-manager
/// profile/activate). Decoded from the `ActivateGenerationCommand`; ported
/// from `lojix-cli/src/activate.rs`.
#[derive(Debug, Clone)]
enum Activation {
    System(SystemActivation),
    Home(HomeActivation),
}

impl Activation {
    /// `daemon_host` is the node the dispatching daemon runs on, so a System
    /// activation can detect a self-targeting deploy and route around the
    /// self-Switch deadlock; `None` when no daemon host context is available.
    fn from_command(
        command: &nexus::ActivateGenerationCommand,
        daemon_host: Option<&ordinary::NodeName>,
    ) -> std::result::Result<Self, String> {
        let target = SshTarget::root_at_node(&command.cluster_name, &command.node_name)?;
        let store_path = command.closure_path.payload().clone();
        match &command.profile {
            nexus::ActivationProfile::System(action) => Ok(Self::System(SystemActivation {
                deployment_identifier: command.deployment_identifier.clone(),
                target,
                node_name: command.node_name.clone(),
                daemon_host: daemon_host.cloned(),
                store_path,
                action: *action,
            })),
            nexus::ActivationProfile::Home(profile) => {
                let user = HorizonUserName::try_new(profile.user.payload().clone())
                    .map_err(|error| format!("invalid user name for home activation: {error}"))?;
                Ok(Self::Home(HomeActivation {
                    node_name: command.node_name.clone(),
                    target,
                    user,
                    store_path,
                    mode: profile.mode,
                }))
            }
        }
    }

    async fn run(&self) -> std::result::Result<(), String> {
        match self {
            Self::System(activation) => activation.run().await,
            Self::Home(activation) => activation.run().await,
        }
    }
}

/// System activation on the target (ported from
/// `lojix-cli/src/activate.rs::SystemActivation`).
///
/// `Boot`/`Switch`/`Test`: one ssh call running `switch-to-configuration
/// <action>` directly (Boot/Switch first set the system profile). `BootOnce`:
/// one ssh call wrapping the boot-once script in `systemd-run --unit=<name>
/// --collect --wait --service-type=oneshot /bin/sh -c '…'` — owned by PID 1,
/// not the dispatcher's ssh, so a network blip that kills the ssh leaves the
/// unit running on the target to completion.
#[derive(Debug, Clone)]
struct SystemActivation {
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
    action: ordinary::SystemAction,
}

impl SystemActivation {
    /// Invocation for the simple Boot/Switch/Test path. `None` for the
    /// non-simple actions (BootOnce uses `systemd_run_invocation`; Eval/Build
    /// do not activate).
    fn ssh_invocation(&self) -> Option<NixCommand> {
        let action_word = match self.action {
            ordinary::SystemAction::Boot => "boot",
            ordinary::SystemAction::Switch => "switch",
            ordinary::SystemAction::Test => "test",
            ordinary::SystemAction::BootOnce
            | ordinary::SystemAction::Eval
            | ordinary::SystemAction::Build => return None,
        };
        let store = &self.store_path;
        let remote_command = if matches!(self.action, ordinary::SystemAction::Test) {
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
    /// (the rollback target) is read from `bootctl status`'s `Current Entry`
    /// (the running generation), `NEW` is derived from the system profile's
    /// `readlink` (canonical latest). reboot 1 lands NEW, reboot 2+ returns to
    /// OLD — headless-safe rollback (`boot_once_script()`).
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
             SYSTEM_LINK=$(readlink /nix/var/nix/profiles/system)\n\
             GENERATION=$(echo \"$SYSTEM_LINK\" | sed -E 's/^system-([0-9]+)-link$/\\1/')\n\
             NEW=\"nixos-generation-$GENERATION.conf\"\n\
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
        matches!(self.action, ordinary::SystemAction::Switch)
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
    /// foreground path's `ssh_invocation`, NOT a boot-once entry — then the EFI
    /// reconcile (`bootctl set-default` the running generation, clear any stale
    /// one-shot) the foreground Switch path runs via `reconcile_efi`. The whole
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
             SYSTEM_LINK=$(readlink /nix/var/nix/profiles/system)\n\
             GENERATION=$(echo \"$SYSTEM_LINK\" | sed -E 's/^system-([0-9]+)-link$/\\1/')\n\
             ENTRY=\"nixos-generation-$GENERATION.conf\"\n\
             [ -f \"/boot/loader/entries/$ENTRY\" ]\n\
             bootctl set-default \"$ENTRY\"\n\
             bootctl set-oneshot ''\n"
        )
    }

    /// Whether this action reconciles EFI bootloader vars after activation.
    /// `Boot`/`Switch` write `loader.conf`'s default but not EFI's
    /// `LoaderEntryDefault`; reconcile claims it explicitly and clears any
    /// stale one-shot. `Test` is non-persistent; `BootOnce` is its own thing
    /// (`requires_efi_reconcile()`).
    fn requires_efi_reconcile(&self) -> bool {
        matches!(
            self.action,
            ordinary::SystemAction::Boot | ordinary::SystemAction::Switch
        )
    }

    /// `readlink /nix/var/nix/profiles/system` — stdout parsed via
    /// `SystemProfileLink` to derive the `nixos-generation-N.conf` entry.
    fn step_readlink_system_profile_invocation(&self) -> NixCommand {
        self.target.remote_invocation(ShellCommand::from_raw(
            "readlink /nix/var/nix/profiles/system",
        ))
    }

    /// `bootctl set-default <entry>` — points EFI `LoaderEntryDefault` at the
    /// just-installed generation.
    fn step_set_efi_default_invocation(&self, entry: &BootEntry) -> NixCommand {
        self.target
            .remote_invocation(ShellCommand::from_raw(format!(
                "bootctl set-default {}",
                entry.as_str()
            )))
    }

    /// `bootctl set-oneshot ''` — clears any pending EFI one-shot from a prior
    /// BootOnce so it does not hijack the next reboot.
    fn step_clear_efi_oneshot_invocation(&self) -> NixCommand {
        self.target
            .remote_invocation(ShellCommand::from_raw("bootctl set-oneshot ''"))
    }

    async fn run(&self) -> std::result::Result<(), String> {
        if self.runs_detached_self_switch() {
            return self.run_self_switch().await;
        }
        match self.action {
            ordinary::SystemAction::BootOnce => self.run_boot_once().await,
            _ => self.run_simple().await,
        }
    }

    /// Deadlock-free self-Switch: run the full Switch activation inside a
    /// PID-1-owned transient unit (the BootOnce mechanism, carrying Switch
    /// semantics) so `switch-to-configuration switch` restarting the dispatching
    /// daemon does not kill the activation's foreground ssh (report 150).
    async fn run_self_switch(&self) -> std::result::Result<(), String> {
        let unit_name = self.self_switch_unit_name();
        self.detached_invocation(&unit_name, self.self_switch_script())
            .run()
            .await
            .map(|_| ())
    }

    async fn run_simple(&self) -> std::result::Result<(), String> {
        match self.ssh_invocation() {
            Some(invocation) => invocation.run().await.map(|_| ())?,
            None => {
                return Err(format!("no simple activation for action {:?}", self.action));
            }
        }
        if self.requires_efi_reconcile() {
            self.reconcile_efi().await?;
        }
        Ok(())
    }

    async fn reconcile_efi(&self) -> std::result::Result<(), String> {
        let output = self.step_readlink_system_profile_invocation().run().await?;
        let link = SystemProfileLink::try_new(output.trim())?;
        let entry = link.generation().boot_entry();
        self.step_set_efi_default_invocation(&entry).run().await?;
        self.step_clear_efi_oneshot_invocation().run().await?;
        Ok(())
    }

    async fn run_boot_once(&self) -> std::result::Result<(), String> {
        let unit_name = self.unit_name();
        self.systemd_run_invocation(&unit_name)
            .run()
            .await
            .map(|_| ())
    }
}

/// Home activation on the target (ported from
/// `lojix-cli/src/activate.rs::HomeActivation`). `Profile`/`Activate` set the
/// home-manager profile as the target user, then `Activate` additionally runs
/// the activation package. Includes the local fast-path: skip ssh entirely
/// when the dispatcher already is the requested user on the target node.
#[derive(Debug, Clone)]
struct HomeActivation {
    node_name: ordinary::NodeName,
    target: SshTarget,
    user: HorizonUserName,
    store_path: String,
    mode: meta::HomeMode,
}

impl HomeActivation {
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

    fn remote_profile_invocation(&self) -> NixCommand {
        self.user_target()
            .remote_invocation(ShellCommand::from_raw(format!(
                "nix-env -p \"$HOME/.local/state/nix/profiles/home-manager\" --set {}",
                ShellArgument::new(self.store_path.clone()).to_command_text(),
            )))
    }

    fn remote_activate_invocation(&self) -> NixCommand {
        self.user_target().remote_invocation(ShellCommand::from_raw(
            ShellArgument::new(format!("{}/activate", self.store_path)).to_command_text(),
        ))
    }

    async fn run(&self) -> std::result::Result<(), String> {
        match self.mode {
            meta::HomeMode::Build => Ok(()),
            meta::HomeMode::Profile => self.run_profile().await,
            meta::HomeMode::Activate => {
                self.run_profile().await?;
                self.run_activate().await
            }
        }
    }

    async fn run_profile(&self) -> std::result::Result<(), String> {
        if !self.is_local_context().await {
            return self.remote_profile_invocation().run().await.map(|_| ());
        }
        let home = std::env::var("HOME")
            .map_err(|_| "HOME is unset for local home activation".to_string())?;
        self.local_profile_invocation(Path::new(&home))
            .run()
            .await
            .map(|_| ())
    }

    async fn run_activate(&self) -> std::result::Result<(), String> {
        if !self.is_local_context().await {
            return self.remote_activate_invocation().run().await.map(|_| ());
        }
        self.local_activate_invocation().run().await.map(|_| ())
    }

    /// The local fast-path predicate: the dispatcher is already the requested
    /// user on the target node, so activation runs locally without ssh.
    async fn is_local_context(&self) -> bool {
        self.current_user().as_deref() == Some(self.user.as_str())
            && self.current_node().await.as_deref() == Some(self.node_name.payload().as_str())
    }

    fn user_target(&self) -> SshTarget {
        self.target.with_user(&self.user)
    }

    fn current_user(&self) -> Option<String> {
        std::env::var("USER")
            .or_else(|_| std::env::var("LOGNAME"))
            .ok()
    }

    async fn current_node(&self) -> Option<String> {
        // The local-context node match compares the dispatcher's short hostname
        // against the deploy cursor's node name (the same comparison lojix-cli
        // makes with `hostname -s`).
        let output = NixCommand::new("hostname", vec!["-s".to_string()])
            .run()
            .await
            .ok()?;
        Some(output.trim().to_string())
    }
}

/// The `system-N-link` symlink target of `/nix/var/nix/profiles/system`, parsed
/// to its generation number (ported from
/// `lojix-cli/src/activate.rs::SystemProfileLink`).
#[derive(Debug, Clone, PartialEq, Eq)]
struct SystemProfileLink {
    generation: SystemGeneration,
}

impl SystemProfileLink {
    fn try_new(link: &str) -> std::result::Result<Self, String> {
        let number = link
            .strip_prefix("system-")
            .and_then(|rest| rest.strip_suffix("-link"))
            .and_then(|number| number.parse::<u64>().ok())
            .ok_or_else(|| format!("invalid system profile link: {link}"))?;
        Ok(Self {
            generation: SystemGeneration(number),
        })
    }

    fn generation(&self) -> SystemGeneration {
        self.generation
    }
}

/// A NixOS system generation number, projecting to its EFI boot-entry name.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct SystemGeneration(u64);

impl SystemGeneration {
    fn boot_entry(self) -> BootEntry {
        BootEntry(format!("nixos-generation-{}.conf", self.0))
    }
}

/// A systemd-boot EFI loader entry filename (`nixos-generation-N.conf`).
#[derive(Debug, Clone, PartialEq, Eq)]
struct BootEntry(String);

impl BootEntry {
    fn as_str(&self) -> &str {
        &self.0
    }
}

/// A typed `nix` / `nix-store` invocation. Holds the program name and its
/// argument vector so the same value can be inspected before it runs; `run`
/// spawns it via `tokio::process::Command` and returns captured stdout or a
/// failure detail string. Constructors model the lojix-cli invocations.
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

    /// Resolve the toplevel `.drvPath` BEFORE the build, INSTANTIATING against
    /// the target store so target-only `.drv`s resolve (Spirit ufjd / 0a9p /
    /// lc28, report 150; verified 2026-06-20). A build-on-target node's config
    /// references model `.drv`s (multi-tens-of-gigabyte GGUFs) that exist ONLY
    /// in the target's own store, so the eval must instantiate there or it fails
    /// with `... .drv does not exist`. For `BuildTarget::TargetStore` the eval
    /// adds `--store <uri>` ALONE — pointing instantiation at the target store —
    /// and deliberately NOT `--eval-store auto`: `--eval-store auto` pins
    /// instantiation local, which is exactly the failure mode (an `--eval-store
    /// auto --store <uri>` eval still cannot find the target-only `.drv`). The
    /// build step (`build_closure_in_store`) now uses the SAME flags —
    /// `--store <uri>` ALONE, NO `--eval-store auto` — so eval and build operate
    /// consistently on the target store: the eval instantiates the toplevel
    /// `.drv` INTO the target store and the build finds and realizes it there.
    /// `Local` (target IS the daemon host) adds no store flags — the host store
    /// already holds everything. `Remote` keeps the eval host-local too (no
    /// store redirect): the remote builder is for the BUILD, not the eval, and
    /// the `.drv` is instantiated against the daemon host's store, matching
    /// `build_closure_remote`. The `--override-input` flags and `.drvPath`
    /// selector are preserved in every case.
    fn eval_drv_path(
        attribute: &str,
        overrides: &[nexus::FlakeInputOverride],
        target: &nexus::BuildTarget,
    ) -> Self {
        let mut arguments = vec![
            "eval".to_string(),
            "--refresh".to_string(),
            "--raw".to_string(),
        ];
        match target {
            nexus::BuildTarget::TargetStore(store) => {
                arguments.extend(Self::store_options(store.payload()));
            }
            nexus::BuildTarget::Local | nexus::BuildTarget::Remote(_) => {}
        }
        arguments.extend(Self::override_input_options(overrides));
        arguments.push(format!("{attribute}.drvPath"));
        Self::new("nix", arguments)
    }

    /// The shared `--store <uri>` argument pair, the one place the store-URI
    /// argument is formatted for nix. Both the eval (instantiate against the
    /// target store) and the build (realize into the target store) reuse it; the
    /// `--eval-store auto` flag differs between them and is added by the caller,
    /// not here.
    fn store_options(store_uri: &str) -> Vec<String> {
        vec!["--store".to_string(), store_uri.to_string()]
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
    /// auto-pickup check (Unit 2b). The installable already names the
    /// `#checks.<system>.vm-<node>` attribute (an `runNixOSTest` derivation
    /// whose realised output IS the check result), so it is passed verbatim —
    /// no `^*` output selector and no `.drvPath` indirection (unlike the deploy
    /// build, which threads a `.drv` path). Exit status IS pass/fail; the
    /// printed line is the realised check out-path.
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

    fn build_closure_remote(closure_path: &str, substituters: &[nexus::ExtraSubstituter]) -> Self {
        let mut arguments = vec![
            "build".to_string(),
            "--no-link".to_string(),
            "--print-out-paths".to_string(),
            "--option".to_string(),
            "max-jobs".to_string(),
            "0".to_string(),
            "--builders".to_string(),
            "@/etc/nix/machines".to_string(),
            Self::output_installable(closure_path),
        ];
        arguments.extend(Self::substituter_options(substituters));
        Self::new("nix", arguments)
    }

    /// Build-on-target (Spirit ufjd / 0a9p / lc28, report 150; verified
    /// 2026-06-20): realize the closure in the TARGET node's own store instead
    /// of the daemon host's. Build-on-target now does EVAL AND BUILD ENTIRELY on
    /// the target store — `--store <uri>` ALONE, NO `--eval-store auto` —
    /// consistent with the eval step (`eval_drv_path`). The eval instantiates
    /// the toplevel `.drv` INTO the target store (0.3.9, `--store <uri>` alone),
    /// so the `.drv` lives in the target store; the build must use the SAME
    /// store to find and realize it. Re-adding `--eval-store auto` makes the
    /// build look in the LOCAL store for the toplevel `.drv` it cannot find
    /// there, reintroducing the live failure `error: path
    /// '/nix/store/...nixos-system-...drv' is not valid`. With `--store <uri>`
    /// alone the drv resolves valid and builds on the target (verified: ~184/185
    /// derivations realized on prometheus). `--store <store_uri>` also directs
    /// REALIZATION — every output path build or substitute, including any
    /// multi-tens-of-gigabyte model NAR — into the target store, so the daemon
    /// host never holds the node's model-bearing closure. `store_uri` is the
    /// `ssh-ng://root@<node>.<cluster>.criome` form (`SshTarget::ssh_uri`). The
    /// `^*` output selector resolves the threaded `.drv` to its realised
    /// outputs, same as the local build. The `--store <uri>` pair comes from the
    /// shared `store_options`, the single place that argument is formatted.
    fn build_closure_in_store(
        closure_path: &str,
        store_uri: &str,
        substituters: &[nexus::ExtraSubstituter],
    ) -> Self {
        let mut arguments = vec![
            "build".to_string(),
            "--no-link".to_string(),
            "--print-out-paths".to_string(),
        ];
        arguments.extend(Self::store_options(store_uri));
        arguments.push(Self::output_installable(closure_path));
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
            arguments.push(override_input.name.clone());
            arguments.push(format!(
                "{}?narHash={}",
                override_input.reference.url, override_input.reference.nix_archive_hash
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

    async fn run(&self) -> std::result::Result<String, String> {
        let output = Command::new(&self.program)
            .args(&self.arguments)
            .output()
            .await
            .map_err(|error| format!("failed to spawn {}: {error}", self.program))?;
        if output.status.success() {
            Ok(String::from_utf8_lossy(&output.stdout).into_owned())
        } else {
            Err(format!(
                "{} {} exited with {}: {}",
                self.program,
                self.arguments.join(" "),
                output.status,
                String::from_utf8_lossy(&output.stderr).trim()
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
            nexus::EffectCommand::VerifyContainedGate(command) => {
                self.run_gate_verification(command).await
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

impl sema::SemaEngine for SchemaRuntime {
    fn apply_inner(
        &mut self,
        input: sema::sema::Sema<sema::sema::WriteInput>,
    ) -> sema::sema::Sema<sema::sema::WriteOutput> {
        let origin_route = input.origin_route();
        self.apply_sema(input.into_root())
            .with_origin_route(origin_route)
    }

    fn observe_inner(
        &self,
        input: sema::sema::Sema<sema::sema::ReadInput>,
    ) -> sema::sema::Sema<sema::sema::ReadOutput> {
        let origin_route = input.origin_route();
        self.observe_sema(input.into_root())
            .with_origin_route(origin_route)
    }
}

#[cfg(test)]
mod tests {
    //! Unit/argv/snapshot tests for the S4a command port: closure-threading
    //! onto the activate command, and the faithful `lojix-cli` command shapes
    //! (`SshTarget` addressing, `ClosureCopy`, `SystemActivation`,
    //! `HomeActivation`). The construction is unit-testable here; the on-node
    //! behavior is proven later at S5 on a live VM.

    use super::*;

    const STORE: &str = "/nix/store/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-toplevel";

    fn system_submission(action: ordinary::SystemAction) -> sema::DeploySubmission {
        sema::DeploySubmission::System(meta::SystemDeployment {
            production_node: meta::ProductionNode {
                cluster_name: ordinary::ClusterName::new("alpha"),
                node_name: ordinary::NodeName::new("node-1"),
            },
            deployment_kind: ordinary::DeploymentKind::OsOnly,
            proposal_source: ordinary::ProposalSource::new("/dev/null"),
            flake_reference: ordinary::FlakeReference::new("github:owner/repo"),
            system_action: action,
            builder_override: None.into(),
            extra_substituters: Vec::new().into(),
            build_attribute: None.into(),
        })
    }

    fn system_pipeline(action: ordinary::SystemAction) -> DeployPipeline {
        DeployPipeline::from_submission(
            ordinary::DeploymentIdentifier::new(1),
            ordinary::GenerationIdentifier::new(1),
            SchemaRuntime::marker(0),
            system_submission(action),
        )
    }

    fn cluster() -> ordinary::ClusterName {
        ordinary::ClusterName::new("alpha")
    }

    fn node() -> ordinary::NodeName {
        ordinary::NodeName::new("node-1")
    }

    // ---- Step 1: closure-threading onto the activate command ----

    #[test]
    fn activate_command_carries_built_closure_path() {
        // The cursor captures the built path via `set_closure_path` (the
        // `ClosureBuilt` arm); `advance_after_phase` at `BuildingRecorded` reads
        // it onto the fired ActivateGeneration command — same non-empty path,
        // never dropped between build and activate (risk R2).
        let mut engine = SchemaRuntime::new();
        engine.active_deploy = Some(system_pipeline(ordinary::SystemAction::Switch));
        let built = ordinary::ClosurePath::new(STORE);
        engine.set_closure_path(built.clone());
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
        engine.active_deploy = Some(system_pipeline(ordinary::SystemAction::Switch));
        engine.set_stage(DeployStage::BuildingRecorded);

        match engine.advance_after_phase() {
            nexus::NexusAction::ReplyToSignal(nexus::SignalOutput::MetaOutput(
                meta::Output::DeployRejected(_),
            )) => {}
            other => panic!("expected DeployRejected, got {other:?}"),
        }
    }

    #[test]
    fn activation_commit_requires_closure_path() {
        let mut pipeline = system_pipeline(ordinary::SystemAction::Switch);
        assert!(pipeline.activation_commit().is_none());
        pipeline.closure_path = Some(ordinary::ClosurePath::new(STORE));
        let commit = pipeline.activation_commit().expect("commit with closure");
        assert_eq!(commit.closure_path.payload(), STORE);
    }

    // ---- Step 2: the reject-guard opens the activating actions ----

    #[test]
    fn guard_accepts_every_declared_action() {
        for action in [
            ordinary::SystemAction::Eval,
            ordinary::SystemAction::Build,
            ordinary::SystemAction::Boot,
            ordinary::SystemAction::Switch,
            ordinary::SystemAction::Test,
            ordinary::SystemAction::BootOnce,
        ] {
            let request = meta::DeployRequest::System(meta::SystemDeployment {
                production_node: meta::ProductionNode {
                    cluster_name: cluster(),
                    node_name: node(),
                },
                deployment_kind: ordinary::DeploymentKind::OsOnly,
                proposal_source: ordinary::ProposalSource::new("/dev/null"),
                flake_reference: ordinary::FlakeReference::new("github:owner/repo"),
                system_action: action,
                builder_override: None.into(),
                extra_substituters: Vec::new().into(),
                build_attribute: None.into(),
            });
            assert!(
                SchemaRuntime::unsupported_deploy_reason(&request).is_none(),
                "System {action:?} should be supported"
            );
        }
        for mode in [
            meta::HomeMode::Build,
            meta::HomeMode::Profile,
            meta::HomeMode::Activate,
        ] {
            let request = meta::DeployRequest::Home(meta::HomeDeployment {
                production_node: meta::ProductionNode {
                    cluster_name: cluster(),
                    node_name: node(),
                },
                user_name: ordinary::UserName::new("li"),
                proposal_source: ordinary::ProposalSource::new("/dev/null"),
                flake_reference: ordinary::FlakeReference::new("github:owner/repo"),
                home_mode: mode,
                builder_override: None.into(),
                extra_substituters: Vec::new().into(),
            });
            assert!(
                SchemaRuntime::unsupported_deploy_reason(&request).is_none(),
                "Home {mode:?} should be supported"
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
    fn remote_build_uses_daemon_machine_file_and_disables_local_fallback() {
        let invocation = NixCommand::build_closure_remote(DERIVATION, &[]);
        assert_eq!(invocation.program(), "nix");
        let argv = invocation.joined_arguments();
        assert!(argv.contains("--builders @/etc/nix/machines"), "{argv}");
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

    // ---- Step 3: SSH addressing — root@<criome_domain>, never bare node ----

    #[test]
    fn ssh_target_addresses_root_at_criome_domain() {
        let target = SshTarget::root_at_node(&cluster(), &node()).expect("target");
        assert_eq!(target.as_ssh_arg(), "root@node-1.alpha.criome");
        assert_eq!(target.ssh_uri(), "ssh-ng://root@node-1.alpha.criome");
    }

    fn copy_command(source: nexus::BuildTarget) -> nexus::CopyClosureCommand {
        nexus::CopyClosureCommand {
            generation_identifier: ordinary::GenerationIdentifier::new(1),
            cluster_name: cluster(),
            node_name: node(),
            closure_path: ordinary::ClosurePath::new(STORE),
            source,
        }
    }

    // ---- Step 3: copy argv — always --substitute-on-destination, target only ----

    #[test]
    fn copy_from_dispatcher_uses_to_only_with_substitute() {
        let copy = ClosureCopy::from_command(&copy_command(nexus::BuildTarget::Local))
            .expect("copy")
            .expect("local build copies from the dispatcher store");
        let invocation = copy.invocation();
        assert_eq!(invocation.program(), "nix");
        let argv = invocation.joined_arguments();
        assert!(argv.contains("--substitute-on-destination"), "{argv}");
        assert!(
            argv.contains("--to ssh-ng://root@node-1.alpha.criome"),
            "{argv}"
        );
        assert!(!argv.contains("--from"), "{argv}");
        assert!(argv.contains(STORE), "{argv}");
    }

    #[test]
    fn copy_remote_builder_still_uses_dispatcher_to_target_copy() {
        let builder = ordinary::NodeName::new("builder-node");
        let source = nexus::BuildTarget::Remote(nexus::BuilderNode::new(builder));
        let copy = ClosureCopy::from_command(&copy_command(source))
            .expect("copy")
            .expect("a remote builder copies its result from the dispatcher store");
        let invocation = copy.invocation();
        let argv = invocation.joined_arguments();
        assert!(argv.contains("--substitute-on-destination"), "{argv}");
        assert!(!argv.contains("--from"), "{argv}");
        assert!(
            argv.contains("--to ssh-ng://root@node-1.alpha.criome"),
            "{argv}"
        );
    }

    #[test]
    fn copy_builder_equals_target_still_runs_idempotent_target_copy() {
        let source = nexus::BuildTarget::Remote(nexus::BuilderNode::new(node()));
        let copy = ClosureCopy::from_command(&copy_command(source))
            .expect("copy")
            .expect("a remote builder copies its result from the dispatcher store");
        let invocation = copy.invocation();
        let argv = invocation.joined_arguments();
        assert!(!argv.contains("--from"), "{argv}");
        assert!(
            argv.contains("--to ssh-ng://root@node-1.alpha.criome"),
            "{argv}"
        );
    }

    // ---- build-on-target: target-store build copy is a no-op ----

    #[test]
    fn target_store_build_skips_the_copy_entirely() {
        // A build-on-target build already realized the closure in the target
        // node's own store (Spirit lc28 / report 150), so there is nothing to
        // push: `from_command` returns `None` and no `ssh-ng` transfer runs.
        let source =
            nexus::BuildTarget::target_store("ssh-ng://root@node-1.alpha.criome".to_string());
        let copy = ClosureCopy::from_command(&copy_command(source)).expect("copy");
        assert!(
            copy.is_none(),
            "target-store build must skip the copy; the closure is already on the target"
        );
    }

    fn activate_command(
        profile: nexus::ActivationProfile,
        kind: ordinary::ActivationKind,
    ) -> nexus::ActivateGenerationCommand {
        nexus::ActivateGenerationCommand {
            deployment_identifier: ordinary::DeploymentIdentifier::new(7),
            generation_identifier: ordinary::GenerationIdentifier::new(1),
            cluster_name: cluster(),
            node_name: node(),
            closure_path: ordinary::ClosurePath::new(STORE),
            activation_kind: kind,
            profile,
        }
    }

    fn system_activation(action: ordinary::SystemAction) -> SystemActivation {
        system_activation_on_host(action, None)
    }

    /// A System activation built with an explicit daemon-host context so a test
    /// can target a self-Switch (`daemon_host == node-1`, the command's node) or
    /// a foreign-target Switch (`daemon_host == None` or a different node).
    fn system_activation_on_host(
        action: ordinary::SystemAction,
        daemon_host: Option<ordinary::NodeName>,
    ) -> SystemActivation {
        match Activation::from_command(
            &activate_command(
                nexus::ActivationProfile::System(action),
                ordinary::ActivationKind::Switch,
            ),
            daemon_host.as_ref(),
        )
        .expect("activation")
        {
            Activation::System(activation) => activation,
            Activation::Home(_) => panic!("expected System activation"),
        }
    }

    // ---- Step 3: per-action System activation argv, no $CLOSURE token ----

    #[test]
    fn switch_activation_has_store_path_and_switch_subcommand_no_closure_token() {
        let activation = system_activation(ordinary::SystemAction::Switch);
        let invocation = activation.ssh_invocation().expect("switch invocation");
        assert_eq!(invocation.program(), "ssh");
        let argv = invocation.joined_arguments();
        assert!(argv.contains("root@node-1.alpha.criome"), "{argv}");
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
    fn boot_activation_runs_switch_to_configuration_boot_then_reconciles_efi() {
        let activation = system_activation(ordinary::SystemAction::Boot);
        let invocation = activation.ssh_invocation().expect("boot invocation");
        let argv = invocation.joined_arguments();
        assert!(
            argv.contains(&format!("{STORE}/bin/switch-to-configuration boot")),
            "{argv}"
        );
        assert!(!argv.contains("$CLOSURE"), "{argv}");
        assert!(activation.requires_efi_reconcile());
        // EFI reconcile commands are present and correctly shaped.
        let readlink = activation.step_readlink_system_profile_invocation();
        assert!(
            readlink
                .joined_arguments()
                .contains("readlink /nix/var/nix/profiles/system"),
            "{}",
            readlink.joined_arguments()
        );
        let entry = BootEntry("nixos-generation-42.conf".to_string());
        let set_default = activation.step_set_efi_default_invocation(&entry);
        assert!(
            set_default
                .joined_arguments()
                .contains("bootctl set-default nixos-generation-42.conf"),
            "{}",
            set_default.joined_arguments()
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
        let activation = system_activation(ordinary::SystemAction::Test);
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
        let activation = system_activation(ordinary::SystemAction::BootOnce);
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
        let activation = system_activation_on_host(ordinary::SystemAction::Switch, Some(node()));
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

    #[test]
    fn foreign_target_switch_keeps_the_foreground_path() {
        // A Switch targeting a DIFFERENT node than the daemon host (or with no
        // daemon-host context) must NOT take the detached shape — the foreground
        // ssh is not at risk there.
        let foreign = system_activation_on_host(
            ordinary::SystemAction::Switch,
            Some(ordinary::NodeName::new("some-other-node")),
        );
        assert!(!foreign.runs_detached_self_switch());
        let no_context = system_activation(ordinary::SystemAction::Switch);
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
        let boot = system_activation_on_host(ordinary::SystemAction::Boot, Some(node()));
        assert!(!boot.runs_detached_self_switch());
        let boot_once = system_activation_on_host(ordinary::SystemAction::BootOnce, Some(node()));
        assert!(!boot_once.runs_detached_self_switch());
    }

    // ---- Step 3: BootOnce transient-unit argv + script snapshot ----

    #[test]
    fn boot_once_systemd_run_argv_shape() {
        let activation = system_activation(ordinary::SystemAction::BootOnce);
        let invocation = activation.systemd_run_invocation("lojix-boot-once-abc-def");
        assert_eq!(invocation.program(), "ssh");
        let argv = invocation.joined_arguments();
        assert!(argv.contains("root@node-1.alpha.criome"), "{argv}");
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
        let activation = system_activation(ordinary::SystemAction::BootOnce);
        assert_eq!(activation.unit_name(), "lojix-boot-once-deploy-7");
    }

    #[test]
    fn activation_unit_name_matches_the_resume_cursor_unit() {
        // The activation-side `unit_name` and the resume-side
        // `DeployJob::boot_once_unit` must produce the SAME string for one
        // deployment so the crash-resume `PollActivationUnit` polls the unit the
        // activation actually started (report 150). Both go through
        // `DeploymentIdentifier::boot_once_unit_name`.
        let mut pipeline = system_pipeline(ordinary::SystemAction::BootOnce);
        pipeline.deployment_identifier = ordinary::DeploymentIdentifier::new(7);
        let cursor_unit = pipeline.boot_once_unit().expect("BootOnce records a unit");
        let activation = system_activation(ordinary::SystemAction::BootOnce);
        assert_eq!(activation.unit_name(), cursor_unit);
    }

    #[test]
    fn non_boot_once_deploy_records_no_resume_unit() {
        // A non-BootOnce action has no transient unit to poll; copy is
        // idempotent and activation re-runs safely, so the cursor records None.
        let pipeline = system_pipeline(ordinary::SystemAction::Switch);
        assert!(pipeline.boot_once_unit().is_none());
    }

    // ---- build-on-target: selection of the build store (report 150) ----

    fn configuration_on_host(host: &str) -> RuntimeConfiguration {
        let mut configuration = RuntimeConfiguration::test_default();
        configuration.daemon_host = ordinary::NodeName::new(host);
        configuration
    }

    #[test]
    fn build_on_a_different_target_realizes_in_the_target_store() {
        // The reason for the fix: deploying node-1 from a daemon hosted on
        // ouranos must NOT realize node-1's (model-bearing) closure on ouranos.
        // The build target is the target node's own store over ssh-ng.
        let pipeline = system_pipeline(ordinary::SystemAction::BootOnce);
        let configuration = configuration_on_host("ouranos");
        match pipeline.build_target(&configuration) {
            nexus::BuildTarget::TargetStore(store) => {
                assert_eq!(store.payload(), "ssh-ng://root@node-1.alpha.criome");
            }
            other => panic!("expected a target-store build for a remote node, got {other:?}"),
        }
    }

    #[test]
    fn build_on_the_daemon_host_stays_local() {
        // Deploying the daemon's own host (e.g. ouranos from the ouranos-hosted
        // daemon) must stay local — its store already holds any model closure,
        // and an ssh-ng-to-self build would be wrong. `system_pipeline` targets
        // node `node-1`, so a daemon hosted on `node-1` builds locally.
        let pipeline = system_pipeline(ordinary::SystemAction::BootOnce);
        let configuration = configuration_on_host("node-1");
        assert!(matches!(
            pipeline.build_target(&configuration),
            nexus::BuildTarget::Local
        ));
    }

    #[test]
    fn explicit_builder_override_wins_over_build_on_target() {
        // An operator-named builder still dispatches to that Nix builder machine
        // regardless of daemon host — the build-on-target decision only governs
        // the default (no-builder) path.
        let mut pipeline = system_pipeline(ordinary::SystemAction::BootOnce);
        pipeline.builder = Some(ordinary::NodeName::new("big-builder"));
        let configuration = configuration_on_host("ouranos");
        assert!(matches!(
            pipeline.build_target(&configuration),
            nexus::BuildTarget::Remote(_)
        ));
    }

    #[test]
    fn target_store_build_realizes_on_target_store_no_eval_store_auto() {
        // The target-store nix invocation must operate ENTIRELY on the target
        // store — `--store <uri>` ALONE, NO `--eval-store auto` — consistent
        // with the eval step. The eval instantiates the toplevel `.drv` INTO the
        // target store, so the build must use the same store to FIND it;
        // re-adding `--eval-store auto` makes the build look in the local store
        // and fails with `... .drv is not valid`. Still selects the drv outputs
        // with `^*`.
        let invocation = NixCommand::build_closure_in_store(
            DERIVATION,
            "ssh-ng://root@node-1.alpha.criome",
            &[],
        );
        assert_eq!(invocation.program(), "nix");
        let argv = invocation.joined_arguments();
        assert!(
            argv.contains("--store ssh-ng://root@node-1.alpha.criome"),
            "eval+build must target the node store: {argv}"
        );
        assert!(
            !argv.contains("--eval-store"),
            "build must NOT pin instantiation local — `--eval-store auto` makes \
             the build look in the local store for the target-only `.drv` and \
             reintroduces the `.drv is not valid` failure: {argv}"
        );
        assert!(argv.contains("--print-out-paths"), "{argv}");
        assert!(
            argv.contains(&format!("{DERIVATION}^*")),
            "target-store build must carry the `^*` output selector: {argv}"
        );
        // A target-store build NEVER offloads to the daemon machine file — that
        // would copy the result back into the daemon host store (report 150).
        assert!(!argv.contains("--builders"), "{argv}");
    }

    #[test]
    fn target_store_eval_instantiates_against_the_target_store_no_eval_store_auto() {
        // The Eval step resolves `.drvPath` BEFORE the build by INSTANTIATING
        // against the target store. A build-on-target node's config references
        // model `.drv`s that live ONLY in the target store, so the eval must add
        // `--store <uri>` ALONE — NOT `--eval-store auto --store <uri>` —
        // because `--eval-store auto` pins instantiation local and the
        // target-only `.drv` then `... .drv does not exist` (verified
        // 2026-06-20, report 150). The BUILD step
        // (`build_closure_in_store`) now uses the SAME `--store <uri>` alone, so
        // eval and build operate consistently on the target store. The
        // `--override-input` flags and `.drvPath` selector are preserved.
        let store = nexus::BuildTarget::TargetStore(nexus::TargetStore::new(
            "ssh-ng://root@node-1.alpha.criome",
        ));
        let invocation = NixCommand::eval_drv_path(".#toplevel", &[], &store);
        assert_eq!(invocation.program(), "nix");
        let argv = invocation.joined_arguments();
        assert!(
            argv.contains("--store ssh-ng://root@node-1.alpha.criome"),
            "eval must instantiate against the target store: {argv}"
        );
        assert!(
            !argv.contains("--eval-store"),
            "eval must NOT pin instantiation local — `--eval-store auto` is the \
             failure mode for target-only drvs: {argv}"
        );
        assert!(argv.contains("--refresh"), "{argv}");
        assert!(argv.contains("--raw"), "{argv}");
        assert!(argv.ends_with(".#toplevel.drvPath"), "{argv}");
    }

    #[test]
    fn local_eval_reads_the_daemon_host_store_with_no_redirect() {
        // A daemon-host target (`Local`) keeps the host-local eval — its store
        // already holds everything the config references, and a store redirect
        // would be wrong. No `--store` / `--eval-store` flags are added.
        let invocation = NixCommand::eval_drv_path(".#toplevel", &[], &nexus::BuildTarget::Local);
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
        let target = nexus::BuildTarget::Remote(nexus::BuilderNode::new(ordinary::NodeName::new(
            "builder-1",
        )));
        let invocation = NixCommand::eval_drv_path(".#toplevel", &[], &target);
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
        let activation = system_activation(ordinary::SystemAction::BootOnce);
        let expected = format!(
            "export PATH=/run/current-system/sw/bin:/run/wrappers/bin:$PATH\n\
             set -eu\n\
             CLOSURE='{STORE}'\n\
             OLD=$(bootctl status | awk -F': *' '/Current Entry:/ {{print $2}}')\n\
             [ -n \"$OLD\" ]\n\
             nix-env -p /nix/var/nix/profiles/system --set \"$CLOSURE\"\n\
             \"$CLOSURE/bin/switch-to-configuration\" boot\n\
             SYSTEM_LINK=$(readlink /nix/var/nix/profiles/system)\n\
             GENERATION=$(echo \"$SYSTEM_LINK\" | sed -E 's/^system-([0-9]+)-link$/\\1/')\n\
             NEW=\"nixos-generation-$GENERATION.conf\"\n\
             [ -f \"/boot/loader/entries/$NEW\" ]\n\
             [ \"$NEW\" != \"$OLD\" ]\n\
             bootctl set-default \"$OLD\"\n\
             bootctl set-oneshot \"$NEW\"\n\
             echo \"boot-once: oneshot=$NEW persistent-default=$OLD (=running generation)\"\n",
        );
        assert_eq!(activation.boot_once_script(), expected);
    }

    // ---- Step 3: system profile link parse + EFI entry derivation ----

    #[test]
    fn system_profile_link_parses_generation_and_derives_entry() {
        let link = SystemProfileLink::try_new("system-42-link").expect("link");
        assert_eq!(
            link.generation().boot_entry().as_str(),
            "nixos-generation-42.conf"
        );
        assert!(SystemProfileLink::try_new("not-a-link").is_err());
    }

    // ---- Step 3: Home activation argv ----

    fn home_activation(mode: meta::HomeMode) -> HomeActivation {
        let profile = nexus::ActivationProfile::Home(nexus::HomeActivationProfile {
            mode,
            user: ordinary::UserName::new("li"),
        });
        match Activation::from_command(
            &activate_command(profile, ordinary::ActivationKind::Switch),
            None,
        )
        .expect("activation")
        {
            Activation::Home(activation) => activation,
            Activation::System(_) => panic!("expected Home activation"),
        }
    }

    #[test]
    fn home_remote_profile_addresses_user_at_criome_domain() {
        let activation = home_activation(meta::HomeMode::Profile);
        let invocation = activation.remote_profile_invocation();
        assert_eq!(invocation.program(), "ssh");
        let argv = invocation.joined_arguments();
        assert!(argv.contains("li@node-1.alpha.criome"), "{argv}");
        assert!(
            argv.contains("nix-env -p \"$HOME/.local/state/nix/profiles/home-manager\" --set"),
            "{argv}"
        );
        assert!(argv.contains(STORE), "{argv}");
    }

    #[test]
    fn home_remote_activate_runs_activate_package() {
        let activation = home_activation(meta::HomeMode::Activate);
        let invocation = activation.remote_activate_invocation();
        let argv = invocation.joined_arguments();
        assert!(argv.contains("li@node-1.alpha.criome"), "{argv}");
        assert!(argv.contains(&format!("{STORE}/activate")), "{argv}");
    }

    // ---- per-deploy secrets provisioning ----

    fn secret_file(name: &str) -> ClusterSecretFile {
        ClusterSecretFile::new(PathBuf::from(format!("/cluster/secrets/{name}")))
    }

    #[test]
    fn sops_attribute_name_is_the_filename_stem_verbatim() {
        // No case transform: the `.sops` filename stem becomes the attribute
        // name exactly as written (the coordinated goldragon rename gives each
        // file its exact camelCase consumer name). Only the `.sops` suffix is
        // stripped.
        assert_eq!(
            secret_file("routerWifiSaePasswords.sops")
                .attribute_name()
                .expect("utf8 stem"),
            "routerWifiSaePasswords"
        );
        assert_eq!(
            secret_file("localLlmApiToken.sops")
                .attribute_name()
                .expect("utf8 stem"),
            "localLlmApiToken"
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
        let source = ordinary::ProposalSource::new(
            "/git/github.com/LiGoldragon/goldragon/datom.nota".to_string(),
        );
        let directory = ClusterSecretsDirectory::from_proposal_source(&source);
        assert_eq!(
            directory.path,
            PathBuf::from("/git/github.com/LiGoldragon/goldragon/secrets")
        );
    }

    #[test]
    fn absent_secrets_directory_yields_no_files() {
        let source = ordinary::ProposalSource::new(
            "/nonexistent/path/that/has/no/secrets/datom.nota".to_string(),
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
        // are named with their exact camelCase consumer name (coordinated
        // goldragon rename); the attribute is the stem verbatim.
        fs::write(
            secrets_directory.join("routerWifiSaePasswords.sops"),
            b"opaque",
        )
        .expect("write sops file");
        fs::write(secrets_directory.join("localLlmApiToken.sops"), b"opaque")
            .expect("write sops file");
        // a non-.sops file in the directory is ignored
        fs::write(secrets_directory.join("README.md"), b"ignore me").expect("write readme");

        let generated =
            std::env::temp_dir().join(format!("lojix-secrets-gen-{}", std::process::id()));
        let _ = fs::remove_dir_all(&generated);
        let source = ordinary::ProposalSource::new(
            source_directory
                .join("datom.nota")
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
            flake.contains("localLlmApiToken = ./localLlmApiToken.sops;"),
            "{flake}"
        );
        assert!(
            flake.contains("routerWifiSaePasswords = ./routerWifiSaePasswords.sops;"),
            "{flake}"
        );
        assert!(
            !flake.contains("README"),
            "non-sops files excluded: {flake}"
        );
        assert!(
            generated.join("routerWifiSaePasswords.sops").is_file(),
            "ciphertext copied into the generated input"
        );
        assert!(
            generated.join("localLlmApiToken.sops").is_file(),
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
                .join("datom.nota")
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
        let source = ordinary::ProposalSource::new("/nonexistent/bootstrap/datom.nota".to_string());
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
