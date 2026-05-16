use std::path::{Path, PathBuf};

use crate::error::{Error, Result};
use crate::process::{ProcessInvocation, ProcessToolchain, ShellArgument, ShellCommand};
use crate::runtime::RuntimeConfiguration;
use crate::wire;
use horizon_lib::name::{
    ClusterName as HorizonClusterName, CriomeDomainName, NodeName as HorizonNodeName,
    UserName as HorizonUserName,
};
use horizon_lib::species::System;
use horizon_lib::view::Node;
use horizon_lib::{ClusterProposal, Horizon, Viewpoint};
use horizon_nota_codec::NotaDecode as HorizonNotaDecode;
use kameo::Actor;
use kameo::actor::{ActorRef, Spawn};
use kameo::error::Infallible;
use kameo::message::{Context, Message};

const HORIZON_FLAKE_TEMPLATE: &str = "{
  outputs = _: {
    horizon = builtins.fromJSON (builtins.readFile ./horizon.json);
  };
}
";

const SOPS_FILE_EXTENSION: &str = "sops";

#[derive(Debug, Clone)]
pub struct EventLogActor {
    deployment_observations: Vec<wire::DeploymentObservation>,
}

impl EventLogActor {
    pub fn new() -> Self {
        Self {
            deployment_observations: Vec::new(),
        }
    }
}

impl Default for EventLogActor {
    fn default() -> Self {
        Self::new()
    }
}

impl Actor for EventLogActor {
    type Args = Self;
    type Error = Infallible;

    async fn on_start(
        state: Self::Args,
        _actor_ref: ActorRef<Self>,
    ) -> std::result::Result<Self, Self::Error> {
        Ok(state)
    }
}

pub struct AppendDeploymentObservation {
    pub observation: wire::DeploymentObservation,
}

impl Message<AppendDeploymentObservation> for EventLogActor {
    type Reply = std::result::Result<(), Infallible>;

    async fn handle(
        &mut self,
        message: AppendDeploymentObservation,
        _context: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.deployment_observations.push(message.observation);
        Ok(())
    }
}

pub struct DeploymentObservationSnapshot {
    pub subscription: wire::DeploymentObservationSubscription,
}

impl Message<DeploymentObservationSnapshot> for EventLogActor {
    type Reply = std::result::Result<Vec<wire::DeploymentObservation>, Infallible>;

    async fn handle(
        &mut self,
        message: DeploymentObservationSnapshot,
        _context: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        Ok(self
            .deployment_observations
            .iter()
            .filter(|observation| message.subscription.matches(observation))
            .cloned()
            .collect())
    }
}

trait DeploymentObservationFilter {
    fn matches(&self, observation: &wire::DeploymentObservation) -> bool;
}

impl DeploymentObservationFilter for wire::DeploymentObservationSubscription {
    fn matches(&self, observation: &wire::DeploymentObservation) -> bool {
        let Some(expected) = &self.deployment else {
            return true;
        };
        observation.deployment_id() == Some(expected)
    }
}

trait DeploymentObservationIdentity {
    fn deployment_id(&self) -> Option<&wire::DeploymentId>;
}

impl DeploymentObservationIdentity for wire::DeploymentObservation {
    fn deployment_id(&self) -> Option<&wire::DeploymentId> {
        match &self.phase {
            wire::DeploymentPhase::DeploymentSubmitted(phase) => Some(&phase.deployment),
            wire::DeploymentPhase::DeploymentBuilding(phase) => Some(&phase.deployment),
            wire::DeploymentPhase::DeploymentBuilt(phase) => Some(&phase.deployment),
            wire::DeploymentPhase::ClosureCopying(phase) => Some(&phase.deployment),
            wire::DeploymentPhase::ActivationRunning(phase) => Some(&phase.deployment),
            wire::DeploymentPhase::ActivationSucceeded(phase) => Some(&phase.deployment),
            wire::DeploymentPhase::DeploymentFailed(phase) => Some(&phase.deployment),
        }
    }
}

pub struct DeploymentActor {
    configuration: RuntimeConfiguration,
    event_log: ActorRef<EventLogActor>,
}

impl DeploymentActor {
    pub fn new(configuration: RuntimeConfiguration, event_log: ActorRef<EventLogActor>) -> Self {
        Self {
            configuration,
            event_log,
        }
    }
}

impl Actor for DeploymentActor {
    type Args = Self;
    type Error = Infallible;

    async fn on_start(
        state: Self::Args,
        _actor_ref: ActorRef<Self>,
    ) -> std::result::Result<Self, Self::Error> {
        Ok(state)
    }
}

pub struct StartDeployment {
    pub deployment: wire::DeploymentId,
    pub submission: wire::DeploymentSubmission,
}

impl Message<StartDeployment> for DeploymentActor {
    type Reply = std::result::Result<wire::Reply, Infallible>;

    async fn handle(
        &mut self,
        message: StartDeployment,
        _context: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        if let Err(error) = BuildOnlyRequest::validate_fast(&message.submission) {
            return Ok(wire::Reply::DeploymentRejected(error.into_rejection()));
        }

        let job = BuildJobActor::spawn(BuildJobActor::new(
            self.configuration.clone(),
            self.event_log.clone(),
            message.deployment.clone(),
            message.submission,
        ));
        if job.tell(RunBuildJob).send().await.is_err() {
            return Ok(wire::Reply::DeploymentRejected(
                DeploymentError::new("build job actor stopped before accepting work")
                    .into_rejection(),
            ));
        }

        Ok(wire::Reply::DeploymentAccepted(wire::DeploymentAccepted {
            deployment: message.deployment,
        }))
    }
}

struct BuildJobActor {
    configuration: RuntimeConfiguration,
    event_log: ActorRef<EventLogActor>,
    deployment: wire::DeploymentId,
    submission: Option<wire::DeploymentSubmission>,
}

impl BuildJobActor {
    fn new(
        configuration: RuntimeConfiguration,
        event_log: ActorRef<EventLogActor>,
        deployment: wire::DeploymentId,
        submission: wire::DeploymentSubmission,
    ) -> Self {
        Self {
            configuration,
            event_log,
            deployment,
            submission: Some(submission),
        }
    }

    async fn append(&self, phase: wire::DeploymentPhase) {
        let _ = self
            .event_log
            .ask(AppendDeploymentObservation {
                observation: wire::DeploymentObservation { phase },
            })
            .await;
    }

    async fn fail(&self, error: Error) {
        self.append(wire::DeploymentPhase::DeploymentFailed(
            wire::DeploymentFailed {
                deployment: self.deployment.clone(),
                reason: failure_text(error.to_string()),
            },
        ))
        .await;
    }
}

impl Actor for BuildJobActor {
    type Args = Self;
    type Error = Infallible;

    async fn on_start(
        state: Self::Args,
        _actor_ref: ActorRef<Self>,
    ) -> std::result::Result<Self, Self::Error> {
        Ok(state)
    }
}

struct RunBuildJob;

impl Message<RunBuildJob> for BuildJobActor {
    type Reply = ();

    async fn handle(
        &mut self,
        _message: RunBuildJob,
        context: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        let Some(submission) = self.submission.take() else {
            context.stop();
            return;
        };

        self.append(wire::DeploymentPhase::DeploymentSubmitted(
            wire::DeploymentSubmitted {
                deployment: self.deployment.clone(),
            },
        ))
        .await;

        match BuildOnlyRequest::from_submission(&self.configuration, submission) {
            Ok(request) => {
                self.append(wire::DeploymentPhase::DeploymentBuilding(
                    wire::DeploymentBuilding {
                        deployment: self.deployment.clone(),
                        builder: request.builder_selection().clone(),
                    },
                ))
                .await;
                match request.run().await {
                    Ok(result) => {
                        self.append(wire::DeploymentPhase::DeploymentBuilt(
                            wire::DeploymentBuilt {
                                deployment: self.deployment.clone(),
                                result,
                            },
                        ))
                        .await;
                    }
                    Err(error) => self.fail(error).await,
                }
            }
            Err(error) => self.fail(error).await,
        }

        context.stop();
    }
}

pub struct BuildOnlyRequest {
    state_directory: PathBuf,
    process_toolchain: ProcessToolchain,
    cluster: HorizonClusterName,
    node: HorizonNodeName,
    source: ProposalSource,
    flake: FlakeReference,
    plan: BuildOnlyPlan,
    builder: wire::BuilderSelection,
    substituters: Vec<HorizonNodeName>,
}

impl BuildOnlyRequest {
    fn validate_fast(
        submission: &wire::DeploymentSubmission,
    ) -> std::result::Result<(), DeploymentError> {
        let _ = BuildOnlyPlan::from_wire(&submission.plan)?;
        match &submission.builder {
            wire::BuilderSelection::BuildLocally(_) => Err(DeploymentError::new(
                "local builds are disabled; use DispatcherChoosesBuilder or NamedBuilder",
            )),
            wire::BuilderSelection::DispatcherChoosesBuilder(_)
            | wire::BuilderSelection::NamedBuilder(_) => Ok(()),
        }
    }

    fn from_submission(
        configuration: &RuntimeConfiguration,
        submission: wire::DeploymentSubmission,
    ) -> Result<Self> {
        Self::validate_fast(&submission)
            .map_err(|error| Error::DeploymentRejected(error.message))?;
        Ok(Self {
            state_directory: configuration.state_directory().to_path_buf(),
            process_toolchain: configuration.process_toolchain().clone(),
            cluster: horizon_cluster_name(&submission.cluster)?,
            node: horizon_node_name(&submission.node)?,
            source: ProposalSource::new(submission.source.as_str()),
            flake: FlakeReference::new(submission.flake.as_str()),
            plan: BuildOnlyPlan::from_wire(&submission.plan)
                .map_err(|error| Error::DeploymentRejected(error.message))?,
            builder: submission.builder,
            substituters: submission
                .substituters
                .iter()
                .map(horizon_node_name)
                .collect::<Result<Vec<_>>>()?,
        })
    }

    fn builder_selection(&self) -> &wire::BuilderSelection {
        &self.builder
    }

    pub async fn run(&self) -> Result<wire::BuildResult> {
        let proposal = self.source.load()?;
        let viewpoint = Viewpoint {
            cluster: self.cluster.clone(),
            node: self.node.clone(),
        };
        let horizon = proposal.project(&viewpoint)?;
        self.plan.validate_home_user(&horizon)?;
        let builder = BuilderPolicy::new(&horizon).resolve(&self.builder)?;
        let extra_substituters =
            ExtraSubstituters::from_horizon_nodes(&horizon, &self.substituters)?;
        let materialized = ArtifactMaterialization::new(
            self.state_directory.clone(),
            horizon,
            self.cluster.clone(),
            self.node.clone(),
            self.source.clone(),
            self.process_toolchain.clone(),
            self.plan.deployment_shape(),
        )
        .materialize()
        .await?;
        let references = RemoteInputStage::from_artifact(
            builder.clone(),
            self.process_toolchain.clone(),
            &materialized,
        )
        .run()
        .await?;
        let store_path = NixBuild {
            flake: self.flake.clone(),
            horizon_ref: references.horizon_ref,
            system_ref: references.system_ref,
            deployment_ref: references.deployment_ref,
            secrets_ref: references.secrets_ref,
            extra_substituters,
            plan: self.plan.clone(),
            builder,
            process_toolchain: self.process_toolchain.clone(),
        }
        .run()
        .await?;
        Ok(wire::BuildResult::RealizedStorePath(
            wire::RealizedStorePath { store_path },
        ))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct DeploymentError {
    message: String,
}

impl DeploymentError {
    fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }

    fn into_rejection(self) -> wire::DeploymentRejected {
        wire::DeploymentRejected {
            reason: wire::DeploymentRejectionReason::InvalidRequest,
            detail: Some(failure_text(self.message)),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum BuildOnlyPlan {
    FullOs,
    OsOnly,
    Home { user: HorizonUserName },
}

impl BuildOnlyPlan {
    fn from_wire(plan: &wire::DeploymentPlan) -> std::result::Result<Self, DeploymentError> {
        match plan {
            wire::DeploymentPlan::FullOsDeployment(deployment)
                if deployment.action == wire::SystemAction::Build =>
            {
                Ok(Self::FullOs)
            }
            wire::DeploymentPlan::OsOnlyDeployment(deployment)
                if deployment.action == wire::SystemAction::Build =>
            {
                Ok(Self::OsOnly)
            }
            wire::DeploymentPlan::HomeOnlyDeployment(deployment)
                if deployment.mode == wire::HomeMode::Build =>
            {
                Ok(Self::Home {
                    user: horizon_user_name(&deployment.user)
                        .map_err(|error| DeploymentError::new(error.to_string()))?,
                })
            }
            _ => Err(DeploymentError::new(
                "this runtime slice accepts build-only deployments; activation and eval actions are disabled",
            )),
        }
    }

    fn validate_home_user(&self, horizon: &Horizon) -> Result<()> {
        if let Self::Home { user } = self
            && !horizon.users.contains_key(user)
        {
            return Err(Error::DeploymentRejected(format!(
                "user {user} is not present in projected horizon users"
            )));
        }
        Ok(())
    }

    fn deployment_shape(&self) -> Option<DeploymentShape> {
        match self {
            Self::FullOs => Some(DeploymentShape::home_enabled()),
            Self::OsOnly => Some(DeploymentShape::home_disabled()),
            Self::Home { .. } => None,
        }
    }

    fn target_attribute(&self, flake: &FlakeReference) -> String {
        match self {
            Self::FullOs | Self::OsOnly => format!(
                "{}#nixosConfigurations.target.config.system.build.toplevel",
                flake.as_str()
            ),
            Self::Home { user } => format!(
                "{}#homeConfigurations.{}.activationPackage",
                flake.as_str(),
                user.as_str()
            ),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct DeploymentShape {
    include_home: bool,
}

impl DeploymentShape {
    fn home_enabled() -> Self {
        Self { include_home: true }
    }

    fn home_disabled() -> Self {
        Self {
            include_home: false,
        }
    }

    fn cache_name(self) -> &'static str {
        if self.include_home {
            "home-on"
        } else {
            "home-off"
        }
    }

    fn flake_text(self) -> &'static str {
        if self.include_home {
            "{\n  outputs = _: {\n    deployment = {\n      includeHome = true;\n    };\n  };\n}\n"
        } else {
            "{\n  outputs = _: {\n    deployment = {\n      includeHome = false;\n    };\n  };\n}\n"
        }
    }
}

struct BuilderPolicy<'horizon> {
    horizon: &'horizon Horizon,
}

impl<'horizon> BuilderPolicy<'horizon> {
    fn new(horizon: &'horizon Horizon) -> Self {
        Self { horizon }
    }

    fn resolve(&self, selection: &wire::BuilderSelection) -> Result<SshTarget> {
        match selection {
            wire::BuilderSelection::BuildLocally(_) => Err(Error::DeploymentRejected(
                "local builds are disabled in build-only daemon slice".to_string(),
            )),
            wire::BuilderSelection::NamedBuilder(named) => {
                let name = horizon_node_name(&named.node)?;
                let node = self.node_by_name(&name)?;
                if !node.is_remote_nix_builder {
                    return Err(Error::DeploymentRejected(format!(
                        "builder {name} is not a remote Nix builder in projected horizon"
                    )));
                }
                Ok(SshTarget::from_node(node))
            }
            wire::BuilderSelection::DispatcherChoosesBuilder(_) => self
                .horizon
                .ex_nodes
                .values()
                .find(|node| node.is_remote_nix_builder)
                .map(SshTarget::from_node)
                .ok_or_else(|| {
                    Error::DeploymentRejected(
                        "DispatcherChoosesBuilder found no remote Nix builder in projected horizon"
                            .to_string(),
                    )
                }),
        }
    }

    fn node_by_name(&self, name: &HorizonNodeName) -> Result<&'horizon Node> {
        if self.horizon.node.name == *name {
            return Ok(&self.horizon.node);
        }
        self.horizon.ex_nodes.get(name).ok_or_else(|| {
            Error::DeploymentRejected(format!(
                "builder node {name} not found in projected horizon"
            ))
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct SshTarget {
    user: String,
    domain: CriomeDomainName,
}

impl SshTarget {
    fn from_node(node: &Node) -> Self {
        Self {
            user: "root".to_string(),
            domain: node.criome_domain_name.clone(),
        }
    }

    fn as_ssh_argument(&self) -> String {
        format!("{}@{}", self.user, self.domain.as_str())
    }

    fn remote_invocation(
        &self,
        process_toolchain: &ProcessToolchain,
        remote_command: ShellCommand,
    ) -> ProcessInvocation {
        ProcessInvocation::new(process_toolchain.ssh()).with_arguments([
            "-o".to_string(),
            "BatchMode=yes".to_string(),
            self.as_ssh_argument(),
            remote_command.as_str().to_string(),
        ])
    }
}

impl std::fmt::Display for SshTarget {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{}@{}", self.user, self.domain.as_str())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ProposalSource(PathBuf);

impl ProposalSource {
    fn new(path: impl Into<PathBuf>) -> Self {
        Self(path.into())
    }

    fn as_path(&self) -> &Path {
        &self.0
    }

    fn load(&self) -> Result<ClusterProposal> {
        let text = std::fs::read_to_string(&self.0)?;
        let mut decoder = horizon_nota_codec::Decoder::new(&text);
        Ok(ClusterProposal::decode(&mut decoder)?)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct FlakeReference(String);

impl FlakeReference {
    fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct FlakeInputReference {
    url: String,
    nar_hash: NarHash,
}

impl FlakeInputReference {
    fn from_local_path(path: &Path, nar_hash: NarHash) -> Self {
        Self {
            url: format!("path:{}", path.display()),
            nar_hash,
        }
    }

    fn flake_ref(&self) -> String {
        format!("{}?narHash={}", self.url, self.nar_hash.as_url_parameter())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct NarHash(String);

impl NarHash {
    fn new(text: impl Into<String>) -> Result<Self> {
        let text = text.into();
        if text.starts_with("sha256-") {
            Ok(Self(text))
        } else {
            Err(Error::DeploymentRejected(format!(
                "invalid NAR hash from nix: {text}"
            )))
        }
    }

    fn as_url_parameter(&self) -> String {
        self.0
            .replace('=', "%3D")
            .replace('+', "%2B")
            .replace('/', "%2F")
    }

    fn short_code(&self) -> String {
        self.0
            .trim_start_matches("sha256-")
            .chars()
            .filter(|character| character.is_ascii_alphanumeric())
            .take(12)
            .collect::<String>()
            .to_ascii_lowercase()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct NixSystemName(&'static str);

impl NixSystemName {
    fn from_system(system: System) -> Self {
        match system {
            System::X86_64Linux => Self("x86_64-linux"),
            System::Aarch64Linux => Self("aarch64-linux"),
        }
    }

    fn as_str(&self) -> &'static str {
        self.0
    }
}

struct HorizonDirectory(PathBuf);

impl HorizonDirectory {
    fn create(root: &Path, cluster: &HorizonClusterName, node: &HorizonNodeName) -> Result<Self> {
        let directory = root
            .join("generated-inputs")
            .join("horizon")
            .join(cluster.as_str())
            .join(node.as_str());
        std::fs::create_dir_all(&directory)?;
        Ok(Self(directory))
    }

    fn write(&self, horizon: &Horizon) -> Result<()> {
        let json = serde_json::to_string_pretty(horizon)?;
        std::fs::write(self.0.join("horizon.json"), json)?;
        std::fs::write(self.0.join("flake.nix"), HORIZON_FLAKE_TEMPLATE)?;
        Ok(())
    }

    async fn nar_hash(&self, process_toolchain: &ProcessToolchain) -> Result<NarHash> {
        NarHashInput::from_directory(process_toolchain, &self.0)
            .calculate()
            .await
    }

    fn path(&self) -> &Path {
        &self.0
    }
}

struct SystemDirectory(PathBuf);

impl SystemDirectory {
    fn create(root: &Path, system: System) -> Result<Self> {
        let directory = root
            .join("generated-inputs")
            .join("system")
            .join(NixSystemName::from_system(system).as_str());
        std::fs::create_dir_all(&directory)?;
        Ok(Self(directory))
    }

    fn write(&self, system: System) -> Result<()> {
        let name = NixSystemName::from_system(system).as_str();
        std::fs::write(
            self.0.join("flake.nix"),
            format!(
                "{{
  outputs = _: {{
    system = \"{name}\";
  }};
}}
"
            ),
        )?;
        Ok(())
    }

    async fn nar_hash(&self, process_toolchain: &ProcessToolchain) -> Result<NarHash> {
        NarHashInput::from_directory(process_toolchain, &self.0)
            .calculate()
            .await
    }

    fn path(&self) -> &Path {
        &self.0
    }
}

struct DeploymentDirectory(PathBuf);

impl DeploymentDirectory {
    fn create(root: &Path, shape: DeploymentShape) -> Result<Self> {
        let directory = root
            .join("generated-inputs")
            .join("deployment")
            .join(shape.cache_name());
        std::fs::create_dir_all(&directory)?;
        Ok(Self(directory))
    }

    fn write(&self, shape: DeploymentShape) -> Result<()> {
        std::fs::write(self.0.join("flake.nix"), shape.flake_text())?;
        Ok(())
    }

    async fn nar_hash(&self, process_toolchain: &ProcessToolchain) -> Result<NarHash> {
        NarHashInput::from_directory(process_toolchain, &self.0)
            .calculate()
            .await
    }

    fn path(&self) -> &Path {
        &self.0
    }
}

struct SecretsDirectory(PathBuf);

impl SecretsDirectory {
    fn create(root: &Path, cluster: &HorizonClusterName, node: &HorizonNodeName) -> Result<Self> {
        let directory = root
            .join("generated-inputs")
            .join("secrets")
            .join(cluster.as_str())
            .join(node.as_str());
        std::fs::create_dir_all(&directory)?;
        Ok(Self(directory))
    }

    fn write(&self, source: &ClusterSecrets) -> Result<()> {
        for entry in source.entries() {
            std::fs::copy(&entry.source_path, self.0.join(&entry.file_name))?;
        }
        std::fs::write(self.0.join("flake.nix"), source.flake_text())?;
        Ok(())
    }

    async fn nar_hash(&self, process_toolchain: &ProcessToolchain) -> Result<NarHash> {
        NarHashInput::from_directory(process_toolchain, &self.0)
            .calculate()
            .await
    }

    fn path(&self) -> &Path {
        &self.0
    }
}

struct ClusterSecrets {
    entries: Vec<SecretEntry>,
}

struct SecretEntry {
    secret_name: String,
    file_name: String,
    source_path: PathBuf,
}

impl ClusterSecrets {
    fn from_proposal_source(source: &ProposalSource) -> Option<Self> {
        let root = source.as_path().parent().unwrap_or_else(|| Path::new("."));
        let secrets_directory = root.join("secrets");
        let read = std::fs::read_dir(&secrets_directory).ok()?;
        let mut entries = Vec::new();
        for item in read.flatten() {
            let path = item.path();
            if path.extension().and_then(|extension| extension.to_str())
                != Some(SOPS_FILE_EXTENSION)
            {
                continue;
            }
            let file_name = path.file_name()?.to_str()?.to_string();
            let secret_name = path.file_stem()?.to_str()?.to_string();
            entries.push(SecretEntry {
                secret_name,
                file_name,
                source_path: path,
            });
        }
        if entries.is_empty() {
            return None;
        }
        entries.sort_by(|left, right| left.secret_name.cmp(&right.secret_name));
        Some(Self { entries })
    }

    fn entries(&self) -> &[SecretEntry] {
        &self.entries
    }

    fn flake_text(&self) -> String {
        let mut body = String::from("{\n  outputs = _: {\n    sopsFiles = {\n");
        for entry in &self.entries {
            body.push_str(&format!(
                "      \"{}\" = ./{};\n",
                entry.secret_name, entry.file_name,
            ));
        }
        body.push_str("    };\n  };\n}\n");
        body
    }
}

struct NarHashInput<'directory> {
    process_toolchain: &'directory ProcessToolchain,
    directory: &'directory Path,
}

impl<'directory> NarHashInput<'directory> {
    fn from_directory(
        process_toolchain: &'directory ProcessToolchain,
        directory: &'directory Path,
    ) -> Self {
        Self {
            process_toolchain,
            directory,
        }
    }

    async fn calculate(&self) -> Result<NarHash> {
        let output = ProcessInvocation::new(self.process_toolchain.nix())
            .with_arguments(["hash", "path", "--type", "sha256", "--sri"])
            .with_argument(self.directory.display().to_string())
            .capture_stdout()
            .await?;
        NarHash::new(output.stdout().trim())
    }
}

struct MaterializedArtifact {
    horizon_directory: HorizonDirectory,
    system_directory: SystemDirectory,
    deployment_directory: Option<DeploymentDirectory>,
    secrets_directory: Option<SecretsDirectory>,
    horizon_nar_hash: NarHash,
    system_nar_hash: NarHash,
    deployment_nar_hash: Option<NarHash>,
    secrets_nar_hash: Option<NarHash>,
}

struct ArtifactMaterialization {
    state_directory: PathBuf,
    horizon: Horizon,
    cluster: HorizonClusterName,
    node: HorizonNodeName,
    proposal_source: ProposalSource,
    process_toolchain: ProcessToolchain,
    deployment_shape: Option<DeploymentShape>,
}

impl ArtifactMaterialization {
    fn new(
        state_directory: PathBuf,
        horizon: Horizon,
        cluster: HorizonClusterName,
        node: HorizonNodeName,
        proposal_source: ProposalSource,
        process_toolchain: ProcessToolchain,
        deployment_shape: Option<DeploymentShape>,
    ) -> Self {
        Self {
            state_directory,
            horizon,
            cluster,
            node,
            proposal_source,
            process_toolchain,
            deployment_shape,
        }
    }

    async fn materialize(&self) -> Result<MaterializedArtifact> {
        let horizon_directory =
            HorizonDirectory::create(&self.state_directory, &self.cluster, &self.node)?;
        horizon_directory.write(&self.horizon)?;
        let horizon_nar_hash = horizon_directory.nar_hash(&self.process_toolchain).await?;

        let system_directory =
            SystemDirectory::create(&self.state_directory, self.horizon.node.system)?;
        system_directory.write(self.horizon.node.system)?;
        let system_nar_hash = system_directory.nar_hash(&self.process_toolchain).await?;

        let (deployment_directory, deployment_nar_hash) = match self.deployment_shape {
            None => (None, None),
            Some(shape) => {
                let directory = DeploymentDirectory::create(&self.state_directory, shape)?;
                directory.write(shape)?;
                let nar_hash = directory.nar_hash(&self.process_toolchain).await?;
                (Some(directory), Some(nar_hash))
            }
        };

        let (secrets_directory, secrets_nar_hash) =
            match ClusterSecrets::from_proposal_source(&self.proposal_source) {
                None => (None, None),
                Some(source) => {
                    let directory =
                        SecretsDirectory::create(&self.state_directory, &self.cluster, &self.node)?;
                    directory.write(&source)?;
                    let nar_hash = directory.nar_hash(&self.process_toolchain).await?;
                    (Some(directory), Some(nar_hash))
                }
            };

        Ok(MaterializedArtifact {
            horizon_directory,
            system_directory,
            deployment_directory,
            secrets_directory,
            horizon_nar_hash,
            system_nar_hash,
            deployment_nar_hash,
            secrets_nar_hash,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct BuildInputReferences {
    horizon_ref: FlakeInputReference,
    system_ref: FlakeInputReference,
    deployment_ref: Option<FlakeInputReference>,
    secrets_ref: Option<FlakeInputReference>,
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

#[derive(Debug, Clone, PartialEq, Eq)]
struct GeneratedInput {
    name: GeneratedInputName,
    local_path: PathBuf,
    nar_hash: NarHash,
}

impl GeneratedInput {
    fn from_artifact(artifact: &MaterializedArtifact) -> Vec<Self> {
        let mut inputs = vec![
            Self {
                name: GeneratedInputName::Horizon,
                local_path: artifact.horizon_directory.path().to_path_buf(),
                nar_hash: artifact.horizon_nar_hash.clone(),
            },
            Self {
                name: GeneratedInputName::System,
                local_path: artifact.system_directory.path().to_path_buf(),
                nar_hash: artifact.system_nar_hash.clone(),
            },
        ];
        if let (Some(directory), Some(nar_hash)) = (
            &artifact.deployment_directory,
            &artifact.deployment_nar_hash,
        ) {
            inputs.push(Self {
                name: GeneratedInputName::Deployment,
                local_path: directory.path().to_path_buf(),
                nar_hash: nar_hash.clone(),
            });
        }
        if let (Some(directory), Some(nar_hash)) =
            (&artifact.secrets_directory, &artifact.secrets_nar_hash)
        {
            inputs.push(Self {
                name: GeneratedInputName::Secrets,
                local_path: directory.path().to_path_buf(),
                nar_hash: nar_hash.clone(),
            });
        }
        inputs
    }

    fn root_segment(&self) -> String {
        format!("{}-{}", self.name.as_str(), self.nar_hash.short_code())
    }

    fn remote_directory(&self, root: &RemoteInputRoot) -> RemoteInputDirectory {
        root.input_directory(self.name)
    }

    fn flake_ref(&self, root: &RemoteInputRoot) -> FlakeInputReference {
        FlakeInputReference::from_local_path(
            self.remote_directory(root).as_path(),
            self.nar_hash.clone(),
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct RemoteInputRoot(PathBuf);

impl RemoteInputRoot {
    fn from_inputs(inputs: &[GeneratedInput]) -> Self {
        let key = inputs
            .iter()
            .map(GeneratedInput::root_segment)
            .collect::<Vec<_>>()
            .join("_");
        Self(PathBuf::from("/var/tmp/lojix/generated-inputs").join(key))
    }

    fn input_directory(&self, name: GeneratedInputName) -> RemoteInputDirectory {
        RemoteInputDirectory(self.0.join(name.as_str()))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct RemoteInputDirectory(PathBuf);

impl RemoteInputDirectory {
    fn as_path(&self) -> &Path {
        &self.0
    }

    fn create_invocation(
        &self,
        target: &SshTarget,
        process_toolchain: &ProcessToolchain,
    ) -> ProcessInvocation {
        target.remote_invocation(
            process_toolchain,
            ShellCommand::from_raw(format!(
                "mkdir -p {}",
                ShellArgument::new(&self.0.display().to_string()).to_command_text()
            )),
        )
    }

    fn rsync_invocation(
        &self,
        target: &SshTarget,
        process_toolchain: &ProcessToolchain,
        local_path: &Path,
    ) -> ProcessInvocation {
        ProcessInvocation::new(process_toolchain.rsync()).with_arguments([
            "-a".to_string(),
            "--delete".to_string(),
            format!("{}/", local_path.display()),
            format!("{}:{}/", target, self.0.display()),
        ])
    }
}

struct RemoteInputStage {
    target: SshTarget,
    process_toolchain: ProcessToolchain,
    inputs: Vec<GeneratedInput>,
}

impl RemoteInputStage {
    fn from_artifact(
        target: SshTarget,
        process_toolchain: ProcessToolchain,
        artifact: &MaterializedArtifact,
    ) -> Self {
        Self {
            target,
            process_toolchain,
            inputs: GeneratedInput::from_artifact(artifact),
        }
    }

    async fn run(&self) -> Result<BuildInputReferences> {
        let root = RemoteInputRoot::from_inputs(&self.inputs);
        for input in &self.inputs {
            let remote_directory = input.remote_directory(&root);
            remote_directory
                .create_invocation(&self.target, &self.process_toolchain)
                .expect_success()
                .await?;
            remote_directory
                .rsync_invocation(&self.target, &self.process_toolchain, &input.local_path)
                .expect_success()
                .await?;
        }
        Ok(BuildInputReferences {
            horizon_ref: self
                .input_ref(GeneratedInputName::Horizon, &root)
                .expect("remote input stage always includes horizon"),
            system_ref: self
                .input_ref(GeneratedInputName::System, &root)
                .expect("remote input stage always includes system"),
            deployment_ref: self.input_ref(GeneratedInputName::Deployment, &root),
            secrets_ref: self.input_ref(GeneratedInputName::Secrets, &root),
        })
    }

    fn input_ref(
        &self,
        name: GeneratedInputName,
        root: &RemoteInputRoot,
    ) -> Option<FlakeInputReference> {
        self.inputs
            .iter()
            .find(|input| input.name == name)
            .map(|input| input.flake_ref(root))
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct ExtraSubstituters {
    entries: Vec<ExtraSubstituter>,
}

impl ExtraSubstituters {
    fn from_horizon_nodes(horizon: &Horizon, names: &[HorizonNodeName]) -> Result<Self> {
        let mut entries = Vec::new();
        for name in names {
            let node = if *name == horizon.node.name {
                &horizon.node
            } else {
                horizon.ex_nodes.get(name).ok_or_else(|| {
                    Error::DeploymentRejected(format!(
                        "substituter node {name} not found in horizon"
                    ))
                })?
            };
            let Some(cache) = &node.nix_cache else {
                return Err(Error::DeploymentRejected(format!(
                    "substituter node {name} is not a Nix cache"
                )));
            };
            let Some(public_key) = &node.nix_pub_key_line else {
                return Err(Error::DeploymentRejected(format!(
                    "substituter node {name} has no Nix public key"
                )));
            };
            entries.push(ExtraSubstituter {
                url: cache.url.clone(),
                public_key: public_key.as_str().to_string(),
            });
        }
        Ok(Self { entries })
    }

    fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    fn urls_text(&self) -> String {
        self.entries
            .iter()
            .map(|entry| entry.url.as_str())
            .collect::<Vec<_>>()
            .join(" ")
    }

    fn public_keys_text(&self) -> String {
        self.entries
            .iter()
            .map(|entry| entry.public_key.as_str())
            .collect::<Vec<_>>()
            .join(" ")
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ExtraSubstituter {
    url: String,
    public_key: String,
}

struct NixBuild {
    flake: FlakeReference,
    horizon_ref: FlakeInputReference,
    system_ref: FlakeInputReference,
    deployment_ref: Option<FlakeInputReference>,
    secrets_ref: Option<FlakeInputReference>,
    extra_substituters: ExtraSubstituters,
    plan: BuildOnlyPlan,
    builder: SshTarget,
    process_toolchain: ProcessToolchain,
}

impl NixBuild {
    fn nix_invocation(&self) -> ProcessInvocation {
        let mut invocation = ProcessInvocation::new(self.process_toolchain.nix())
            .with_arguments([
                "build".to_string(),
                "--refresh".to_string(),
                "--no-link".to_string(),
                "--print-out-paths".to_string(),
                self.plan.target_attribute(&self.flake),
            ])
            .with_arguments([
                "--override-input".to_string(),
                "horizon".to_string(),
                self.horizon_ref.flake_ref(),
                "--override-input".to_string(),
                "system".to_string(),
                self.system_ref.flake_ref(),
            ]);
        if let Some(deployment_ref) = &self.deployment_ref {
            invocation = invocation.with_arguments([
                "--override-input".to_string(),
                "deployment".to_string(),
                deployment_ref.flake_ref(),
            ]);
        }
        if let Some(secrets_ref) = &self.secrets_ref {
            invocation = invocation.with_arguments([
                "--override-input".to_string(),
                "secrets".to_string(),
                secrets_ref.flake_ref(),
            ]);
        }
        if !self.extra_substituters.is_empty() {
            invocation = invocation.with_arguments([
                "--option".to_string(),
                "extra-substituters".to_string(),
                self.extra_substituters.urls_text(),
                "--option".to_string(),
                "extra-trusted-public-keys".to_string(),
                self.extra_substituters.public_keys_text(),
            ]);
        }
        invocation
    }

    async fn run(&self) -> Result<wire::StorePath> {
        let output = self
            .builder
            .remote_invocation(
                &self.process_toolchain,
                self.nix_invocation().to_shell_command(),
            )
            .capture_stdout()
            .await?;
        wire::StorePath::from_text(output.stdout().trim()).map_err(Into::into)
    }
}

fn horizon_cluster_name(value: &wire::ClusterName) -> Result<HorizonClusterName> {
    HorizonClusterName::try_new(value.as_str()).map_err(Into::into)
}

fn horizon_node_name(value: &wire::NodeName) -> Result<HorizonNodeName> {
    HorizonNodeName::try_new(value.as_str()).map_err(Into::into)
}

fn horizon_user_name(value: &wire::UserName) -> Result<HorizonUserName> {
    HorizonUserName::try_new(value.as_str()).map_err(Into::into)
}

fn failure_text(value: impl Into<String>) -> wire::FailureText {
    let mut text = value.into().replace(['\n', '\r'], " ");
    if text.trim().is_empty() {
        text = "deployment failed".to_string();
    }
    wire::FailureText::from_text(text).expect("sanitized failure text")
}
