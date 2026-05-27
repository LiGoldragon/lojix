//! SourceStager — owns the source-materialization plane.
//!
//! The deploy pipeline should not treat a Horizon view as an opaque
//! build argument forever. This actor is the first concrete source
//! staging plane: it receives a schema-emitted `PlanRecord`, derives a
//! content digest, writes an inspectable source artifact under the
//! configured state directory, and returns a schema-emitted
//! `SourceRecord` for the build plane.

use std::path::PathBuf;

use kameo::Actor;
use kameo::actor::ActorRef;
use kameo::error::Infallible;
use kameo::message::{Context, Message};

use crate::error::{Error, Result};
use crate::generated::{ActorReply, ActorRequest, PlanRecord, SourceDigest, SourceRecord};

pub struct SourceStager {
    root_directory: PathBuf,
    last_staged: Option<SourceRecord>,
}

impl SourceStager {
    pub fn new(state_directory: crate::generated::StateDirectory) -> Self {
        Self {
            root_directory: PathBuf::from(state_directory.0).join("sources"),
            last_staged: None,
        }
    }

    pub fn root_directory(&self) -> &std::path::Path {
        &self.root_directory
    }

    pub fn last_staged(&self) -> Option<&SourceRecord> {
        self.last_staged.as_ref()
    }

    async fn stage_sources(&mut self, plan: PlanRecord) -> Result<SourceRecord> {
        tokio::fs::create_dir_all(&self.root_directory).await?;
        let source = SourceRecord::from_plan(&plan);
        let artifact_path = self
            .root_directory
            .join(format!("{}.source", source.source_digest.0));
        tokio::fs::write(&artifact_path, source.artifact_text()).await?;
        self.last_staged = Some(source.clone());
        Ok(source)
    }
}

impl Actor for SourceStager {
    type Args = Self;
    type Error = Infallible;

    async fn on_start(
        state: Self::Args,
        _actor_ref: ActorRef<Self>,
    ) -> std::result::Result<Self, Self::Error> {
        Ok(state)
    }
}

impl Message<ActorRequest> for SourceStager {
    type Reply = Result<ActorReply>;

    async fn handle(
        &mut self,
        message: ActorRequest,
        _context: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        match message {
            ActorRequest::StageSources(plan) => {
                let source = self.stage_sources(plan).await?;
                Ok(ActorReply::SourcesReady(source))
            }
            other => Err(Error::UnexpectedActorRequest {
                actor: "SourceStager",
                request: other.variant_name(),
            }),
        }
    }
}

impl PlanRecord {
    pub fn source_digest(&self) -> SourceDigest {
        let mut hasher = blake3::Hasher::new();
        hasher.update(self.deployment_identifier.0.to_string().as_bytes());
        hasher.update(b"\n");
        hasher.update(self.horizon_view.0.as_bytes());
        hasher.update(b"\n");
        hasher.update(self.target_node.0.as_bytes());
        SourceDigest(hasher.finalize().to_hex().to_string())
    }
}

impl SourceRecord {
    pub fn from_plan(plan: &PlanRecord) -> Self {
        Self {
            deployment_identifier: plan.deployment_identifier.clone(),
            horizon_view: plan.horizon_view.clone(),
            target_node: plan.target_node.clone(),
            source_digest: plan.source_digest(),
        }
    }

    pub fn artifact_text(&self) -> String {
        format!("{}\n", self.to_nota())
    }
}

impl ActorRequest {
    pub fn variant_name(&self) -> &'static str {
        match self {
            Self::AuthorizeDeployment(_) => "AuthorizeDeployment",
            Self::MaterializePlan(_) => "MaterializePlan",
            Self::StageSources(_) => "StageSources",
            Self::ExecuteBuild(_) => "ExecuteBuild",
            Self::CopyClosure(_) => "CopyClosure",
            Self::ActivateGeneration(_) => "ActivateGeneration",
            Self::PinGenerationRoot(_) => "PinGenerationRoot",
            Self::EmitObservation(_) => "EmitObservation",
        }
    }
}

impl ActorReply {
    pub fn variant_name(&self) -> &'static str {
        match self {
            Self::AuthorizationDecided(_) => "AuthorizationDecided",
            Self::PlanReady(_) => "PlanReady",
            Self::SourcesReady(_) => "SourcesReady",
            Self::BuildComplete(_) => "BuildComplete",
            Self::CopyComplete(_) => "CopyComplete",
            Self::ActivationComplete(_) => "ActivationComplete",
            Self::PinReceipt(_) => "PinReceipt",
            Self::ObservationAccepted(_) => "ObservationAccepted",
        }
    }
}
