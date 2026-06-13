use kameo::Actor;
use kameo::actor::ActorRef;
use kameo::error::Infallible;
use kameo::message::{Context, Message};
use signal_criome::{AuthorizationScope, ObjectDigest};

use crate::error::{Error, Result};
use crate::wire;

pub struct CriomeAuthorization {
    policy: CriomeAuthorizationPolicy,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CriomeAuthorizationPolicy {
    GrantForTests,
    Unavailable { reason: String },
}

pub struct AuthorizeDeployment {
    deployment: wire::DeploymentId,
    submission: wire::DeploymentRequest,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeploymentAuthorizationGrant {
    deployment: wire::DeploymentId,
    request_digest: ObjectDigest,
    scope: AuthorizationScope,
}

impl CriomeAuthorization {
    pub fn new(policy: CriomeAuthorizationPolicy) -> Self {
        Self { policy }
    }
}

impl CriomeAuthorizationPolicy {
    pub fn grant_for_tests() -> Self {
        Self::GrantForTests
    }

    pub fn unavailable_until_criome_socket_lands() -> Self {
        Self::Unavailable {
            reason:
                "criome authorization is unavailable until the signal-criome daemon client lands"
                    .to_string(),
        }
    }

    fn authorize(
        &self,
        request: DeploymentAuthorizationRequest,
    ) -> Result<DeploymentAuthorizationGrant> {
        match self {
            Self::GrantForTests => Ok(DeploymentAuthorizationGrant {
                deployment: request.deployment,
                request_digest: request.request_digest,
                scope: request.scope,
            }),
            Self::Unavailable { reason } => Err(Error::DeploymentRejected(reason.clone())),
        }
    }
}

impl AuthorizeDeployment {
    pub fn new(deployment: wire::DeploymentId, submission: wire::DeploymentRequest) -> Self {
        Self {
            deployment,
            submission,
        }
    }
}

impl DeploymentAuthorizationGrant {
    pub fn request_digest(&self) -> &ObjectDigest {
        &self.request_digest
    }

    pub fn scope(&self) -> &AuthorizationScope {
        &self.scope
    }
}

impl Actor for CriomeAuthorization {
    type Args = Self;
    type Error = Infallible;

    async fn on_start(
        state: Self::Args,
        _actor_ref: ActorRef<Self>,
    ) -> std::result::Result<Self, Self::Error> {
        Ok(state)
    }
}

impl Message<AuthorizeDeployment> for CriomeAuthorization {
    type Reply = Result<DeploymentAuthorizationGrant>;

    async fn handle(
        &mut self,
        message: AuthorizeDeployment,
        _context: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        let request = DeploymentAuthorizationRequest::from_submission(
            message.deployment,
            &message.submission,
        )?;
        self.policy.authorize(request)
    }
}

struct DeploymentAuthorizationRequest {
    deployment: wire::DeploymentId,
    request_digest: ObjectDigest,
    scope: AuthorizationScope,
}

impl DeploymentAuthorizationRequest {
    fn from_submission(
        deployment: wire::DeploymentId,
        submission: &wire::DeploymentRequest,
    ) -> Result<Self> {
        let request_digest = submission.canonical_digest()?;
        Ok(Self {
            deployment,
            request_digest: ObjectDigest::new(request_digest.as_str()),
            scope: DeploymentAuthorizationScope::from_submission(submission).into_criome_scope(),
        })
    }
}

struct DeploymentAuthorizationScope {
    text: String,
}

impl DeploymentAuthorizationScope {
    fn from_submission(submission: &wire::DeploymentRequest) -> Self {
        Self {
            text: format!(
                "lojix:{}:{}:{}",
                submission.cluster.as_str(),
                submission.node.as_str(),
                DeploymentPlanScope::from_plan(&submission.plan).as_str(),
            ),
        }
    }

    fn into_criome_scope(self) -> AuthorizationScope {
        AuthorizationScope::new(self.text)
    }
}

struct DeploymentPlanScope {
    text: String,
}

impl DeploymentPlanScope {
    fn from_plan(plan: &wire::DeploymentPlan) -> Self {
        let text = match plan {
            wire::DeploymentPlan::FullOsDeployment(deployment) => {
                format!("full-os:{}", deployment.action.as_scope_segment())
            }
            wire::DeploymentPlan::OsOnlyDeployment(deployment) => {
                format!("os-only:{}", deployment.action.as_scope_segment())
            }
            wire::DeploymentPlan::HomeOnlyDeployment(deployment) => {
                format!(
                    "home:{}:{}",
                    deployment.user.as_str(),
                    deployment.mode.as_scope_segment(),
                )
            }
        };
        Self { text }
    }

    fn as_str(&self) -> &str {
        &self.text
    }
}

trait SystemActionScopeSegment {
    fn as_scope_segment(&self) -> &'static str;
}

impl SystemActionScopeSegment for wire::SystemAction {
    fn as_scope_segment(&self) -> &'static str {
        match self {
            Self::Eval => "eval",
            Self::Build => "build",
            Self::Boot => "boot",
            Self::Switch => "switch",
            Self::Test => "test",
            Self::BootOnce => "boot-once",
        }
    }
}

trait HomeModeScopeSegment {
    fn as_scope_segment(&self) -> &'static str;
}

impl HomeModeScopeSegment for wire::HomeMode {
    fn as_scope_segment(&self) -> &'static str {
        match self {
            Self::Build => "build",
            Self::Profile => "profile",
            Self::Activate => "activate",
        }
    }
}
