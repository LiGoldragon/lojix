//! AuthorizationGate — decides whether a deployment is allowed.
//!
//! Per `skills/abstractions.md` §"Schema-emitted nouns":
//! `DeploymentRequest::authorize(&AuthorizationPolicy)` is a method
//! on the schema-emitted noun; the AuthorizationGate actor's job is
//! to own the policy and run the verb.

use kameo::Actor;
use kameo::actor::ActorRef;
use kameo::error::Infallible;
use kameo::message::{Context, Message};

use crate::generated::{CriomeAuthorization, DeploymentRequest};

/// Authorization policy — the local rule set this gate enforces.
/// For the pilot we model three modes: AllowAll (test fixture),
/// CriomeBackedRequired (rejects everything until criome lands),
/// and DenyAll (explicit lockdown).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AuthorizationPolicy {
    AllowAll,
    CriomeBackedRequired,
    DenyAll,
}

impl AuthorizationPolicy {
    pub fn allow_all_for_tests() -> Self {
        Self::AllowAll
    }
}

/// AuthorizationGate actor. The State field carries the policy —
/// the noun the actor IS.
pub struct AuthorizationGate {
    policy: AuthorizationPolicy,
}

impl AuthorizationGate {
    pub fn new(policy: AuthorizationPolicy) -> Self {
        Self { policy }
    }

    pub fn policy(&self) -> &AuthorizationPolicy {
        &self.policy
    }
}

impl Actor for AuthorizationGate {
    type Args = Self;
    type Error = Infallible;

    async fn on_start(
        state: Self::Args,
        _actor_ref: ActorRef<Self>,
    ) -> std::result::Result<Self, Self::Error> {
        Ok(state)
    }
}

impl DeploymentRequest {
    /// Authorise this deployment request against a policy.
    ///
    /// Per `skills/abstractions.md` §"Schema-emitted nouns": the verb
    /// `authorize` belongs on the schema-emitted noun whose data the
    /// verb reads.
    pub fn authorize(&self, policy: &AuthorizationPolicy) -> CriomeAuthorization {
        match (&self.criome_authorization, policy) {
            (_, AuthorizationPolicy::AllowAll) => self.criome_authorization.clone(),
            (CriomeAuthorization::Bypass, AuthorizationPolicy::CriomeBackedRequired) => {
                CriomeAuthorization::OperatorAllowlist
            }
            (CriomeAuthorization::OperatorAllowlist, AuthorizationPolicy::CriomeBackedRequired) => {
                CriomeAuthorization::OperatorAllowlist
            }
            (CriomeAuthorization::Criome, AuthorizationPolicy::CriomeBackedRequired) => {
                CriomeAuthorization::Criome
            }
            (_, AuthorizationPolicy::DenyAll) => CriomeAuthorization::OperatorAllowlist,
        }
    }
}

/// Ask the AuthorizationGate to decide on a deployment request.
pub struct AuthorizeMessage(pub DeploymentRequest);

impl Message<AuthorizeMessage> for AuthorizationGate {
    type Reply = Result<CriomeAuthorization, std::convert::Infallible>;

    async fn handle(
        &mut self,
        message: AuthorizeMessage,
        _context: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        Ok(message.0.authorize(&self.policy))
    }
}
