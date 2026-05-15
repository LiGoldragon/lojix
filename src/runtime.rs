use kameo::Actor;
use kameo::actor::ActorRef;
use kameo::error::Infallible;
use kameo::message::{Context, Message};

use crate::wire;

pub struct RuntimeRoot {
    next_deployment_observation_token: u64,
    next_cache_retention_observation_token: u64,
}

impl RuntimeRoot {
    pub fn new() -> Self {
        Self {
            next_deployment_observation_token: 1,
            next_cache_retention_observation_token: 1,
        }
    }
}

impl Default for RuntimeRoot {
    fn default() -> Self {
        Self::new()
    }
}

impl Actor for RuntimeRoot {
    type Args = Self;
    type Error = Infallible;

    async fn on_start(
        arguments: Self::Args,
        _actor_ref: ActorRef<Self>,
    ) -> Result<Self, Self::Error> {
        Ok(arguments)
    }
}

pub struct RuntimeRequest {
    pub request: wire::Request,
}

impl Message<RuntimeRequest> for RuntimeRoot {
    type Reply = Result<wire::Reply, Infallible>;

    async fn handle(
        &mut self,
        message: RuntimeRequest,
        _context: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        let reply = match message.request {
            wire::Request::DeploymentSubmission(_) => {
                wire::Reply::DeploymentRejected(wire::DeploymentRejected {
                    reason: wire::DeploymentRejectionReason::InvalidRequest,
                    detail: Some(
                        wire::FailureText::from_text(
                            "deploy pipeline actors are not active in this runtime slice",
                        )
                        .expect("static failure text"),
                    ),
                })
            }
            wire::Request::CacheRetentionRequest(_) => {
                wire::Reply::CacheRetentionRejected(wire::CacheRetentionRejected {
                    reason: wire::CacheRetentionRejectionReason::StoreUnavailable,
                    detail: Some(
                        wire::FailureText::from_text(
                            "cache retention actors are not active in this runtime slice",
                        )
                        .expect("static failure text"),
                    ),
                })
            }
            wire::Request::GenerationQuery(_) => {
                wire::Reply::GenerationListing(wire::GenerationListing {
                    generations: Vec::new(),
                })
            }
            wire::Request::DeploymentObservationSubscription(_) => {
                let token =
                    wire::DeploymentObservationToken::new(self.next_deployment_observation_token);
                self.next_deployment_observation_token += 1;
                wire::Reply::DeploymentObservationSubscriptionOpened(
                    wire::DeploymentObservationSubscriptionOpened {
                        token,
                        observations: Vec::new(),
                    },
                )
            }
            wire::Request::CacheRetentionObservationSubscription(_) => {
                let token = wire::CacheRetentionObservationToken::new(
                    self.next_cache_retention_observation_token,
                );
                self.next_cache_retention_observation_token += 1;
                wire::Reply::CacheRetentionObservationSubscriptionOpened(
                    wire::CacheRetentionObservationSubscriptionOpened {
                        token,
                        observations: Vec::new(),
                    },
                )
            }
            wire::Request::DeploymentObservationRetraction(token) => {
                wire::Reply::DeploymentObservationSubscriptionClosed(
                    wire::DeploymentObservationSubscriptionClosed { token },
                )
            }
            wire::Request::CacheRetentionObservationRetraction(token) => {
                wire::Reply::CacheRetentionObservationSubscriptionClosed(
                    wire::CacheRetentionObservationSubscriptionClosed { token },
                )
            }
        };
        Ok(reply)
    }
}
