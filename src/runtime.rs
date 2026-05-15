use kameo::Actor;
use kameo::actor::ActorRef;
use kameo::error::Infallible;
use kameo::message::{Context, Message};

use crate::wire;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeConfiguration {
    operator_identity: wire::OperatorIdentity,
    owned_cluster: wire::ClusterName,
    peer_daemons: Vec<wire::PeerDaemonBinding>,
}

impl RuntimeConfiguration {
    pub fn from_daemon_configuration(configuration: &wire::LojixDaemonConfiguration) -> Self {
        Self {
            operator_identity: configuration.operator_identity.clone(),
            owned_cluster: configuration.owned_cluster.clone(),
            peer_daemons: configuration.peer_daemons.clone(),
        }
    }

    pub fn for_in_process_tests() -> Self {
        Self {
            operator_identity: wire::OperatorIdentity::from_text("in_process_test")
                .expect("static operator identity"),
            owned_cluster: wire::ClusterName::from_text("test_cluster")
                .expect("static cluster name"),
            peer_daemons: Vec::new(),
        }
    }

    pub fn operator_identity(&self) -> &wire::OperatorIdentity {
        &self.operator_identity
    }

    pub fn owned_cluster(&self) -> &wire::ClusterName {
        &self.owned_cluster
    }

    pub fn peer_daemons(&self) -> &[wire::PeerDaemonBinding] {
        &self.peer_daemons
    }
}

pub struct RuntimeRoot {
    configuration: RuntimeConfiguration,
    next_deployment_observation_token: u64,
    next_cache_retention_observation_token: u64,
}

impl RuntimeRoot {
    pub fn new() -> Self {
        Self::with_configuration(RuntimeConfiguration::for_in_process_tests())
    }

    pub fn with_configuration(configuration: RuntimeConfiguration) -> Self {
        Self {
            configuration,
            next_deployment_observation_token: 1,
            next_cache_retention_observation_token: 1,
        }
    }

    pub fn configuration(&self) -> &RuntimeConfiguration {
        &self.configuration
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
