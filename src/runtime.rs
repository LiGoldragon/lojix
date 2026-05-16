use std::path::{Path, PathBuf};

use kameo::Actor;
use kameo::actor::{ActorRef, Spawn};
use kameo::error::Infallible;
use kameo::message::{Context, Message};

use crate::deploy::{
    DeploymentActor, DeploymentObservationSnapshot, EventLogActor, StartDeployment,
};
use crate::process::ProcessToolchain;
use crate::wire;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeConfiguration {
    operator_identity: wire::OperatorIdentity,
    owned_cluster: wire::ClusterName,
    peer_daemons: Vec<wire::PeerDaemonBinding>,
    state_directory: PathBuf,
    gc_root_directory: PathBuf,
    process_toolchain: ProcessToolchain,
}

impl RuntimeConfiguration {
    pub fn from_daemon_configuration(configuration: &wire::LojixDaemonConfiguration) -> Self {
        Self {
            operator_identity: configuration.operator_identity.clone(),
            owned_cluster: configuration.owned_cluster.clone(),
            peer_daemons: configuration.peer_daemons.clone(),
            state_directory: PathBuf::from(configuration.state_directory.as_str()),
            gc_root_directory: PathBuf::from(configuration.gc_root_directory.as_str()),
            process_toolchain: ProcessToolchain::production(),
        }
    }

    pub fn for_in_process_tests() -> Self {
        Self {
            operator_identity: wire::OperatorIdentity::from_text("in_process_test")
                .expect("static operator identity"),
            owned_cluster: wire::ClusterName::from_text("test_cluster")
                .expect("static cluster name"),
            peer_daemons: Vec::new(),
            state_directory: std::env::temp_dir().join("lojix-in-process-state"),
            gc_root_directory: std::env::temp_dir().join("lojix-in-process-gcroots"),
            process_toolchain: ProcessToolchain::production(),
        }
    }

    pub fn with_process_toolchain(mut self, process_toolchain: ProcessToolchain) -> Self {
        self.process_toolchain = process_toolchain;
        self
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

    pub fn state_directory(&self) -> &Path {
        &self.state_directory
    }

    pub fn gc_root_directory(&self) -> &Path {
        &self.gc_root_directory
    }

    pub fn process_toolchain(&self) -> &ProcessToolchain {
        &self.process_toolchain
    }
}

pub struct RuntimeRoot {
    configuration: RuntimeConfiguration,
    deployment_actor: ActorRef<DeploymentActor>,
    event_log: ActorRef<EventLogActor>,
    next_deployment_identifier: u64,
    next_deployment_observation_token: u64,
    next_cache_retention_observation_token: u64,
}

impl RuntimeRoot {
    pub fn new() -> Self {
        Self::with_configuration(RuntimeConfiguration::for_in_process_tests())
    }

    pub fn with_configuration(configuration: RuntimeConfiguration) -> Self {
        let event_log = EventLogActor::spawn(EventLogActor::new());
        let deployment_actor = DeploymentActor::spawn(DeploymentActor::new(
            configuration.clone(),
            event_log.clone(),
        ));
        Self {
            configuration,
            deployment_actor,
            event_log,
            next_deployment_identifier: 1,
            next_deployment_observation_token: 1,
            next_cache_retention_observation_token: 1,
        }
    }

    pub fn configuration(&self) -> &RuntimeConfiguration {
        &self.configuration
    }

    fn next_deployment(&mut self) -> wire::DeploymentId {
        let value = format!("deployment_{}", self.next_deployment_identifier);
        self.next_deployment_identifier += 1;
        wire::DeploymentId::from_text(value).expect("generated deployment id")
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
            wire::Request::DeploymentSubmission(submission) => {
                let deployment = self.next_deployment();
                match self
                    .deployment_actor
                    .ask(StartDeployment {
                        deployment,
                        submission,
                    })
                    .await
                {
                    Ok(reply) => reply,
                    Err(_) => wire::Reply::DeploymentRejected(wire::DeploymentRejected {
                        reason: wire::DeploymentRejectionReason::InvalidRequest,
                        detail: Some(
                            wire::FailureText::from_text(
                                "deployment actor stopped before accepting work",
                            )
                            .expect("static failure text"),
                        ),
                    }),
                }
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
            wire::Request::DeploymentObservationSubscription(subscription) => {
                let token =
                    wire::DeploymentObservationToken::new(self.next_deployment_observation_token);
                self.next_deployment_observation_token += 1;
                let observations = self
                    .event_log
                    .ask(DeploymentObservationSnapshot { subscription })
                    .await
                    .unwrap_or_default();
                wire::Reply::DeploymentObservationSubscriptionOpened(
                    wire::DeploymentObservationSubscriptionOpened {
                        token,
                        observations,
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
