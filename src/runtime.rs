use std::path::{Path, PathBuf};

use kameo::Actor;
use kameo::actor::{ActorRef, Spawn};
use kameo::error::Infallible;
use kameo::message::{Context, Message};

use crate::deploy::{
    AllocateDeployment, DeploymentActor, EventLogActor, GarbageCollectionRoots,
    OpenDeploymentObservationSubscription, StartDeployment,
};
use crate::error::Result as LojixResult;
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
        let root = unique_in_process_directory();
        Self {
            operator_identity: wire::OperatorIdentity::from_text("in_process_test")
                .expect("static operator identity"),
            owned_cluster: wire::ClusterName::from_text("test_cluster")
                .expect("static cluster name"),
            peer_daemons: Vec::new(),
            state_directory: root.join("state"),
            gc_root_directory: root.join("gcroots"),
            process_toolchain: ProcessToolchain::production(),
        }
    }

    pub fn with_process_toolchain(mut self, process_toolchain: ProcessToolchain) -> Self {
        self.process_toolchain = process_toolchain;
        self
    }

    pub fn with_state_directory(mut self, state_directory: impl Into<PathBuf>) -> Self {
        self.state_directory = state_directory.into();
        self
    }

    pub fn with_gc_root_directory(mut self, gc_root_directory: impl Into<PathBuf>) -> Self {
        self.gc_root_directory = gc_root_directory.into();
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
    garbage_collection_roots: ActorRef<GarbageCollectionRoots>,
    next_cache_retention_observation_token: u64,
}

impl RuntimeRoot {
    pub fn new() -> Self {
        Self::with_configuration(RuntimeConfiguration::for_in_process_tests())
    }

    pub fn with_configuration(configuration: RuntimeConfiguration) -> Self {
        Self::try_with_configuration(configuration).expect("runtime state opens")
    }

    pub fn try_with_configuration(configuration: RuntimeConfiguration) -> LojixResult<Self> {
        let event_log = EventLogActor::spawn(EventLogActor::open(configuration.state_directory())?);
        let garbage_collection_roots = GarbageCollectionRoots::spawn(GarbageCollectionRoots::new(
            configuration.gc_root_directory().to_path_buf(),
        ));
        let deployment_actor = DeploymentActor::spawn(DeploymentActor::new(
            configuration.clone(),
            event_log.clone(),
            garbage_collection_roots.clone(),
        ));
        Ok(Self {
            configuration,
            deployment_actor,
            event_log,
            garbage_collection_roots,
            next_cache_retention_observation_token: 1,
        })
    }

    pub fn configuration(&self) -> &RuntimeConfiguration {
        &self.configuration
    }

    pub fn garbage_collection_roots(&self) -> &ActorRef<GarbageCollectionRoots> {
        &self.garbage_collection_roots
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
                let deployment = match self.event_log.ask(AllocateDeployment).await {
                    Ok(deployment) => deployment,
                    Err(error) => {
                        return Ok(deployment_rejected(format!(
                            "failed to allocate deployment id: {error}"
                        )));
                    }
                };
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
                match self
                    .event_log
                    .ask(OpenDeploymentObservationSubscription { subscription })
                    .await
                {
                    Ok(opened) => wire::Reply::DeploymentObservationSubscriptionOpened(opened),
                    Err(error) => deployment_rejected(format!(
                        "failed to open deployment observation subscription: {error}"
                    )),
                }
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

fn deployment_rejected(message: impl Into<String>) -> wire::Reply {
    wire::Reply::DeploymentRejected(wire::DeploymentRejected {
        reason: wire::DeploymentRejectionReason::InvalidRequest,
        detail: Some(failure_text(message)),
    })
}

fn failure_text(message: impl Into<String>) -> wire::FailureText {
    let message = message.into().replace(['\n', '\r'], " ");
    wire::FailureText::from_text(message).expect("sanitized failure text")
}

fn unique_in_process_directory() -> PathBuf {
    let timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system time after epoch")
        .as_nanos();
    std::env::temp_dir().join(format!(
        "lojix-in-process-{}-{timestamp}",
        std::process::id()
    ))
}
