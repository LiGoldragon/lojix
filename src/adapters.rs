//! Direct typed boundary between generated contracts and the runtime model.
//!
//! Every value whose shape the contracts declare is already the contract's
//! own type inside the runtime, so it crosses this seam unchanged. What is
//! converted here is only what still differs: the engine's ingress and egress
//! enumerations, its five-way Selection, the deploy submission that carries
//! its Horizon definition inside, and the ordinary projections that keep only
//! canonical store paths.

use crate::Payload;
use crate::inspected_text::{NixStorePath, StoreItemShape};
use crate::runtime_model as memory;
use meta_signal_lojix as owner;
use signal_lojix as ordinary;

#[derive(Debug, thiserror::Error)]
#[error("generated contract value cannot be represented by the lojix runtime model")]
pub struct WireShapeError;

pub trait Lowerable<T> {
    fn lower(self) -> Result<T, WireShapeError>;
}
pub trait Raisable<T> {
    fn raise(self) -> Result<T, WireShapeError>;
}

impl From<WireShapeError> for crate::Error {
    fn from(error: WireShapeError) -> Self {
        Self::Wire(error.to_string())
    }
}

impl Lowerable<memory::Selection> for ordinary::Selection {
    fn lower(self) -> Result<memory::Selection, WireShapeError> {
        Ok(match self {
            Self::ByNode(value) => memory::Selection::ByNode(value),
            Self::ByGeneration(value) => memory::Selection::ByGeneration(value),
            Self::ByDeployment(value) => memory::Selection::ByDeployment(value),
        })
    }
}

/// The contract carries the Horizon definition beside the deployment; the
/// runtime submission carries it inside, where the engine reads it.
impl Lowerable<memory::DeploySubmission> for owner::ActualizedDeploySubmission {
    fn lower(self) -> Result<memory::DeploySubmission, WireShapeError> {
        let horizon_definition_option = self.horizon_definition_option;
        Ok(match self.deploy_submission {
            owner::DeploySubmission::Host(host) => {
                memory::DeploySubmission::Host(memory::HostDeployment {
                    cluster_name: host.cluster_name,
                    node_name: host.node_name,
                    host_composition: host.host_composition,
                    proposal_source: host.proposal_source,
                    secrets_input: host.secrets_input,
                    flake_reference: host.flake_reference,
                    deployment_transport: host.deployment_transport,
                    deployment_input_mode: host.deployment_input_mode,
                    horizon_definition_option,
                    deployment_output_selector: host.deployment_output_selector,
                    activation_backend: host.activation_backend,
                    host_deploy_action: host.host_deploy_action,
                    source_revision_policy: host.source_revision_policy,
                    nix_builder_spec_option: host.nix_builder_spec_option,
                    extra_substituter_vector: host.extra_substituter_vector,
                })
            }
            owner::DeploySubmission::UserEnvironment(user) => {
                memory::DeploySubmission::UserEnvironment(memory::UserEnvironmentDeployment {
                    cluster_name: user.cluster_name,
                    node_name: user.node_name,
                    user_name: user.user_name,
                    proposal_source: user.proposal_source,
                    secrets_input: user.secrets_input,
                    flake_reference: user.flake_reference,
                    deployment_transport: user.deployment_transport,
                    deployment_input_mode: user.deployment_input_mode,
                    horizon_definition_option,
                    deployment_output_selector: user.deployment_output_selector,
                    activation_backend: user.activation_backend,
                    user_environment_action: user.user_environment_action,
                    source_revision_policy: user.source_revision_policy,
                    nix_builder_spec_option: user.nix_builder_spec_option,
                    extra_substituter_vector: user.extra_substituter_vector,
                })
            }
        })
    }
}

/// A generation's closure path is shown on the ordinary socket only when it is
/// a canonical store item root.
impl Raisable<ordinary::Generation> for memory::Generation {
    fn raise(self) -> Result<ordinary::Generation, WireShapeError> {
        let closure_path_option = NixStorePath::from(self.closure_path.payload().as_str())
            .is_canonical_item_root()
            .then_some(self.closure_path);
        Ok(ordinary::Generation {
            generation_identifier: self.generation_identifier,
            deployment_identifier: self.deployment_identifier,
            cluster_name: self.cluster_name,
            node_name: self.node_name,
            generation_artifact: self.generation_artifact,
            activation_effect: self.activation_effect,
            generation_slot: self.generation_slot,
            closure_path_option,
            immutable_revision_option: self.immutable_revision_option,
        })
    }
}
impl Raisable<ordinary::GenerationListing> for memory::GenerationListing {
    fn raise(self) -> Result<ordinary::GenerationListing, WireShapeError> {
        Ok(ordinary::GenerationListing {
            generation_vector: self
                .generation_vector
                .into_iter()
                .map(Raisable::raise)
                .collect::<Result<_, _>>()?,
            deployment_record_vector: self.deployment_record_vector,
            database_marker: self.database_marker,
        })
    }
}
/// A test run's closure path is shown on the ordinary socket only when it is
/// a canonical store item root.
impl Raisable<ordinary::TestRunRecord> for memory::TestRunRecord {
    fn raise(self) -> Result<ordinary::TestRunRecord, WireShapeError> {
        let closure_path_option = self
            .closure_path_option
            .filter(|path| NixStorePath::from(path.payload().as_str()).is_canonical_item_root());
        Ok(ordinary::TestRunRecord {
            test_run_identifier: self.test_run_identifier,
            cluster_name: self.cluster_name,
            first_node_name: self.node,
            second_node_name: self.host,
            test_mode: self.test_mode,
            test_run_phase: self.test_run_phase,
            test_outcome: self.test_outcome,
            closure_path_option,
        })
    }
}
impl Raisable<ordinary::TestRunListing> for memory::TestRunListing {
    fn raise(self) -> Result<ordinary::TestRunListing, WireShapeError> {
        Ok(ordinary::TestRunListing {
            test_run_record_vector: self
                .test_run_record_vector
                .into_iter()
                .map(Raisable::raise)
                .collect::<Result<_, _>>()?,
            database_marker: self.database_marker,
        })
    }
}

impl Lowerable<memory::OrdinaryIngress> for ordinary::Input {
    fn lower(self) -> Result<memory::OrdinaryIngress, WireShapeError> {
        Ok(match self {
            ordinary::Input::Configure(value) => memory::OrdinaryIngress::Configure(value),
            ordinary::Input::WatchDeployments(value) => {
                memory::OrdinaryIngress::WatchDeployments(value)
            }
            ordinary::Input::Query(value) => memory::OrdinaryIngress::Query(value.lower()?),
            ordinary::Input::QueryDeploymentEvents(value) => {
                memory::OrdinaryIngress::Query(memory::Selection::ByEventLog(value))
            }
            ordinary::Input::QueryTestRuns(value) => {
                memory::OrdinaryIngress::Query(memory::Selection::ByTestRun(value))
            }
            ordinary::Input::Unwatch(value) => memory::OrdinaryIngress::Unwatch(value),
        })
    }
}
impl Lowerable<memory::MetaIngress> for owner::Input {
    fn lower(self) -> Result<memory::MetaIngress, WireShapeError> {
        Ok(match self {
            owner::Input::Configure(value) => memory::MetaIngress::Configure(value),
            owner::Input::ReverseConfiguration => memory::MetaIngress::ReverseConfiguration,
            owner::Input::Retire(value) => memory::MetaIngress::Retire(value),
            owner::Input::Pin(value) => memory::MetaIngress::Pin(value),
            owner::Input::Deploy(value) => memory::MetaIngress::Deploy(value.lower()?),
            owner::Input::Test(value) => memory::MetaIngress::Test(value),
            owner::Input::Unpin(value) => memory::MetaIngress::Unpin(value),
        })
    }
}

impl Raisable<ordinary::Output> for memory::OrdinaryEgress {
    fn raise(self) -> Result<ordinary::Output, WireShapeError> {
        Ok(match self {
            memory::OrdinaryEgress::Configured(value) => ordinary::Output::Configured(value),
            memory::OrdinaryEgress::ConfigurationRejected(value) => {
                ordinary::Output::ConfigurationRejected(value)
            }
            memory::OrdinaryEgress::TestRunsQueried(value) => {
                ordinary::Output::TestRunsQueried(value.raise()?)
            }
            memory::OrdinaryEgress::UnwatchRejected(value) => {
                ordinary::Output::UnwatchRejected(value)
            }
            memory::OrdinaryEgress::QueryRejected(value) => ordinary::Output::QueryRejected(value),
            memory::OrdinaryEgress::Watching(value) => ordinary::Output::DeploymentsWatched(value),
            memory::OrdinaryEgress::Queried(value) => ordinary::Output::Queried(value.raise()?),
            memory::OrdinaryEgress::DeploymentEventsQueried(value) => {
                ordinary::Output::DeploymentEventsQueried(value)
            }
            memory::OrdinaryEgress::Unwatched(value) => ordinary::Output::Unwatched(value),
            memory::OrdinaryEgress::WatchRejected(value) => ordinary::Output::WatchRejected(value),
            memory::OrdinaryEgress::StoreRefused(detail) => {
                ordinary::Output::Refused(ordinary::Refusal::Store(detail))
            }
        })
    }
}
impl Raisable<owner::Output> for memory::MetaEgress {
    fn raise(self) -> Result<owner::Output, WireShapeError> {
        Ok(match self {
            memory::MetaEgress::Configured(value) => owner::Output::Configured(value),
            memory::MetaEgress::ConfigurationRejected(value) => {
                owner::Output::ConfigurationRejected(value)
            }
            memory::MetaEgress::ConfigurationReversed(value) => {
                owner::Output::ConfigurationReversed(value)
            }
            memory::MetaEgress::PinRejected(value) => owner::Output::PinRejected(value),
            memory::MetaEgress::DeployRefused(value) => owner::Output::DeployRefused(value),
            memory::MetaEgress::DeployRejected(value) => owner::Output::DeployRejected(value),
            memory::MetaEgress::DeployAccepted(value) => owner::Output::DeployAccepted(value),
            memory::MetaEgress::TestRejected(value) => owner::Output::TestRejected(value),
            memory::MetaEgress::Unpinned(value) => owner::Output::Unpinned(value),
            memory::MetaEgress::Tested(value) => owner::Output::Tested(value),
            memory::MetaEgress::UnpinRejected(value) => owner::Output::UnpinRejected(value),
            memory::MetaEgress::DeployTerminal(value) => owner::Output::DeployTerminal(value),
            memory::MetaEgress::Pinned(value) => owner::Output::Pinned(value),
            memory::MetaEgress::RetireRejected(value) => owner::Output::RetireRejected(value),
            memory::MetaEgress::Retired(value) => owner::Output::Retired(value),
            memory::MetaEgress::StoreRefused(detail) => {
                owner::Output::Refused(ordinary::Refusal::Store(detail))
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn marker() -> memory::DatabaseMarker {
        memory::DatabaseMarker {
            commit_sequence: memory::CommitSequence(7),
            state_digest: memory::StateDigest(7),
        }
    }
    fn generation(path: &str) -> memory::Generation {
        memory::Generation {
            generation_identifier: memory::GenerationIdentifier(1),
            deployment_identifier: memory::DeploymentIdentifier(1),
            cluster_name: memory::ClusterName("alpha".into()),
            node_name: memory::NodeName("node-1".into()),
            generation_artifact: memory::GenerationArtifact::BaseHost,
            activation_effect: memory::ActivationEffect::LiveActivation,
            generation_slot: memory::GenerationSlot::Current,
            closure_path: memory::ClosurePath(path.into()),
            immutable_revision_option: None,
        }
    }
    #[test]
    fn ordinary_projection_keeps_only_canonical_store_item_roots() {
        let valid = "/nix/store/0123456789abcdfghijklmnpqrsvwxyz-system-toplevel";
        let visible = memory::OrdinaryEgress::Queried(memory::GenerationListing {
            generation_vector: vec![generation(valid)],
            deployment_record_vector: Vec::new(),
            database_marker: marker(),
        })
        .raise()
        .unwrap();
        assert!(format!("{visible:?}").contains(valid));
        for private in [
            "/home/li/private",
            "/nix/store/short-system",
            "/nix/store/0123456789abcdfghijklmnpqrsvwxyz-secret",
        ] {
            let visible = memory::OrdinaryEgress::Queried(memory::GenerationListing {
                generation_vector: vec![generation(private)],
                deployment_record_vector: Vec::new(),
                database_marker: marker(),
            })
            .raise()
            .unwrap();
            assert!(!format!("{visible:?}").contains(private));
        }
    }
    #[test]
    fn a_store_failure_is_answered_as_a_store_refusal_on_either_socket() {
        let detail = String::from("read durable generations: the file is gone");
        assert_eq!(
            memory::OrdinaryEgress::StoreRefused(detail.clone())
                .raise()
                .unwrap(),
            ordinary::Output::Refused(ordinary::Refusal::Store(detail.clone()))
        );
        assert_eq!(
            memory::MetaEgress::StoreRefused(detail.clone())
                .raise()
                .unwrap(),
            owner::Output::Refused(ordinary::Refusal::Store(detail))
        );
    }
    #[test]
    fn the_split_query_inputs_lower_to_the_selections_the_engine_reads() {
        let range = ordinary::EventLogRange {
            first_event_log_position: ordinary::EventLogPosition(1),
            second_event_log_position: ordinary::EventLogPosition(9),
        };
        assert!(matches!(
            ordinary::Input::QueryDeploymentEvents(range)
                .lower()
                .unwrap(),
            memory::OrdinaryIngress::Query(memory::Selection::ByEventLog(_))
        ));
        let lookup = ordinary::TestRunLookup {
            cluster_name: ordinary::ClusterName("alpha".into()),
            node_name: ordinary::NodeName("node-1".into()),
            test_run_identifier_option: None,
        };
        assert!(matches!(
            ordinary::Input::QueryTestRuns(lookup).lower().unwrap(),
            memory::OrdinaryIngress::Query(memory::Selection::ByTestRun(_))
        ));
    }
    #[test]
    fn a_contract_value_reaches_the_engine_as_it_was_sent() {
        let close = ordinary::SubscriptionClose {
            subscription_token: ordinary::SubscriptionToken(-1),
        };
        assert_eq!(
            ordinary::Input::Unwatch(close.clone()).lower().unwrap(),
            memory::OrdinaryIngress::Unwatch(close)
        );
    }
}
