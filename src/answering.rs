//! Each input a peer sends is answered through its contract's generated
//! dispatch, `input.answer(&answerer)`, on either socket.
//!
//! The answerer bears the generated `Answering` capabilities of both
//! contracts over the shared store. A capability answers with its contract's
//! own output: what the input's output carries, a rejection the store chose,
//! or `Refused` in the store's own words when the store could not carry the
//! input out. A deploy or test is admitted here, its durable row written
//! first, and then handed to the actor that runs it; its effects stay on that
//! actor.

use std::sync::Arc;

use meta_signal_lojix::{
    AcceptedTest, ActualizedDeploySubmission, AppliedPin, AppliedRetire, AppliedUnpin,
    DeployHandle, Output as MetaOutput, PinRequest, RetireRequest, TestRequest, UnpinRequest,
};
use signal_lojix::{
    ConfigurationReceipt, DeploymentWatch, EventLogPage, EventLogRange, GenerationListing,
    LojixNexusConfiguration, Output as OrdinaryOutput, QueryRejectionReason, RejectedQuery,
    RejectedUnwatch, RejectedWatch, Selection, StoreFailure, SubscriptionClose, SubscriptionClosed,
    SubscriptionOpened, SubscriptionToken, TestRunListing, TestRunLookup, UnwatchRejectionReason,
    WatchRejectionReason,
};

use crate::daemon::{JobExecutors, JobLaunching as _};
use crate::deployment_watch::WatchesDeployments as _;
use crate::runtime_model::{Operation, Outcome};
use crate::schema_runtime::{
    DeployDriving as _, DeploySubmissionOutcome, Operating as _, RejectionVocabulary as _,
    RuntimeConfiguration, RuntimeCore as _, SchemaRuntime, TestDriving as _, TestSubmissionOutcome,
};
use crate::{CommitMarking as _, DurableStore as _, Store};

/// One input being answered against the shared store, with the job executors
/// an admitted deploy or test is handed to.
pub struct Answerer {
    store: Arc<Store>,
    configuration: Arc<RuntimeConfiguration>,
    executors: JobExecutors,
}

/// Making an answerer, and the per-call engine each capability works through.
pub trait Answers: Sized {
    fn over(
        store: Arc<Store>,
        configuration: Arc<RuntimeConfiguration>,
        executors: JobExecutors,
    ) -> Self;

    /// A fresh engine over the shared store. Each capability takes its own,
    /// so an admitted job's cursor leaves with the engine that holds it.
    fn engine(&self) -> SchemaRuntime;
}

impl Answers for Answerer {
    fn over(
        store: Arc<Store>,
        configuration: Arc<RuntimeConfiguration>,
        executors: JobExecutors,
    ) -> Self {
        Self {
            store,
            configuration,
            executors,
        }
    }

    fn engine(&self) -> SchemaRuntime {
        SchemaRuntime::with_store_and_configuration(self.store.clone(), self.configuration.clone())
    }
}

/// A contract's output for an input its store could not carry out:
/// `Refused`, in the store's own words.
pub trait RefusedByStore {
    fn refused_by_store(failure: StoreFailure) -> Self;
}

impl RefusedByStore for OrdinaryOutput {
    fn refused_by_store(failure: StoreFailure) -> Self {
        Self::Refused(failure)
    }
}

impl RefusedByStore for MetaOutput {
    fn refused_by_store(failure: StoreFailure) -> Self {
        Self::Refused(failure)
    }
}

/// A store outcome no capability asks for, answered as the store's own
/// failure: the store answered the operation with something it never makes.
trait Unasked {
    fn unasked<Output: RefusedByStore>(&self, operation: &str) -> Output;
}

impl<Outcome: std::fmt::Debug> Unasked for Outcome {
    fn unasked<Output: RefusedByStore>(&self, operation: &str) -> Output {
        Output::refused_by_store(StoreFailure(format!(
            "the store answered {operation} with {self:?}"
        )))
    }
}

impl signal_lojix::Answering for Answerer {
    fn configure(
        &self,
        lojix_nexus_configuration: &LojixNexusConfiguration,
    ) -> Result<ConfigurationReceipt, OrdinaryOutput> {
        match self.engine().operate(Operation::OrdinaryConfigure(
            lojix_nexus_configuration.clone(),
        )) {
            Outcome::OrdinaryConfigured(receipt) => Ok(receipt),
            Outcome::OrdinaryConfigurationRejected(rejection) => {
                Err(OrdinaryOutput::ConfigurationRejected(rejection))
            }
            Outcome::StoreRefused(failure) => Err(OrdinaryOutput::refused_by_store(failure)),
            other => Err(other.unasked("an ordinary Configure")),
        }
    }

    /// Issue a subscription token and answer `DeploymentsWatched`. The watch is
    /// registered with its filter before the commit sequence is read, so every
    /// phase event committed after that sequence reaches it; the connection
    /// that carried it then claims the watcher's stream.
    fn watch_deployments(
        &self,
        deployment_watch: &DeploymentWatch,
    ) -> Result<SubscriptionOpened, OrdinaryOutput> {
        let subscription_token = self.store.next_subscription_token();
        self.store
            .deployment_watchers()
            .register(subscription_token, deployment_watch.clone());
        match self.store.commit_sequence() {
            Ok(commit_sequence) => Ok(SubscriptionOpened {
                subscription_token: SubscriptionToken(subscription_token),
                commit_sequence: signal_lojix::CommitSequence(commit_sequence),
            }),
            Err(_) => {
                self.store.deployment_watchers().end(subscription_token);
                Err(OrdinaryOutput::WatchRejected(RejectedWatch {
                    watch_rejection_reason: WatchRejectionReason::StreamUnavailable,
                }))
            }
        }
    }

    fn query(&self, selection: &Selection) -> Result<GenerationListing, OrdinaryOutput> {
        match self
            .engine()
            .operate(Operation::QueryGenerations(selection.clone()))
        {
            Outcome::GenerationsQueried(listing) => Ok(listing),
            other => Err(other.answered_as_query("a Query")),
        }
    }

    fn query_deployment_events(
        &self,
        event_log_range: &EventLogRange,
    ) -> Result<EventLogPage, OrdinaryOutput> {
        match self
            .engine()
            .operate(Operation::ReadEventLog(event_log_range.clone()))
        {
            Outcome::EventLogRead(page) => Ok(page),
            other => Err(other.answered_as_query("a QueryDeploymentEvents")),
        }
    }

    fn query_test_runs(
        &self,
        test_run_lookup: &TestRunLookup,
    ) -> Result<TestRunListing, OrdinaryOutput> {
        match self
            .engine()
            .operate(Operation::QueryTestRuns(test_run_lookup.clone()))
        {
            Outcome::TestRunsQueried(listing) => Ok(listing),
            other => Err(other.answered_as_query("a QueryTestRuns")),
        }
    }

    /// End a live deployment watch, which closes the connection serving it.
    /// A token issued and no longer live is already closed; any other token
    /// is unknown.
    fn unwatch(
        &self,
        subscription_close: &SubscriptionClose,
    ) -> Result<SubscriptionClosed, OrdinaryOutput> {
        let subscription_token = subscription_close.subscription_token;
        let token = subscription_token.0;
        if self.store.deployment_watchers().end(token) {
            return Ok(SubscriptionClosed { subscription_token });
        }
        let unwatch_rejection_reason = if self.store.subscription_token_issued(token) {
            UnwatchRejectionReason::SubscriptionAlreadyClosed
        } else {
            UnwatchRejectionReason::SubscriptionTokenUnknown
        };
        Err(OrdinaryOutput::UnwatchRejected(RejectedUnwatch {
            unwatch_rejection_reason,
            subscription_token,
        }))
    }
}

/// A read that did not answer with what its query asked for.
trait QueryMissing {
    /// A missed read is the query's rejection, at the marker it was read
    /// against; a failed store is `Refused`.
    fn answered_as_query(self, operation: &str) -> OrdinaryOutput;
}

impl QueryMissing for Outcome {
    fn answered_as_query(self, operation: &str) -> OrdinaryOutput {
        match self {
            Self::ReadMissed(report) => OrdinaryOutput::QueryRejected(RejectedQuery {
                query_rejection_reason: QueryRejectionReason::GenerationUnknown,
                database_marker: report.database_marker,
            }),
            Self::StoreRefused(failure) => OrdinaryOutput::refused_by_store(failure),
            other => other.unasked(operation),
        }
    }
}

impl meta_signal_lojix::Answering for Answerer {
    fn configure(
        &self,
        lojix_nexus_configuration: &LojixNexusConfiguration,
    ) -> Result<ConfigurationReceipt, MetaOutput> {
        match self
            .engine()
            .operate(Operation::MetaConfigure(lojix_nexus_configuration.clone()))
        {
            Outcome::MetaConfigured(receipt) => Ok(receipt),
            Outcome::MetaConfigurationRejected(rejection) => {
                Err(MetaOutput::ConfigurationRejected(rejection))
            }
            Outcome::StoreRefused(failure) => Err(MetaOutput::refused_by_store(failure)),
            other => Err(other.unasked("a privileged Configure")),
        }
    }

    fn reverse_configuration(&self) -> Result<ConfigurationReceipt, MetaOutput> {
        match self.engine().operate(Operation::ReverseConfiguration) {
            Outcome::ConfigurationReversed(receipt) => Ok(receipt),
            Outcome::MetaConfigurationRejected(rejection) => {
                Err(MetaOutput::ConfigurationRejected(rejection))
            }
            Outcome::StoreRefused(failure) => Err(MetaOutput::refused_by_store(failure)),
            other => Err(other.unasked("a ReverseConfiguration")),
        }
    }

    fn retire(&self, retire_request: &RetireRequest) -> Result<AppliedRetire, MetaOutput> {
        match self
            .engine()
            .operate(Operation::RetireGeneration(retire_request.clone()))
        {
            Outcome::GenerationRetired(applied) => Ok(applied),
            Outcome::WriteRejected(report) => Err(MetaOutput::RetireRejected(
                meta_signal_lojix::RejectedRetire {
                    retire_rejection_reason: report.rejection_reason.retire_reason(),
                    database_marker: report.database_marker,
                },
            )),
            Outcome::StoreRefused(failure) => Err(MetaOutput::refused_by_store(failure)),
            other => Err(other.unasked("a Retire")),
        }
    }

    fn pin(&self, pin_request: &PinRequest) -> Result<AppliedPin, MetaOutput> {
        match self
            .engine()
            .operate(Operation::PinGeneration(pin_request.clone()))
        {
            Outcome::GenerationPinned(applied) => Ok(applied),
            Outcome::WriteRejected(report) => {
                Err(MetaOutput::PinRejected(meta_signal_lojix::RejectedPin {
                    pin_rejection_reason: report.rejection_reason.pin_reason(),
                    database_marker: report.database_marker,
                }))
            }
            Outcome::StoreRefused(failure) => Err(MetaOutput::refused_by_store(failure)),
            other => Err(other.unasked("a Pin")),
        }
    }

    /// Admit a deploy: reserve one of the deploy executor's slots, write the
    /// deployment's durable record and job row, and hand the engine holding
    /// its cursor to the deploy-job actor, which runs the pipeline. Over the
    /// cap, the deploy is rejected `DeploymentInFlight` with a record of its
    /// own. The answer is the admission; the deployment's end is observed by
    /// its identifier.
    fn deploy(
        &self,
        actualized_deploy_submission: &ActualizedDeploySubmission,
    ) -> Result<DeployHandle, MetaOutput> {
        let mut engine = self.engine();
        let Some(slot) = self.executors.reserve_deploy() else {
            return Err(engine
                .reject_deployment_in_flight(actualized_deploy_submission.clone())
                .into());
        };
        match engine.submit_deploy(actualized_deploy_submission.clone()) {
            DeploySubmissionOutcome::Accepted(handle) => {
                self.executors.launch_deploy(engine, slot);
                Ok(handle)
            }
            other => Err(other.into()),
        }
    }

    /// Admit a test: reserve one of the test executor's slots, write the
    /// test run's pending row, and hand the engine holding its cursor to the
    /// test-job actor, which runs the dispatch. Over the cap, the test is
    /// rejected `SubstrateUnavailable`.
    fn test(&self, test_request: &TestRequest) -> Result<AcceptedTest, MetaOutput> {
        let mut engine = self.engine();
        let Some(slot) = self.executors.reserve_test() else {
            return match self.store.commit_sequence() {
                Ok(commit_sequence) => {
                    Err(MetaOutput::TestRejected(meta_signal_lojix::RejectedTest {
                        test_rejection_reason:
                            meta_signal_lojix::TestRejectionReason::SubstrateUnavailable,
                        database_marker: signal_lojix::DatabaseMarker::at_commit(commit_sequence),
                    }))
                }
                Err(error) => Err(MetaOutput::refused_by_store(StoreFailure(format!(
                    "read durable state marker for test rejection: {error}"
                )))),
            };
        };
        match engine.submit_test(test_request.clone()) {
            TestSubmissionOutcome::Accepted(accepted) => {
                self.executors.launch_test(engine, slot);
                Ok(accepted)
            }
            TestSubmissionOutcome::Rejected(rejected) => Err(MetaOutput::TestRejected(rejected)),
        }
    }

    fn unpin(&self, unpin_request: &UnpinRequest) -> Result<AppliedUnpin, MetaOutput> {
        match self
            .engine()
            .operate(Operation::UnpinGeneration(unpin_request.clone()))
        {
            Outcome::GenerationUnpinned(applied) => Ok(applied),
            Outcome::WriteRejected(report) => Err(MetaOutput::UnpinRejected(
                meta_signal_lojix::RejectedUnpin {
                    unpin_rejection_reason: report.rejection_reason.unpin_reason(),
                    database_marker: report.database_marker,
                },
            )),
            Outcome::StoreRefused(failure) => Err(MetaOutput::refused_by_store(failure)),
            other => Err(other.unasked("an Unpin")),
        }
    }
}

/// A deploy admission that was not accepted, as the output that answers it.
impl From<DeploySubmissionOutcome> for MetaOutput {
    fn from(outcome: DeploySubmissionOutcome) -> Self {
        match outcome {
            DeploySubmissionOutcome::Accepted(handle) => Self::DeployAccepted(handle),
            DeploySubmissionOutcome::Rejected(rejected) => Self::DeployRejected(rejected),
            DeploySubmissionOutcome::Refused(refused) => Self::DeployRefused(refused),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_store_failure_is_answered_refused_in_the_store_s_words_on_either_socket() {
        let failure = StoreFailure(String::from("read durable generations: the file is gone"));
        assert_eq!(
            OrdinaryOutput::refused_by_store(failure.clone()),
            OrdinaryOutput::Refused(failure.clone())
        );
        assert_eq!(
            MetaOutput::refused_by_store(failure.clone()),
            MetaOutput::Refused(failure)
        );
    }

    #[test]
    fn a_missed_read_is_the_query_s_rejection_and_a_failed_store_is_refused() {
        let marker = signal_lojix::DatabaseMarker::at_commit(7);
        let missed = Outcome::ReadMissed(crate::runtime_model::RejectionReport {
            rejection_reason: crate::runtime_model::RejectionReason::EventLogPositionOutOfRange,
            database_marker: marker.clone(),
        });
        assert_eq!(
            missed.answered_as_query("a QueryDeploymentEvents"),
            OrdinaryOutput::QueryRejected(RejectedQuery {
                query_rejection_reason: QueryRejectionReason::GenerationUnknown,
                database_marker: marker,
            })
        );
        assert_eq!(
            Outcome::StoreRefused(StoreFailure("the file is gone".into()))
                .answered_as_query("a Query"),
            OrdinaryOutput::Refused(StoreFailure("the file is gone".into()))
        );
    }
}
