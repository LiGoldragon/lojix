//! Methods attached to the schema-emitted nouns for the executor flow:
//! `Input -> SemaCommand -> SemaResponse -> Output`.
//!
//! The Output reply variants each carry a `DatabaseMarker` (a
//! commit-sequence + state-hash record). Nexus populates the marker
//! from sema-engine's transaction state right before sending the
//! reply back through the Signal plane.

use crate::generated::{
    AcceptedReply, DatabaseMarker, DeploymentRequest, GenerationSelector, HelpAnswerReply,
    HelpQuery, HelpReply, Input, ObservationRecord, ObservedReply, Output, PlanRecord,
    RejectedReply, RejectionReason, SemaCommand, SemaResponse, SnapshotReply,
};

/// Result of lowering an `Input` to executor work.
///
/// Forward-only inputs (Help, Cancel-with-no-effect) yield a payload
/// that Nexus stamps with a DatabaseMarker before turning into the
/// final Output. State-involving inputs yield a `SemaCommand` to
/// drive the SEMA writer; Nexus pairs the resulting SemaResponse
/// with a DatabaseMarker the same way.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Lowered {
    StateInvolving(SemaCommand),
    ForwardOnly(ForwardOnlyReply),
}

/// A payload-shaped reply ready to be stamped with the current
/// DatabaseMarker and turned into an `Output`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ForwardOnlyReply {
    HelpAnswer(HelpReply),
    Accepted(crate::generated::DeploymentIdentifier),
    Rejected(RejectionReason),
}

impl ForwardOnlyReply {
    /// Pair this forward-only reply with a database marker into a typed Output.
    pub fn stamped(self, marker: DatabaseMarker) -> Output {
        match self {
            Self::HelpAnswer(help_reply) => Output::HelpAnswer(HelpAnswerReply {
                help_reply,
                database_marker: marker,
            }),
            Self::Accepted(deployment_identifier) => Output::Accepted(AcceptedReply {
                deployment_identifier,
                database_marker: marker,
            }),
            Self::Rejected(rejection_reason) => Output::Rejected(RejectedReply {
                rejection_reason,
                database_marker: marker,
            }),
        }
    }
}

impl Input {
    /// Lower this Input to either a SemaCommand (state-involving) or
    /// a forward-only payload. Nexus stamps the database marker.
    pub fn lower_to_sema_command(self) -> Lowered {
        match self {
            Self::Submit(request) => {
                Lowered::StateInvolving(SemaCommand::RecordPlan(request.into_plan_record()))
            }
            Self::Cancel(identifier) => {
                Lowered::ForwardOnly(ForwardOnlyReply::Accepted(identifier))
            }
            Self::Query(selector) => {
                Lowered::StateInvolving(SemaCommand::QueryGeneration(selector))
            }
            Self::Help(query) => {
                Lowered::ForwardOnly(ForwardOnlyReply::HelpAnswer(query.into_help_reply()))
            }
        }
    }
}

impl DeploymentRequest {
    /// Materialise a plan record from this deployment request. The
    /// plan-record carries the cluster/horizon view; the deployment
    /// identifier is assigned by the SEMA writer when the plan is
    /// recorded.
    pub fn into_plan_record(self) -> PlanRecord {
        PlanRecord {
            // The deployment identifier slot is overwritten by the
            // Store when it allocates the next id. The marker zero is
            // intentional and never observed outside Store::apply.
            deployment_identifier: crate::generated::DeploymentIdentifier(0),
            horizon_view: self.horizon_view,
            target_node: self.target_node,
        }
    }
}

impl HelpQuery {
    /// Turn a help query into a help reply. For the pilot, the reply
    /// echoes the help topic and lists the four root operations.
    pub fn into_help_reply(self) -> HelpReply {
        let topic = self.0.0;
        HelpReply(format!(
            "lojix-next help [topic={topic}]: operations are Submit, Cancel, Query, Help"
        ))
    }
}

impl SemaResponse {
    /// Map a SemaResponse + a DatabaseMarker to the user-facing
    /// Output. Every SEMA response maps deterministically; this is the
    /// executor's reply shaping step. Nexus owns the marker
    /// population, so this method takes it as a separate object.
    pub fn into_output(self, marker: DatabaseMarker) -> Output {
        match self {
            Self::Acknowledged(_command_id) => {
                // For Submit, the deployment identifier was carried
                // into the plan record before this point; the Nexus
                // pairs the acknowledgement with the assigned
                // deployment id. For other Acks not paired with a
                // richer reply, we surface a synthetic Accepted with
                // deployment id 0.
                Output::Accepted(AcceptedReply {
                    deployment_identifier: crate::generated::DeploymentIdentifier(0),
                    database_marker: marker,
                })
            }
            Self::GenerationLedgerEntry(record) => Output::Snapshot(SnapshotReply {
                generation_record: record,
                database_marker: marker,
            }),
            Self::ObservationStreamEntry(record) => Output::Observation(ObservedReply {
                observation_record: record,
                database_marker: marker,
            }),
            Self::Missed(detail) => {
                // Map Missed (lookup not found / state error) to a
                // typed rejection; the detail string is dropped by the
                // pilot's Output shape, which only carries a typed
                // rejection reason.
                let _ = detail;
                Output::Rejected(RejectedReply {
                    rejection_reason: RejectionReason::MalformedRequest,
                    database_marker: marker,
                })
            }
        }
    }
}

impl GenerationSelector {
    /// Read the underlying deployment-identifier this selector targets.
    pub fn target_deployment(&self) -> &crate::generated::DeploymentIdentifier {
        &self.0
    }
}

impl Output {
    /// Read the DatabaseMarker on whichever reply variant this Output
    /// carries. Every Output carries one — the marker is the
    /// transaction proof that the daemon attached to this reply.
    pub fn database_marker(&self) -> &DatabaseMarker {
        match self {
            Self::Accepted(reply) => &reply.database_marker,
            Self::Rejected(reply) => &reply.database_marker,
            Self::Observation(reply) => &reply.database_marker,
            Self::Snapshot(reply) => &reply.database_marker,
            Self::HelpAnswer(reply) => &reply.database_marker,
        }
    }
}

impl ObservationRecord {
    /// Turn this observation into the typed reply that Nexus stamps.
    pub fn into_observed_reply(self, marker: DatabaseMarker) -> ObservedReply {
        ObservedReply {
            observation_record: self,
            database_marker: marker,
        }
    }
}
