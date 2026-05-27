//! Methods attached to the schema-emitted nouns for the executor flow:
//! `Input -> SemaCommand -> SemaResponse -> Output`.

use crate::generated::{
    DeploymentRequest, GenerationSelector, HelpQuery, HelpReply, Input, Output, PlanRecord,
    SemaCommand, SemaResponse,
};

/// Result of lowering an `Input` to executor work.
///
/// Forward-only inputs (Help, Cancel-with-no-effect) yield an
/// immediate `Output`; state-involving inputs yield a `SemaCommand`
/// to drive the SEMA writer.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Lowered {
    StateInvolving(SemaCommand),
    ForwardOnly(Output),
}

impl Input {
    /// Lower this Input to either a SemaCommand (state-involving) or
    /// a forward-only Output. The Engine composes the SEMA round-trip
    /// for the state-involving case.
    pub fn lower_to_sema_command(self) -> Lowered {
        match self {
            Self::Submit(request) => {
                Lowered::StateInvolving(SemaCommand::RecordPlan(request.into_plan_record()))
            }
            Self::Cancel(identifier) => Lowered::ForwardOnly(Output::Accepted(identifier)),
            Self::Query(selector) => {
                Lowered::StateInvolving(SemaCommand::QueryGeneration(selector))
            }
            Self::Help(query) => Lowered::ForwardOnly(Output::HelpAnswer(query.into_help_reply())),
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
    /// Map a SemaResponse back to the user-facing Output. Every SEMA
    /// response maps deterministically; this is the executor's reply
    /// shaping step.
    pub fn into_output(self) -> Output {
        match self {
            Self::Acknowledged(_command_id) => {
                // For Submit, the deployment identifier was carried into
                // the plan record before this point; the Engine pairs
                // the acknowledgement with the assigned deployment id.
                // For other Acks not paired with a richer reply, we
                // surface a synthetic Accepted with deployment id 0.
                Output::Accepted(crate::generated::DeploymentIdentifier(0))
            }
            Self::GenerationLedgerEntry(record) => Output::Snapshot(record),
            Self::ObservationStreamEntry(record) => Output::Observation(record),
            Self::Missed(detail) => {
                // Map Missed (lookup not found / state error) to a
                // typed rejection; the detail string is dropped by the
                // pilot's Output shape, which only carries a typed
                // rejection reason. The Engine can pre-format Output
                // with a richer mapping when a follow-on schema grows
                // a `Rejected (RejectionReason Detail)` variant.
                let _ = detail;
                Output::Rejected(crate::generated::RejectionReason::MalformedRequest)
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
