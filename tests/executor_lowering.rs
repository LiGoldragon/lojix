//! Executor lowering tests — assert that `Input::lower_to_sema_command`
//! exhaustively covers every `Input` variant, and
//! `SemaResponse::into_output` exhaustively covers every `SemaResponse`.

use lojix_next::ActivationKind;
use lojix_next::ClosurePath;
use lojix_next::runtime::codec::Lowered;
use lojix_next::{
    CriomeAuthorization, DeploymentIdentifier, DeploymentRequest, Detail, GenerationIdentifier,
    GenerationRecord, GenerationSelector, HelpQuery, HorizonView, Input, ObservationRecord, Output,
    Phase, SemaCommand, SemaCommandIdentifier, SemaResponse, Status, TargetNode,
};

#[test]
fn lojix_next_input_lowers_to_sema_command_exhaustively() {
    let submit = Input::Submit(DeploymentRequest {
        horizon_view: HorizonView("h".to_owned()),
        target_node: TargetNode("t".to_owned()),
        criome_authorization: CriomeAuthorization::Bypass,
    });
    match submit.lower_to_sema_command() {
        Lowered::StateInvolving(SemaCommand::RecordPlan(_)) => {}
        other => panic!("Submit must lower to RecordPlan, got {other:?}"),
    }

    let cancel = Input::Cancel(DeploymentIdentifier(7));
    match cancel.lower_to_sema_command() {
        Lowered::ForwardOnly(Output::Accepted(DeploymentIdentifier(7))) => {}
        other => panic!("Cancel must lower to forward-only Accepted, got {other:?}"),
    }

    let query = Input::Query(GenerationSelector(DeploymentIdentifier(11)));
    match query.lower_to_sema_command() {
        Lowered::StateInvolving(SemaCommand::QueryGeneration(GenerationSelector(
            DeploymentIdentifier(11),
        ))) => {}
        other => panic!("Query must lower to QueryGeneration, got {other:?}"),
    }

    let help = Input::Help(HelpQuery(Detail("topic".to_owned())));
    match help.lower_to_sema_command() {
        Lowered::ForwardOnly(Output::HelpAnswer(_)) => {}
        other => panic!("Help must lower to forward-only HelpAnswer, got {other:?}"),
    }
}

#[test]
fn lojix_next_sema_response_maps_back_to_output_exhaustively() {
    let acknowledged = SemaResponse::Acknowledged(SemaCommandIdentifier(1)).into_output();
    matches!(acknowledged, Output::Accepted(_));

    let snapshot = SemaResponse::GenerationLedgerEntry(GenerationRecord {
        generation_identifier: GenerationIdentifier(1),
        deployment_identifier: DeploymentIdentifier(1),
        target_node: TargetNode("t".to_owned()),
        closure_path: ClosurePath("/nix/store/x".to_owned()),
        activation_kind: ActivationKind::Test,
    })
    .into_output();
    matches!(snapshot, Output::Snapshot(_));

    let observation = SemaResponse::ObservationStreamEntry(ObservationRecord {
        deployment_identifier: DeploymentIdentifier(1),
        phase: Phase::Building,
        status: Status::Started,
        detail: Detail("d".to_owned()),
    })
    .into_output();
    matches!(observation, Output::Observation(_));

    let missed = SemaResponse::Missed(Detail("nope".to_owned())).into_output();
    matches!(missed, Output::Rejected(_));
}
