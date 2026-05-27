//! Wire-frame symmetry over the schema-emitted root types.

use lojix_next::{
    AcceptedReply, CriomeAuthorization, DeploymentIdentifier, DeploymentRequest, HorizonView,
    Input, Output, TargetNode,
};

#[test]
fn lojix_next_input_output_round_trip_rkyv() {
    let input = Input::Submit(DeploymentRequest {
        horizon_view: HorizonView("horizon: minimal".to_owned()),
        target_node: TargetNode("nspawn-dune".to_owned()),
        criome_authorization: CriomeAuthorization::OperatorAllowlist,
    });
    let frame = input.encode_signal_frame().expect("encode input frame");
    let (route, decoded) = Input::decode_signal_frame(&frame).expect("decode input frame");
    assert_eq!(route, input.route());
    assert_eq!(decoded, input);

    let output = Output::Accepted(AcceptedReply::new(DeploymentIdentifier(42)));
    let frame = output.encode_signal_frame().expect("encode output frame");
    let (route, decoded) = Output::decode_signal_frame(&frame).expect("decode output frame");
    assert_eq!(route, output.route());
    assert_eq!(decoded, output);
}
