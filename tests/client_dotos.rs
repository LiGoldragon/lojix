//! Thin-client Datom ingress tests.
//!
//! Each public CLI accepts one canonical inline Datom request and lowers it to
//! exactly one generated contract input. Ordinary and owner traffic retain
//! their separate authority-tier contracts; neither client reads a
//! caller-selected request file.

use std::ffi::OsString;

use lojix::client::{MetaClient, OrdinaryClient};
use protos::Textualizable;

fn text(value: &str) -> protos::Text {
    protos::Text::try_from(value).expect("fixture text")
}

fn ordinary_query() -> signal_lojix::Request {
    signal_lojix::Request::Query(signal_lojix::Selection::ByNode(signal_lojix::NodeSelector(
        text("alpha"),
        text("node-1"),
        None,
    )))
}

fn owner_pin() -> meta_signal_lojix::Request {
    meta_signal_lojix::Request::Pin(meta_signal_lojix::PinRequest(
        text("alpha"),
        text("node-1"),
        42.into(),
        text("keep"),
    ))
}

#[test]
fn public_clients_reject_non_single_inline_datom_arguments() {
    let inline = ordinary_query().textualize();
    let cases = [
        Vec::new(),
        vec![OsString::from("--help")],
        vec![OsString::from("--pretty")],
        vec![OsString::from("/tmp/not-an-inline-request.datom")],
        vec![OsString::from(&inline), OsString::from("extra")],
    ];
    for arguments in cases {
        assert!(
            OrdinaryClient::from_arguments(arguments).is_err(),
            "ordinary CLI must reject every non-single-inline shape"
        );
    }

    assert!(MetaClient::from_arguments(Vec::new()).is_err());
    assert!(
        MetaClient::from_arguments([OsString::from("--pretty")]).is_err(),
        "owner CLI must reject presentation flags"
    );
}

#[test]
fn ordinary_client_decodes_canonical_inline_datom() {
    let input = ordinary_query();
    let client = OrdinaryClient::from_arguments([OsString::from(input.textualize())])
        .expect("decode ordinary Datom request");
    assert_eq!(client.input(), &input);
}

#[test]
fn meta_client_decodes_canonical_inline_datom() {
    let input = owner_pin();
    let client = MetaClient::from_arguments([OsString::from(input.textualize())])
        .expect("decode owner Datom request");
    assert_eq!(client.input(), &input);
}
