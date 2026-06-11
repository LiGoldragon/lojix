//! Thin-client NOTA edge tests.
//!
//! The daemon socket wire remains rkyv signal frames. These tests cover the
//! human-facing CLI adapter: inline NOTA, `.nota` files, signal-encoded files,
//! authority-tier classification, and reply rendering.

#[cfg(not(feature = "nota-text"))]
use lojix::Error;
#[cfg(feature = "nota-text")]
use lojix::client::ClientReply;
use lojix::client::ClientRequest;
use meta_signal_lojix::schema::lib as meta;
use signal_lojix::schema::lib as ordinary;
use triad_runtime::{ComponentArgument, ComponentCommand};

fn ordinary_query() -> ordinary::Input {
    ordinary::Input::Query(ordinary::Query::new(ordinary::Selection::ByNode(
        ordinary::NodeSelector {
            cluster_name: ordinary::ClusterName::new("alpha"),
            node_name: ordinary::NodeName::new("node-1"),
            kind: None,
        },
    )))
}

fn owner_pin() -> meta::Input {
    meta::Input::Pin(meta::Pin::new(meta::PinRequest {
        cluster_name: ordinary::ClusterName::new("alpha"),
        node_name: ordinary::NodeName::new("node-1"),
        generation_identifier: ordinary::GenerationIdentifier::new(42),
        pin_label: ordinary::PinLabel::new("keep"),
    }))
}

fn argument_from_single(argument: impl Into<String>) -> ComponentArgument {
    ComponentCommand::from_arguments([argument.into()])
        .nota_argument()
        .expect("single component argument")
}

fn argument_from_path(path: &std::path::Path) -> ComponentArgument {
    argument_from_single(path.display().to_string())
}

#[test]
#[cfg(not(feature = "nota-text"))]
fn inline_nota_requires_text_feature() {
    let argument = argument_from_single("(Query ((ByNode (alpha node-1 None))))");
    let error = ClientRequest::from_argument(argument).expect_err("inline NOTA must reject");
    assert!(matches!(error, Error::NotaTextUnsupported));
}

#[test]
#[cfg(not(feature = "nota-text"))]
fn nota_file_requires_text_feature() {
    let directory = tempfile::tempdir().expect("tempdir");
    let path = directory.path().join("query.nota");
    std::fs::write(&path, "(Query ((ByNode (alpha node-1 None))))").expect("write NOTA file");

    let error =
        ClientRequest::from_argument(argument_from_path(&path)).expect_err("NOTA file must reject");
    assert!(matches!(error, Error::NotaTextUnsupported));
}

#[test]
#[cfg(feature = "nota-text")]
fn inline_nota_classifies_ordinary_request() {
    let input = ordinary_query();
    let argument = argument_from_single(input.to_string());

    let request = ClientRequest::from_argument(argument).expect("decode ordinary NOTA");

    assert_eq!(request, ClientRequest::Ordinary(input));
}

#[test]
#[cfg(feature = "nota-text")]
fn inline_nota_classifies_owner_request() {
    let input = owner_pin();
    let argument = argument_from_single(input.to_string());

    let request = ClientRequest::from_argument(argument).expect("decode owner NOTA");

    assert_eq!(request, ClientRequest::Owner(input));
}

#[test]
#[cfg(feature = "nota-text")]
fn nota_file_classifies_ordinary_request() {
    let directory = tempfile::tempdir().expect("tempdir");
    let path = directory.path().join("query.nota");
    let input = ordinary_query();
    std::fs::write(&path, input.to_string()).expect("write NOTA request");

    let request =
        ClientRequest::from_argument(argument_from_path(&path)).expect("decode ordinary NOTA file");

    assert_eq!(request, ClientRequest::Ordinary(input));
}

#[test]
fn non_nota_file_classifies_owner_signal_frame() {
    let directory = tempfile::tempdir().expect("tempdir");
    let path = directory.path().join("pin.rkyv");
    let input = owner_pin();
    std::fs::write(
        &path,
        input
            .encode_signal_frame()
            .expect("encode owner signal frame"),
    )
    .expect("write signal frame");

    let request =
        ClientRequest::from_argument(argument_from_path(&path)).expect("decode owner signal file");

    assert_eq!(request, ClientRequest::Owner(input));
}

#[test]
fn non_nota_file_classifies_ordinary_signal_frame() {
    let directory = tempfile::tempdir().expect("tempdir");
    let path = directory.path().join("query.rkyv");
    let input = ordinary_query();
    std::fs::write(
        &path,
        input
            .encode_signal_frame()
            .expect("encode ordinary signal frame"),
    )
    .expect("write signal frame");

    let request = ClientRequest::from_argument(argument_from_path(&path))
        .expect("decode ordinary signal file");

    assert_eq!(request, ClientRequest::Ordinary(input));
}

#[test]
#[cfg(feature = "nota-text")]
fn client_reply_renders_nota_when_text_feature_is_enabled() {
    let output = ordinary::Output::Queried(ordinary::Queried::new(ordinary::GenerationListing {
        generations: Vec::new(),
        database_marker: ordinary::DatabaseMarker {
            commit_sequence: ordinary::CommitSequence::new(7),
            state_digest: ordinary::StateDigest::new(7),
        },
    }));
    let reply = ClientReply::Ordinary(output.clone());

    assert_eq!(reply.to_string(), output.to_string());
    assert!(reply.to_string().starts_with("(Queried"));
}
