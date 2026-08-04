//! Thin-client Dotos edge tests.
//!
//! Each CLI accepts one Dotos request and lowers it to exactly one public
//! contract input.  Ordinary and owner traffic remain structurally separate.

#[cfg(not(feature = "dotos-text"))]
use lojix::Error;
use lojix::client::{MetaClient, OrdinaryClient};
use meta_signal_lojix::schema::lib as meta;
use signal_lojix::schema::lib as ordinary;
use triad_runtime::{ComponentArgument, ComponentCommand};

fn ordinary_query() -> ordinary::Input {
    ordinary::Input::query(ordinary::Selection::ByNode(ordinary::NodeSelector {
        cluster_name: ordinary::ClusterName::new("alpha"),
        node_name: ordinary::NodeName::new("node-1"),
        optional_requested_generation_artifact: None,
    }))
}

fn owner_pin() -> meta::Input {
    meta::Input::pin(meta::PinRequest {
        cluster_name: meta::ClusterName::new("alpha"),
        node_name: meta::NodeName::new("node-1"),
        generation_identifier: meta::GenerationIdentifier::new(42),
        pin_label: meta::PinLabel::new("keep"),
    })
}

fn dotos_argument(argument: impl Into<String>) -> ComponentArgument {
    ComponentCommand::from_arguments([argument.into()])
        .dotos_argument()
        .expect("one Dotos component argument")
}

#[test]
#[cfg(not(feature = "dotos-text"))]
fn inline_dotos_requires_text_feature() {
    let error =
        OrdinaryClient::from_argument(dotos_argument("(Query (ByNode (alpha node-1 None)))"))
            .expect_err("inline Dotos must reject without its parser");
    assert!(matches!(error, Error::DotosTextUnsupported));
}

#[test]
#[cfg(feature = "dotos-text")]
fn ordinary_client_decodes_inline_dotos() {
    let input = ordinary_query();
    let client = OrdinaryClient::from_argument(dotos_argument(input.to_string()))
        .expect("decode ordinary Dotos");
    assert_eq!(client.input(), &input);
}

#[test]
#[cfg(feature = "dotos-text")]
fn meta_client_decodes_inline_dotos() {
    let input = owner_pin();
    let client =
        MetaClient::from_argument(dotos_argument(input.to_string())).expect("decode owner Dotos");
    assert_eq!(client.input(), &input);
}

#[test]
#[cfg(feature = "dotos-text")]
fn ordinary_client_decodes_dotos_file() {
    let directory = tempfile::tempdir().expect("tempdir");
    let path = directory.path().join("query.dotos");
    let input = ordinary_query();
    std::fs::write(&path, input.to_string()).expect("write Dotos request");

    let client = OrdinaryClient::from_argument(dotos_argument(path.display().to_string()))
        .expect("decode ordinary Dotos file");
    assert_eq!(client.input(), &input);
}
