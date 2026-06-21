//! Thin-client NOTA edge tests.
//!
//! The daemon socket wire remains rkyv signal frames. These tests cover the
//! human-facing CLI adapters: inline NOTA, `.nota` files, and signal-encoded
//! files decode into exactly one contract `Input` per tier. There is no
//! cross-tier classification — the ordinary `lojix` client parses only
//! `signal-lojix`, the `meta-lojix` client parses only `meta-signal-lojix`, so
//! the prior unified client's audit-R7 short-header collision cannot arise.

#[cfg(not(feature = "nota-text"))]
use lojix::Error;
use lojix::client::{MetaClient, OrdinaryClient};
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
        production_node: meta::ProductionNode {
            cluster_name: ordinary::ClusterName::new("alpha"),
            node_name: ordinary::NodeName::new("node-1"),
        },
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
fn ordinary_inline_nota_requires_text_feature() {
    let argument = argument_from_single("(Query ((ByNode (alpha node-1 None))))");
    let error = OrdinaryClient::from_argument(argument).expect_err("inline NOTA must reject");
    assert!(matches!(error, Error::NotaTextUnsupported));
}

#[test]
#[cfg(not(feature = "nota-text"))]
fn meta_inline_nota_requires_text_feature() {
    let argument = argument_from_single("(Pin ((alpha node-1 42 keep)))");
    let error = MetaClient::from_argument(argument).expect_err("inline NOTA must reject");
    assert!(matches!(error, Error::NotaTextUnsupported));
}

#[test]
#[cfg(not(feature = "nota-text"))]
fn nota_file_requires_text_feature() {
    let directory = tempfile::tempdir().expect("tempdir");
    let path = directory.path().join("query.nota");
    std::fs::write(&path, "(Query ((ByNode (alpha node-1 None))))").expect("write NOTA file");

    let error = OrdinaryClient::from_argument(argument_from_path(&path))
        .expect_err("NOTA file must reject");
    assert!(matches!(error, Error::NotaTextUnsupported));
}

#[test]
#[cfg(feature = "nota-text")]
fn ordinary_client_decodes_inline_query() {
    let input = ordinary_query();
    let argument = argument_from_single(input.to_string());

    let client = OrdinaryClient::from_argument(argument).expect("decode ordinary NOTA");

    assert_eq!(client.input(), &input);
}

#[test]
#[cfg(feature = "nota-text")]
fn meta_client_decodes_inline_pin() {
    let input = owner_pin();
    let argument = argument_from_single(input.to_string());

    let client = MetaClient::from_argument(argument).expect("decode meta NOTA");

    assert_eq!(client.input(), &input);
}

/// The routine test shorthand: `meta-lojix '(Test (Check [mercury]))'`. The CLI
/// decodes it through the generated meta parser (no hand-parsing); the daemon
/// lowers the cluster/host/mode from TestDefaults (report 54).
#[cfg(feature = "nota-text")]
fn owner_test_check() -> meta::Input {
    meta::Input::Test(meta::Test::new(meta::TestRequest::Check(
        meta::QuickCheck::new(vec![ordinary::NodeName::new("mercury")]),
    )))
}

/// The full test form: explicit node selection, host, and mode.
#[cfg(feature = "nota-text")]
fn owner_test_run() -> meta::Input {
    meta::Input::Test(meta::Test::new(meta::TestRequest::Run(meta::TestRun {
        cluster_name: ordinary::ClusterName::new("goldragon"),
        node_selection: meta::NodeSelection::Nodes(vec![ordinary::NodeName::new("mercury")]),
        host_selection: ordinary::HostSelection::OnHost(ordinary::NodeName::new("prometheus")),
        test_mode: ordinary::TestMode::Hermetic,
    })))
}

/// The test-run read: `lojix '(Query (ByTestRun (goldragon mercury None)))'`.
#[cfg(feature = "nota-text")]
fn ordinary_query_test_run() -> ordinary::Input {
    ordinary::Input::Query(ordinary::Query::new(ordinary::Selection::ByTestRun(
        ordinary::TestRunLookup {
            cluster_name: ordinary::ClusterName::new("goldragon"),
            node_name: ordinary::NodeName::new("mercury"),
            run: None,
        },
    )))
}

#[test]
#[cfg(feature = "nota-text")]
fn meta_client_decodes_inline_test_check() {
    let input = owner_test_check();
    let argument = argument_from_single(input.to_string());

    let client = MetaClient::from_argument(argument).expect("decode meta Test Check NOTA");

    assert_eq!(client.input(), &input);
}

#[test]
#[cfg(feature = "nota-text")]
fn meta_client_decodes_inline_test_run() {
    let input = owner_test_run();
    let argument = argument_from_single(input.to_string());

    let client = MetaClient::from_argument(argument).expect("decode meta Test Run NOTA");

    assert_eq!(client.input(), &input);
}

#[test]
#[cfg(feature = "nota-text")]
fn ordinary_client_decodes_inline_by_test_run_query() {
    let input = ordinary_query_test_run();
    let argument = argument_from_single(input.to_string());

    let client = OrdinaryClient::from_argument(argument).expect("decode ordinary ByTestRun NOTA");

    assert_eq!(client.input(), &input);
}

#[test]
#[cfg(feature = "nota-text")]
fn ordinary_client_decodes_nota_file() {
    let directory = tempfile::tempdir().expect("tempdir");
    let path = directory.path().join("query.nota");
    let input = ordinary_query();
    std::fs::write(&path, input.to_string()).expect("write NOTA request");

    let client = OrdinaryClient::from_argument(argument_from_path(&path))
        .expect("decode ordinary NOTA file");

    assert_eq!(client.input(), &input);
}

#[test]
fn ordinary_client_decodes_signal_frame() {
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

    let client = OrdinaryClient::from_argument(argument_from_path(&path))
        .expect("decode ordinary signal file");

    assert_eq!(client.input(), &input);
}

#[test]
fn meta_client_decodes_signal_frame() {
    let directory = tempfile::tempdir().expect("tempdir");
    let path = directory.path().join("pin.rkyv");
    let input = owner_pin();
    std::fs::write(
        &path,
        input
            .encode_signal_frame()
            .expect("encode meta signal frame"),
    )
    .expect("write signal frame");

    let client =
        MetaClient::from_argument(argument_from_path(&path)).expect("decode meta signal file");

    assert_eq!(client.input(), &input);
}

#[test]
#[cfg(feature = "nota-text")]
fn ordinary_output_renders_nota() {
    let output = ordinary::Output::Queried(ordinary::Queried::new(ordinary::GenerationListing {
        generations: Vec::new(),
        database_marker: ordinary::DatabaseMarker {
            commit_sequence: ordinary::CommitSequence::new(7),
            state_digest: ordinary::StateDigest::new(7),
        },
    }));

    assert!(output.to_string().starts_with("(Queried"));
}
