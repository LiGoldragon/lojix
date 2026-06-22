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
            kind: None.into(),
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

/// The contained deploy surface is ordinary: the caller names the node profile,
/// contained target, optional proposal source override, and flake reference.
#[cfg(feature = "nota-text")]
fn ordinary_deploy_contained() -> ordinary::Input {
    ordinary::Input::DeployContained(ordinary::DeployContained::new(
        ordinary::DeployContainedRequest {
            node_profile: ordinary::NodeProfile {
                cluster_name: ordinary::ClusterName::new("goldragon"),
                node_name: ordinary::NodeName::new("mercury"),
                kind: None.into(),
            },
            contained_target: ordinary::ContainedTarget::HermeticVm,
            source: Some(ordinary::ProposalSource::new("/var/lib/lojix/cluster.nota")).into(),
            flake_reference: ordinary::FlakeReference::new(
                "github:LiGoldragon/CriomOS-test-cluster/main",
            ),
        },
    ))
}

/// Verification is also ordinary and carries a typed body.
#[cfg(feature = "nota-text")]
fn ordinary_verify_contained() -> ordinary::Input {
    ordinary::Input::VerifyContained(ordinary::VerifyContained::new(
        ordinary::ContainedVerification {
            contained_run_identifier: ordinary::ContainedRunIdentifier::new(42),
            verification_body: ordinary::VerificationBody::Gate,
        },
    ))
}

/// The test-run read: `lojix '(Query (ByContainedRun (goldragon mercury None)))'`.
#[cfg(feature = "nota-text")]
fn ordinary_query_contained_run() -> ordinary::Input {
    ordinary::Input::Query(ordinary::Query::new(ordinary::Selection::ByContainedRun(
        ordinary::ContainedRunLookup {
            cluster_name: ordinary::ClusterName::new("goldragon"),
            node_name: ordinary::NodeName::new("mercury"),
            run: None.into(),
        },
    )))
}

#[test]
#[cfg(feature = "nota-text")]
fn ordinary_client_decodes_inline_deploy_contained() {
    let input = ordinary_deploy_contained();
    let argument = argument_from_single(input.to_string());

    let client =
        OrdinaryClient::from_argument(argument).expect("decode ordinary DeployContained NOTA");

    assert_eq!(client.input(), &input);
}

#[test]
#[cfg(feature = "nota-text")]
fn ordinary_client_decodes_inline_verify_contained() {
    let input = ordinary_verify_contained();
    let argument = argument_from_single(input.to_string());

    let client =
        OrdinaryClient::from_argument(argument).expect("decode ordinary VerifyContained NOTA");

    assert_eq!(client.input(), &input);
}

#[test]
#[cfg(feature = "nota-text")]
fn ordinary_client_decodes_inline_by_contained_run_query() {
    let input = ordinary_query_contained_run();
    let argument = argument_from_single(input.to_string());

    let client =
        OrdinaryClient::from_argument(argument).expect("decode ordinary ByContainedRun NOTA");

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
        generations: Vec::new().into(),
        database_marker: ordinary::DatabaseMarker {
            commit_sequence: ordinary::CommitSequence::new(7),
            state_digest: ordinary::StateDigest::new(7),
        },
    }));

    assert!(output.to_string().starts_with("(Queried"));
}
