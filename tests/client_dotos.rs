//! Thin-client Dotos edge tests.
//!
//! Each public CLI accepts one inline DOTOS/NOTA request and lowers it to
//! exactly one public contract input. Ordinary and owner traffic remain
//! structurally separate, and neither client reads a caller-selected file.

use std::ffi::OsString;

use lojix::Error;
use lojix::client::{MetaClient, OrdinaryClient};
#[cfg(feature = "dotos-text")]
use meta_signal_lojix::schema::lib as meta;
#[cfg(feature = "dotos-text")]
use signal_lojix::schema::lib as ordinary;
use triad_runtime::{ComponentArgument, ComponentCommand};

const ORDINARY_QUERY: &str = "Query.ByNode.(alpha node-1 None)";
#[cfg(feature = "dotos-text")]
const OWNER_PIN: &str = "Pin.(alpha node-1 42 keep)";

#[cfg(feature = "dotos-text")]
fn ordinary_query() -> ordinary::z2VTvQ {
    dotos::DotosSource::new(ORDINARY_QUERY)
        .parse()
        .expect("ordinary Interface Dotos")
}

#[cfg(feature = "dotos-text")]
fn owner_pin() -> meta::z2VW7Q {
    dotos::DotosSource::new(OWNER_PIN)
        .parse()
        .expect("owner Interface Dotos")
}

fn dotos_argument(argument: impl Into<String>) -> ComponentArgument {
    ComponentCommand::from_arguments([argument.into()])
        .dotos_argument()
        .expect("one Dotos component argument")
}

#[test]
#[cfg(not(feature = "dotos-text"))]
fn inline_dotos_requires_text_feature() {
    let error = OrdinaryClient::from_argument(dotos_argument(ORDINARY_QUERY))
        .expect_err("inline Dotos must reject without its parser");
    assert!(matches!(error, Error::DotosTextUnsupported));
}

#[test]
fn public_clients_reject_file_argument_variants() {
    let directory = tempfile::tempdir().expect("tempdir");
    let dotos_path = directory.path().join("request.dotos");
    let signal_path = directory.path().join("request.sema");
    std::fs::write(&dotos_path, "not read").expect("write dotos witness");
    std::fs::write(&signal_path, "not read").expect("write signal witness");

    let dotos_file = dotos_argument(dotos_path.display().to_string());
    let signal_file = ComponentCommand::from_arguments([signal_path.display().to_string()])
        .signal_file_argument()
        .expect("signal file argument");

    assert!(matches!(
        OrdinaryClient::from_argument(dotos_file),
        Err(Error::InlineDotosRequired)
    ));
    assert!(matches!(
        MetaClient::from_argument(signal_file),
        Err(Error::InlineDotosRequired)
    ));
}

#[test]
fn public_clients_reject_zero_extra_flags_and_raw_paths() {
    let inline = ORDINARY_QUERY;
    let cases = [
        Vec::new(),
        vec![OsString::from("--help")],
        vec![OsString::from("--pretty")],
        vec![OsString::from("/tmp/not-an-inline-request.dotos")],
        vec![OsString::from(inline), OsString::from("extra")],
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
#[cfg(feature = "dotos-text")]
fn ordinary_client_decodes_inline_dotos() {
    let input = ordinary_query();
    let client = OrdinaryClient::from_arguments([OsString::from(ORDINARY_QUERY)])
        .expect("decode ordinary Dotos");
    assert_eq!(client.input(), &input);
}

#[test]
#[cfg(feature = "dotos-text")]
fn meta_client_decodes_inline_dotos() {
    let input = owner_pin();
    let client =
        MetaClient::from_arguments([OsString::from(OWNER_PIN)]).expect("decode owner Dotos");
    assert_eq!(client.input(), &input);
}
