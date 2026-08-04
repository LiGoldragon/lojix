use lojix::Store;
use lojix::inspection::{
    DatabaseInspection, SchemaInspection, StoreInspectionCommand, StoreInspector,
    TableInspectionStatus,
};
use lojix::schema::sema::{self as ordinary, GcRoot, LiveGeneration};
use redb::{Database, TableDefinition};
use tempfile::TempDir;

const RAW_LIVE_SET: TableDefinition<String, &[u8]> = TableDefinition::new("live-set");
const RAW_EVENT_LOG: TableDefinition<String, &[u8]> = TableDefinition::new("event-log");

fn activation(generation_identifier: u64) -> (LiveGeneration, GcRoot) {
    let generation = ordinary::GenerationIdentifier::new(generation_identifier);
    let cluster = ordinary::ClusterName::new("fixture-cluster");
    let node = ordinary::NodeName::new("dune");
    let closure = ordinary::ClosurePath::new(
        "/nix/store/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-inspection-closure",
    );
    (
        LiveGeneration {
            deployment_identifier: ordinary::DeploymentIdentifier::new(generation_identifier),
            generation_identifier: generation.clone(),
            cluster_name: cluster.clone(),
            node_name: node.clone(),
            deployment_environment: ordinary::DeploymentEnvironment::HostEnvironment,
            generation_artifact: ordinary::GenerationArtifact::BaseHost,
            activation_effect: ordinary::ActivationEffect::LiveActivation,
            generation_slot: ordinary::GenerationSlot::Current,
            closure_path: closure.clone(),
            source_revision_record: ordinary::SourceRevisionRecord {
                source_revision_policy: ordinary::SourceRevisionPolicy::ResolveAndRecord,
                requested_ref: ordinary::FlakeReference::new("github:owner/repo/main"),
                resolved_ref: ordinary::FlakeReference::new(
                    "github:owner/repo?rev=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                ),
                string: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_string(),
            },
        },
        GcRoot {
            generation_identifier: generation,
            cluster_name: cluster,
            node_name: node,
            generation_slot: ordinary::GenerationSlot::Current,
            closure_path: closure,
            optional_pin_label: None,
        },
    )
}

#[test]
fn missing_store_path_is_reported_without_creating_file() {
    let directory = TempDir::new().expect("tempdir");
    let path = directory.path().join("missing.sema");

    let inspection = StoreInspector::new(&path).inspect();

    assert_eq!(inspection.database(), &DatabaseInspection::MissingPath);
    assert!(
        !path.exists(),
        "the read-only inspector must not create a missing store path"
    );
}

#[test]
fn registered_tables_with_no_rows_report_empty() {
    let directory = TempDir::new().expect("tempdir");
    let path = directory.path().join("lojix.sema");
    {
        let _store = Store::open(&path).expect("open empty store");
    }

    let inspection = StoreInspector::new(&path).inspect();

    assert!(matches!(inspection.database(), DatabaseInspection::Opened));
    assert_eq!(
        inspection.schema(),
        &SchemaInspection::Matches { version: 4 }
    );
    assert_eq!(
        inspection
            .table_named("live-set")
            .expect("live-set inspection")
            .status(),
        &TableInspectionStatus::Empty
    );
    assert_eq!(
        inspection
            .table_named("event-log")
            .expect("event-log inspection")
            .status(),
        &TableInspectionStatus::Empty
    );
}

#[test]
fn populated_generation_rows_report_row_counts() {
    let directory = TempDir::new().expect("tempdir");
    let path = directory.path().join("lojix.sema");
    {
        let store = Store::open(&path).expect("open store");
        let (generation, root) = activation(1);
        store
            .record_activation(generation, root)
            .expect("record activation");
    }

    let inspection = StoreInspector::new(&path).inspect();

    assert_eq!(
        inspection
            .table_named("live-set")
            .expect("live-set inspection")
            .status(),
        &TableInspectionStatus::Readable { row_count: 1 }
    );
    assert_eq!(
        inspection
            .table_named("gc-roots")
            .expect("gc-roots inspection")
            .status(),
        &TableInspectionStatus::Readable { row_count: 1 }
    );
}

#[test]
fn unreadable_generation_and_event_rows_report_decode_failures() {
    let directory = TempDir::new().expect("tempdir");
    let path = directory.path().join("lojix.sema");
    {
        let _store = Store::open(&path).expect("open store");
    }
    let database = Database::open(&path).expect("open redb for disposable corruption");
    let transaction = database.begin_write().expect("begin write");
    {
        let corrupt_bytes: &[u8] = b"not-rkyv";
        let mut live_set = transaction.open_table(RAW_LIVE_SET).expect("live-set");
        live_set
            .insert("not-current-schema".to_string(), &corrupt_bytes)
            .expect("corrupt live-set row");
        let mut event_log = transaction.open_table(RAW_EVENT_LOG).expect("event-log");
        event_log
            .insert("0".to_string(), &corrupt_bytes)
            .expect("corrupt event-log row");
    }
    transaction.commit().expect("commit corruption");
    drop(database);

    let inspection = StoreInspector::new(&path).inspect();

    assert!(
        matches!(
            inspection
                .table_named("live-set")
                .expect("live-set inspection")
                .status(),
            TableInspectionStatus::DecodeFailed { .. }
        ),
        "live-set should report rows unreadable under the current schema"
    );
    assert!(
        matches!(
            inspection
                .table_named("event-log")
                .expect("event-log inspection")
                .status(),
            TableInspectionStatus::DecodeFailed { .. }
        ),
        "event-log should report rows unreadable under the current schema"
    );
}

#[test]
fn command_requires_one_inline_inspect_store_object() {
    use std::ffi::OsString;

    assert!(StoreInspectionCommand::from_arguments(Vec::new()).is_err());
    assert!(StoreInspectionCommand::from_arguments([OsString::from("--state")]).is_err());
    assert!(StoreInspectionCommand::from_arguments([OsString::from("--pretty")]).is_err());
    assert!(StoreInspectionCommand::from_arguments([OsString::from("lojix.sema")]).is_err());
    assert!(
        StoreInspectionCommand::from_arguments([
            OsString::from("(InspectStore /tmp/lojix.sema)"),
            OsString::from("extra"),
        ])
        .is_err()
    );
    assert!(
        StoreInspectionCommand::from_arguments([OsString::from("(InspectStore /tmp/lojix.sema)",)])
            .is_ok()
    );
}
