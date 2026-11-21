use lojix::Payload as _;
use lojix::Store;
use lojix::inspection::{
    DatabaseInspection, SchemaInspection, StoreInspectionCommand, StoreInspector,
    TableInspectionStatus,
};
use lojix::runtime_model::{self as ordinary, GcRoot, LiveGeneration};
use lojix::{DurableStore as _, GenerationLedger as _};
use lojix::{
    OfflineCommand as _, inspection::InspectedTables as _, inspection::StoreInspecting as _,
};
use redb::{Database, TableDefinition};
use tempfile::TempDir;

const RAW_LIVE_SET: TableDefinition<String, &[u8]> = TableDefinition::new("live-set");
const RAW_EVENT_LOG: TableDefinition<String, &[u8]> = TableDefinition::new("event-log");

fn activation(generation_identifier: u64) -> (LiveGeneration, GcRoot) {
    let generation = ordinary::GenerationIdentifier::new(generation_identifier);
    let cluster = ordinary::ClusterName::from("fixture-cluster");
    let node = ordinary::NodeName::from("dune");
    let closure = ordinary::ClosurePath::from(
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
                requested_ref: ordinary::FlakeReference::from("github:owner/repo/main"),
                resolved_ref: ordinary::FlakeReference::from(
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
    let path = directory.path().join("missing.memory");

    let inspection = StoreInspector { path: path.clone() }.inspect();

    assert_eq!(inspection.database, DatabaseInspection::MissingPath);
    assert!(
        !path.exists(),
        "the read-only inspector must not create a missing store path"
    );
}

#[test]
fn registered_tables_with_no_rows_report_empty() {
    let directory = TempDir::new().expect("tempdir");
    let path = directory.path().join("lojix.memory");
    {
        let _store = Store::open(&path).expect("open empty store");
    }

    let inspection = StoreInspector { path: path.clone() }.inspect();

    assert!(matches!(inspection.database, DatabaseInspection::Opened));
    assert_eq!(inspection.schema, SchemaInspection::Matches { version: 5 });
    assert_eq!(
        inspection
            .table_named("live-set")
            .expect("live-set inspection")
            .status,
        TableInspectionStatus::Empty
    );
    assert_eq!(
        inspection
            .table_named("event-log")
            .expect("event-log inspection")
            .status,
        TableInspectionStatus::Empty
    );
}

#[test]
fn populated_generation_rows_report_row_counts() {
    let directory = TempDir::new().expect("tempdir");
    let path = directory.path().join("lojix.memory");
    {
        let store = Store::open(&path).expect("open store");
        let (generation, root) = activation(1);
        store
            .record_activation(generation, root)
            .expect("record activation");
    }

    let inspection = StoreInspector { path: path.clone() }.inspect();

    assert_eq!(
        inspection
            .table_named("live-set")
            .expect("live-set inspection")
            .status,
        TableInspectionStatus::Readable { row_count: 1 }
    );
    assert_eq!(
        inspection
            .table_named("gc-roots")
            .expect("gc-roots inspection")
            .status,
        TableInspectionStatus::Readable { row_count: 1 }
    );
}

#[test]
fn unreadable_generation_and_event_rows_report_decode_failures() {
    let directory = TempDir::new().expect("tempdir");
    let path = directory.path().join("lojix.memory");
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

    let inspection = StoreInspector { path: path.clone() }.inspect();

    assert!(
        matches!(
            inspection
                .table_named("live-set")
                .expect("live-set inspection")
                .status,
            TableInspectionStatus::DecodeFailed { .. }
        ),
        "live-set should report rows unreadable under the current schema"
    );
    assert!(
        matches!(
            inspection
                .table_named("event-log")
                .expect("event-log inspection")
                .status,
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
    assert!(StoreInspectionCommand::from_arguments([OsString::from("lojix.memory")]).is_err());
    assert!(
        StoreInspectionCommand::from_arguments([
            OsString::from("InspectStore.{ /tmp/lojix.memory }"),
            OsString::from("extra"),
        ])
        .is_err()
    );
    assert!(
        StoreInspectionCommand::from_arguments([OsString::from(
            "InspectStore.{ /tmp/lojix.memory }",
        )])
        .is_ok()
    );
}

#[test]
fn deploy_job_key_inspection_emits_only_numeric_keys_and_statuses() {
    use std::ffi::OsString;

    let directory = TempDir::new().expect("tempdir");
    let path = directory.path().join("frozen.memory");
    let command = StoreInspectionCommand::from_arguments([OsString::from(format!(
        "InspectDeployJobKeys.{{ {} }}",
        path.display()
    ))])
    .expect("keys-only request");
    assert_eq!(command.run().to_string(), "deploy-job - InvalidPath\n");
    assert!(!path.exists());

    {
        let _store = Store::open(&path).expect("fixture store");
    }
    assert_eq!(command.run().to_string(), "deploy-job - Empty\n");
    let database = Database::open(&path).expect("open disposable redb");
    let write = database.begin_write().expect("write fixture rows");
    {
        let mut jobs = write
            .open_table(TableDefinition::<String, &[u8]>::new("deploy-job"))
            .expect("deploy-job");
        let job = ordinary::DeployJob {
            deployment_identifier: ordinary::DeploymentIdentifier::new(664),
            generation_identifier: ordinary::GenerationIdentifier::new(664),
            cluster_name: ordinary::ClusterName::from("test-cluster"),
            node_name: ordinary::NodeName::from("test-node"),
            deploy_job_phase: ordinary::DeployJobPhase::Copying,
            optional_closure_path: None,
            source_revision_policy: ordinary::SourceRevisionPolicy::RequireImmutable,
            flake_reference: ordinary::FlakeReference::from("private-fixture-body"),
            optional_flake_reference: None,
            resolved_revision: None,
            deployment_transport: ordinary::DeploymentTransport {
                nix_store_uri: ordinary::NixStoreUri::from("ssh-ng://private-fixture-transport"),
                ssh_destination: ordinary::SshDestination::from("root@private-fixture-transport"),
            },
            deployment_input_mode: ordinary::DeploymentInputMode::Direct,
            deployment_output_selector: ordinary::DeploymentOutputSelector::new(
                ordinary::FlakeAttribute::from("private-fixture-output"),
            ),
            activation_backend: ordinary::ActivationBackend::NixosSystemdBootV1,
            optional_nix_builder_spec: None,
            boot_once_unit: None,
            optional_generation_slot: None,
            persisted_flake_input_override_vector: Vec::new(),
            deploy_resume_stage: ordinary::DeployResumeStage::CopyClosure,
            optional_phase_receipt: None,
            optional_deploy_submission: None,
        };
        let bytes = rkyv::to_bytes::<rkyv::rancor::Error>(&job).expect("archive fixture only");
        jobs.insert("664".to_string(), bytes.as_slice()).unwrap();
        jobs.insert("665".to_string(), bytes.as_slice()).unwrap();
        jobs.insert("9".to_string(), b"private-bad-bytes".as_slice())
            .unwrap();
        jobs.insert("private-nonnumeric-key".to_string(), bytes.as_slice())
            .unwrap();
    }
    write.commit().unwrap();
    drop(database);
    let original = std::fs::read(&path).unwrap();
    assert_eq!(
        command.run().to_string(),
        "deploy-job - InvalidKey\ndeploy-job 9 DecodeFailed\ndeploy-job 664 Decoded\ndeploy-job 665 KeyMismatch\n"
    );
    assert_eq!(std::fs::read(&path).unwrap(), original);
}

#[cfg(unix)]
#[test]
fn deploy_job_keys_refuses_symlinks_without_printing_the_path() {
    use std::ffi::OsString;
    let directory = TempDir::new().expect("tempdir");
    let file = directory.path().join("real");
    let link = directory.path().join("alias");
    std::fs::write(&file, b"not-opened").unwrap();
    std::os::unix::fs::symlink(&file, &link).unwrap();
    let command = StoreInspectionCommand::from_arguments([OsString::from(format!(
        "InspectDeployJobKeys.{{ {} }}",
        link.display()
    ))])
    .unwrap();
    assert_eq!(command.run().to_string(), "deploy-job - InvalidPath\n");
}
