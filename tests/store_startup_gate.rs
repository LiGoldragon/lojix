use std::path::Path;

use lojix::Store;
use lojix::inspection::{InspectedTables as _, StoreInspecting as _, StoreInspector};
use lojix::quarantine::{OpenedStore as _, QuarantineRetirement as _, QuarantinedRow};
use lojix::runtime_model::{self as ordinary, GcRoot, LiveGeneration};
use lojix::{DurableStore as _, GenerationLedger as _};
use lojix::{Error, Payload as _};
use redb::{Database, TableDefinition};
use tempfile::TempDir;

const RAW_LIVE_SET: TableDefinition<String, &[u8]> = TableDefinition::new("live-set");
const RAW_DEPLOY_JOB: TableDefinition<String, &[u8]> = TableDefinition::new("deploy-job");
const RAW_NEXUS_CONFIGURATION: TableDefinition<String, &[u8]> =
    TableDefinition::new("nexus-configuration");

/// Bytes no current Lojix record reads: what a row archived in an earlier
/// layout looks like to the current schema.
const UNDECODABLE_DEPLOY_JOB: &[u8] = b"deploy-job archived before the Horizon bump";
const UNDECODABLE_CONFIGURATION: &[u8] = b"nexus configuration archived before the bump";

fn activation(generation_identifier: i64) -> (LiveGeneration, GcRoot) {
    let generation = ordinary::GenerationIdentifier::new(generation_identifier);
    let cluster = ordinary::ClusterName::new("fixture-cluster".into());
    let node = ordinary::NodeName::new("dune".into());
    let closure = ordinary::ClosurePath::new(
        "/nix/store/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-startup-gate-closure".into(),
    );
    (
        LiveGeneration {
            deployment_identifier: ordinary::DeploymentIdentifier::new(generation_identifier),
            generation_identifier: generation,
            cluster_name: cluster.clone(),
            node_name: node.clone(),
            deployment_environment: ordinary::DeploymentEnvironment::HostEnvironment,
            generation_artifact: ordinary::GenerationArtifact::BaseHost,
            activation_effect: ordinary::ActivationEffect::LiveActivation,
            generation_slot: ordinary::GenerationSlot::Current,
            closure_path: closure.clone(),
            source_revision_record: ordinary::SourceRevisionRecord {
                source_revision_policy: ordinary::SourceRevisionPolicy::ResolveAndRecord,
                requested_reference: ordinary::RequestedReference(ordinary::FlakeReference::new(
                    "github:owner/repo/main".into(),
                )),
                resolved_reference: ordinary::ResolvedReference(ordinary::FlakeReference::new(
                    "github:owner/repo?rev=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".into(),
                )),
                resolved_revision: ordinary::ResolvedRevision(
                    "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_string(),
                ),
            },
        },
        GcRoot {
            generation_identifier: generation,
            cluster_name: cluster,
            node_name: node,
            generation_slot: ordinary::GenerationSlot::Current,
            closure_path: closure,
            pin_label_option: None,
        },
    )
}

#[test]
fn current_store_reopens_through_startup_gate() {
    let directory = TempDir::new().expect("tempdir");
    let path = directory.path().join("lojix.memory");
    {
        let store = Store::open(&path).expect("open current store");
        let (generation, root) = activation(1);
        store
            .record_activation(generation, root)
            .expect("record activation");
    }

    let store = Store::open(&path).expect("reopen current store");
    let generations = store
        .matching_live_generations(|generation| *generation.generation_identifier.payload() == 1)
        .expect("query generation after startup gate");

    assert_eq!(generations.len(), 1);
}

/// A current store holding one live generation, then one deploy-job row and the
/// Nexus configuration row replaced by bytes the current schema cannot read.
fn store_with_undecodable_rows(path: &Path) {
    {
        let store = Store::open(path).expect("create current store");
        let (generation, root) = activation(1);
        store
            .record_activation(generation, root)
            .expect("record activation");
    }
    let database = Database::open(path).expect("open redb to plant undecodable rows");
    let transaction = database.begin_write().expect("begin write");
    {
        let mut deploy_jobs = transaction.open_table(RAW_DEPLOY_JOB).expect("deploy-job");
        deploy_jobs
            .insert("19".to_string(), &UNDECODABLE_DEPLOY_JOB)
            .expect("plant undecodable deploy-job row");
        let mut configuration = transaction
            .open_table(RAW_NEXUS_CONFIGURATION)
            .expect("nexus-configuration");
        configuration
            .insert("desired".to_string(), &UNDECODABLE_CONFIGURATION)
            .expect("plant undecodable configuration row");
    }
    transaction.commit().expect("commit undecodable rows");
}

const RAW_QUARANTINE: TableDefinition<String, &[u8]> = TableDefinition::new("quarantined-row");

/// Open `path` and expect the refusal that names `table` and `key`, with the
/// store left byte for byte as it was.
fn assert_refused_by_name(path: &Path, table: &str, key: &str) {
    let before = std::fs::read(path).expect("read the store before its open");
    let error = Store::open(path).expect_err("a row no record reads refuses the store");
    match &error {
        Error::UndecodableRow {
            table: refused_table,
            key: refused_key,
            ..
        } => assert_eq!((refused_table.as_str(), refused_key.as_str()), (table, key)),
        other => panic!("expected an undecodable-row refusal, got {other}"),
    }
    assert_eq!(
        std::fs::read(path).expect("read the store after its refused open"),
        before,
        "a refused open leaves the store untouched"
    );
}

#[test]
fn an_undecodable_row_refuses_the_store_by_name() {
    let directory = TempDir::new().expect("tempdir");
    let path = directory.path().join("lojix.memory");
    store_with_undecodable_rows(&path);

    let before = StoreInspector { path: path.clone() }.inspect();
    assert_eq!(
        before.undecodable_row_count(),
        2,
        "the inspector counts every row the next open refuses:\n{before}"
    );
    assert_refused_by_name(&path, "deploy-job", "19");
}

#[test]
fn a_malformed_live_set_row_refuses_the_store_by_name() {
    let directory = TempDir::new().expect("tempdir");
    let path = directory.path().join("lojix.memory");
    {
        let _store = Store::open(&path).expect("create current store envelope");
    }
    let database = Database::open(&path).expect("open redb for disposable legacy row");
    let transaction = database.begin_write().expect("begin write");
    {
        let corrupt_bytes: &[u8] = b"not-current-lojix-row";
        let mut live_set = transaction.open_table(RAW_LIVE_SET).expect("live-set");
        live_set
            .insert("not-current-schema".to_string(), &corrupt_bytes)
            .expect("write malformed legacy row");
    }
    transaction.commit().expect("commit malformed row");
    drop(database);

    assert_refused_by_name(&path, "live-set", "not-current-schema");
}

fn set_aside(table: &str, key: &str, archive: &[u8]) -> QuarantinedRow {
    QuarantinedRow {
        table: table.to_string(),
        key: key.to_string(),
        quarantine_number: 1,
        archive: archive.to_vec(),
        decode_error: format!("{table} {key} no longer decoded"),
    }
}

#[test]
fn rows_0_10_6_set_aside_are_dropped_at_open_and_reported() {
    let directory = TempDir::new().expect("tempdir");
    let path = directory.path().join("lojix.memory");
    {
        let store = Store::open(&path).expect("create current store");
        let (generation, root) = activation(1);
        store
            .record_activation(generation, root)
            .expect("record activation");
    }
    let rows = vec![
        set_aside("deploy-job", "19", UNDECODABLE_DEPLOY_JOB),
        set_aside("nexus-configuration", "desired", UNDECODABLE_CONFIGURATION),
    ];
    let database = Database::open(&path).expect("open redb to plant set-aside rows");
    let transaction = database.begin_write().expect("begin write");
    {
        let mut quarantine = transaction
            .open_table(RAW_QUARANTINE)
            .expect("quarantined-row");
        for row in &rows {
            let archive = rkyv::to_bytes::<rkyv::rancor::Error>(row).expect("archive");
            quarantine
                .insert(format!("{}/{}/1", row.table, row.key), archive.as_slice())
                .expect("plant a set-aside row");
        }
    }
    transaction.commit().expect("commit set-aside rows");
    drop(database);

    let before = StoreInspector { path: path.clone() }.inspect();
    assert_eq!(before.quarantined_row_count(), 2, "{before}");
    assert_eq!(before.undecodable_row_count(), 0, "{before}");

    let store = Store::open(&path).expect("a store holding set-aside rows opens");
    let mut dropped = store.opening().dropped_quarantined_rows.clone();
    dropped.sort_by(|left, right| left.table.cmp(&right.table));
    assert_eq!(dropped, rows);
    assert!(
        store
            .quarantined_rows()
            .expect("quarantine reads")
            .is_empty()
    );
    let generations = store
        .matching_live_generations(|generation| *generation.generation_identifier.payload() == 1)
        .expect("query served after the drop");
    assert_eq!(generations.len(), 1, "every other row stays where it is");
    drop(store);

    let reopened = Store::open(&path).expect("reopen");
    assert!(reopened.opening().dropped_quarantined_rows.is_empty());
    drop(reopened);
    let after = StoreInspector { path: path.clone() }.inspect();
    assert!(
        after
            .to_string()
            .contains("Quarantine quarantined_row_count=0 undecodable_row_count=0"),
        "{after}"
    );
}

#[test]
fn a_dropped_row_is_reported_on_the_opening_line() {
    let row = QuarantinedRow {
        table: "deploy-job".to_string(),
        key: "19".to_string(),
        quarantine_number: 1,
        archive: UNDECODABLE_DEPLOY_JOB.to_vec(),
        decode_error: "fixture".to_string(),
    };
    assert_eq!(
        row.to_string(),
        "QuarantinedRowDropped.{ deploy-job 19 «fixture» }"
    );
}
