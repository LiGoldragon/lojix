use std::path::Path;

use lojix::Store;
use lojix::inspection::{InspectedTables as _, StoreInspecting as _, StoreInspector};
use lojix::quarantine::{OpenedStore as _, QuarantinedRow, RowQuarantine as _};
use lojix::runtime_model::{self as ordinary, GcRoot, LiveGeneration};
use lojix::{DeploymentLedger as _, DurableStore as _, GenerationLedger as _};
use lojix::{LojixNexusConfigurable as _, NexusConfiguration, NexusPersistable as _, Payload as _};
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

fn activation(generation_identifier: u64) -> (LiveGeneration, GcRoot) {
    let generation = ordinary::GenerationIdentifier::new(generation_identifier);
    let cluster = ordinary::ClusterName::from("fixture-cluster");
    let node = ordinary::NodeName::from("dune");
    let closure = ordinary::ClosurePath::from(
        "/nix/store/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-startup-gate-closure",
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
fn current_store_reopens_through_startup_gate() {
    let directory = TempDir::new().expect("tempdir");
    let path = directory.path().join("lojix.sema");
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

fn quarantined(table: &str, key: &str, archive: &[u8]) -> impl Fn(&QuarantinedRow) -> bool {
    let (table, key, archive) = (table.to_string(), key.to_string(), archive.to_vec());
    move |row| {
        row.table == table
            && row.key == key
            && row.quarantine_number == 1
            && row.archive == archive
            && !row.decode_error.is_empty()
    }
}

#[test]
fn undecodable_deploy_job_and_configuration_rows_are_quarantined_and_the_store_serves() {
    let directory = TempDir::new().expect("tempdir");
    let path = directory.path().join("lojix.sema");
    store_with_undecodable_rows(&path);

    let before = StoreInspector { path: path.clone() }.inspect();
    assert_eq!(
        before.undecodable_row_count(),
        2,
        "the inspector must count what the next open will set aside:\n{before}"
    );
    assert_eq!(before.quarantined_row_count(), 0);

    let store = Store::open(&path).expect("a store with undecodable rows still opens");

    let opening = store.opening();
    assert_eq!(opening.quarantined_rows.len(), 2, "{opening:?}");
    assert!(opening.quarantined_rows.iter().any(quarantined(
        "deploy-job",
        "19",
        UNDECODABLE_DEPLOY_JOB
    )));
    assert!(opening.quarantined_rows.iter().any(quarantined(
        "nexus-configuration",
        "desired",
        UNDECODABLE_CONFIGURATION
    )));
    assert!(opening.configuration_rebuilt);
    assert_eq!(
        store.quarantined_rows().expect("persisted quarantine"),
        opening.quarantined_rows
    );

    assert_eq!(
        store
            .nexus_configuration_state()
            .expect("rebuilt configuration row")
            .desired_configuration,
        NexusConfiguration::built_in(),
        "the configuration row is rebuilt from the configuration a fresh store is seeded with"
    );
    assert!(
        store.deploy_jobs().expect("deploy jobs read").is_empty(),
        "an undecodable deploy-job row is never served"
    );
    let generations = store
        .matching_live_generations(|generation| *generation.generation_identifier.payload() == 1)
        .expect("query served after quarantine");
    assert_eq!(generations.len(), 1, "rows that decode stay where they are");
    drop(store);

    let reopened = Store::open(&path).expect("reopen after quarantine");
    assert!(
        reopened.opening().quarantined_rows.is_empty(),
        "a second open sets nothing aside"
    );
    assert!(!reopened.opening().configuration_rebuilt);
    assert_eq!(
        reopened.quarantined_rows().expect("quarantine kept").len(),
        2
    );
    drop(reopened);

    let after = StoreInspector { path: path.clone() }.inspect();
    assert_eq!(after.quarantined_row_count(), 2, "{after}");
    assert_eq!(after.undecodable_row_count(), 0, "{after}");
    assert!(
        after
            .to_string()
            .contains("Quarantine quarantined_row_count=2 undecodable_row_count=0")
    );
}

#[test]
fn an_undecodable_row_is_reported_on_the_opening_line() {
    let row = QuarantinedRow {
        table: "deploy-job".to_string(),
        key: "19".to_string(),
        quarantine_number: 1,
        archive: UNDECODABLE_DEPLOY_JOB.to_vec(),
        decode_error: "fixture".to_string(),
    };
    assert_eq!(
        row.to_string(),
        "RowQuarantined.{ deploy-job 19 «fixture» }"
    );
}

#[test]
fn a_row_quarantined_again_under_the_same_key_is_kept_beside_the_first() {
    let directory = TempDir::new().expect("tempdir");
    let path = directory.path().join("lojix.sema");
    store_with_undecodable_rows(&path);
    drop(Store::open(&path).expect("first quarantine"));

    let database = Database::open(&path).expect("open redb to plant a second row");
    let transaction = database.begin_write().expect("begin write");
    {
        let mut deploy_jobs = transaction.open_table(RAW_DEPLOY_JOB).expect("deploy-job");
        deploy_jobs
            .insert("19".to_string(), &UNDECODABLE_CONFIGURATION)
            .expect("plant a second undecodable row under the same key");
    }
    transaction.commit().expect("commit second row");
    drop(database);

    let store = Store::open(&path).expect("second quarantine");
    let rows = store.quarantined_rows().expect("quarantine");
    let under_key: Vec<_> = rows
        .iter()
        .filter(|row| row.table == "deploy-job" && row.key == "19")
        .map(|row| (row.quarantine_number, row.archive.clone()))
        .collect();
    assert_eq!(
        under_key,
        vec![
            (1, UNDECODABLE_DEPLOY_JOB.to_vec()),
            (2, UNDECODABLE_CONFIGURATION.to_vec()),
        ]
    );
    assert!(!store.opening().configuration_rebuilt);
}

#[test]
fn a_malformed_live_set_row_is_quarantined_not_refused() {
    let directory = TempDir::new().expect("tempdir");
    let path = directory.path().join("lojix.sema");
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

    let store = Store::open(&path).expect("startup sets the malformed row aside");
    assert_eq!(
        store
            .opening()
            .quarantined_rows
            .iter()
            .map(|row| (row.table.as_str(), row.key.as_str()))
            .collect::<Vec<_>>(),
        vec![("live-set", "not-current-schema")]
    );
    assert!(
        store
            .matching_live_generations(|_| true)
            .expect("live set reads")
            .is_empty()
    );
}
