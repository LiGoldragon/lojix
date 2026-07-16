//! Schema-one reconstruction acceptance tests live beside the public library
//! surface so the source database is only ever opened through read-only redb.

use std::path::Path;

use lojix::Store;
use lojix::reconstruction::{OmittedDeployJobReason, SchemaOneReconstructor};
use lojix::schema::sema::{DeployJobPhase, GcRoot, LiveGeneration};
use redb::{Database, TableDefinition};
use signal_lojix::schema::lib as ordinary;
use tempfile::TempDir;

const META: TableDefinition<&str, u64> = TableDefinition::new("__sema_meta");
const SCHEMA_VERSION: &str = "schema_version";

fn activation(identifier: u64) -> (LiveGeneration, GcRoot) {
    let generation_identifier = ordinary::GenerationIdentifier::new(identifier);
    let cluster_name = ordinary::ClusterName::new("goldragon");
    let node_name = ordinary::NodeName::new("dune");
    let closure_path = ordinary::ClosurePath::new("/nix/store/schema-one");
    (
        LiveGeneration {
            deployment_identifier: ordinary::DeploymentIdentifier::new(identifier),
            generation_identifier: generation_identifier.clone(),
            cluster_name: cluster_name.clone(),
            node_name: node_name.clone(),
            generation_artifact: ordinary::GenerationArtifact::BaseHost,
            activation_effect: ordinary::ActivationEffect::LiveActivation,
            generation_slot: ordinary::GenerationSlot::Current,
            closure_path: closure_path.clone(),
            source_revision_record: ordinary::SourceRevisionRecord {
                source_revision_policy: ordinary::SourceRevisionPolicy::ResolveAndRecord,
                requested_ref: ordinary::FlakeReference::new("github:owner/repo"),
                resolved_ref: ordinary::FlakeReference::new("github:owner/repo?rev=abc"),
                string: "abc".to_string(),
            },
        },
        GcRoot {
            generation_identifier,
            cluster_name,
            node_name,
            generation_slot: ordinary::GenerationSlot::Current,
            closure_path,
            optional_pin_label: None,
        },
    )
}

fn source(directory: &TempDir) -> std::path::PathBuf {
    let path = directory.path().join("schema-one.sema");
    let database = Database::create(&path).expect("create schema-one fixture");
    let write = database.begin_write().expect("begin fixture write");
    {
        let mut meta = write.open_table(META).expect("metadata table");
        meta.insert(SCHEMA_VERSION, 1).expect("schema one marker");
    }
    write.commit().expect("commit schema marker");
    path
}

fn row(path: &Path, table: &str, key: &str, bytes: &[u8]) {
    let database = Database::open(path).expect("open fixture for row");
    let write = database.begin_write().expect("begin row write");
    let definition: TableDefinition<&str, &[u8]> = TableDefinition::new(table);
    write
        .open_table(definition)
        .expect("row table")
        .insert(key, bytes)
        .expect("insert row");
    write.commit().expect("commit row");
}

#[test]
fn valid_schema_one_store_reconstructs_to_a_reopenable_schema_two_store() {
    let directory = tempfile::tempdir().expect("temporary paths");
    let source = source(&directory);
    let (generation, root) = activation(1);
    row(
        &source,
        "live-set",
        "1",
        rkyv::to_bytes::<rkyv::rancor::Error>(&generation)
            .expect("encode generation")
            .as_ref(),
    );
    row(
        &source,
        "gc-roots",
        "1",
        rkyv::to_bytes::<rkyv::rancor::Error>(&root)
            .expect("encode root")
            .as_ref(),
    );
    let destination = directory.path().join("schema-two.sema");
    let source_before = std::fs::read(&source).expect("source bytes before reconstruction");

    let report = SchemaOneReconstructor::new(&source, &destination)
        .reconstruct()
        .expect("valid reconstruction");
    assert_eq!(report.generations, 1);
    assert_eq!(report.gc_roots, 1);
    assert!(report.omitted_deploy_jobs.is_empty());
    assert_eq!(
        std::fs::read(&source).expect("source bytes after reconstruction"),
        source_before,
        "the source store stays byte-for-byte untouched"
    );
    let reopened = Store::open(&destination).expect("reopen final store");
    assert_eq!(
        reopened
            .matching_live_generations(|_| true)
            .expect("read reconstructed generation")
            .len(),
        1
    );
}

#[test]
fn mismatched_generation_root_rejects_without_creating_destination() {
    let directory = tempfile::tempdir().expect("temporary paths");
    let source = source(&directory);
    let (generation, mut root) = activation(1);
    root.node_name = ordinary::NodeName::new("other");
    row(
        &source,
        "live-set",
        "1",
        rkyv::to_bytes::<rkyv::rancor::Error>(&generation)
            .expect("encode generation")
            .as_ref(),
    );
    row(
        &source,
        "gc-roots",
        "1",
        rkyv::to_bytes::<rkyv::rancor::Error>(&root)
            .expect("encode root")
            .as_ref(),
    );
    let destination = directory.path().join("schema-two.sema");

    assert!(
        SchemaOneReconstructor::new(&source, &destination)
            .reconstruct()
            .is_err()
    );
    assert!(
        !destination.exists(),
        "validation must precede destination creation"
    );
}

#[test]
fn corrupt_source_row_rejects_without_creating_destination() {
    let directory = tempfile::tempdir().expect("temporary paths");
    let source = source(&directory);
    let database = Database::open(&source).expect("open corrupt fixture");
    let write = database.begin_write().expect("begin corrupt write");
    let definition: TableDefinition<&str, &[u8]> = TableDefinition::new("live-set");
    write
        .open_table(definition)
        .expect("live table")
        .insert("bad", &[1, 2, 3][..])
        .expect("corrupt row");
    write.commit().expect("commit corrupt row");
    let destination = directory.path().join("schema-two.sema");

    assert!(
        SchemaOneReconstructor::new(&source, &destination)
            .reconstruct()
            .is_err()
    );
    assert!(!destination.exists());
}

#[test]
fn existing_destination_is_rejected_idempotently_without_writes() {
    let directory = tempfile::tempdir().expect("temporary paths");
    let source = source(&directory);
    let destination = directory.path().join("schema-two.sema");
    std::fs::write(&destination, b"keep").expect("existing destination");

    for _ in 0..2 {
        assert!(
            SchemaOneReconstructor::new(&source, &destination)
                .reconstruct()
                .is_err()
        );
        assert_eq!(
            std::fs::read(&destination).expect("destination remains"),
            b"keep"
        );
    }
}

// The legacy deploy-job fixture uses the exact schema-one layout: it lacks a
// DeploySubmission, so reconstruction must report its omission rather than
// manufacture a host or user-environment request.
#[test]
fn legacy_pre_activation_job_is_omitted_with_typed_reason() {
    let directory = tempfile::tempdir().expect("temporary paths");
    let source = source(&directory);
    let job = lojix::reconstruction::test_fixture::legacy_job(7, DeployJobPhase::Building);
    row(
        &source,
        "deploy-job",
        "7",
        rkyv::to_bytes::<rkyv::rancor::Error>(&job)
            .expect("encode legacy job")
            .as_ref(),
    );
    let destination = directory.path().join("schema-two.sema");

    let report = SchemaOneReconstructor::new(&source, &destination)
        .reconstruct()
        .expect("job omission is reconstructable");
    assert_eq!(report.omitted_deploy_jobs.len(), 1);
    assert_eq!(report.omitted_deploy_jobs[0].deployment_identifier, 7);
    assert_eq!(
        report.omitted_deploy_jobs[0].reason,
        OmittedDeployJobReason::MissingDeploySubmission
    );
}
