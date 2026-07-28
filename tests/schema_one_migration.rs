use std::fs;
use std::os::unix::fs::{MetadataExt, PermissionsExt};
use std::path::{Path, PathBuf};

use lojix::Store;
use lojix::reconstruction::{
    MigrationOutcome, MigrationPaths, StoreCounts, StoreMigrationCommand, StoreMigrator,
};
use lojix::schema::sema::{
    ContainerLifecycleRecord, ContainerName, ContainerState, DeployJob, DeployJobPhase,
    EventLogEntry, GcRoot, LiveGeneration, LoggedEvent, StoredTestRun,
};
use redb::{Database, TableDefinition};
use sema_engine::{
    Engine as SemaDatabase, EngineOpen, FamilyName, SchemaHash, SchemaVersion, TableDescriptor,
    TableName,
};
use signal_lojix::schema::lib as ordinary;
use tempfile::TempDir;

const RAW_LIVE_SET: TableDefinition<String, &[u8]> = TableDefinition::new("live-set");

#[derive(Clone)]
struct FixtureRows {
    generations: Vec<LiveGeneration>,
    roots: Vec<GcRoot>,
    events: Vec<EventLogEntry>,
    containers: Vec<ContainerLifecycleRecord>,
    jobs: Vec<DeployJob>,
    tests: Vec<StoredTestRun>,
}

impl FixtureRows {
    fn complete() -> Self {
        let (generation_four, root_four) = activation(4, ordinary::GenerationSlot::Rollback);
        let (generation_twelve, root_twelve) = activation(12, ordinary::GenerationSlot::Current);
        let container = container(20);
        Self {
            generations: vec![generation_four, generation_twelve],
            roots: vec![root_four, root_twelve],
            events: vec![
                deployment_event(100, 12),
                EventLogEntry {
                    event_log_position: ordinary::EventLogPosition::new(20),
                    record: LoggedEvent::Container(container.clone()),
                },
                deployment_event(3, 4),
            ],
            containers: vec![container],
            jobs: vec![DeployJob {
                deployment_identifier: ordinary::DeploymentIdentifier::new(50),
                generation_identifier: ordinary::GenerationIdentifier::new(51),
                cluster_name: ordinary::ClusterName::new("goldragon"),
                node_name: ordinary::NodeName::new("zeus"),
                phase: DeployJobPhase::Building,
                closure_path: None,
                source_revision_policy: ordinary::SourceRevisionPolicy::RequireImmutable,
                requested_ref: ordinary::FlakeReference::new(
                    "github:LiGoldragon/CriomOS?rev=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                ),
                resolved_ref: None,
                resolved_revision: None,
                resolved_target: None,
                boot_once_unit: None,
            }],
            tests: vec![StoredTestRun {
                test_run_identifier: ordinary::TestRunIdentifier::new(60),
                cluster_name: ordinary::ClusterName::new("goldragon"),
                node_name: ordinary::NodeName::new("mercury"),
                host: ordinary::NodeName::new("ouranos"),
                mode: ordinary::TestMode::Hermetic,
                phase: ordinary::TestRunPhase::Completed,
                outcome: ordinary::TestOutcome::Passed,
                closure_path: Some(ordinary::ClosurePath::new("/nix/store/test-run")),
            }],
        }
    }

    fn counts(&self) -> StoreCounts {
        StoreCounts {
            generations: self.generations.len(),
            gc_roots: self.roots.len(),
            event_log_entries: self.events.len(),
            container_lifecycle_records: self.containers.len(),
            deploy_jobs: self.jobs.len(),
            test_runs: self.tests.len(),
        }
    }
}

fn activation(identifier: u64, slot: ordinary::GenerationSlot) -> (LiveGeneration, GcRoot) {
    let generation_identifier = ordinary::GenerationIdentifier::new(identifier);
    let cluster_name = ordinary::ClusterName::new("goldragon");
    let node_name = ordinary::NodeName::new("zeus");
    let closure_path = ordinary::ClosurePath::new(format!("/nix/store/schema-one-{identifier}"));
    (
        LiveGeneration {
            deployment_identifier: ordinary::DeploymentIdentifier::new(identifier + 100),
            generation_identifier: generation_identifier.clone(),
            cluster_name: cluster_name.clone(),
            node_name: node_name.clone(),
            generation_artifact: ordinary::GenerationArtifact::UserEnvironment,
            activation_effect: ordinary::ActivationEffect::LiveActivation,
            generation_slot: slot,
            closure_path: closure_path.clone(),
            source_revision_record: ordinary::SourceRevisionRecord {
                policy: ordinary::SourceRevisionPolicy::RequireImmutable,
                requested_ref: ordinary::FlakeReference::new(
                    "github:LiGoldragon/CriomOS?rev=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                ),
                resolved_ref: ordinary::FlakeReference::new(
                    "github:LiGoldragon/CriomOS?rev=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                ),
                resolved_revision: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_string(),
            },
        },
        GcRoot {
            generation_identifier,
            cluster_name,
            node_name,
            generation_slot: slot,
            closure_path,
            label: None,
        },
    )
}

fn deployment_event(position: u64, generation: u64) -> EventLogEntry {
    EventLogEntry {
        event_log_position: ordinary::EventLogPosition::new(position),
        record: LoggedEvent::Deployment(ordinary::DeploymentPhaseEvent {
            deployment_identifier: ordinary::DeploymentIdentifier::new(generation + 100),
            generation_identifier: ordinary::GenerationIdentifier::new(generation),
            cluster_name: ordinary::ClusterName::new("goldragon"),
            node_name: ordinary::NodeName::new("zeus"),
            deployment_phase: ordinary::DeploymentPhase::Activated,
            event_log_position: ordinary::EventLogPosition::new(position),
            detail: None,
            source_revision: None,
        }),
    }
}

fn container(position: u64) -> ContainerLifecycleRecord {
    ContainerLifecycleRecord {
        cluster_name: ordinary::ClusterName::new("goldragon"),
        node_name: ordinary::NodeName::new("zeus"),
        container: ContainerName::new("migration-witness"),
        state: ContainerState::Started,
        event_log_position: ordinary::EventLogPosition::new(position),
    }
}

fn schema_one_store(path: &Path, rows: &FixtureRows) {
    let mut database = SemaDatabase::open(EngineOpen::new(path, SchemaVersion::new(1)))
        .expect("open true schema-one engine");
    let live_set = database
        .register_table(TableDescriptor::new(
            TableName::new("live-set"),
            FamilyName::new("LiveSetFamily"),
            SchemaHash::new([1; 32]),
        ))
        .expect("register live set");
    let roots = database
        .register_table(TableDescriptor::new(
            TableName::new("gc-roots"),
            FamilyName::new("GcRootsFamily"),
            SchemaHash::new([2; 32]),
        ))
        .expect("register roots");
    let events = database
        .register_table(TableDescriptor::new(
            TableName::new("event-log"),
            FamilyName::new("EventLogFamily"),
            SchemaHash::new([3; 32]),
        ))
        .expect("register events");
    let containers = database
        .register_table(TableDescriptor::new(
            TableName::new("container-lifecycle"),
            FamilyName::new("ContainerLifecycleFamily"),
            SchemaHash::new([4; 32]),
        ))
        .expect("register containers");
    let jobs = database
        .register_table(TableDescriptor::new(
            TableName::new("deploy-job"),
            FamilyName::new("DeployJobFamily"),
            SchemaHash::new([5; 32]),
        ))
        .expect("register jobs");
    let tests = database
        .register_table(TableDescriptor::new(
            TableName::new("test-run"),
            FamilyName::new("TestRunFamily"),
            SchemaHash::new([6; 32]),
        ))
        .expect("register tests");

    let mut seed = database.begin_atomic_commit();
    for generation in rows.generations.clone() {
        seed = seed.assert(live_set, generation);
    }
    for root in rows.roots.clone() {
        seed = seed.assert(roots, root);
    }
    for container in rows.containers.clone() {
        seed = seed.assert(containers, container);
    }
    for event in rows.events.clone() {
        seed = seed.assert(events, event);
    }
    for job in rows.jobs.clone() {
        seed = seed.assert(jobs, job);
    }
    for run in rows.tests.clone() {
        seed = seed.assert(tests, run);
    }
    database.commit_atomic(seed).expect("seed schema one");
}

fn fixture() -> (TempDir, PathBuf, FixtureRows) {
    let directory = tempfile::tempdir().expect("tempdir");
    let path = directory.path().join("lojix.sema");
    let rows = FixtureRows::complete();
    schema_one_store(&path, &rows);
    (directory, path, rows)
}

fn mode(metadata: &fs::Metadata) -> u32 {
    metadata.mode() & 0o7777
}

#[test]
fn true_schema_one_store_migrates_all_six_tables_and_preserves_backup_metadata() {
    let (_directory, path, rows) = fixture();
    fs::set_permissions(&path, fs::Permissions::from_mode(0o640)).expect("set source mode");
    let before = fs::read(&path).expect("source bytes");
    let before_metadata = fs::metadata(&path).expect("source metadata");
    let paths = MigrationPaths::for_store(&path);

    let outcome = StoreMigrator::new(&path).migrate().expect("migrate");
    assert_eq!(
        outcome,
        MigrationOutcome::Migrated {
            path: path.clone(),
            backup: paths.backup().to_path_buf(),
            counts: rows.counts(),
        }
    );
    assert_eq!(fs::read(paths.backup()).expect("backup bytes"), before);
    let backup_metadata = fs::metadata(paths.backup()).expect("backup metadata");
    let current_metadata = fs::metadata(&path).expect("current metadata");
    for metadata in [&backup_metadata, &current_metadata] {
        assert_eq!(metadata.uid(), before_metadata.uid());
        assert_eq!(metadata.gid(), before_metadata.gid());
        assert_eq!(mode(metadata), 0o640);
    }

    let store = Store::open(&path).expect("reopen schema two");
    let mut generations = store
        .matching_live_generations(|_| true)
        .expect("generations");
    generations.sort_by_key(|generation| *generation.generation_identifier.payload());
    let mut expected_generations = rows.generations;
    expected_generations.sort_by_key(|generation| *generation.generation_identifier.payload());
    assert_eq!(generations, expected_generations);
    let mut roots = store.gc_roots().expect("roots");
    roots.sort_by_key(|root| *root.generation_identifier.payload());
    let mut expected_roots = rows.roots;
    expected_roots.sort_by_key(|root| *root.generation_identifier.payload());
    assert_eq!(roots, expected_roots);
    assert_eq!(store.deploy_jobs().expect("jobs"), rows.jobs);
    assert_eq!(store.test_runs().expect("tests"), rows.tests);
    let mut events = store
        .event_log_in_range(0, u64::MAX)
        .expect("events after migration");
    events.sort_by_key(|event| *event.event_log_position.payload());
    let mut expected_events = rows.events;
    expected_events.sort_by_key(|event| *event.event_log_position.payload());
    assert_eq!(events, expected_events);
    drop(store);
    assert_eq!(
        raw_records::<ContainerLifecycleRecord>(&path, "container-lifecycle"),
        rows.containers
    );
}

#[test]
fn catalog_registered_but_unmaterialized_empty_tables_migrate_as_empty() {
    let directory = tempfile::tempdir().expect("tempdir");
    let path = directory.path().join("empty-physical-tables.sema");
    let mut rows = FixtureRows::complete();
    rows.events
        .retain(|event| !matches!(event.record, LoggedEvent::Container(_)));
    rows.containers.clear();
    rows.jobs.clear();
    rows.tests.clear();
    schema_one_store(&path, &rows);

    let outcome = StoreMigrator::new(&path)
        .migrate()
        .expect("migrate registered empty tables");

    assert!(matches!(
        outcome,
        MigrationOutcome::Migrated { counts, .. } if counts == rows.counts()
    ));
    let store = Store::open(&path).expect("reopen schema two");
    assert!(store.deploy_jobs().expect("empty jobs").is_empty());
    assert!(store.test_runs().expect("empty tests").is_empty());
}

#[test]
fn sparse_history_keeps_event_and_identifier_allocators_above_imported_state() {
    let (_directory, path, _rows) = fixture();
    StoreMigrator::new(&path).migrate().expect("migrate");
    let store = Store::open(&path).expect("open migrated store");

    assert!(store.next_event_log_position().expect("next event") > 100);
    assert!(store.next_generation_identifier().expect("next generation") > 12);
    assert!(store.next_deployment_identifier().expect("next deploy") > 112);

    let position = store.next_event_log_position().expect("allocate event");
    store
        .append_event_log_entry(deployment_event(position, 12))
        .expect("append above sparse imported history");
    assert_eq!(
        store
            .event_log_in_range(position, position + 1)
            .expect("new event")
            .len(),
        1
    );
}

#[test]
fn retry_recovers_tool_owned_partial_staging_without_changing_backup() {
    let (_directory, path, rows) = fixture();
    let before = fs::read(&path).expect("source bytes");
    let paths = MigrationPaths::for_store(&path);
    fs::hard_link(&path, paths.backup()).expect("simulate durable backup phase");
    fs::hard_link(paths.backup(), paths.staging_owner()).expect("simulate staging owner phase");
    fs::write(paths.staging(), b"partial staging").expect("simulate interrupted staging");

    let outcome = StoreMigrator::new(&path)
        .migrate()
        .expect("retry migration");
    assert!(matches!(
        outcome,
        MigrationOutcome::Migrated { counts, .. } if counts == rows.counts()
    ));
    assert_eq!(fs::read(paths.backup()).expect("backup bytes"), before);
    assert!(!paths.staging().exists());
    assert!(!paths.staging_owner().exists());
    Store::open(&path).expect("reopen after retry");
}

#[test]
fn second_run_on_schema_two_is_byte_for_byte_no_op() {
    let (_directory, path, _rows) = fixture();
    StoreMigrator::new(&path)
        .migrate()
        .expect("first migration");
    let before = fs::read(&path).expect("schema two bytes");

    let outcome = StoreMigrator::new(&path)
        .migrate()
        .expect("idempotent retry");

    assert!(matches!(outcome, MigrationOutcome::AlreadyCurrent { .. }));
    assert_eq!(fs::read(&path).expect("schema two after retry"), before);
}

#[test]
fn corrupt_source_refuses_before_backup_or_replacement() {
    let (_directory, path, _rows) = fixture();
    let database = Database::open(&path).expect("open fixture");
    let write = database.begin_write().expect("begin corrupt write");
    {
        write
            .open_table(RAW_LIVE_SET)
            .expect("live table")
            .insert("4".to_string(), &b"not-rkyv"[..])
            .expect("corrupt row");
    }
    write.commit().expect("commit corruption");
    drop(database);
    let before = fs::read(&path).expect("corrupt source bytes");
    let paths = MigrationPaths::for_store(&path);

    assert!(StoreMigrator::new(&path).migrate().is_err());
    assert_eq!(fs::read(&path).expect("source unchanged"), before);
    assert!(!paths.backup().exists());
    assert!(!paths.staging().exists());
}

#[test]
fn mismatched_root_slot_and_one_way_container_relation_are_rejected() {
    let directory = tempfile::tempdir().expect("tempdir");
    let root_path = directory.path().join("root-mismatch.sema");
    let mut root_rows = FixtureRows::complete();
    root_rows.roots[0].generation_slot = ordinary::GenerationSlot::Current;
    schema_one_store(&root_path, &root_rows);
    assert!(StoreMigrator::new(&root_path).migrate().is_err());
    assert!(!MigrationPaths::for_store(&root_path).backup().exists());

    let event_path = directory.path().join("event-mismatch.sema");
    let mut event_rows = FixtureRows::complete();
    event_rows.containers.clear();
    schema_one_store(&event_path, &event_rows);
    assert!(StoreMigrator::new(&event_path).migrate().is_err());
    assert!(!MigrationPaths::for_store(&event_path).backup().exists());
}

#[test]
fn nested_event_position_and_raw_key_mismatch_are_rejected() {
    let directory = tempfile::tempdir().expect("tempdir");
    let nested_path = directory.path().join("nested-mismatch.sema");
    let mut nested_rows = FixtureRows::complete();
    let LoggedEvent::Deployment(record) = &mut nested_rows.events[0].record else {
        panic!("deployment fixture");
    };
    record.event_log_position = ordinary::EventLogPosition::new(99);
    schema_one_store(&nested_path, &nested_rows);
    assert!(StoreMigrator::new(&nested_path).migrate().is_err());

    let key_path = directory.path().join("key-mismatch.sema");
    let key_rows = FixtureRows::complete();
    schema_one_store(&key_path, &key_rows);
    let database = Database::open(&key_path).expect("open key fixture");
    let write = database.begin_write().expect("begin key mismatch");
    {
        let bytes =
            rkyv::to_bytes::<rkyv::rancor::Error>(&key_rows.generations[0]).expect("encode row");
        write
            .open_table(RAW_LIVE_SET)
            .expect("live table")
            .insert("999".to_string(), bytes.as_ref())
            .expect("insert mismatched key");
    }
    write.commit().expect("commit key mismatch");
    drop(database);
    assert!(StoreMigrator::new(&key_path).migrate().is_err());
}

#[test]
fn conflicting_backup_and_unowned_staging_refuse_without_clobbering() {
    let directory = tempfile::tempdir().expect("tempdir");
    let backup_path = directory.path().join("backup-conflict.sema");
    let rows = FixtureRows::complete();
    schema_one_store(&backup_path, &rows);
    let backup_paths = MigrationPaths::for_store(&backup_path);
    fs::write(backup_paths.backup(), b"conflicting backup").expect("conflict backup");
    let canonical_before = fs::read(&backup_path).expect("canonical before");
    assert!(StoreMigrator::new(&backup_path).migrate().is_err());
    assert_eq!(
        fs::read(backup_paths.backup()).expect("conflict preserved"),
        b"conflicting backup"
    );
    assert_eq!(
        fs::read(&backup_path).expect("canonical preserved"),
        canonical_before
    );

    let staging_path = directory.path().join("staging-conflict.sema");
    schema_one_store(&staging_path, &rows);
    let staging_paths = MigrationPaths::for_store(&staging_path);
    fs::write(staging_paths.staging(), b"unowned staging").expect("conflict staging");
    let canonical_before = fs::read(&staging_path).expect("canonical before");
    assert!(StoreMigrator::new(&staging_path).migrate().is_err());
    assert_eq!(
        fs::read(staging_paths.staging()).expect("staging preserved"),
        b"unowned staging"
    );
    assert_eq!(
        fs::read(&staging_path).expect("canonical preserved"),
        canonical_before
    );

    let metadata_path = directory.path().join("metadata-conflict.sema");
    schema_one_store(&metadata_path, &rows);
    fs::set_permissions(&metadata_path, fs::Permissions::from_mode(0o640)).expect("canonical mode");
    let metadata_paths = MigrationPaths::for_store(&metadata_path);
    fs::copy(&metadata_path, metadata_paths.backup()).expect("matching backup bytes");
    fs::set_permissions(metadata_paths.backup(), fs::Permissions::from_mode(0o600))
        .expect("conflicting backup mode");
    let canonical_before = fs::read(&metadata_path).expect("canonical before");
    assert!(StoreMigrator::new(&metadata_path).migrate().is_err());
    assert_eq!(
        fs::read(&metadata_path).expect("canonical preserved"),
        canonical_before
    );
    assert_eq!(
        mode(&fs::metadata(metadata_paths.backup()).expect("backup preserved")),
        0o600
    );
}

#[test]
fn command_accepts_exactly_one_positional_store_path() {
    assert!(
        StoreMigrationCommand::from_arguments([std::ffi::OsString::from("lojix.sema")]).is_ok()
    );
    assert!(StoreMigrationCommand::from_arguments([std::ffi::OsString::from("--store")]).is_err());
    assert!(
        StoreMigrationCommand::from_arguments([
            std::ffi::OsString::from("one"),
            std::ffi::OsString::from("two"),
        ])
        .is_err()
    );
}

fn raw_records<Record>(path: &Path, table: &'static str) -> Vec<Record>
where
    Record: rkyv::Archive,
    Record::Archived: rkyv::Deserialize<Record, rkyv::api::high::HighDeserializer<rkyv::rancor::Error>>
        + for<'validation> rkyv::bytecheck::CheckBytes<
            rkyv::rancor::Strategy<
                rkyv::validation::Validator<
                    rkyv::validation::archive::ArchiveValidator<'validation>,
                    rkyv::validation::shared::SharedValidator,
                >,
                rkyv::rancor::Error,
            >,
        >,
{
    use redb::{ReadableDatabase, ReadableTable};
    let database = redb::ReadOnlyDatabase::open(path).expect("open raw read-only");
    let transaction = database.begin_read().expect("raw read transaction");
    let definition: TableDefinition<String, &[u8]> = TableDefinition::new(table);
    let table = transaction.open_table(definition).expect("raw table");
    table
        .iter()
        .expect("raw rows")
        .map(|row| {
            let (_key, value) = row.expect("raw row");
            rkyv::from_bytes::<Record, rkyv::rancor::Error>(value.value())
                .expect("decode raw record")
        })
        .collect()
}
