//! The store fixtures read row for row as they were written.
//!
//! Every row of every family is read from the file undecoded, decoded as the
//! family's current record (a deploy job also as the 0.10.6 layout its
//! upgrade reads), and archived again; the new archive must be the stored
//! bytes exactly. A row that reads back to its own bytes was read whole, with
//! nothing dropped and nothing moved. Each fixture is checked as it lies in
//! the repository and again after the Nexus's own open path has run over a
//! copy of it, or been refused.
//!
//! Each check also prints one line per family, `table rows=N read=N
//! digest=<sha256 of every key and stored archive>`, so the same listing
//! taken from another build of Lojix can be compared line for line.

use std::path::Path;

use lojix::quarantine::OpenedStore as _;
use lojix::quarantine::QuarantinedRow;
use lojix::runtime_model::{
    ContainerLifecycleRecord, DeployJob, DeploymentOutboxRecord, EventLogEntry, GcRoot,
    IdentifierAllocation, LiveGeneration, NexusConfigurationRecord, PendingTransitionIntent,
    StoredDeploymentRecord, StoredTestRun,
};
use lojix::upgrade::EarlierDeployJob;
use lojix::{
    DeploymentLedger as _, DurableStore as _, Error, LojixNexusConfigurable as _, LojixRecord,
};
use lojix::{NexusConfiguration, Store};
use redb::{ReadableDatabase as _, ReadableTable as _, TableHandle as _};
use sha2::{Digest as _, Sha256};
use tempfile::TempDir;

/// A family's table and the check that one stored row reads back to its own
/// bytes: `None` when it does not decode, `Some(equal)` when it does.
type Family = (&'static str, fn(&[u8]) -> Option<bool>);

macro_rules! reads_back {
    ($record:ty, $bytes:expr) => {
        rkyv::from_bytes::<$record, rkyv::rancor::Error>($bytes)
            .ok()
            .and_then(|value| rkyv::to_bytes::<rkyv::rancor::Error>(&value).ok())
            .map(|again| again.as_slice() == $bytes)
    };
}

macro_rules! family {
    ($record:ty) => {
        (
            <$record as LojixRecord>::TABLE,
            (|bytes: &[u8]| -> Option<bool> { reads_back!($record, bytes) })
                as fn(&[u8]) -> Option<bool>,
        )
    };
    ($record:ty, $earlier:ty) => {
        (
            <$record as LojixRecord>::TABLE,
            (|bytes: &[u8]| -> Option<bool> {
                reads_back!($record, bytes).or_else(|| reads_back!($earlier, bytes))
            }) as fn(&[u8]) -> Option<bool>,
        )
    };
}

fn families() -> Vec<Family> {
    vec![
        family!(LiveGeneration),
        family!(GcRoot),
        family!(EventLogEntry),
        family!(ContainerLifecycleRecord),
        family!(DeployJob, EarlierDeployJob),
        family!(StoredTestRun),
        family!(StoredDeploymentRecord),
        family!(IdentifierAllocation),
        family!(DeploymentOutboxRecord),
        family!(PendingTransitionIntent),
        family!(NexusConfigurationRecord),
        family!(QuarantinedRow),
    ]
}

/// What one family of one store holds and how it reads.
#[derive(Debug, PartialEq, Eq)]
struct FamilyReading {
    table: &'static str,
    rows: usize,
    read_whole: usize,
    undecodable: Vec<String>,
    digest: String,
}

impl std::fmt::Display for FamilyReading {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "{} rows={} read={} undecodable={:?} digest={}",
            self.table, self.rows, self.read_whole, self.undecodable, self.digest
        )
    }
}

/// Read every family of the store at `path` without opening it through the
/// engine, and print what was read under `label`.
fn read_rows(label: &str, path: &Path) -> Vec<FamilyReading> {
    let database = redb::ReadOnlyDatabase::open(path).expect("open the store read-only");
    let transaction = database.begin_read().expect("read transaction");
    let present: Vec<String> = transaction
        .list_tables()
        .expect("list tables")
        .map(|table| table.name().to_string())
        .collect();
    let mut readings = Vec::new();
    for (table, reads) in families() {
        let mut reading = FamilyReading {
            table,
            rows: 0,
            read_whole: 0,
            undecodable: Vec::new(),
            digest: String::new(),
        };
        let mut digest = Sha256::new();
        if present.iter().any(|name| name == table) {
            let rows = transaction
                .open_table(redb::TableDefinition::<String, &[u8]>::new(table))
                .expect("open family table");
            for entry in rows.iter().expect("iterate family") {
                let (key, archive) = entry.expect("row");
                let (key, archive) = (key.value(), archive.value());
                digest.update(key.as_bytes());
                digest.update([0]);
                digest.update(archive);
                reading.rows += 1;
                match reads(archive) {
                    Some(true) => reading.read_whole += 1,
                    Some(false) => panic!("{label}: {table} {key} decodes to other bytes"),
                    None => reading.undecodable.push(key),
                }
            }
        }
        reading.digest = format!("{:x}", digest.finalize());
        println!("{label}: {reading}");
        readings.push(reading);
    }
    readings
}

fn copied(directory: &TempDir, name: &str, bytes: &[u8]) -> std::path::PathBuf {
    let path = directory.path().join(name);
    std::fs::write(&path, bytes).expect("copy fixture");
    path
}

fn undecodable(readings: &[FamilyReading]) -> Vec<(&'static str, String)> {
    readings
        .iter()
        .flat_map(|reading| {
            reading
                .undecodable
                .iter()
                .map(|key| (reading.table, key.clone()))
        })
        .collect()
}

#[test]
fn the_pre_nexus_v5_fixture_reads_row_for_row() {
    let bytes = include_bytes!("fixtures/lojix-v5-pre-nexus-cf231859.memory");
    let directory = TempDir::new().expect("tempdir");
    let path = copied(&directory, "lojix.memory", bytes);
    let readings = read_rows("pre-nexus", &path);
    assert!(undecodable(&readings).is_empty(), "{readings:#?}");
    assert!(readings.iter().map(|reading| reading.rows).sum::<usize>() > 0);
}

#[test]
fn the_horizon_0_12_fixture_is_refused_and_reads_as_it_was_written() {
    let bytes = include_bytes!("fixtures/lojix-v5-horizon-0.12-deploy-job.memory");
    let directory = TempDir::new().expect("tempdir");
    let path = copied(&directory, "lojix.memory", bytes);
    let before = read_rows("horizon-0.12 as written", &path);
    assert_eq!(
        undecodable(&before),
        [("deploy-job", "1".to_string())],
        "only the Horizon 0.12 deploy job is unread"
    );
    let error = Store::open(&path).expect_err("no upgrade reads a Horizon 0.12 deploy job");
    assert!(
        matches!(&error, Error::UndecodableRow { table, key, .. } if table == "deploy-job" && key == "1"),
        "{error}"
    );
    let after = read_rows("horizon-0.12 after its refused open", &path);
    assert_eq!(after, before, "a refused open writes no row");
}

#[test]
fn the_lojix_8_1_0_fixture_is_refused_at_conversion() {
    let bytes = include_bytes!("fixtures/lojix-8.1.0-store.sema");
    let directory = TempDir::new().expect("tempdir");
    let state = directory.path().join("lojix");
    std::fs::create_dir_all(&state).expect("state directory");
    std::fs::write(state.join("lojix.sema"), bytes).expect("copy fixture");
    let error = NexusConfiguration::discover_default_store_path(state.clone())
        .expect_err("the converted copy holds the Horizon 0.12 deploy job");
    assert!(
        matches!(&error, Error::LegacyStoreConversion { .. }),
        "{error}"
    );
    assert!(!state.join("lojix.memory").exists(), "nothing is installed");
}

#[test]
fn the_lojix_0_10_6_deploy_jobs_upgrade_on_open_and_every_other_row_reads_as_written() {
    let bytes = include_bytes!("fixtures/lojix-0.10.6-deploy-jobs.memory");
    let directory = TempDir::new().expect("tempdir");
    let path = copied(&directory, "lojix.memory", bytes);
    let before = read_rows("0.10.6 deploy jobs as written", &path);
    assert!(undecodable(&before).is_empty(), "{before:#?}");
    assert_eq!(
        before
            .iter()
            .find(|reading| reading.table == "deploy-job")
            .map(|reading| reading.rows),
        Some(2)
    );

    let store = Store::open(&path).expect("the Nexus opens and upgrades the fixture");
    assert_eq!(store.opening().upgraded_deploy_jobs, Some(2));
    let jobs = store.deploy_jobs().expect("the upgraded jobs read");
    let submissions: Vec<_> = jobs
        .iter()
        .map(|job| {
            let submission = job
                .actualized_deploy_submission_option
                .as_ref()
                .expect("an admitted job carries its submission");
            (
                job.deployment_identifier.0,
                matches!(
                    submission.deploy_submission,
                    meta_signal_lojix::DeploySubmission::Host(_)
                ),
                submission.horizon_definition_option.is_some(),
            )
        })
        .collect();
    assert_eq!(
        submissions,
        [(1, true, true), (2, false, false)],
        "the Host deploy keeps its Horizon definition beside it; the Direct user deploy has none"
    );
    drop(store);

    let after = read_rows("0.10.6 deploy jobs after open", &path);
    assert!(undecodable(&after).is_empty(), "{after:#?}");
    for (written, opened) in before.iter().zip(&after) {
        if written.table != "deploy-job" {
            assert_eq!(opened, written, "only the deploy-job family moves");
        }
    }
    let again = Store::open(&path).expect("reopen");
    assert_eq!(
        again.opening().upgraded_deploy_jobs,
        None,
        "the upgrade runs once"
    );
}

/// The same reading over a store named by `LOJIX_STORE_COPY`, for a copy of a
/// live store; nothing is opened through the engine and nothing is written.
#[test]
#[ignore = "reads the store copy named by LOJIX_STORE_COPY"]
fn a_store_copy_reads_row_for_row() {
    let path = std::env::var_os("LOJIX_STORE_COPY").expect("LOJIX_STORE_COPY");
    let readings = read_rows("store copy", Path::new(&path));
    assert!(undecodable(&readings).is_empty(), "{readings:#?}");
}
