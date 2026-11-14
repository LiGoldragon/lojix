//! The three store fixtures read row for row as they were written.
//!
//! Every row of every family is read from the file undecoded, decoded as the
//! family's current record, and archived again; the new archive must be the
//! stored bytes exactly. A row that reads back to its own bytes was read
//! whole, with nothing dropped and nothing moved. Each fixture is checked as
//! it lies in the repository and again after the Nexus's own open path has
//! run over a copy of it.
//!
//! Each check also prints one line per family, `table rows=N read=N
//! digest=<sha256 of every key and stored archive>`, so the same listing
//! taken from another build of Lojix can be compared line for line.

use std::path::Path;

use lojix::quarantine::QuarantinedRow;
use lojix::runtime_model::{
    ContainerLifecycleRecord, DeployJob, DeploymentOutboxRecord, EventLogEntry, GcRoot,
    IdentifierAllocation, LiveGeneration, PendingTransitionIntent, StoredDeploymentRecord,
    StoredTestRun,
};
use lojix::{
    DurableStore as _, LojixNexusConfigurable as _, LojixRecord, NexusConfiguration,
    NexusConfigurationRecord, NexusPersistable as _, Store,
};
use redb::{ReadableDatabase as _, ReadableTable as _, TableHandle as _};
use sha2::{Digest as _, Sha256};
use tempfile::TempDir;

/// A family's table and the check that one stored row reads back to its own
/// bytes: `None` when it does not decode, `Some(equal)` when it does.
type Family = (&'static str, fn(&[u8]) -> Option<bool>);

macro_rules! family {
    ($record:ty) => {
        (
            <$record as LojixRecord>::TABLE,
            (|bytes: &[u8]| -> Option<bool> {
                let value = rkyv::from_bytes::<$record, rkyv::rancor::Error>(bytes).ok()?;
                let again = rkyv::to_bytes::<rkyv::rancor::Error>(&value).ok()?;
                Some(again.as_slice() == bytes)
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
        family!(DeployJob),
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
fn the_horizon_0_12_fixture_reads_row_for_row_before_and_after_its_open() {
    let bytes = include_bytes!("fixtures/lojix-v5-horizon-0.12-deploy-job.memory");
    let directory = TempDir::new().expect("tempdir");
    let path = copied(&directory, "lojix.memory", bytes);
    let before = read_rows("horizon-0.12 as written", &path);
    assert_eq!(
        undecodable(&before),
        [("deploy-job", "1".to_string())],
        "only the Horizon 0.12 deploy job is unread"
    );
    drop(Store::open(&path).expect("the Nexus opens the fixture"));
    let after = read_rows("horizon-0.12 after open", &path);
    assert!(undecodable(&after).is_empty(), "{after:#?}");
}

#[test]
fn the_lojix_8_1_0_fixture_reads_row_for_row_after_its_conversion() {
    let bytes = include_bytes!("fixtures/lojix-8.1.0-store.sema");
    let directory = TempDir::new().expect("tempdir");
    let state = directory.path().join("lojix");
    std::fs::create_dir_all(&state).expect("state directory");
    std::fs::write(state.join("lojix.sema"), bytes).expect("copy fixture");
    let path = NexusConfiguration::discover_default_store_path(state)
        .expect("the Nexus converts the 8.1.0 store");
    // The conversion opens the store it writes, so the Horizon 0.12 deploy
    // job is already set aside by then.
    let converted = read_rows("8.1.0 converted", &path);
    assert!(undecodable(&converted).is_empty(), "{converted:#?}");
    drop(
        Store::open_with_default_configuration(&path, NexusConfiguration::built_in())
            .expect("the converted store opens"),
    );
    let after = read_rows("8.1.0 after open", &path);
    assert_eq!(after, converted, "a second open changes no row");
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
