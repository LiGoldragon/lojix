//! The retired quarantine, and the refusal that replaces it.
//!
//! Lojix 0.10.6 moved a row it could not decode into the `quarantined-row`
//! family, keeping its bytes and its decode error, and served what remained.
//! Memory is upgraded, never accommodated: a layout change carries its
//! upgrade, so nothing is set aside any more. A row that no family reads
//! refuses the store, naming its table, its key and why it did not decode.
//!
//! The `quarantined-row` family keeps its 0.10.6 identity, so the store
//! stays one 0.10.6 opens, and every open empties it in one commit,
//! reporting each row it drops as `QuarantinedRowDropped.{ <table> <key>
//! «<error>» }`. Its record is the frozen 0.10.6 layout, hand-written,
//! because it holds raw bytes no Ethos type carries.

use memory_engine::{EngineRecord, QueryPlan, RecordKey, TableReference};
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};

use crate::runtime_model::{
    ContainerLifecycleRecord, DeployJob, DeploymentOutboxRecord, EventLogEntry, GcRoot,
    IdentifierAllocation, LiveGeneration, NexusConfigurationRecord, PendingTransitionIntent,
    StoredDeploymentRecord, StoredTestRun,
};
use crate::{Error, LojixDirectory, LojixRecord, Result, Store};

/// One row 0.10.6 set aside because it did not decode, as 0.10.6 wrote it.
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Clone, Debug, PartialEq, Eq)]
pub struct QuarantinedRow {
    /// The family table the row was found in, such as `deploy-job`.
    pub table: String,
    /// The row's stored key in that table.
    pub key: String,
    /// 1 for the first row of this table and key set aside, then 2, ...
    pub quarantine_number: u64,
    /// The row's original archive bytes, unchanged.
    pub archive: Vec<u8>,
    /// Why 0.10.6 did not read it.
    pub decode_error: String,
}

impl EngineRecord for QuarantinedRow {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(format!(
            "{}/{}/{}",
            self.table, self.key, self.quarantine_number
        ))
    }
}

impl LojixRecord for QuarantinedRow {
    const TABLE: &'static str = "quarantined-row";
    const FAMILY: &'static str = "QuarantinedRowFamily";
    const SCHEMA_HASH: [u8; 32] = [13; 32];
    const ROLE: &'static str = "rows 0.10.6 set aside, dropped at the next open";

    fn table(directory: &LojixDirectory) -> TableReference<Self> {
        directory.quarantined_rows
    }
}

/// The line the Nexus logs for each set-aside row its open dropped.
impl std::fmt::Display for QuarantinedRow {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "QuarantinedRowDropped.{{ {} {} «{}» }}",
            self.table, self.key, self.decode_error
        )
    }
}

/// What opening a store did beyond resuming it: the deploy-job rows it
/// upgraded, when the family was still in its 0.10.6 layout, and the
/// set-aside rows it dropped.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct StoreOpening {
    pub upgraded_deploy_jobs: Option<usize>,
    pub dropped_quarantined_rows: Vec<QuarantinedRow>,
}

/// What the open that produced a store did beyond resuming it.
pub trait OpenedStore {
    fn opening(&self) -> &StoreOpening;
}

impl OpenedStore for Store {
    fn opening(&self) -> &StoreOpening {
        &self.opening
    }
}

/// The lines an opening says, one per event, in the order they happened.
pub trait OpeningReport {
    fn report_lines(&self) -> Vec<String>;
}

impl OpeningReport for StoreOpening {
    fn report_lines(&self) -> Vec<String> {
        let mut lines: Vec<String> = self
            .upgraded_deploy_jobs
            .iter()
            .map(|rows| format!("FamilyUpgraded.{{ {} {rows} }}", DeployJob::TABLE))
            .collect();
        lines.extend(
            self.dropped_quarantined_rows
                .iter()
                .map(ToString::to_string),
        );
        lines
    }
}

/// Emptying the retired quarantine and refusing what no family reads.
pub trait QuarantineRetirement {
    /// Retract every row of the quarantine family in one commit, answering
    /// the rows dropped; a store with none answers empty and writes nothing.
    fn drop_quarantined_rows(&self) -> Result<Vec<QuarantinedRow>>;

    /// Every set-aside row the family still holds.
    fn quarantined_rows(&self) -> Result<Vec<QuarantinedRow>>;

    /// Read every row of every family by itself; the first that does not
    /// decode refuses the store, named by its table and key.
    fn refuse_undecodable_rows(&self) -> Result<()>;

    /// The same, for one family.
    fn refuse_undecodable<Record: LojixRecord>(&self) -> Result<()>
    where
        Record::Archived: RkyvDeserialize<Record, rkyv::api::high::HighDeserializer<rkyv::rancor::Error>>
            + for<'validation> rkyv::bytecheck::CheckBytes<
                rkyv::rancor::Strategy<
                    rkyv::validation::Validator<
                        rkyv::validation::archive::ArchiveValidator<'validation>,
                        rkyv::validation::shared::SharedValidator,
                    >,
                    rkyv::rancor::Error,
                >,
            >;

    /// Every row of one table as its stored key and archive bytes, read
    /// through the engine's read-only storage reader, so a row no record
    /// reads is still found and named.
    fn stored_archives(&self, table: &'static str) -> Result<Vec<(String, Vec<u8>)>>;
}

impl QuarantineRetirement for Store {
    fn drop_quarantined_rows(&self) -> Result<Vec<QuarantinedRow>> {
        let rows = self.quarantined_rows()?;
        if rows.is_empty() {
            return Ok(rows);
        }
        let commit = rows
            .iter()
            .fold(self.database.begin_atomic_commit(), |commit, row| {
                commit.retract(self.directory.quarantined_rows, row.record_key())
            });
        self.database.commit_atomic(commit)?;
        Ok(rows)
    }

    fn quarantined_rows(&self) -> Result<Vec<QuarantinedRow>> {
        Ok(self
            .database
            .match_records(QueryPlan::all(self.directory.quarantined_rows))?
            .records()
            .to_vec())
    }

    fn refuse_undecodable_rows(&self) -> Result<()> {
        self.refuse_undecodable::<LiveGeneration>()?;
        self.refuse_undecodable::<GcRoot>()?;
        self.refuse_undecodable::<EventLogEntry>()?;
        self.refuse_undecodable::<ContainerLifecycleRecord>()?;
        self.refuse_undecodable::<DeployJob>()?;
        self.refuse_undecodable::<StoredTestRun>()?;
        self.refuse_undecodable::<StoredDeploymentRecord>()?;
        self.refuse_undecodable::<IdentifierAllocation>()?;
        self.refuse_undecodable::<DeploymentOutboxRecord>()?;
        self.refuse_undecodable::<PendingTransitionIntent>()?;
        self.refuse_undecodable::<NexusConfigurationRecord>()?;
        self.refuse_undecodable::<QuarantinedRow>()
    }

    fn refuse_undecodable<Record: LojixRecord>(&self) -> Result<()>
    where
        Record::Archived: RkyvDeserialize<Record, rkyv::api::high::HighDeserializer<rkyv::rancor::Error>>
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
        for (key, archive) in self.stored_archives(Record::TABLE)? {
            if let Err(error) = rkyv::from_bytes::<Record, rkyv::rancor::Error>(&archive) {
                return Err(Error::UndecodableRow {
                    path: self.path.clone(),
                    table: Record::TABLE.to_string(),
                    key,
                    error: error.to_string(),
                });
            }
        }
        Ok(())
    }

    fn stored_archives(&self, table: &'static str) -> Result<Vec<(String, Vec<u8>)>> {
        let definition = redb::TableDefinition::<String, &[u8]>::new(table);
        Ok(self
            .database
            .storage_reader()
            .read(|transaction| {
                let table = match transaction.open_table(definition) {
                    Ok(table) => table,
                    Err(redb::TableError::TableDoesNotExist(_)) => return Ok(Vec::new()),
                    Err(error) => return Err(error.into()),
                };
                let mut rows = Vec::new();
                for entry in redb::ReadableTable::iter(&table)? {
                    let (key, archive) = entry?;
                    rows.push((key.value(), archive.value().to_vec()));
                }
                Ok(rows)
            })
            .map_err(memory_engine::Error::from)?)
    }
}
