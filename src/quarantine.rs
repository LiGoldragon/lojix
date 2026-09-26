//! Rows that no longer decode under the current schema are moved aside when
//! the store opens, never served and never allowed to stop the Nexus.
//!
//! A schema change in a type a stored row carries — a `HorizonDefinition`
//! inside a deploy-job submission or the Nexus configuration's test defaults —
//! leaves rows archived in the earlier layout. Reading such a row as the
//! current type fails, and before this module one such row refused the whole
//! store at startup. Now each row of every family is read by itself; a row
//! that decodes stays where it is, and any other row is moved, in one atomic
//! commit with its retraction, into the `quarantined-row` family, which keeps
//! its table, its stored key, its original archive bytes and the decode error.
//! Nothing carries an earlier layout forward: a quarantined row is kept only
//! as evidence, and the Nexus serves what remains.

use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use sema_engine::{EngineRecord, QueryPlan, RecordKey, TableName, TableReference};

use crate::runtime_model::{
    ContainerLifecycleRecord, DeployJob, DeploymentOutboxRecord, DeploymentRecord, EventLogEntry,
    GcRoot, IdentifierAllocation, LiveGeneration, PendingTransitionIntent, StoredTestRun,
};
use crate::{LojixDirectory, LojixRecord, NexusConfigurationRecord, Result, Store};

/// One stored row moved out of its family because it did not decode under the
/// current schema. It is evidence, not state: nothing reads it back as the
/// record it once was.
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
    /// Why the current schema did not read it.
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
    const ROLE: &'static str = "rows set aside because they no longer decode";

    fn table(directory: &LojixDirectory) -> TableReference<Self> {
        directory.quarantined_rows
    }
}

/// The line the Nexus logs for each row it set aside while opening.
impl std::fmt::Display for QuarantinedRow {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "RowQuarantined.{{ {} {} «{}» }}",
            self.table, self.key, self.decode_error
        )
    }
}

/// A family row named to the engine only so that a row no schema reads can be
/// retracted. Its archive is empty, so it checks against any bytes; it is
/// never asserted and never read for content. An exception to typed rows,
/// taken only for moving such a row aside.
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Clone, Debug, PartialEq, Eq)]
pub(crate) struct UnreadRow;

/// What opening a store did beyond resuming it: the rows it set aside, and
/// whether it had to rebuild the Nexus configuration row because the stored
/// one was among them.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct StoreOpening {
    pub quarantined_rows: Vec<QuarantinedRow>,
    pub configuration_rebuilt: bool,
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
            .quarantined_rows
            .iter()
            .map(ToString::to_string)
            .collect();
        if self.configuration_rebuilt {
            lines.push(format!(
                "NexusConfigurationRebuilt.{{ {} BuiltIn }}",
                NexusConfigurationRecord::TABLE
            ));
        }
        lines
    }
}

/// Setting aside, at open, every stored row the current schema does not read.
pub trait RowQuarantine {
    /// Read every row of every family by itself and move each one that does
    /// not decode into the quarantine family. Answers with the rows this call
    /// moved; a store with none answers empty and writes nothing.
    fn quarantine_undecodable_rows(&self) -> Result<Vec<QuarantinedRow>>;

    /// The same, for one family.
    fn quarantine_undecodable<Record: LojixRecord>(&self) -> Result<Vec<QuarantinedRow>>
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
    /// through the engine's read-only storage reader. Reading the archive
    /// undecoded is an exception taken only here: it is how a row that no
    /// schema reads is found and kept whole.
    fn stored_archives(&self, table: &'static str) -> Result<Vec<(String, Vec<u8>)>>;

    /// Every row set aside so far, by this open and earlier ones.
    fn quarantined_rows(&self) -> Result<Vec<QuarantinedRow>>;
}

impl RowQuarantine for Store {
    fn quarantine_undecodable_rows(&self) -> Result<Vec<QuarantinedRow>> {
        let mut moved = Vec::new();
        moved.extend(self.quarantine_undecodable::<LiveGeneration>()?);
        moved.extend(self.quarantine_undecodable::<GcRoot>()?);
        moved.extend(self.quarantine_undecodable::<EventLogEntry>()?);
        moved.extend(self.quarantine_undecodable::<ContainerLifecycleRecord>()?);
        moved.extend(self.quarantine_undecodable::<DeployJob>()?);
        moved.extend(self.quarantine_undecodable::<StoredTestRun>()?);
        moved.extend(self.quarantine_undecodable::<DeploymentRecord>()?);
        moved.extend(self.quarantine_undecodable::<IdentifierAllocation>()?);
        moved.extend(self.quarantine_undecodable::<DeploymentOutboxRecord>()?);
        moved.extend(self.quarantine_undecodable::<PendingTransitionIntent>()?);
        moved.extend(self.quarantine_undecodable::<NexusConfigurationRecord>()?);
        Ok(moved)
    }

    fn quarantine_undecodable<Record: LojixRecord>(&self) -> Result<Vec<QuarantinedRow>>
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
        let mut moved = Vec::new();
        for (key, archive) in self.stored_archives(Record::TABLE)? {
            let Err(decode_error) = rkyv::from_bytes::<Record, rkyv::rancor::Error>(&archive)
            else {
                continue;
            };
            let quarantine_number = self
                .quarantined_rows()?
                .iter()
                .filter(|row| row.table == Record::TABLE && row.key == key)
                .count() as u64
                + 1;
            let row = QuarantinedRow {
                table: Record::TABLE.to_string(),
                key: key.clone(),
                quarantine_number,
                archive,
                decode_error: decode_error.to_string(),
            };
            let unread = TableReference::<UnreadRow>::new(TableName::new(Record::TABLE));
            self.database.commit_atomic(
                self.database
                    .begin_atomic_commit()
                    .retract(unread, RecordKey::new(key))
                    .assert(self.directory.quarantined_rows, row.clone()),
            )?;
            moved.push(row);
        }
        Ok(moved)
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
            .map_err(sema_engine::Error::from)?)
    }

    fn quarantined_rows(&self) -> Result<Vec<QuarantinedRow>> {
        Ok(self
            .database
            .match_records(QueryPlan::all(self.directory.quarantined_rows))?
            .records()
            .to_vec())
    }
}
