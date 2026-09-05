//! Read-only store inspection for operator diagnostics.
//!
//! This module deliberately bypasses `Store::open`: registering tables through
//! `sema-engine` is a write when a catalog entry is missing. The inspector opens
//! redb in read-only mode and decodes known Lojix tables independently, so
//! generation/event-log schema failures can be diagnosed without changing the
//! inspected store.

use std::collections::BTreeSet;
use std::ffi::OsString;
use std::path::{Path, PathBuf};

use datom_codec::{Actualizable, IncorporationBudget, Potential};
use redb::{ReadableDatabase, ReadableTable, ReadableTableMetadata, TableDefinition};
use rkyv::api::high::HighDeserializer;
use rkyv::bytecheck::CheckBytes;
use rkyv::rancor::{self, Strategy};
use rkyv::validation::Validator;
use rkyv::validation::archive::ArchiveValidator;
use rkyv::validation::shared::SharedValidator;
use rkyv::{Archive, Deserialize as RkyvDeserialize};
use sema_engine::TableRegistration;

use crate::runtime_model::{
    ContainerLifecycleRecord, DeployJob, DeploymentRecord, EventLogEntry, GcRoot,
    IdentifierAllocation, LiveGeneration, StoredTestRun,
};
use crate::{Error, Result, single_inline_datom_argument};

#[path = "ingress.rs"]
mod ingress;

const CATALOG_TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("__sema_engine_catalog");
const META_TABLE: TableDefinition<&str, u64> = TableDefinition::new("__sema_meta");
const SCHEMA_VERSION_KEY: &str = "schema_version";
const LIVE_SET_INSPECTION: TableInspectionTarget<LiveGeneration> =
    TableInspectionTarget::new("live-set", "current generation rows");
const GC_ROOTS_INSPECTION: TableInspectionTarget<GcRoot> =
    TableInspectionTarget::new("gc-roots", "gc root rows");
const EVENT_LOG_INSPECTION: TableInspectionTarget<EventLogEntry> =
    TableInspectionTarget::new("event-log", "deployment event-log rows");
const CONTAINER_LIFECYCLE_INSPECTION: TableInspectionTarget<ContainerLifecycleRecord> =
    TableInspectionTarget::new("container-lifecycle", "container lifecycle rows");
const DEPLOY_JOB_INSPECTION: TableInspectionTarget<DeployJob> =
    TableInspectionTarget::new("deploy-job", "in-flight deploy job rows");
const TEST_RUN_INSPECTION: TableInspectionTarget<StoredTestRun> =
    TableInspectionTarget::new("test-run", "test-run rows");
const DEPLOYMENT_RECORD_INSPECTION: TableInspectionTarget<DeploymentRecord> =
    TableInspectionTarget::new("deployment-record", "durable deployment correlation rows");
const IDENTIFIER_ALLOCATION_INSPECTION: TableInspectionTarget<IdentifierAllocation> =
    TableInspectionTarget::new("identifier-allocation", "global identifier high-water row");

pub struct StoreInspectionCommand {
    path: PathBuf,
}

impl StoreInspectionCommand {
    pub fn from_environment() -> Result<Self> {
        Self::from_arguments(std::env::args_os().skip(1))
    }

    pub fn from_arguments(arguments: impl IntoIterator<Item = OsString>) -> Result<Self> {
        let text = single_inline_datom_argument(arguments)?;
        let path = parse_inspect_store_request(&text)?;
        Ok(Self {
            path: PathBuf::from(path),
        })
    }

    pub fn run(&self) -> StoreInspection {
        StoreInspector::new(self.path.clone()).inspect()
    }
}

/// Decode exactly one inline current Datom `InspectStore.{ <path> }` request.
/// The CLI never hands its operand to a file-classifying component parser, so
/// an existing request-like path remains plain rejected text rather than input.
fn parse_inspect_store_request(text: &str) -> Result<String> {
    let request = Potential::<ingress::InspectionRequest>::from(text.to_owned())
        .actualize(IncorporationBudget::try_from(16_384).expect("static ingress budget"))
        .map_err(|fault| Error::DatomRequestText(format!("{fault:?}")))?;
    let ingress::InspectionRequest::InspectStore(ingress::InspectStore(path)) = request;
    Ok(path.to_string())
}

pub struct StoreInspector {
    path: PathBuf,
}

impl StoreInspector {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self { path: path.into() }
    }

    pub fn inspect(&self) -> StoreInspection {
        if !self.path.exists() {
            return StoreInspection {
                path: self.path.clone(),
                database: DatabaseInspection::MissingPath,
                schema: SchemaInspection::Unreadable {
                    message: "store path does not exist".to_string(),
                },
                catalog: CatalogInspection::Unreadable {
                    message: "store path does not exist".to_string(),
                },
                tables: Vec::new(),
            };
        }

        let database = match redb::ReadOnlyDatabase::open(&self.path) {
            Ok(database) => database,
            Err(error) => {
                return StoreInspection {
                    path: self.path.clone(),
                    database: DatabaseInspection::OpenFailed {
                        message: error.to_string(),
                    },
                    schema: SchemaInspection::Unreadable {
                        message: "database did not open".to_string(),
                    },
                    catalog: CatalogInspection::Unreadable {
                        message: "database did not open".to_string(),
                    },
                    tables: Vec::new(),
                };
            }
        };

        let catalog = StoreCatalogReader::new(&database).read();
        let registered_tables = catalog.registered_tables();
        StoreInspection {
            path: self.path.clone(),
            database: DatabaseInspection::Opened,
            schema: StoreSchemaReader::new(&database).read(),
            tables: StoreTableReader::new(&database, registered_tables).inspect_all(),
            catalog,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoreInspection {
    path: PathBuf,
    database: DatabaseInspection,
    schema: SchemaInspection,
    catalog: CatalogInspection,
    tables: Vec<TableInspection>,
}

impl StoreInspection {
    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn database(&self) -> &DatabaseInspection {
        &self.database
    }

    pub fn schema(&self) -> &SchemaInspection {
        &self.schema
    }

    pub fn catalog(&self) -> &CatalogInspection {
        &self.catalog
    }

    pub fn tables(&self) -> &[TableInspection] {
        &self.tables
    }

    pub fn table_named(&self, name: &str) -> Option<&TableInspection> {
        self.tables.iter().find(|table| table.name() == name)
    }
}

impl std::fmt::Display for StoreInspection {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(formatter, "StoreInspection {}", self.path.display())?;
        writeln!(formatter, "Database {}", self.database)?;
        writeln!(formatter, "Schema {}", self.schema)?;
        writeln!(formatter, "Catalog {}", self.catalog)?;
        for table in &self.tables {
            writeln!(formatter, "Table {table}")?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DatabaseInspection {
    Opened,
    MissingPath,
    OpenFailed { message: String },
}

impl std::fmt::Display for DatabaseInspection {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Opened => write!(formatter, "opened-read-only"),
            Self::MissingPath => write!(formatter, "missing-path"),
            Self::OpenFailed { message } => write!(formatter, "open-failed [{message}]"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SchemaInspection {
    Matches { version: u32 },
    Mismatched { expected: u32, found: u32 },
    Missing,
    Unreadable { message: String },
}

impl std::fmt::Display for SchemaInspection {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Matches { version } => write!(formatter, "matches version={version}"),
            Self::Mismatched { expected, found } => {
                write!(formatter, "mismatched expected={expected} found={found}")
            }
            Self::Missing => write!(formatter, "missing"),
            Self::Unreadable { message } => write!(formatter, "unreadable [{message}]"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CatalogInspection {
    Readable { registered_tables: BTreeSet<String> },
    Missing,
    Unreadable { message: String },
}

impl CatalogInspection {
    fn registered_tables(&self) -> BTreeSet<String> {
        match self {
            Self::Readable { registered_tables } => registered_tables.clone(),
            Self::Missing | Self::Unreadable { .. } => BTreeSet::new(),
        }
    }
}

impl std::fmt::Display for CatalogInspection {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Readable { registered_tables } => {
                write!(
                    formatter,
                    "readable registered_tables={}",
                    registered_tables.len()
                )
            }
            Self::Missing => write!(formatter, "missing"),
            Self::Unreadable { message } => write!(formatter, "unreadable [{message}]"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableInspection {
    name: &'static str,
    role: &'static str,
    status: TableInspectionStatus,
}

impl TableInspection {
    pub fn name(&self) -> &'static str {
        self.name
    }

    pub fn role(&self) -> &'static str {
        self.role
    }

    pub fn status(&self) -> &TableInspectionStatus {
        &self.status
    }
}

impl std::fmt::Display for TableInspection {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "{} role=\"{}\" {}",
            self.name, self.role, self.status
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TableInspectionStatus {
    Missing,
    Empty,
    Readable { row_count: usize },
    ReadFailed { message: String },
    DecodeFailed { message: String },
}

impl std::fmt::Display for TableInspectionStatus {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Missing => write!(formatter, "missing"),
            Self::Empty => write!(formatter, "empty row_count=0"),
            Self::Readable { row_count } => write!(formatter, "readable row_count={row_count}"),
            Self::ReadFailed { message } => write!(formatter, "read-failed [{message}]"),
            Self::DecodeFailed { message } => write!(formatter, "decode-failed [{message}]"),
        }
    }
}

struct StoreSchemaReader<'database> {
    database: &'database redb::ReadOnlyDatabase,
}

impl<'database> StoreSchemaReader<'database> {
    fn new(database: &'database redb::ReadOnlyDatabase) -> Self {
        Self { database }
    }

    fn read(&self) -> SchemaInspection {
        let transaction = match self.database.begin_read() {
            Ok(transaction) => transaction,
            Err(error) => {
                return SchemaInspection::Unreadable {
                    message: error.to_string(),
                };
            }
        };
        let table = match transaction.open_table(META_TABLE) {
            Ok(table) => table,
            Err(redb::TableError::TableDoesNotExist(_)) => return SchemaInspection::Missing,
            Err(error) => {
                return SchemaInspection::Unreadable {
                    message: error.to_string(),
                };
            }
        };
        let found = match table.get(SCHEMA_VERSION_KEY) {
            Ok(Some(value)) => value.value() as u32,
            Ok(None) => return SchemaInspection::Missing,
            Err(error) => {
                return SchemaInspection::Unreadable {
                    message: error.to_string(),
                };
            }
        };
        let expected = crate::LOJIX_SCHEMA_VERSION.value();
        if found == expected {
            SchemaInspection::Matches { version: found }
        } else {
            SchemaInspection::Mismatched { expected, found }
        }
    }
}

struct StoreCatalogReader<'database> {
    database: &'database redb::ReadOnlyDatabase,
}

impl<'database> StoreCatalogReader<'database> {
    fn new(database: &'database redb::ReadOnlyDatabase) -> Self {
        Self { database }
    }

    fn read(&self) -> CatalogInspection {
        let transaction = match self.database.begin_read() {
            Ok(transaction) => transaction,
            Err(error) => {
                return CatalogInspection::Unreadable {
                    message: error.to_string(),
                };
            }
        };
        let table = match transaction.open_table(CATALOG_TABLE) {
            Ok(table) => table,
            Err(redb::TableError::TableDoesNotExist(_)) => return CatalogInspection::Missing,
            Err(error) => {
                return CatalogInspection::Unreadable {
                    message: error.to_string(),
                };
            }
        };

        let mut registered_tables = BTreeSet::new();
        let rows = match table.iter() {
            Ok(rows) => rows,
            Err(error) => {
                return CatalogInspection::Unreadable {
                    message: error.to_string(),
                };
            }
        };
        for row in rows {
            let (_key, value) = match row {
                Ok(row) => row,
                Err(error) => {
                    return CatalogInspection::Unreadable {
                        message: error.to_string(),
                    };
                }
            };
            let registration =
                match rkyv::from_bytes::<TableRegistration, rancor::Error>(value.value()) {
                    Ok(registration) => registration,
                    Err(error) => {
                        return CatalogInspection::Unreadable {
                            message: error.to_string(),
                        };
                    }
                };
            registered_tables.insert(registration.table_name().to_string());
        }
        CatalogInspection::Readable { registered_tables }
    }
}

struct StoreTableReader<'database> {
    database: &'database redb::ReadOnlyDatabase,
    registered_tables: BTreeSet<String>,
}

impl<'database> StoreTableReader<'database> {
    fn new(
        database: &'database redb::ReadOnlyDatabase,
        registered_tables: BTreeSet<String>,
    ) -> Self {
        Self {
            database,
            registered_tables,
        }
    }

    fn inspect_all(&self) -> Vec<TableInspection> {
        vec![
            self.inspect(LIVE_SET_INSPECTION),
            self.inspect(GC_ROOTS_INSPECTION),
            self.inspect(EVENT_LOG_INSPECTION),
            self.inspect(CONTAINER_LIFECYCLE_INSPECTION),
            self.inspect(DEPLOY_JOB_INSPECTION),
            self.inspect(TEST_RUN_INSPECTION),
            self.inspect(DEPLOYMENT_RECORD_INSPECTION),
            self.inspect(IDENTIFIER_ALLOCATION_INSPECTION),
        ]
    }

    fn inspect<RecordValue>(&self, target: TableInspectionTarget<RecordValue>) -> TableInspection
    where
        RecordValue: Archive + 'static,
        <RecordValue as Archive>::Archived: RkyvDeserialize<RecordValue, HighDeserializer<rancor::Error>>
            + for<'validation> CheckBytes<
                Strategy<Validator<ArchiveValidator<'validation>, SharedValidator>, rancor::Error>,
            >,
    {
        let registered = self.registered_tables.contains(target.name);
        TableInspection {
            name: target.name,
            role: target.role,
            status: self.inspect_rows(target, registered),
        }
    }

    fn inspect_rows<RecordValue>(
        &self,
        target: TableInspectionTarget<RecordValue>,
        registered: bool,
    ) -> TableInspectionStatus
    where
        RecordValue: Archive + 'static,
        <RecordValue as Archive>::Archived: RkyvDeserialize<RecordValue, HighDeserializer<rancor::Error>>
            + for<'validation> CheckBytes<
                Strategy<Validator<ArchiveValidator<'validation>, SharedValidator>, rancor::Error>,
            >,
    {
        let transaction = match self.database.begin_read() {
            Ok(transaction) => transaction,
            Err(error) => {
                return TableInspectionStatus::ReadFailed {
                    message: error.to_string(),
                };
            }
        };
        let table = match transaction.open_table(target.definition()) {
            Ok(table) => table,
            Err(redb::TableError::TableDoesNotExist(_)) if registered => {
                return TableInspectionStatus::Empty;
            }
            Err(redb::TableError::TableDoesNotExist(_)) => {
                return TableInspectionStatus::Missing;
            }
            Err(error) => {
                return TableInspectionStatus::ReadFailed {
                    message: error.to_string(),
                };
            }
        };
        if table.is_empty().unwrap_or(false) {
            return TableInspectionStatus::Empty;
        }
        let rows = match table.iter() {
            Ok(rows) => rows,
            Err(error) => {
                return TableInspectionStatus::ReadFailed {
                    message: error.to_string(),
                };
            }
        };
        let mut row_count = 0;
        for row in rows {
            let (_key, value) = match row {
                Ok(row) => row,
                Err(error) => {
                    return TableInspectionStatus::ReadFailed {
                        message: error.to_string(),
                    };
                }
            };
            if let Err(error) = rkyv::from_bytes::<RecordValue, rancor::Error>(value.value()) {
                return TableInspectionStatus::DecodeFailed {
                    message: error.to_string(),
                };
            }
            row_count += 1;
        }
        TableInspectionStatus::Readable { row_count }
    }
}

#[derive(Clone, Copy)]
struct TableInspectionTarget<RecordValue> {
    name: &'static str,
    role: &'static str,
    record: std::marker::PhantomData<RecordValue>,
}

impl<RecordValue> TableInspectionTarget<RecordValue> {
    const fn new(name: &'static str, role: &'static str) -> Self {
        Self {
            name,
            role,
            record: std::marker::PhantomData,
        }
    }

    fn definition(self) -> TableDefinition<'static, String, &'static [u8]> {
        TableDefinition::new(self.name)
    }
}
