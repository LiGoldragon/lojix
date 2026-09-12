//! Read-only store inspection for operator diagnostics.
//!
//! This module deliberately bypasses `Store::open`: registering tables through
//! `sema-engine` is a write when a catalog entry is missing. The inspector opens
//! redb in read-only mode and decodes known Lojix tables independently, so
//! generation/event-log schema failures can be diagnosed without changing the
//! inspected store.

use std::collections::BTreeSet;
use std::ffi::OsString;
use std::path::PathBuf;

use datom_codec::{Actualizing, Potential};
use redb::{ReadableDatabase, ReadableTable, ReadableTableMetadata, TableDefinition};
use rkyv::Deserialize as RkyvDeserialize;
use rkyv::api::high::HighDeserializer;
use rkyv::bytecheck::CheckBytes;
use rkyv::rancor::{self, Strategy};
use rkyv::validation::Validator;
use rkyv::validation::archive::ArchiveValidator;
use rkyv::validation::shared::SharedValidator;
use sema_engine::TableRegistration;

use crate::runtime_model::{
    ContainerLifecycleRecord, DeployJob, DeploymentRecord, EventLogEntry, GcRoot,
    IdentifierAllocation, LiveGeneration, StoredTestRun,
};
use crate::{Error, InlineDatomArguments as _, LojixRecord, OfflineCommand, Result, ingress};

const CATALOG_TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("__sema_engine_catalog");
const META_TABLE: TableDefinition<&str, u64> = TableDefinition::new("__sema_meta");
const SCHEMA_VERSION_KEY: &str = "schema_version";
pub struct StoreInspectionCommand {
    path: PathBuf,
}

impl OfflineCommand for StoreInspectionCommand {
    type Outcome = StoreInspection;

    fn from_arguments(arguments: impl IntoIterator<Item = OsString>) -> Result<Self> {
        let text = (arguments).single_inline_datom()?;
        let path = text.inspected_store_path()?;
        Ok(Self {
            path: PathBuf::from(path),
        })
    }

    fn run(&self) -> StoreInspection {
        StoreInspector {
            path: self.path.clone(),
        }
        .inspect()
    }
}

/// Reading the CLI's one inline operand as the request it must be.
trait InspectionRequestText {
    /// Decode exactly one inline current Datom `InspectStore.{ <path> }`
    /// request and answer with the store path it names. The CLI never hands
    /// its operand to a file-classifying component parser, so an existing
    /// request-like path remains plain rejected text rather than input.
    fn inspected_store_path(&self) -> Result<String>;
}

impl InspectionRequestText for str {
    fn inspected_store_path(&self) -> Result<String> {
        let request = Potential::<ingress::InspectionRequest>::from(self.to_owned())
            .actualize(&mut <crate::Ingress as crate::Budgeted>::budget())
            .map_err(|fault| Error::DatomRequestText(format!("{fault:?}")))?;
        let ingress::InspectionRequest::InspectStore(ingress::InspectStore { string: path }) =
            request;
        Ok(path)
    }
}

pub struct StoreInspector {
    pub path: PathBuf,
}

/// Reading a store file without changing it. Registering tables through
/// `sema-engine` is a write when a catalog entry is missing, so the inspector
/// opens redb read-only and decodes the known families itself.
pub trait StoreInspecting {
    fn inspect(&self) -> StoreInspection;
}

impl StoreInspecting for StoreInspector {
    fn inspect(&self) -> StoreInspection {
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

        let catalog = StoreCatalogReader {
            database: &database,
        }
        .read();
        let registered_tables = catalog.registered_tables();
        StoreInspection {
            path: self.path.clone(),
            database: DatabaseInspection::Opened,
            schema: StoreSchemaReader {
                database: &database,
            }
            .read(),
            tables: StoreTableReader {
                database: &database,
                registered_tables,
            }
            .read(),
            catalog,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoreInspection {
    pub path: PathBuf,
    pub database: DatabaseInspection,
    pub schema: SchemaInspection,
    pub catalog: CatalogInspection,
    pub tables: Vec<TableInspection>,
}

/// Finding one family's row in a whole-store report.
pub trait InspectedTables {
    fn table_named(&self, name: &str) -> Option<&TableInspection>;
}

impl InspectedTables for StoreInspection {
    fn table_named(&self, name: &str) -> Option<&TableInspection> {
        self.tables.iter().find(|table| table.name == name)
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

/// What a catalog read says the store has registered, whether or not it could
/// be read at all.
trait RegisteredTables {
    fn registered_tables(&self) -> BTreeSet<String>;
}

impl RegisteredTables for CatalogInspection {
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
    pub name: &'static str,
    pub role: &'static str,
    pub status: TableInspectionStatus,
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

/// One part of a store report, read from an already-open read-only database.
trait InspectionReader {
    type Read;

    fn read(&self) -> Self::Read;
}

impl InspectionReader for StoreSchemaReader<'_> {
    type Read = SchemaInspection;

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

impl InspectionReader for StoreCatalogReader<'_> {
    type Read = CatalogInspection;

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

impl InspectionReader for StoreTableReader<'_> {
    type Read = Vec<TableInspection>;

    fn read(&self) -> Vec<TableInspection> {
        vec![
            self.inspect::<LiveGeneration>(),
            self.inspect::<GcRoot>(),
            self.inspect::<EventLogEntry>(),
            self.inspect::<ContainerLifecycleRecord>(),
            self.inspect::<DeployJob>(),
            self.inspect::<StoredTestRun>(),
            self.inspect::<DeploymentRecord>(),
            self.inspect::<IdentifierAllocation>(),
        ]
    }
}

/// Reading one known family's rows back out of a store, to say whether they are
/// there and whether they still decode.
trait FamilyInspecting {
    fn inspect<Record: LojixRecord>(&self) -> TableInspection
    where
        Record::Archived: RkyvDeserialize<Record, HighDeserializer<rancor::Error>>
            + for<'validation> CheckBytes<
                Strategy<Validator<ArchiveValidator<'validation>, SharedValidator>, rancor::Error>,
            >;

    fn inspect_rows<Record: LojixRecord>(&self, registered: bool) -> TableInspectionStatus
    where
        Record::Archived: RkyvDeserialize<Record, HighDeserializer<rancor::Error>>
            + for<'validation> CheckBytes<
                Strategy<Validator<ArchiveValidator<'validation>, SharedValidator>, rancor::Error>,
            >;
}

impl FamilyInspecting for StoreTableReader<'_> {
    fn inspect<Record: LojixRecord>(&self) -> TableInspection
    where
        Record::Archived: RkyvDeserialize<Record, HighDeserializer<rancor::Error>>
            + for<'validation> CheckBytes<
                Strategy<Validator<ArchiveValidator<'validation>, SharedValidator>, rancor::Error>,
            >,
    {
        let registered = self.registered_tables.contains(Record::TABLE);
        TableInspection {
            name: Record::TABLE,
            role: Record::ROLE,
            status: self.inspect_rows::<Record>(registered),
        }
    }

    fn inspect_rows<Record: LojixRecord>(&self, registered: bool) -> TableInspectionStatus
    where
        Record::Archived: RkyvDeserialize<Record, HighDeserializer<rancor::Error>>
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
        let table =
            match transaction.open_table(TableDefinition::<String, &[u8]>::new(Record::TABLE)) {
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
            if let Err(error) = rkyv::from_bytes::<Record, rancor::Error>(value.value()) {
                return TableInspectionStatus::DecodeFailed {
                    message: error.to_string(),
                };
            }
            row_count += 1;
        }
        TableInspectionStatus::Readable { row_count }
    }
}
