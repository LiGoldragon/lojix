//! Offline, copy-only schema conversion. No daemon, worker, socket or transport.
mod mapping;

use mapping::Result;
use new::quarantine::OpenedStore as _;
use new::{DurableStore as _, LegacyConfigurationArchivable as _};
use old::LegacyConfigurationArchivable as _;
use old::Payload as _;
use redb::{ReadableDatabase, ReadableTable, TableDefinition, TableHandle};
use rkyv::rancor::Error;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::fs::File;
use std::io::{Read, Seek, SeekFrom, Write};
use std::os::fd::AsRawFd;
use std::os::unix::fs::{MetadataExt, PermissionsExt};
use std::path::{Component, Path, PathBuf};

const META: TableDefinition<&str, u64> = TableDefinition::new("__sema_meta");
const CATALOG: TableDefinition<&str, &[u8]> = TableDefinition::new("__sema_engine_catalog");
const COUNTERS: TableDefinition<&str, &[u8]> = TableDefinition::new("__sema_engine_counters");
const FAMILIES: &[(&str, &str, u8)] = &[
    ("live-set", "LiveSetFamily", 1),
    ("gc-roots", "GcRootsFamily", 2),
    ("event-log", "EventLogFamily", 3),
    ("container-lifecycle", "ContainerLifecycleFamily", 4),
    ("deploy-job", "DeployJobFamily", 5),
    ("test-run", "TestRunFamily", 6),
    ("deployment-record", "DeploymentRecordFamily", 7),
    ("identifier-allocation", "IdentifierAllocationFamily", 8),
    ("deployment-outbox", "DeploymentOutboxFamily", 9),
    (
        "pending-transition-intent",
        "PendingTransitionIntentFamily",
        11,
    ),
    ("nexus-configuration", "NexusConfigurationFamily", 12),
];

#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
struct OldConfigurationRow {
    state: nexus::ConfigurationState<old_signal::LojixNexusConfiguration>,
}
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
struct NewConfigurationRow {
    state: new::NexusConfigurationState,
}

fn require(condition: bool, reason: &str) -> Result<()> {
    if condition {
        Ok(())
    } else {
        Err(reason.into())
    }
}
fn digest(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}
fn opened(path: &Path, directory: bool) -> Result<File> {
    require(
        path.is_absolute()
            && path
                .components()
                .all(|c| matches!(c, Component::RootDir | Component::Normal(_))),
        "absolute canonical paths required",
    )?;
    let fd = rustix::fs::openat2(
        rustix::fs::CWD,
        path,
        rustix::fs::OFlags::RDONLY
            | rustix::fs::OFlags::CLOEXEC
            | if directory {
                rustix::fs::OFlags::DIRECTORY
            } else {
                rustix::fs::OFlags::empty()
            },
        rustix::fs::Mode::empty(),
        rustix::fs::ResolveFlags::NO_SYMLINKS,
    )?;
    let file = File::from(fd);
    require(
        directory || (file.metadata()?.is_file() && file.metadata()?.mode() & 0o077 == 0),
        "input files must be regular and private",
    )?;
    require(path.canonicalize()? == path, "path alias rejected")?;
    Ok(file)
}
fn bytes(file: &mut File) -> Result<Vec<u8>> {
    file.seek(SeekFrom::Start(0))?;
    let mut out = Vec::new();
    file.read_to_end(&mut out)?;
    Ok(out)
}
fn identity(file: &File) -> Result<(u64, u64, u64, i64, i64)> {
    let m = file.metadata()?;
    Ok((m.dev(), m.ino(), m.len(), m.mtime(), m.mtime_nsec()))
}
fn same_directory(file: &File, path: &Path) -> Result<()> {
    let held = file.metadata()?;
    let named = opened(path, true)?.metadata()?;
    require(
        (held.dev(), held.ino()) == (named.dev(), named.ino()),
        "publication directory was replaced",
    )
}
fn proc_path(file: &File) -> PathBuf {
    PathBuf::from(format!("/proc/self/fd/{}", file.as_raw_fd()))
}
fn write_private(directory: &File, name: &str, data: &[u8], mode: u32) -> Result<()> {
    let fd = rustix::fs::openat(
        directory,
        name,
        rustix::fs::OFlags::WRONLY
            | rustix::fs::OFlags::CREATE
            | rustix::fs::OFlags::EXCL
            | rustix::fs::OFlags::NOFOLLOW
            | rustix::fs::OFlags::CLOEXEC,
        rustix::fs::Mode::from_raw_mode(0o600),
    )?;
    let mut f = File::from(fd);
    f.write_all(data)?;
    f.set_permissions(std::fs::Permissions::from_mode(mode & 0o777))?;
    sync(&f)?;
    Ok(())
}
fn raw_rows(db: &redb::ReadOnlyDatabase, name: &str) -> Result<Vec<(String, Vec<u8>)>> {
    let tx = db.begin_read()?;
    let table = tx.open_table(TableDefinition::<String, &[u8]>::new(name))?;
    table
        .iter()?
        .map(|row| {
            let (k, v) = row?;
            Ok((k.value(), v.value().to_vec()))
        })
        .collect()
}
fn numeric(
    db: &redb::ReadOnlyDatabase,
    table: TableDefinition<&str, u64>,
) -> Result<BTreeMap<String, u64>> {
    let tx = db.begin_read()?;
    let t = tx.open_table(table)?;
    t.iter()?
        .map(|r| {
            let (k, v) = r?;
            Ok((k.value().to_string(), v.value()))
        })
        .collect()
}
fn engine_counters(db: &redb::ReadOnlyDatabase) -> Result<BTreeMap<String, u64>> {
    let tx = db.begin_read()?;
    let table = tx.open_table(COUNTERS)?;
    table
        .iter()?
        .map(|r| {
            let (k, v) = r?;
            Ok((
                k.value().to_string(),
                rkyv::from_bytes::<u64, Error>(v.value())?,
            ))
        })
        .collect()
}
fn check_key(key: &str, record: &impl sema_engine::EngineRecord) -> Result<()> {
    require(
        record.record_key().to_owned_string() == key,
        "record key differs from archived identity",
    )
}
fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}
// Table digests use SHA-256 over a domain prefix and ordered length-framed
// raw key/value pairs. Lengths are unsigned 64-bit big-endian bytes.
fn table_digest(name: &str, rows: &[(String, Vec<u8>)]) -> String {
    let mut h = Sha256::new();
    h.update(b"LojixV5V6Migration/1/table\0");
    h.update((name.len() as u64).to_be_bytes());
    h.update(name.as_bytes());
    for (k, v) in rows {
        h.update((k.len() as u64).to_be_bytes());
        h.update(k.as_bytes());
        h.update((v.len() as u64).to_be_bytes());
        h.update(v);
    }
    format!("{:x}", h.finalize())
}
fn framed_numeric(name: &str, values: &BTreeMap<String, u64>) -> Value {
    let mut h = Sha256::new();
    h.update(b"LojixV5V6Migration/1/table\0");
    h.update((name.len() as u64).to_be_bytes());
    h.update(name.as_bytes());
    let mut records = Vec::new();
    for (k, v) in values {
        let data = v.to_le_bytes();
        h.update((k.len() as u64).to_be_bytes());
        h.update(k.as_bytes());
        h.update(8u64.to_be_bytes());
        h.update(data);
        records.push(
            json!({"key_hex":hex(k.as_bytes()),"value_sha256":digest(&data),"value_bytes":8}),
        );
    }
    json!({"sha256":format!("{:x}",h.finalize()),"records":records})
}
fn engine_snapshot(db: &redb::ReadOnlyDatabase) -> Result<BTreeMap<String, Value>> {
    let tx = db.begin_read()?;
    let mut snapshots = BTreeMap::new();
    let names = tx
        .list_tables()?
        .map(|t| t.name().to_string())
        .collect::<BTreeSet<_>>();
    require(
        tx.list_multimap_tables()?.next().is_none(),
        "unknown multimap table",
    )?;
    macro_rules! table {
        ($name:expr, $key:ty, $encode:expr) => {{
            if names.contains($name) {
                let table=tx.open_table(TableDefinition::<$key,&[u8]>::new($name))?;
                let mut rows=Vec::new();
                for row in table.iter()? {let(k,v)=row?;rows.push(($encode(k.value()),v.value().to_vec()));}
                rows.sort_by(|a,b|a.0.cmp(&b.0));
                let mut h=Sha256::new();h.update(b"LojixV5V6Migration/1/table\0");
                h.update(($name.len() as u64).to_be_bytes());h.update($name.as_bytes());
                let mut records=Vec::new();
                for(k,v) in &rows {h.update((k.len() as u64).to_be_bytes());h.update(k);h.update((v.len() as u64).to_be_bytes());h.update(v);
                    records.push(json!({"key_hex":hex(k),"value_sha256":digest(v),"value_bytes":v.len()}));}
                if matches!($name,"__sema_engine_compaction_intent"|"__sema_engine_staging_slot"|"__sema_engine_outbox") {require(rows.is_empty(),"pending engine work holds migration")?;}
                snapshots.insert($name.to_string(),json!({"sha256":format!("{:x}",h.finalize()),"records":records}));
            }
        }}
    }
    for name in [
        "__sema_headers",
        "__sema_engine_chain_head",
        "__sema_engine_versioning_policy",
        "__sema_engine_compaction_intent",
        "__sema_engine_staging_slot",
        "__sema_engine_mirror_cursor",
    ] {
        table!(name, &str, |k: &str| k.as_bytes().to_vec());
    }
    for name in [
        "__sema_engine_commit_log",
        "__sema_engine_versioned_commit_log",
        "__sema_engine_subscriptions",
        "__sema_engine_checkpoints",
        "__sema_engine_outbox",
    ] {
        table!(name, u64, |k: u64| k.to_le_bytes().to_vec());
    }
    table!(
        "__sema_engine_checkpoint_segments",
        &[u8; 32],
        |k: &[u8; 32]| k.to_vec()
    );
    if names.contains("__sema_engine_identified_counters") {
        let table = tx.open_table(TableDefinition::<String, u64>::new(
            "__sema_engine_identified_counters",
        ))?;
        let mut records = Vec::new();
        let mut h = Sha256::new();
        let name = "__sema_engine_identified_counters";
        h.update(b"LojixV5V6Migration/1/table\0");
        h.update((name.len() as u64).to_be_bytes());
        h.update(name.as_bytes());
        for row in table.iter()? {
            let (k, v) = row?;
            let key = k.value();
            let val = v.value().to_le_bytes();
            h.update((key.len() as u64).to_be_bytes());
            h.update(key.as_bytes());
            h.update(8u64.to_be_bytes());
            h.update(val);
            records.push(
                json!({"key_hex":hex(key.as_bytes()),"value_sha256":digest(&val),"value_bytes":8}),
            );
        }
        snapshots.insert(
            name.into(),
            json!({"sha256":format!("{:x}",h.finalize()),"records":records}),
        );
    }
    let allowed = FAMILIES
        .iter()
        .map(|(n, _, _)| n.to_string())
        .chain(snapshots.keys().cloned())
        .chain(
            [
                "quarantined-row",
                "__sema_meta",
                "__sema_engine_catalog",
                "__sema_engine_counters",
            ]
            .map(str::to_string),
        )
        .collect::<BTreeSet<_>>();
    require(names.is_subset(&allowed), "unknown engine/domain table")?;
    Ok(snapshots)
}
fn sidecar_names(source: &Path) -> Result<BTreeSet<String>> {
    let prefix = format!(
        "{}.schema-",
        source
            .file_name()
            .ok_or("source basename")?
            .to_string_lossy()
    );
    std::fs::read_dir(source.parent().ok_or("source parent")?)?
        .filter_map(|e| match e {
            Ok(e) => {
                let n = e.file_name().to_string_lossy().to_string();
                n.starts_with(&prefix).then_some(Ok(n))
            }
            Err(e) => Some(Err(e.into())),
        })
        .collect()
}
fn sync(file: &File) -> Result<()> {
    static CALL: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
    let n = CALL.fetch_add(1, std::sync::atomic::Ordering::SeqCst) + 1;
    fault(&format!("fsync-{n}-before"))?;
    file.sync_all()?;
    fault(&format!("fsync-{n}-after"))
}
fn changed_rows(
    db: &redb::ReadOnlyDatabase,
    desired: &new::NexusConfiguration,
) -> Result<(BTreeMap<String, Vec<(String, Vec<u8>)>>, Vec<Value>)> {
    use old::runtime_model as o;
    let mut mapped = BTreeMap::new();
    let mut evidence = Vec::new();
    let mut containers: BTreeMap<(String, String, String), (u64, o::ContainerState)> =
        BTreeMap::new();
    macro_rules! unchanged {
        ($type:ident,$key:expr,$data:expr) => {{
            let v = rkyv::from_bytes::<o::$type, Error>($data)?;
            check_key($key, &v)?;
            let _: new::runtime_model::$type = rkyv::from_bytes::<_, Error>($data)?;
            $data.to_vec()
        }};
    }
    for (name, _, _) in FAMILIES {
        let mut rows = raw_rows(db, name)?;
        rows.sort_by(|a, b| a.0.as_bytes().cmp(b.0.as_bytes()));
        let source_table_sha256 = table_digest(name, &rows);
        if *name == "nexus-configuration" {
            require(
                rows.len() == 1,
                "exactly one existing Nexus configuration required",
            )?;
        }
        let mut target = Vec::new();
        let mut receipts = Vec::new();
        for (key, data) in rows {
            let mut transformation = "unchanged";
            let value = match *name {
                "live-set" => unchanged!(LiveGeneration, &key, &data),
                "gc-roots" => unchanged!(GcRoot, &key, &data),
                "event-log" => unchanged!(EventLogEntry, &key, &data),
                "deployment-record" => unchanged!(DeploymentRecord, &key, &data),
                "identifier-allocation" => unchanged!(IdentifierAllocation, &key, &data),
                "container-lifecycle" => {
                    let v = rkyv::from_bytes::<o::ContainerLifecycleRecord, Error>(&data)?;
                    check_key(&key, &v)?;
                    let position = *v.event_log_position.payload();
                    let id = (
                        v.cluster_name.payload().clone(),
                        v.node_name.payload().clone(),
                        v.container_name.payload().clone(),
                    );
                    if containers
                        .get(&id)
                        .is_none_or(|(previous, _)| position > *previous)
                    {
                        containers.insert(id, (position, v.container_state));
                    }
                    unchanged!(ContainerLifecycleRecord, &key, &data)
                }
                "test-run" => {
                    let v = rkyv::from_bytes::<o::StoredTestRun, Error>(&data)?;
                    require(
                        matches!(
                            v.test_run_phase,
                            o::TestRunPhase::Completed | o::TestRunPhase::Failed
                        ),
                        "live test run holds migration",
                    )?;
                    unchanged!(StoredTestRun, &key, &data)
                }
                "deployment-outbox" => {
                    let v = rkyv::from_bytes::<o::DeploymentOutboxRecord, Error>(&data)?;
                    require(
                        v.outbox_delivery_state == o::OutboxDeliveryState::Acknowledged,
                        "unacknowledged outbox holds migration",
                    )?;
                    unchanged!(DeploymentOutboxRecord, &key, &data)
                }
                "pending-transition-intent" => {
                    let v = rkyv::from_bytes::<o::PendingTransitionIntent, Error>(&data)?;
                    require(
                        v.transition_intent_state == o::TransitionIntentState::Acknowledged,
                        "pending intent holds migration",
                    )?;
                    unchanged!(PendingTransitionIntent, &key, &data)
                }
                "deploy-job" => {
                    let v = rkyv::from_bytes::<o::DeployJob, Error>(&data)?;
                    check_key(&key, &v)?;
                    let n = mapping::job(v)?;
                    check_key(&key, &n)?;
                    transformation = "drop-wan";
                    rkyv::to_bytes::<Error>(&n)?.to_vec()
                }
                "nexus-configuration" => {
                    require(key == "singleton", "unknown configuration key")?;
                    let v = rkyv::from_bytes::<OldConfigurationRow, Error>(&data)?;
                    let mut expected = mapping::configuration(v.state.desired_configuration)?;
                    expected.ordinary_socket_path = desired.ordinary_socket_path.clone();
                    expected.owner_socket_path = desired.owner_socket_path.clone();
                    expected.state_directory_path = desired.state_directory_path.clone();
                    require(
                        expected == *desired,
                        "regenerated archive would override meta-owned configuration beyond WAN/path mapping",
                    )?;
                    let n = NewConfigurationRow {
                        state: new::NexusConfigurationState {
                            desired_configuration: desired.clone(),
                            meta_configure_occurred: v.state.meta_configure_occurred,
                        },
                    };
                    transformation = "configuration-path-relocation";
                    rkyv::to_bytes::<Error>(&n)?.to_vec()
                }
                _ => return Err("unknown family".into()),
            };
            receipts.push(json!({"key_hex":key.as_bytes().iter().map(|b|format!("{b:02x}")).collect::<String>(),"source_sha256":digest(&data),"destination_sha256":digest(&value),"transformation":transformation}));
            target.push((key, value));
        }
        evidence.push(json!({"family":name,"source_table_sha256":source_table_sha256,"destination_table_sha256":table_digest(name,&target),"records":receipts}));
        mapped.insert(name.to_string(), target);
    }
    require(
        containers
            .values()
            .all(|(_, s)| matches!(s, o::ContainerState::Stopped | o::ContainerState::Failed)),
        "live container lifecycle holds migration",
    )?;
    Ok((mapped, evidence))
}

fn migrate(args: Vec<PathBuf>) -> Result<Value> {
    require(
        args.len() == 3,
        "expected OLD_STARTUP_ARCHIVE NEW_STARTUP_ARCHIVE DESTINATION_DIRECTORY",
    )?;
    let (old_archive, new_archive, destination) = (&args[0], &args[1], &args[2]);
    let mut old_file = opened(old_archive, false)?;
    let old_id = identity(&old_file)?;
    let old_bytes = bytes(&mut old_file)?;
    let mut new_file = opened(new_archive, false)?;
    let new_id = identity(&new_file)?;
    let new_bytes = bytes(&mut new_file)?;
    let legacy = old::LegacyStartupConfiguration::from_rkyv_file(&proc_path(&old_file))?;
    let startup = new::LegacyStartupConfiguration::from_rkyv_file(&proc_path(&new_file))?;
    let source = PathBuf::from(&legacy.store_path);
    let mut source_file = opened(&source, false)?;
    let source_id = identity(&source_file)?;
    let source_bytes = bytes(&mut source_file)?;
    let parent = destination.parent().ok_or("destination parent required")?;
    let parent_file = opened(parent, true)?;
    let name = destination
        .file_name()
        .ok_or("destination name required")?
        .to_str()
        .ok_or("UTF-8 destination name required")?;
    require(
        destination.is_absolute()
            && destination
                .components()
                .all(|c| matches!(c, Component::RootDir | Component::Normal(_))),
        "canonical destination required",
    )?;
    require(!destination.exists(), "destination already exists")?;
    require(
        startup.store_path == destination.join("store.db").to_string_lossy()
            && startup.state_directory_path == destination.to_string_lossy(),
        "new archive must own destination/store.db and destination state directory",
    )?;
    let paths = [source.clone(), old_archive.clone(), new_archive.clone()];
    require(
        paths.iter().all(|p| !p.starts_with(destination))
            && BTreeSet::from([
                (source_id.0, source_id.1),
                (old_id.0, old_id.1),
                (new_id.0, new_id.1),
            ])
            .len()
                == 3,
        "source/archive/destination overlap rejected",
    )?;
    let stage_name = format!(".{name}.migration-{}", std::process::id());
    let stage = parent.join(&stage_name);
    require(
        paths.iter().all(|p| !p.starts_with(&stage)),
        "staging overlap rejected",
    )?;
    rustix::fs::mkdirat(
        &parent_file,
        stage_name.as_str(),
        rustix::fs::Mode::from_raw_mode(0o700),
    )?;
    let stage_fd = File::from(rustix::fs::openat(
        &parent_file,
        stage_name.as_str(),
        rustix::fs::OFlags::RDONLY
            | rustix::fs::OFlags::DIRECTORY
            | rustix::fs::OFlags::NOFOLLOW
            | rustix::fs::OFlags::CLOEXEC,
        rustix::fs::Mode::empty(),
    )?);
    rustix::fs::mkdirat(
        &stage_fd,
        "rollback",
        rustix::fs::Mode::from_raw_mode(0o700),
    )?;
    let rollback_fd = File::from(rustix::fs::openat(
        &stage_fd,
        "rollback",
        rustix::fs::OFlags::RDONLY
            | rustix::fs::OFlags::DIRECTORY
            | rustix::fs::OFlags::NOFOLLOW
            | rustix::fs::OFlags::CLOEXEC,
        rustix::fs::Mode::empty(),
    )?);
    write_private(
        &rollback_fd,
        "store.db",
        &source_bytes,
        source_file.metadata()?.mode(),
    )?;
    write_private(
        &rollback_fd,
        "startup.rkyv",
        &old_bytes,
        old_file.metadata()?.mode(),
    )?;
    let mut sidecars = Vec::new();
    let mut sidecar_handles = Vec::new();
    let source_name = source
        .file_name()
        .ok_or("source basename required")?
        .to_string_lossy();
    let original_sidecars = sidecar_names(&source)?;
    for n in &original_sidecars {
        let sidecar_path = source.parent().unwrap().join(n);
        if n.starts_with(&format!("{source_name}.schema-")) {
            let suffix = &n[source_name.len()..];
            require(
                suffix == ".schema-pre-v3.backup",
                "pending or unknown migration sidecar holds migration",
            )?;
            let mut f = opened(&sidecar_path, false)?;
            let id = identity(&f)?;
            let data = bytes(&mut f)?;
            require(
                ![source_id, old_id, new_id]
                    .iter()
                    .any(|original| (original.0, original.1) == (id.0, id.1)),
                "sidecar aliases source/archive",
            )?;
            write_private(
                &rollback_fd,
                &format!("store.db{suffix}"),
                &data,
                f.metadata()?.mode(),
            )?;
            sidecars.push(json!({"suffix":suffix,"sha256":digest(&data),"mode":f.metadata()?.mode() & 0o777,"source_retained":true,"copied_to_rollback":true}));
            sidecar_handles.push((sidecar_path, f, id, digest(&data)));
        }
    }
    sync(&rollback_fd)?;
    let db = redb::ReadOnlyDatabase::open(proc_path(&rollback_fd).join("store.db"))?;
    let meta = numeric(&db, META)?;
    require(
        meta.get("schema_version") == Some(&5),
        "source must have schema 5",
    )?;
    let counters = engine_counters(&db)?;
    require(
        counters.get("engine_storage_layout") == Some(&7),
        "unknown engine storage layout",
    )?;
    let tx = db.begin_read()?;
    let table = tx.open_table(CATALOG)?;
    let mut catalogs = Vec::new();
    let mut expected: BTreeMap<String, (String, u8)> = FAMILIES
        .iter()
        .map(|(t, f, h)| (t.to_string(), (f.to_string(), *h)))
        .collect();
    expected.insert(
        "quarantined-row".into(),
        ("QuarantinedRowFamily".into(), 13),
    );
    for row in table.iter()? {
        let (k, v) = row?;
        let r = rkyv::from_bytes::<sema_engine::TableRegistration, Error>(v.value())?;
        let (family, hash) = expected
            .remove(r.table_name())
            .ok_or("unknown/duplicate catalog family")?;
        require(
            k.value() == r.table_name()
                && r.identity().family().as_str() == family
                && r.identity().schema_hash().bytes() == &[hash; 32],
            "invalid source catalog identity",
        )?;
        catalogs.push((k.value().to_string(), r));
    }
    require(expected.is_empty(), "missing source family")?;
    drop(table);
    drop(tx);
    require(
        raw_rows(&db, "quarantined-row")?.is_empty(),
        "quarantine needs separate resolution",
    )?;
    let tx = db.begin_read()?;
    let engine_before = engine_snapshot(&db)?;
    // Table roster is retained; no engine history table is removed or rebuilt.
    let roster = tx
        .list_tables()?
        .map(|t| Ok(t.name().to_string()))
        .collect::<Result<Vec<_>>>()?;
    drop(tx);
    fault("after-stage")?;
    let desired = new::NexusConfiguration::from(&startup);
    let (mapped, family_receipts) = changed_rows(&db, &desired)?;
    drop(db);
    write_private(&stage_fd, "store.db", &source_bytes, 0o600)?;
    write_private(
        &stage_fd,
        "startup.rkyv",
        &new_bytes,
        new_file.metadata()?.mode(),
    )?;
    let target = proc_path(&stage_fd).join("store.db");
    let database = redb::Database::open(&target)?;
    let write = database.begin_write()?;
    write.open_table(META)?.insert("schema_version", 6)?;
    let mut catalog = write.open_table(CATALOG)?;
    let mut catalog_receipts = Vec::new();
    let mut expected_catalog = BTreeMap::new();
    for (key, r) in catalogs {
        let before = *r.identity().schema_hash().bytes();
        let after = match key.as_str() {
            "deploy-job" => [15; 32],
            "nexus-configuration" => [16; 32],
            _ => before,
        };
        let n = sema_engine::TableRegistration::new(sema_engine::FamilyIdentity::new(
            sema_engine::FamilyName::new(r.identity().family().as_str()),
            sema_engine::SchemaHash::new(after),
            sema_engine::TableName::new(
                FAMILIES
                    .iter()
                    .find(|(t, _, _)| *t == key)
                    .map(|(t, _, _)| *t)
                    .unwrap_or("quarantined-row"),
            ),
        ));
        let encoded = rkyv::to_bytes::<Error>(&n)?;
        catalog.insert(key.as_str(), encoded.as_slice())?;
        expected_catalog.insert(key.clone(), encoded.to_vec());
        catalog_receipts.push(json!({"table":key,"source_hash":before,"destination_hash":after}));
    }
    drop(catalog);
    for name in ["deploy-job", "nexus-configuration"] {
        let mut table = write.open_table(TableDefinition::<String, &[u8]>::new(name))?;
        for (key, value) in &mapped[name] {
            table.insert(key, value.as_slice())?;
        }
    }
    write.commit()?;
    drop(database);
    // Store-only reopening cannot construct or dispatch the Nexus runtime.
    let store = new::Store::open(&target)?;
    require(
        store.opening().quarantined_rows.is_empty() && !store.opening().configuration_rebuilt,
        "reopen changed or quarantined configuration/records",
    )?;
    drop(store);
    let db = redb::ReadOnlyDatabase::open(&target)?;
    for (name, expected) in &mapped {
        require(
            raw_rows(&db, name)? == *expected,
            "reopen changed domain rows",
        )?;
    }
    require(
        engine_snapshot(&db)? == engine_before,
        "reopen changed engine history/header records",
    )?;
    require(
        engine_counters(&db)? == counters,
        "reopen changed engine counters",
    )?;
    let mut expected_meta = meta.clone();
    expected_meta.insert("schema_version".into(), 6);
    require(
        numeric(&db, META)? == expected_meta,
        "unexpected header change",
    )?;
    let tx = db.begin_read()?;
    let catalog = tx.open_table(CATALOG)?;
    let actual_catalog = catalog
        .iter()?
        .map(|r| {
            let (k, v) = r?;
            Ok((k.value().to_string(), v.value().to_vec()))
        })
        .collect::<Result<BTreeMap<_, _>>>()?;
    require(
        actual_catalog == expected_catalog,
        "reopen changed catalog mapping",
    )?;
    drop(catalog);
    let after_roster = tx
        .list_tables()?
        .map(|t| Ok(t.name().to_string()))
        .collect::<Result<Vec<_>>>()?;
    require(after_roster == roster, "reopen changed table roster")?;
    drop(tx);
    drop(db);
    let target_file = opened_internal(&stage_fd, "store.db")?;
    target_file.set_permissions(std::fs::Permissions::from_mode(
        source_file.metadata()?.mode() & 0o777,
    ))?;
    sync(&target_file)?;
    // Recheck source handles and names after all conversion/reopen work.
    for (path, file, id, sha) in [
        (&source, &mut source_file, source_id, digest(&source_bytes)),
        (old_archive, &mut old_file, old_id, digest(&old_bytes)),
        (new_archive, &mut new_file, new_id, digest(&new_bytes)),
    ] {
        require(
            identity(file)? == id
                && identity(&opened(path, false)?)? == id
                && digest(&bytes(file)?) == sha,
            "source tuple changed during migration",
        )?;
    }
    for (path, file, id, sha) in &mut sidecar_handles {
        require(
            identity(file)? == *id
                && identity(&opened(path, false)?)? == *id
                && digest(&bytes(file)?) == *sha,
            "sidecar changed during migration",
        )?;
    }
    require(
        sidecar_names(&source)? == original_sidecars,
        "sidecar roster changed during migration",
    )?;
    let mut target_read = opened_internal(&stage_fd, "store.db")?;
    let mut manifest = json!({"schema":"LojixV5V6Migration/1","committed":false,"source_schema":5,"destination_schema":6,"source_sha256":digest(&source_bytes),"destination_sha256":digest(&bytes(&mut target_read)?),"source_archive_sha256":digest(&old_bytes),"destination_archive_sha256":digest(&new_bytes),"families":family_receipts,"catalog":catalog_receipts,"engine_counters":counters,"engine_counter_digest":framed_numeric("__sema_engine_counters",&counters),"engine_meta_source_digest":framed_numeric("__sema_meta",&meta),"engine_meta_destination_digest":framed_numeric("__sema_meta",&expected_meta),"engine_meta_source":meta,"engine_meta_destination":expected_meta,"engine_tables":roster,"engine_records":engine_before,"digest_algorithm":"SHA-256","digest_framing":"ASCII LojixV5V6Migration/1/table followed by NUL; u64be table-name byte length + UTF-8 table name; ordered rows of u64be key length + raw key bytes + u64be value length + raw value bytes","sidecars":sidecars,"source_preserved":true,"store_only_reopen":true,"key_encoding":"UTF-8 bytes as lowercase hex; ascending raw-byte key order","old_runtime_pin":"3fc95f0cf4eaf14ff62898c4783ebbc670fdf96b","new_runtime_version":"9.0.0","new_runtime_source_baseline":"94d8b69a546560e9d3c8300c6114e8dac7df67ea","converter_source_revision":option_env!("LOJIX_MIGRATION_SOURCE_REVISION").unwrap_or("unqualified-local-build"),"paths":{"source":source,"destination":destination,"old_archive":old_archive,"new_archive":new_archive}});
    write_private(
        &stage_fd,
        "migration.json",
        &serde_json::to_vec(&manifest)?,
        0o600,
    )?;
    sync(&stage_fd)?;
    sync(&parent_file)?;
    fault("before-publish")?;
    same_directory(&parent_file, parent)?;
    same_directory(&stage_fd, &stage)?;
    rustix::fs::renameat_with(
        &parent_file,
        stage_name.as_str(),
        &parent_file,
        name,
        rustix::fs::RenameFlags::NOREPLACE,
    )?;
    sync(&parent_file)?;
    same_directory(&parent_file, parent)?;
    same_directory(&stage_fd, destination)?;
    fault("after-publish")?;
    fault("before-commit-receipt")?;
    manifest["verified_receipt_sha256"] = json!(digest(&serde_json::to_vec(&manifest)?));
    manifest["committed"] = json!(true);
    write_private(
        &stage_fd,
        "migration.committed.json",
        &serde_json::to_vec(&manifest)?,
        0o600,
    )?;
    fault("commit-receipt-written")?;
    sync(&stage_fd)?;
    sync(&parent_file)?;
    same_directory(&parent_file, parent)?;
    same_directory(&stage_fd, destination)?;
    fault("before-success")?;
    Ok(manifest)
}
fn opened_internal(directory: &File, name: &str) -> Result<File> {
    Ok(File::from(rustix::fs::openat(
        directory,
        name,
        rustix::fs::OFlags::RDONLY | rustix::fs::OFlags::NOFOLLOW | rustix::fs::OFlags::CLOEXEC,
        rustix::fs::Mode::empty(),
    )?))
}
fn fault(phase: &str) -> Result<()> {
    if std::env::var("LOJIX_MIGRATION_PAUSE_AT").ok().as_deref() == Some(phase) {
        eprintln!("{}", json!({"qualification_barrier":phase}));
        std::io::stderr().flush()?;
        let mut resume = [0u8; 1];
        std::io::stdin().read_exact(&mut resume)?;
    }

    require(
        std::env::var("LOJIX_MIGRATION_FAIL_AT").ok().as_deref() != Some(phase),
        "injected qualification failure; destination unaccepted",
    )
}
fn main() {
    match migrate(std::env::args_os().skip(1).map(PathBuf::from).collect()) {
        Ok(value) => println!("{value}"),
        Err(error) => {
            eprintln!(
                "{}",
                json!({"schema":"LojixV5V6Migration/1","committed":false,"error":error.to_string()})
            );
            std::process::exit(2);
        }
    }
}
