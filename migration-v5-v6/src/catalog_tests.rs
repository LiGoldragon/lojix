use super::*;
use std::sync::atomic::{AtomicU64, Ordering};

struct Fixture(PathBuf);
impl Fixture {
    fn new() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let path = std::env::temp_dir().join(format!(
            "migration-catalog-{}-{}", std::process::id(), NEXT.fetch_add(1, Ordering::SeqCst)
        ));
        std::fs::create_dir(&path).unwrap();
        Self(path)
    }
    fn store(&self) -> PathBuf { self.0.join("store.db") }
}
impl Drop for Fixture {
    fn drop(&mut self) { std::fs::remove_dir_all(&self.0).unwrap(); }
}

#[test]
fn authentic_old_registered_quarantine_is_empty_without_a_physical_table() {
    let fixture = Fixture::new();
    let store = old::Store::open(fixture.store()).unwrap();
    drop(store);
    let db = redb::ReadOnlyDatabase::open(fixture.store()).unwrap();
    let tx = db.begin_read().unwrap();
    assert!(tx.open_table(CATALOG).unwrap().get("quarantined-row").unwrap().is_some());
    assert!(!tx.list_tables().unwrap().any(|t| t.name() == "quarantined-row"));
    assert!(raw_rows(&db, "quarantined-row").unwrap().is_empty());
}

#[test]
fn known_name_without_its_store_registration_is_not_empty() {
    let fixture = Fixture::new();
    let db = redb::Database::create(fixture.store()).unwrap();
    let tx = db.begin_write().unwrap();
    drop(tx.open_table(CATALOG).unwrap());
    tx.commit().unwrap();
    drop(db);
    let db = redb::ReadOnlyDatabase::open(fixture.store()).unwrap();
    assert!(raw_rows(&db, "quarantined-row").is_err(),
        "an absent known table needs this store's registration, not a static family name");
}
