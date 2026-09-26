//! A v5 store whose one in-flight deploy-job row carries a `HorizonDefinition`
//! archived in the Horizon 0.12 layout.
//!
//! `fixtures/lojix-v5-horizon-0.12-deploy-job.sema` was written once by this
//! crate at horizon-rs `ee8d6f8` (the Horizon 0.12 layout Lojix 7.0.0 carries):
//! a Horizon-mode `Deploy.Host` of `node-1` in cluster `alpha` was admitted
//! through `SchemaRuntime::submit_deploy` and the store was closed with the
//! row still in flight under key `1` — the shape the live Nexus holds while it
//! deploys its own host. The file is not regenerated: after the Horizon bump,
//! regenerating it would write the new layout and prove nothing.
//!
//! This crate now carries the Horizon 0.13 layout. The same store opens, the
//! inspector counts `deploy-job 1` as undecodable before the open, the open
//! quarantines it, and the store still serves.

use lojix::DurableStore as _;
use lojix::inspection::{InspectedTables as _, StoreInspecting as _, StoreInspector};
use lojix::quarantine::{OpenedStore as _, RowQuarantine as _};
use lojix::{DeploymentLedger as _, Store};
use tempfile::TempDir;

const FIXTURE: &[u8] = include_bytes!("fixtures/lojix-v5-horizon-0.12-deploy-job.sema");

#[test]
fn a_horizon_0_12_deploy_job_row_is_quarantined_under_the_horizon_0_13_layout() {
    let directory = TempDir::new().expect("tempdir");
    let path = directory.path().join("lojix.sema");
    std::fs::write(&path, FIXTURE).expect("copy fixture store");

    let before = StoreInspector { path: path.clone() }.inspect();
    assert_eq!(
        before.undecodable_row_count(),
        1,
        "the inspector must count the Horizon 0.12 row the open will set aside:\n{before}"
    );

    let store = Store::open(&path).expect("the store still opens across the Horizon bump");

    let opening = store.opening();
    assert_eq!(opening.quarantined_rows.len(), 1, "{opening:?}");
    let row = &opening.quarantined_rows[0];
    assert_eq!(row.table, "deploy-job");
    assert_eq!(row.key, "1");
    assert!(!row.archive.is_empty());
    assert!(!row.decode_error.is_empty());
    assert!(!opening.configuration_rebuilt, "{opening:?}");
    assert_eq!(
        store.quarantined_rows().expect("persisted quarantine"),
        opening.quarantined_rows
    );
    assert!(
        store.deploy_jobs().expect("deploy jobs read").is_empty(),
        "a Horizon 0.12 deploy-job row is never served"
    );
    drop(store);

    let reopened = Store::open(&path).expect("reopen after quarantine");
    assert!(
        reopened.opening().quarantined_rows.is_empty(),
        "a second open sets nothing aside"
    );
}
