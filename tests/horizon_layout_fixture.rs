//! A v5 store whose one in-flight deploy-job row carries a `HorizonDefinition`
//! archived in the Horizon 0.12 layout.
//!
//! `fixtures/lojix-v5-horizon-0.12-deploy-job.memory` was written once by this
//! crate at horizon-rs `ee8d6f8` (the Horizon 0.12 layout Lojix 7.0.0 carries):
//! a Horizon-mode `Deploy.Host` of `node-1` in cluster `alpha` was admitted
//! through `SchemaRuntime::submit_deploy` and the store was closed with the
//! row still in flight under key `1` — the shape the live Nexus holds while it
//! deploys its own host. The file is not regenerated: after the Horizon bump,
//! regenerating it would write the new layout and prove nothing.
//!
//! This crate carries a later Horizon layout, and a store is upgraded, never
//! accommodated: no record of this Lojix reads the Horizon 0.12 row, so the
//! open is refused, naming the row's table and key, and the file is left as
//! it was. The inspector counts `deploy-job 1` as undecodable.

use lojix::inspection::{InspectedTables as _, StoreInspecting as _, StoreInspector};
use lojix::{DurableStore as _, Error, Store};
use tempfile::TempDir;

const FIXTURE: &[u8] = include_bytes!("fixtures/lojix-v5-horizon-0.12-deploy-job.memory");

#[test]
fn a_horizon_0_12_deploy_job_row_refuses_the_store_and_leaves_it_untouched() {
    let directory = TempDir::new().expect("tempdir");
    let path = directory.path().join("lojix.memory");
    std::fs::write(&path, FIXTURE).expect("copy fixture store");

    let before = StoreInspector { path: path.clone() }.inspect();
    assert_eq!(
        before.undecodable_row_count(),
        1,
        "the inspector counts the Horizon 0.12 row the open refuses:\n{before}"
    );

    let error = Store::open(&path).expect_err("a row no layout reads refuses the store");
    match &error {
        Error::UndecodableRow { table, key, .. } => {
            assert_eq!(
                (table.as_str(), key.as_str()),
                ("deploy-job", "1"),
                "{error}"
            )
        }
        other => panic!("expected the deploy-job row to refuse the store, got {other}"),
    }
    assert_eq!(
        std::fs::read(&path).expect("read the refused store"),
        FIXTURE,
        "a refused open leaves the store byte for byte"
    );
}
