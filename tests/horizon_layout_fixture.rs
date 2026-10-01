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
//! This crate carries Horizon 0.14 and store schema 6. The old store is
//! refused intact; an in-flight job must not disappear through quarantine.

use lojix::DurableStore as _;
use lojix::Store;
use tempfile::TempDir;

const FIXTURE: &[u8] = include_bytes!("fixtures/lojix-v5-horizon-0.12-deploy-job.sema");

#[test]
fn old_horizon_deploy_job_fixture_is_refused_without_quarantine_or_loss() {
    let directory = TempDir::new().expect("tempdir");
    let path = directory.path().join("lojix.sema");
    std::fs::write(&path, FIXTURE).expect("copy immutable v5 fixture");
    let before = std::fs::read(&path).expect("original store");
    assert!(
        Store::open(&path).is_err(),
        "v6 must not reinterpret a v5 in-flight job"
    );
    assert_eq!(std::fs::read(&path).expect("preserved store"), before);
}
