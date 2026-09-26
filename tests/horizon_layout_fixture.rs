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
//! Under the Horizon 0.12 layout the row reads and nothing is set aside. The
//! commit that repins Horizon changes this test to the other side of the
//! bump: the same store opens, `deploy-job 1` is quarantined, and the store
//! still serves.

use lojix::DurableStore as _;
use lojix::quarantine::OpenedStore as _;
use lojix::runtime_model::{DeployJob, DeploySubmission};
use lojix::{DeploymentLedger as _, Store};
use tempfile::TempDir;

const FIXTURE: &[u8] = include_bytes!("fixtures/lojix-v5-horizon-0.12-deploy-job.sema");

#[test]
fn a_horizon_0_12_deploy_job_row_reads_under_the_horizon_0_12_layout() {
    let directory = TempDir::new().expect("tempdir");
    let path = directory.path().join("lojix.sema");
    std::fs::write(&path, FIXTURE).expect("copy fixture store");

    let store = Store::open(&path).expect("open the fixture store");

    assert!(
        store.opening().quarantined_rows.is_empty(),
        "{:?}",
        store.opening()
    );
    let jobs: Vec<DeployJob> = store.deploy_jobs().expect("deploy jobs read");
    assert_eq!(jobs.len(), 1);
    match jobs[0].optional_deploy_submission.as_ref() {
        Some(DeploySubmission::Host(host)) => {
            let definition = host
                .horizon_definition_option
                .as_ref()
                .expect("a Horizon-mode row carries its definition");
            assert_eq!(definition.cluster_definition.cluster_name, "alpha");
        }
        other => panic!("expected a Horizon-mode host submission, got {other:?}"),
    }
}
