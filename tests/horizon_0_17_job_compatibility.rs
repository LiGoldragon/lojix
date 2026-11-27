//! A frozen v5 store written by Lojix 0.10.9 with its deployed Horizon 0.17.2
//! dependency (`4d206ff`). The one in-flight deploy job embeds a complete
//! `HorizonDefinition` containing a payload-bearing `TailnetController`
//! capability. The fixture is immutable old-writer evidence for additive
//! Horizon capability releases.

use lojix::DurableStore as _;
use lojix::Payload as _;
use lojix::quarantine::{OpenedStore as _, RowQuarantine as _};
use lojix::{DeploymentLedger as _, Store};
use tempfile::TempDir;

const FIXTURE: &[u8] = include_bytes!("fixtures/lojix-v5-horizon-0.17-deploy-job.memory");

#[test]
fn a_horizon_0_17_deploy_job_remains_decodable() {
    let directory = TempDir::new().expect("tempdir");
    let path = directory.path().join("lojix.memory");
    std::fs::write(&path, FIXTURE).expect("copy frozen old-writer store");

    let store = Store::open(&path).expect("open old-writer store");
    assert!(
        store.opening().quarantined_rows.is_empty(),
        "{:?}",
        store.opening()
    );
    assert!(
        store
            .quarantined_rows()
            .expect("read quarantine")
            .is_empty()
    );
    let jobs = store.deploy_jobs().expect("decode old deploy job");
    assert_eq!(jobs.len(), 1);
    assert_eq!(*jobs[0].deployment_identifier.payload(), 1);
    drop(store);

    let reopened = Store::open(&path).expect("reopen old-writer store");
    assert_eq!(
        reopened.deploy_jobs().expect("decode after reopen").len(),
        1
    );
    assert!(reopened.opening().quarantined_rows.is_empty());
}
