//! up9b client->daemon disconnect-survival tests: a meta `Deploy` records the
//! submission, replies the `DeployHandle` handle immediately, and runs its
//! pipeline on the daemon-owned deploy-job actor — decoupled from the request
//! task that submitted it. These are unit/integration tests with no node: the
//! pipeline parks on a test `EffectBarrier` (no `nix` shells out), so ordering
//! is deterministic. The live SSH-drop continuation is proven at S5.

use std::sync::Arc;
use std::time::Duration;

use lojix::Store;
use lojix::daemon::{AdmitDeploy, DeployAdmission, DeployJobs, ReconcilePersistedJobs};
use lojix::schema::sema::{self, DeployJob, DeployJobPhase};
use lojix::schema_runtime::{
    DeployJobResumption, DeploySubmissionOutcome, EffectBarrier, RuntimeConfiguration,
    SchemaRuntime,
};
use meta_signal_lojix::schema::lib as meta;
use signal_lojix::schema::lib as ordinary;
use tempfile::TempDir;

fn store() -> (TempDir, Arc<Store>) {
    let directory = TempDir::new().expect("tempdir");
    let store = Store::open(directory.path().join("lojix.sema")).expect("open store");
    (directory, Arc::new(store))
}

/// A host deploy whose proposal source does not exist, so the pipeline fails
/// fast at the first effect once the barrier opens (no node, no real flake).
fn deploy_request() -> meta::DeployRequest {
    meta::DeployRequest::Host(meta::HostDeployment {
        cluster_name: ordinary::ClusterName::new("alpha"),
        node_name: ordinary::NodeName::new("node-1"),
        host_composition: ordinary::HostComposition::BaseHost,
        source: ordinary::ProposalSource::new("/dev/null"),
        flake: ordinary::FlakeReference::new("path:/does/not/exist"),
        host_deploy_action: ordinary::HostDeployAction::ActivateNow,
        source_revision_policy: meta::SourceRevisionPolicy::ResolveAndRecord,
        builder: None,
        substituters: Vec::new(),
        build_attribute: None,
    })
}

fn job_row(store: &Store, deployment_identifier: u64) -> Option<DeployJob> {
    store
        .deploy_jobs()
        .expect("read deploy jobs")
        .into_iter()
        .find(|job| *job.deployment_identifier.payload() == deployment_identifier)
}

/// Poll the durable store until `predicate` holds or the bound elapses. Used to
/// observe that the detached pipeline made progress AFTER the admit reply
/// already returned — never a fixed sleep.
async fn await_until(store: &Store, mut predicate: impl FnMut(&Store) -> bool) -> bool {
    for _ in 0..200 {
        if predicate(store) {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    predicate(store)
}

// ---- (1) the submit step replies the accepted handle synchronously, before
// the pipeline runs at all ----

#[tokio::test]
async fn submit_deploy_accepts_and_persists_submitted_row_before_pipeline() {
    let (_directory, store) = store();
    let mut engine = SchemaRuntime::with_store_and_configuration(
        store.clone(),
        Arc::new(RuntimeConfiguration::test_default()),
    );

    let outcome = engine.submit_deploy(deploy_request());

    let accepted = match outcome {
        DeploySubmissionOutcome::Accepted(accepted) => accepted,
        DeploySubmissionOutcome::Rejected(rejected) => {
            panic!("expected acceptance, got {rejected:?}")
        }
    };
    // The handle carries a real deployment identifier the client re-observes by.
    assert_eq!(*accepted.deployment_identifier.payload(), 1);
    // The in-flight cursor is left set for the job actor to drive.
    assert_eq!(
        engine
            .active_deployment_identifier()
            .map(|identifier| *identifier.payload()),
        Some(1)
    );
    // The durable job row is written at Submitted — synchronously, before any
    // effect ran (the pipeline driver was never invoked here).
    let job = job_row(&store, 1).expect("submitted job row");
    assert_eq!(job.phase, DeployJobPhase::Submitted);
    assert_eq!(
        job.resolved_target.as_deref(),
        Some("root@node-1.alpha.criome")
    );
}

// ---- (2) the accepted handle is replied while the pipeline is still parked,
// and the pipeline completes on the daemon-owned executor after the admit
// caller's future is gone (reply-before-completion + drop-doesn't-cancel) ----

#[tokio::test]
async fn deploy_replies_accepted_before_pipeline_completes_and_survives_caller_drop() {
    let (_directory, store) = store();
    let barrier = EffectBarrier::held();
    let configuration = Arc::new(RuntimeConfiguration::test_with_effect_barrier(
        barrier.clone(),
    ));
    let jobs = DeployJobs::start(store.clone(), configuration, 8).await;

    // Submit on a SEPARATE task — the stand-in for the client's connection /
    // request task — then JOIN it, so the submitting task fully returns the
    // accepted handle and is DROPPED while the pipeline is still parked on the
    // held barrier. This mechanically removes the request task before the deploy
    // can complete, rather than merely asserting it is gone.
    let submit_task = tokio::spawn({
        let jobs = jobs.clone();
        async move {
            jobs.ask(AdmitDeploy {
                request: deploy_request(),
            })
            .await
            .expect("admit reply")
        }
    });
    let admission = submit_task
        .await
        .expect("the submitting task returns the accepted handle and ends");
    // `submit_task`'s JoinHandle was consumed by `.await`: the request task is
    // now joined and gone.
    let accepted = match admission {
        DeployAdmission::Accepted(accepted) => accepted,
        DeployAdmission::Rejected(rejected) => panic!("expected acceptance, got {rejected:?}"),
    };
    assert_eq!(*accepted.deployment_identifier.payload(), 1);

    // The accepted handle was replied while the pipeline is still parked: the row
    // is Submitted, no phase advanced — reply-before-completion.
    let parked = job_row(&store, 1).expect("submitted row");
    assert_eq!(
        parked.phase,
        DeployJobPhase::Submitted,
        "pipeline must still be parked when the accepted handle is already replied"
    );

    // The submitting task is now gone. ONLY NOW open the barrier — so the deploy
    // can only reach its terminal phase AFTER the task that submitted it has
    // returned and been dropped.
    barrier.open();

    // The pipeline runs to its terminal phase (this bogus-source deploy fails at
    // flake-auth -> Failed) on the daemon-owned executor — proving the deploy
    // outlived the request task that submitted it.
    let reached_terminal = await_until(&store, |store| {
        job_row(store, 1).is_none_or(|job| matches!(job.phase, DeployJobPhase::Failed))
    })
    .await;
    assert!(
        reached_terminal,
        "the detached pipeline must reach a terminal phase after its submitting task was joined and dropped"
    );
}

// ---- (3) the deploy-job cap rejects with DeploymentInFlight when full, and a
// slot frees on completion ----

#[tokio::test]
async fn deploy_job_cap_rejects_when_full_and_frees_on_completion() {
    let (_directory, store) = store();
    let barrier = EffectBarrier::held();
    let configuration = Arc::new(RuntimeConfiguration::test_with_effect_barrier(
        barrier.clone(),
    ));
    // Cap of one: a single parked deploy fills the executor.
    let jobs = DeployJobs::start(store.clone(), configuration, 1).await;

    let first = jobs
        .ask(AdmitDeploy {
            request: deploy_request(),
        })
        .await
        .expect("first admit");
    assert!(
        matches!(first, DeployAdmission::Accepted(_)),
        "first deploy fits the cap"
    );

    // The second deploy is over the cap (the first is parked, holding the slot)
    // and is refused with the typed DeploymentInFlight reason.
    let second = jobs
        .ask(AdmitDeploy {
            request: deploy_request(),
        })
        .await
        .expect("second admit");
    match second {
        DeployAdmission::Rejected(rejected) => assert_eq!(
            rejected.deploy_rejection_reason,
            meta::DeployRejectionReason::DeploymentInFlight
        ),
        DeployAdmission::Accepted(accepted) => {
            panic!("the cap must reject the second deploy, got {accepted:?}")
        }
    }

    // Release the first deploy; its pipeline completes and frees the slot.
    barrier.open();
    let freed = await_until(&store, |store| {
        job_row(store, 1).is_none_or(|job| matches!(job.phase, DeployJobPhase::Failed))
    })
    .await;
    assert!(
        freed,
        "the first deploy must complete and free its cap slot"
    );

    // The barrier is already open, so the retried deploy is admitted and runs;
    // a fresh Accepted proves the cap slot reopened on the first's completion.
    let next = await_until_admitted(&jobs).await;
    assert!(
        matches!(next, DeployAdmission::Accepted(_)),
        "a slot frees on completion: a new deploy is admitted, got {next:?}"
    );
}

/// Retry admission until the cap slot frees (the completion message is
/// processed asynchronously after the pipeline task ends).
async fn await_until_admitted(jobs: &kameo::actor::ActorRef<DeployJobs>) -> DeployAdmission {
    for _ in 0..200 {
        let admission = jobs
            .ask(AdmitDeploy {
                request: deploy_request(),
            })
            .await
            .expect("retry admit");
        if matches!(admission, DeployAdmission::Accepted(_)) {
            return admission;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("the cap slot never freed after completion");
}

// ---- (4) the durable job row's phase cursor drives the restart-resume
// reconcile decision (read-on-start scaffolding) ----

#[test]
fn deploy_job_resumption_decides_per_phase() {
    let make = |phase: DeployJobPhase, boot_once_unit: Option<&str>| sema::DeployJob {
        deployment_identifier: ordinary::DeploymentIdentifier::new(1),
        generation_identifier: ordinary::GenerationIdentifier::new(1),
        cluster_name: ordinary::ClusterName::new("alpha"),
        node_name: ordinary::NodeName::new("node-1"),
        phase,
        closure_path: None,
        source_revision_policy: ordinary::SourceRevisionPolicy::ResolveAndRecord,
        requested_ref: ordinary::FlakeReference::new("github:owner/repo/main"),
        resolved_ref: Some(ordinary::FlakeReference::new(
            "github:owner/repo?rev=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        )),
        resolved_revision: Some("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_string()),
        resolved_target: Some("root@node-1.alpha.criome".to_string()),
        boot_once_unit: boot_once_unit.map(str::to_string),
    };

    // Activating polls the persisted BootOnce unit rather than re-activating.
    assert_eq!(
        make(DeployJobPhase::Activating, Some("lojix-boot-once-deploy-1")).resumption(),
        DeployJobResumption::PollActivationUnit {
            unit: Some("lojix-boot-once-deploy-1".to_string())
        }
    );
    // Pre-activation phases re-drive the pipeline (copy is idempotent).
    for phase in [
        DeployJobPhase::Submitted,
        DeployJobPhase::Building,
        DeployJobPhase::Built,
        DeployJobPhase::Copying,
    ] {
        assert_eq!(
            make(phase, None).resumption(),
            DeployJobResumption::RestartPipeline
        );
    }
    // Terminal phases have nothing to resume.
    for phase in [DeployJobPhase::Activated, DeployJobPhase::Failed] {
        assert_eq!(
            make(phase, None).resumption(),
            DeployJobResumption::AlreadyTerminal
        );
    }
}

// ---- (5) the daemon reads persisted in-flight rows on start (the read-on-start
// half of durable resume): a pre-activation row left by a crashed daemon is
// reconciled away so it does not wedge the cap ----

#[tokio::test]
async fn reconcile_on_start_clears_a_stale_pre_activation_job_row() {
    let (_directory, store) = store();
    // Simulate a daemon that crashed mid-build: a Building job row persisted.
    store
        .upsert_deploy_job(sema::DeployJob {
            deployment_identifier: ordinary::DeploymentIdentifier::new(7),
            generation_identifier: ordinary::GenerationIdentifier::new(7),
            cluster_name: ordinary::ClusterName::new("alpha"),
            node_name: ordinary::NodeName::new("node-1"),
            phase: DeployJobPhase::Building,
            closure_path: None,
            source_revision_policy: ordinary::SourceRevisionPolicy::ResolveAndRecord,
            requested_ref: ordinary::FlakeReference::new("github:owner/repo/main"),
            resolved_ref: Some(ordinary::FlakeReference::new(
                "github:owner/repo?rev=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            )),
            resolved_revision: Some("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_string()),
            resolved_target: Some("root@node-1.alpha.criome".to_string()),
            boot_once_unit: None,
        })
        .expect("seed crashed-daemon job row");
    assert!(
        job_row(&store, 7).is_some(),
        "the stale row is present pre-start"
    );

    let configuration = Arc::new(RuntimeConfiguration::test_default());
    let jobs = DeployJobs::start(store.clone(), configuration, 8).await;
    jobs.ask(ReconcilePersistedJobs).await.expect("reconcile");

    assert!(
        job_row(&store, 7).is_none(),
        "the read-on-start reconcile clears the stale pre-activation row"
    );
}
