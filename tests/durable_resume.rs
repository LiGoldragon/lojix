//! Self-resume + restart-safe identifiers: the proof that lojix daemon state
//! survives a process restart now that the `Store` is sema-engine-backed
//! (Spirit oh9l durable-first, ur16 self-resume).
//!
//! The test opens a `Store` at a temp `*.sema` path, records an activation (a
//! live generation plus its gc-root), reads the commit sequence and the next
//! generation identifier, DROPS the store (simulating the daemon stopping),
//! REOPENS at the same path, and asserts the live generation, the commit
//! sequence, and the next generation identifier ALL survived. Before durable
//! backing the identifier counters reset to zero on restart — this asserts they
//! resume from the persisted rows instead.

use lojix::Store;
use lojix::schema::sema::{GcRoot, LiveGeneration};
use signal_lojix::schema::lib as ordinary;
use tempfile::TempDir;

/// A live generation and its matching gc-root for one generation identifier.
fn activation(generation_identifier: u64) -> (LiveGeneration, GcRoot) {
    let generation = ordinary::GenerationIdentifier::new(generation_identifier);
    let cluster = ordinary::ClusterName::new("goldragon");
    let node = ordinary::NodeName::new("dune");
    let closure = ordinary::ClosurePath::new("/nix/store/resume-closure");
    (
        LiveGeneration {
            deployment_identifier: ordinary::DeploymentIdentifier::new(generation_identifier),
            generation_identifier: generation.clone(),
            cluster_name: cluster.clone(),
            node_name: node.clone(),
            deployment_kind: ordinary::DeploymentKind::OsOnly,
            activation_kind: ordinary::ActivationKind::Switch,
            generation_slot: ordinary::GenerationSlot::Current,
            closure_path: closure.clone(),
        },
        GcRoot {
            generation_identifier: generation,
            cluster_name: cluster,
            node_name: node,
            generation_slot: ordinary::GenerationSlot::Current,
            closure_path: closure,
            label: None.into(),
        },
    )
}

#[test]
fn store_self_resumes_after_reopen() {
    let directory = TempDir::new().expect("tempdir");
    let path = directory.path().join("lojix.sema");

    // First boot: a virgin store starts empty with genesis counters.
    let (committed_sequence, expected_next_generation) = {
        let store = Store::open(&path).expect("open virgin store");
        assert_eq!(
            store.next_generation_identifier().expect("next generation"),
            1,
            "a virgin live-set issues generation identifier 1"
        );

        let (generation, root) = activation(1);
        store
            .record_activation(generation, root)
            .expect("record activation");

        let committed_sequence = store.commit_sequence().expect("commit sequence");
        assert!(
            committed_sequence > 0,
            "the activation write advanced the durable commit sequence"
        );
        let next_generation = store
            .next_generation_identifier()
            .expect("next generation after activation");
        assert_eq!(
            next_generation, 2,
            "the next generation identifier is one past the persisted maximum"
        );
        (committed_sequence, next_generation)
        // The store drops here — the daemon process stopping.
    };

    // Second boot: reopen the SAME path. `Engine::open` is the resume.
    let store = Store::open(&path).expect("reopen store");

    let live_generations = store
        .matching_live_generations(|_| true)
        .expect("read live set after reopen");
    assert_eq!(
        live_generations.len(),
        1,
        "the live generation survived the restart"
    );
    assert_eq!(
        *live_generations[0].generation_identifier.payload(),
        1,
        "the surviving live generation kept its identifier"
    );

    assert_eq!(
        store
            .commit_sequence()
            .expect("commit sequence after reopen"),
        committed_sequence,
        "the persisted commit sequence resumed at its pre-restart value"
    );
    assert_eq!(
        store
            .next_generation_identifier()
            .expect("next generation after reopen"),
        expected_next_generation,
        "restart-safe identifier issuance resumed from the persisted rows, not zero"
    );
}
