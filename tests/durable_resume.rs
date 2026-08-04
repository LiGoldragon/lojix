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
use lojix::schema::sema::{self as ordinary, GcRoot, LiveGeneration};
use tempfile::TempDir;

/// A live generation and its matching gc-root for one generation identifier.
fn activation(generation_identifier: u64) -> (LiveGeneration, GcRoot) {
    let generation = ordinary::GenerationIdentifier::new(generation_identifier);
    let cluster = ordinary::ClusterName::new("fixture-cluster");
    let node = ordinary::NodeName::new("dune");
    let closure =
        ordinary::ClosurePath::new("/nix/store/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-resume-closure");
    (
        LiveGeneration {
            deployment_identifier: ordinary::DeploymentIdentifier::new(generation_identifier),
            generation_identifier: generation.clone(),
            cluster_name: cluster.clone(),
            node_name: node.clone(),
            deployment_environment: ordinary::DeploymentEnvironment::HostEnvironment,
            generation_artifact: ordinary::GenerationArtifact::BaseHost,
            activation_effect: ordinary::ActivationEffect::LiveActivation,
            generation_slot: ordinary::GenerationSlot::Current,
            closure_path: closure.clone(),
            source_revision_record: ordinary::SourceRevisionRecord {
                source_revision_policy: ordinary::SourceRevisionPolicy::ResolveAndRecord,
                requested_ref: ordinary::FlakeReference::new("github:owner/repo/main"),
                resolved_ref: ordinary::FlakeReference::new(
                    "github:owner/repo?rev=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                ),
                string: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_string(),
            },
        },
        GcRoot {
            generation_identifier: generation,
            cluster_name: cluster,
            node_name: node,
            generation_slot: ordinary::GenerationSlot::Current,
            closure_path: closure,
            optional_pin_label: None,
        },
    )
}

fn activation_in_environment(
    generation_identifier: u64,
    environment: ordinary::DeploymentEnvironment,
) -> (LiveGeneration, GcRoot) {
    let (mut generation, root) = activation(generation_identifier);
    generation.deployment_environment = environment;
    (generation, root)
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

#[test]
fn current_generation_is_unique_per_host_or_user_environment_across_reopen() {
    let directory = TempDir::new().expect("tempdir");
    let path = directory.path().join("lojix.sema");
    {
        let store = Store::open(&path).expect("open store");
        let (first_host, first_host_root) =
            activation_in_environment(1, ordinary::DeploymentEnvironment::HostEnvironment);
        store
            .record_activation(first_host, first_host_root)
            .expect("record first host current");
        let (alice, alice_root) = activation_in_environment(
            2,
            ordinary::DeploymentEnvironment::UserEnvironment(ordinary::UserName::new("alice")),
        );
        store
            .record_activation(alice, alice_root)
            .expect("record alice current");
        let (bob, bob_root) = activation_in_environment(
            3,
            ordinary::DeploymentEnvironment::UserEnvironment(ordinary::UserName::new("bob")),
        );
        store
            .record_activation(bob, bob_root)
            .expect("record bob current");
        let (host_replacement, host_replacement_root) =
            activation_in_environment(4, ordinary::DeploymentEnvironment::HostEnvironment);
        store
            .record_activation(host_replacement, host_replacement_root)
            .expect("replace only host current");
    }

    let reopened = Store::open(&path).expect("reopen store");
    let generations = reopened
        .matching_live_generations(|_| true)
        .expect("read generations");
    let slot = |identifier| {
        generations
            .iter()
            .find(|generation| *generation.generation_identifier.payload() == identifier)
            .expect("generation")
            .generation_slot
    };
    assert_eq!(slot(1), ordinary::GenerationSlot::Recent);
    assert_eq!(slot(2), ordinary::GenerationSlot::Current);
    assert_eq!(slot(3), ordinary::GenerationSlot::Current);
    assert_eq!(slot(4), ordinary::GenerationSlot::Current);
}
