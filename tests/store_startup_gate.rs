use lojix::schema::sema::{GcRoot, LiveGeneration};
use lojix::{Error, Store};
use redb::{Database, TableDefinition};
use signal_lojix::schema::lib as ordinary;
use tempfile::TempDir;

const RAW_LIVE_SET: TableDefinition<String, &[u8]> = TableDefinition::new("live-set");

fn activation(generation_identifier: u64) -> (LiveGeneration, GcRoot) {
    let generation = ordinary::GenerationIdentifier::new(generation_identifier);
    let cluster = ordinary::ClusterName::new("goldragon");
    let node = ordinary::NodeName::new("dune");
    let closure = ordinary::ClosurePath::new("/nix/store/startup-gate-closure");
    (
        LiveGeneration {
            deployment_identifier: ordinary::DeploymentIdentifier::new(generation_identifier),
            generation_identifier: generation.clone(),
            cluster_name: cluster.clone(),
            node_name: node.clone(),
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

#[test]
fn current_store_reopens_through_startup_gate() {
    let directory = TempDir::new().expect("tempdir");
    let path = directory.path().join("lojix.sema");
    {
        let store = Store::open(&path).expect("open current store");
        let (generation, root) = activation(1);
        store
            .record_activation(generation, root)
            .expect("record activation");
    }

    let store = Store::open(&path).expect("reopen current store");
    let generations = store
        .matching_live_generations(|generation| *generation.generation_identifier.payload() == 1)
        .expect("query generation after startup gate");

    assert_eq!(generations.len(), 1);
}

#[test]
fn malformed_legacy_row_is_refused_before_query_handling() {
    let directory = TempDir::new().expect("tempdir");
    let path = directory.path().join("lojix.sema");
    {
        let _store = Store::open(&path).expect("create current store envelope");
    }
    let database = Database::open(&path).expect("open redb for disposable legacy row");
    let transaction = database.begin_write().expect("begin write");
    {
        let corrupt_bytes: &[u8] = b"not-current-lojix-row";
        let mut live_set = transaction.open_table(RAW_LIVE_SET).expect("live-set");
        live_set
            .insert("not-current-schema".to_string(), &corrupt_bytes)
            .expect("write malformed legacy row");
    }
    transaction.commit().expect("commit malformed row");
    drop(database);

    let error = Store::open(&path).expect_err("startup gate should reject malformed row");
    let error_text = error.to_string();

    match error {
        Error::StoreStartupCompatibility { stage, source, .. } => {
            assert_eq!(stage, "validating live-set rows");
            match source.as_ref() {
                sema_engine::Error::Sema(sema_engine::StorageKernelError::RkyvDecode {
                    ..
                })
                | sema_engine::Error::VersionedPayloadDecode { .. } => {}
                _ => panic!("unexpected startup gate source: {source}"),
            }
        }
        other => panic!("unexpected startup error: {other}"),
    }
    assert!(
        !error_text.contains("GenerationUnknown"),
        "startup gate must not defer incompatibility into a domain query miss: {error_text}"
    );
    assert!(
        error_text.contains("lojix-inspect-store"),
        "operator error should name the inspection tool: {error_text}"
    );
}
