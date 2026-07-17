use lojix::schema::sema::{GcRoot, LiveGeneration};
use lojix::{Error, Store};
use redb::{Database, TableDefinition};
use sema_engine::{
    Engine, EngineOpen, FamilyName, SchemaHash, SchemaVersion, TableDescriptor, TableName,
    VersionedHistoryRetention, VersionedStoreName, VersioningPolicy,
};
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
            optional_user_name: None,
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
fn store_schema_version_mismatch_is_typed_at_the_open_boundary() {
    let directory = TempDir::new().expect("tempdir");
    let path = directory.path().join("lojix.sema");
    let _legacy = Engine::open(EngineOpen::new(path.clone(), SchemaVersion::new(1)))
        .expect("create disposable schema-one envelope");
    drop(_legacy);

    let error = Store::open(&path).expect_err("schema mismatch must stop startup");
    match error {
        Error::StoreStartupCompatibility { stage, source, .. } => {
            assert_eq!(stage, "opening sema-engine");
            assert!(
                source.to_string().contains("schema"),
                "diagnostic must attribute the store schema boundary: {source}"
            );
        }
        other => panic!("unexpected startup error: {other}"),
    }
}

#[test]
fn table_family_identity_mismatch_is_typed_before_serving() {
    let directory = TempDir::new().expect("tempdir");
    let path = directory.path().join("lojix.sema");
    let mut incompatible = Engine::open(
        EngineOpen::new(path.clone(), SchemaVersion::new(3)).with_versioning(
            VersioningPolicy::new(VersionedStoreName::new("lojix"))
                .with_retention(VersionedHistoryRetention::new(4_096)),
        ),
    )
    .expect("create disposable schema-three envelope");
    incompatible
        .register_table::<LiveGeneration>(TableDescriptor::new(
            TableName::new("live-set"),
            FamilyName::new("SpiritFamily"),
            SchemaHash::new([99; 32]),
        ))
        .expect("register deliberately incompatible table identity");
    drop(incompatible);

    let error = Store::open(&path).expect_err("table identity mismatch must stop startup");
    match error {
        Error::StoreStartupCompatibility { stage, source, .. } => {
            assert_eq!(stage, "registering live-set table");
            let diagnostic = source.to_string();
            assert!(
                diagnostic.contains("live-set") || diagnostic.contains("family"),
                "diagnostic must attribute table identity: {diagnostic}"
            );
        }
        other => panic!("unexpected startup error: {other}"),
    }
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
