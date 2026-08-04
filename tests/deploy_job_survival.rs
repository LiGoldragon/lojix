use std::collections::BTreeMap;
use std::fs;
use std::path::Path;
use std::sync::Arc;

use dotos::DotosEncode;
use horizon_lib::address::{YggAddress, YggSubnet};
use horizon_lib::domain::DomainConfiguration;
use horizon_lib::io::Io;
use horizon_lib::machine::Machine;
use horizon_lib::magnitude::Magnitude;
use horizon_lib::name::NodeName as HorizonNodeName;
use horizon_lib::proposal::{ClusterProposal, ClusterTrust, NodeProposal, NodePubKeys};
use horizon_lib::pub_key::{NixPubKey, SshPubKey, YggPubKey};
use horizon_lib::species::{Arch, Bootloader, Keyboard, MachineSpecies, NodeSpecies};
use lojix::Store;
use lojix::schema::sema;
use lojix::schema_runtime::{DeploySubmissionOutcome, SchemaRuntime};

fn write_proposal(path: &Path) {
    let node = NodeProposal {
        species: NodeSpecies::EdgeTesting,
        size: Magnitude::Large,
        trust: Magnitude::Max,
        machine: Machine {
            species: MachineSpecies::Metal,
            arch: Some(Arch::X86_64),
            cores: 4,
            model: None,
            mother_board: None,
            super_node: None,
            super_user: None,
            chip_gen: None,
            ram_gb: None,
            disk_gb: None,
            location: None,
            super_nodes: Vec::new(),
        },
        io: Io {
            keyboard: Keyboard::Qwerty,
            bootloader: Bootloader::Uefi,
            disks: BTreeMap::new(),
            swap_devices: Vec::new(),
            compressed_swap: None,
        },
        pub_keys: NodePubKeys {
            ssh: SshPubKey::try_new("AAA=").expect("ssh key"),
            nix: Some(
                NixPubKey::try_new("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA")
                    .expect("nix key"),
            ),
            yggdrasil: Some(horizon_lib::proposal::YggPubKeyEntry {
                pub_key: YggPubKey::try_new("a".repeat(64)).expect("ygg key"),
                address: YggAddress::try_new("200::1").expect("ygg address"),
                subnet: YggSubnet::try_new("300:ca41:6b12:fba").expect("ygg subnet"),
            }),
        },
        link_local_ips: Vec::new(),
        node_ip: None,
        wireguard_pub_key: None,
        nordvpn: false,
        wifi_cert: false,
        wireguard_untrusted_proxies: Vec::new(),
        wants_printing: false,
        wants_hw_video_accel: false,
        router_interfaces: None,
        online: None,
        services: Vec::new(),
    };
    let mut nodes = BTreeMap::new();
    nodes.insert(HorizonNodeName::try_new("node-1").expect("node name"), node);
    let proposal = ClusterProposal {
        nodes,
        users: BTreeMap::new(),
        domains: BTreeMap::new(),
        trust: ClusterTrust {
            cluster: Magnitude::Max,
            clusters: BTreeMap::new(),
            nodes: BTreeMap::new(),
            users: BTreeMap::new(),
        },
        domain_configuration: DomainConfiguration::default(),
    };
    fs::write(path, proposal.to_dotos()).expect("write proposal");
}

fn host_submission(proposal_source: &Path) -> sema::DeploySubmission {
    sema::DeploySubmission::Host(sema::HostDeployment {
        cluster_name: sema::ClusterName::new("alpha"),
        node_name: sema::NodeName::new("node-1"),
        host_composition: sema::HostComposition::BaseHost,
        proposal_source: sema::ProposalSource::new(proposal_source.display().to_string()),
        flake_reference: sema::FlakeReference::new("github:example/fixture"),
        deployment_transport: sema::DeploymentTransport {
            nix_store_uri: sema::NixStoreUri::new("ssh-ng://fixture-copy.invalid"),
            ssh_destination: sema::SshDestination::new("fixture-login@fixture-activate.invalid"),
        },
        deployment_input_mode: sema::DeploymentInputMode::Horizon,
        deployment_output_selector: sema::DeploymentOutputSelector::new(sema::FlakeAttribute::new(
            "checks.fixture-a",
        )),
        activation_backend: sema::ActivationBackend::NixosSystemdBootV1,
        host_deploy_action: sema::HostDeployAction::Realize,
        source_revision_policy: sema::SourceRevisionPolicy::ResolveAndRecord,
        optional_nix_builder_spec: None,
        extra_substituter_vector: Vec::new(),
    })
}

#[test]
fn accepted_deploy_job_survives_a_store_reopen_with_its_correlation_identity() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let path = directory.path().join("lojix.sema");
    let proposal_source = directory.path().join("cluster.dotos");
    write_proposal(&proposal_source);
    let store = Arc::new(Store::open(&path).expect("open store"));
    let mut runtime = SchemaRuntime::with_store(store.clone());
    let accepted = match runtime.submit_deploy(host_submission(&proposal_source)) {
        DeploySubmissionOutcome::Accepted(handle) => handle,
        other => panic!("expected admission, got {other:?}"),
    };
    let identifier = *accepted.deployment_identifier.payload();
    assert_eq!(store.deploy_jobs().expect("job row").len(), 1);
    drop(runtime);
    drop(store);

    let resumed = Store::open(&path).expect("reopen store");
    let job = resumed
        .deploy_jobs()
        .expect("read surviving job")
        .into_iter()
        .next()
        .expect("one surviving job");
    assert_eq!(*job.deployment_identifier.payload(), identifier);
    assert_eq!(job.deploy_job_phase, sema::DeployJobPhase::Submitted);
    assert!(
        resumed
            .deployment_records()
            .expect("read correlations")
            .iter()
            .any(|record| *record.deployment_identifier.payload() == identifier)
    );
}
