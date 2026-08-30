//! Correlation-engine routing witnesses. These stay on the local SEMA side of
//! the Dotos adapter boundary so they can prove durable state without a shell
//! effect or a legacy protocol fixture.

use std::collections::BTreeMap;
use std::fs;
use std::path::Path;

use datomic::Datomic;
use horizon_lib::address::{YggAddress, YggSubnet};
use horizon_lib::domain::DomainConfiguration;
use horizon_lib::io::Io;
use horizon_lib::machine::Machine;
use horizon_lib::magnitude::Magnitude;
use horizon_lib::name::NodeName as HorizonNodeName;
use horizon_lib::proposal::{ClusterProposal, ClusterTrust, NodeProposal, NodePubKeys};
use horizon_lib::pub_key::{NixPubKey, SshPubKey, YggPubKey};
use horizon_lib::species::{Arch, Bootloader, Keyboard, MachineSpecies, NodeSpecies};
use lojix::runtime_model as sema;
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
    fs::write(path, proposal.textualize().as_ref()).expect("write proposal");
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
fn accepted_submission_creates_a_correlated_durable_record() {
    let directory = tempfile::tempdir().expect("temporary proposal directory");
    let proposal_source = directory.path().join("proposal.datom");
    write_proposal(&proposal_source);
    let mut engine = SchemaRuntime::new();
    let accepted = match engine.submit_deploy(host_submission(&proposal_source)) {
        DeploySubmissionOutcome::Accepted(handle) => handle,
        other => panic!("expected accepted submission, got {other:?}"),
    };
    let identifier = *accepted.deployment_identifier.payload();
    assert_ne!(identifier, 0);
    let records = engine.store().deployment_records().expect("read records");
    let record = records
        .into_iter()
        .find(|record| *record.deployment_identifier.payload() == identifier)
        .expect("accepted submission has a durable record");
    assert!(record.optional_admission_marker.is_some());
    assert_eq!(
        record.deployment_lifecycle,
        sema::DeploymentLifecycle::Submitted
    );
    assert!(record.optional_deployment_terminal.is_none());
}

#[test]
fn capacity_rejection_is_a_correlated_terminal_record() {
    let directory = tempfile::tempdir().expect("temporary proposal directory");
    let proposal_source = directory.path().join("proposal.datom");
    write_proposal(&proposal_source);
    let engine = SchemaRuntime::new();
    let rejected = engine.reject_deployment_in_flight(host_submission(&proposal_source));
    let record = rejected.into_payload();
    assert_ne!(*record.deployment_identifier.payload(), 0);
    assert!(record.optional_admission_marker.is_none());
    assert_eq!(
        record.deployment_lifecycle,
        sema::DeploymentLifecycle::Rejected
    );
    assert!(matches!(
        record.optional_deployment_terminal,
        Some(sema::DeploymentTerminal::Rejected(
            sema::DeploymentTerminalReason::DeploymentInFlight
        ))
    ));
    assert!(record.optional_terminal_marker.is_some());
}
