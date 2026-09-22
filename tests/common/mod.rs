#![allow(dead_code)]

use std::path::Path;

use datomic::TextEdge as _;
use horizon_protos::Text;
use signal_lojix::{ClusterProposalWire, horizon_wire_types::*};

pub fn read_horizon(path: &Path) -> ClusterProposalWire {
    let authored = std::fs::read_to_string(path).expect("read Horizon proposal fixture");
    let proposal = Text::<horizon_lib::ClusterProposal>::from(authored.as_str())
        .embody()
        .expect("actualize authored Horizon proposal fixture");
    ClusterProposalWire::try_from(proposal).expect("convert authored proposal to typed Signal")
}

fn machine(species: MachineSpeciesWire, super_node: Option<&str>) -> MachineWire {
    MachineWire {
        species,
        arch: Some(ArchWire::X86_64),
        cores: 4,
        model: None,
        mother_board: None,
        super_node: super_node.map(|name| NodeNameWire(name.into())),
        super_user: None,
        chip_gen: None,
        ram_gb: None,
        disk_gb: None,
        location: None,
        super_nodes: vec![],
    }
}

fn node(name: &str, species: NodeSpeciesWire, machine: MachineWire) -> NodeProposalEntryWire {
    NodeProposalEntryWire {
        name: NodeNameWire(name.into()),
        proposal: NodeProposalWire {
            species,
            size: MagnitudeWire::Max,
            trust: MagnitudeWire::Max,
            machine,
            io: IoWire {
                keyboard: KeyboardWire::Qwerty,
                bootloader: BootloaderWire::Uefi,
                disks: vec![],
                swap_devices: vec![],
                compressed_swap: None,
            },
            pub_keys: NodePubKeysWire {
                ssh: SshPubKeyWire("AAA=".into()),
                nix: None,
                yggdrasil: None,
            },
            link_local_ips: vec![],
            node_ip: None,
            wireguard_pub_key: None,
            nordvpn: false,
            wifi_cert: false,
            wireguard_untrusted_proxies: vec![],
            wants_printing: false,
            wants_hw_video_accel: false,
            router_interfaces: None,
            online: Some(true),
            services: vec![],
        },
    }
}

fn proposal(nodes: Vec<NodeProposalEntryWire>) -> ClusterProposalWire {
    ClusterProposalWire {
        nodes,
        users: vec![],
        domains: vec![],
        trust: ClusterTrustWire {
            cluster: MagnitudeWire::Max,
            clusters: vec![],
            nodes: vec![],
            users: vec![],
        },
        domain_configuration: DomainConfigurationWire {
            internal_suffix: InternalDomainSuffixWire("criome".into()),
            public_cluster_domains: vec![],
        },
    }
}

fn write(path: &Path, value: ClusterProposalWire) {
    let authored = horizon_lib::ClusterProposal::try_from(value)
        .expect("convert typed fixture to authored Horizon proposal");
    std::fs::write(path, authored.textualize().as_ref())
        .expect("write authored Horizon proposal fixture");
}

pub fn write_single_node(path: &Path) {
    write(
        path,
        proposal(vec![node(
            "node-1",
            NodeSpeciesWire::Center,
            machine(MachineSpeciesWire::Metal, None),
        )]),
    );
}

pub fn write_hosted_pair(path: &Path) {
    let mut atlas = node(
        "atlas",
        NodeSpeciesWire::Center,
        machine(MachineSpeciesWire::Metal, None),
    );
    atlas.proposal.services.push(NodeServiceWire::VmHost {
        guest_subnet: TapSubnetWire("169.254.100.0/22".into()),
        kvm: KvmAvailabilityWire::Available,
        maximum_guests: Some(4),
    });
    let beacon = node(
        "beacon",
        NodeSpeciesWire::TestVm,
        machine(MachineSpeciesWire::Pod, Some("atlas")),
    );
    write(path, proposal(vec![atlas, beacon]));
}
