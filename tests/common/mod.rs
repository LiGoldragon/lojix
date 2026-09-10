#![allow(dead_code)]

use std::path::Path;

use datom_codec::Datomizable;
use horizon_lib::*;
use protos::{Protosizable, Textualizable};

pub fn read_horizon(path: &std::path::Path) -> horizon_lib::HorizonDefinition {
    horizon_lib::decode(&std::fs::read_to_string(path).expect("read Horizon fixture"))
        .expect("actualize Horizon fixture")
}

fn hardware() -> Hardware {
    Hardware {
        integer: 4,
        model_name_option: None,
        mother_board_option: None,
        first_integer_option: None,
        second_integer_option: None,
        location_option: None,
    }
}

fn node(name: &str, machine_definition: MachineDefinition) -> NodeDefinition {
    NodeDefinition {
        node_name: name.to_owned(),
        node_variant: NodeVariant::Live(LiveDefinition {}),
        first_magnitude: Magnitude::Max,
        second_magnitude: Magnitude::Max,
        machine_definition,
        node_environment: NodeEnvironment {
            keyboard: Keyboard::Qwerty,
            compressed_swap_option: None,
        },
        node_network: NodeNetwork {
            link_local_ip_vector: vec![],
            node_ip_option: None,
            wireguard_pub_key_option: None,
            wireguard_proxy_vector: vec![],
            router_interfaces_option: None,
        },
        node_keys: NodeKeys {
            ssh_pub_key: "ssh-ed25519 AAAAfixture".to_owned(),
            nix_pub_key_option: None,
            yggdrasil_key_option: None,
        },
        boolean_option: Some(true),
        capabilities: vec![],
    }
}

fn definition(nodes: Vec<NodeDefinition>) -> HorizonDefinition {
    HorizonDefinition {
        horizon_configuration: HorizonConfiguration {
            generic_nodes: vec![],
            domain_configuration: DomainConfiguration {
                string: "criome".to_owned(),
                domain_name_vector: vec![],
            },
        },
        cluster_definition: ClusterDefinition {
            cluster_name: "alpha".to_owned(),
            cluster_nodes: nodes,
            generic_node_names: vec![],
            users: vec![],
            domains: vec![],
            cluster_trust: ClusterTrust {
                magnitude: Magnitude::Max,
                cluster_trust_entry_vector: vec![],
                node_trust_entry_vector: vec![],
                user_trust_entry_vector: vec![],
            },
        },
    }
}

fn write(path: &Path, value: HorizonDefinition) {
    std::fs::write(path, value.datomize(vec![]).protosize().textualize())
        .expect("write HorizonDefinition");
}

pub fn write_single_node(path: &Path) {
    write(
        path,
        definition(vec![node(
            "node-1",
            MachineDefinition::Metal(Metal_Data {
                architecture: Architecture::X86_64,
                hardware: hardware(),
            }),
        )]),
    );
}

pub fn write_hosted_pair(path: &Path) {
    let atlas = node(
        "atlas",
        MachineDefinition::Metal(Metal_Data {
            architecture: Architecture::X86_64,
            hardware: hardware(),
        }),
    );
    let beacon = node(
        "beacon",
        MachineDefinition::VirtualMachine(VirtualMachine_Data {
            virtual_machine_host: VirtualMachineHost::Cluster(Cluster_Data {
                node_name: "atlas".to_owned(),
                node_name_vector: vec![],
                user_name_option: Some("operator".to_owned()),
                architecture_option: None,
            }),
            hardware: hardware(),
            integer_option: Some(20),
        }),
    );
    write(path, definition(vec![atlas, beacon]));
}
