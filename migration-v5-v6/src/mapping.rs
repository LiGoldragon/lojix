//! The only semantic archive transformation: remove the wired WAN selector.
use datom_codec::{Actualizing, Budget, Datomizable, Potential};
use protos::{Protos, Protosizable, ReaderBudget, Textualizable};
use rkyv::rancor::Error;

pub type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

macro_rules! same {
    ($value:expr) => {{
        let bytes = rkyv::to_bytes::<Error>(&$value)?;
        rkyv::from_bytes::<_, Error>(&bytes)?
    }};
}

fn children(value: &mut Protos) -> Result<&mut Vec<Protos>> {
    match value {
        Protos::Enclosed { children, .. } => Ok(children),
        _ => Err("expected typed positional enclosure".into()),
    }
}

pub fn horizon(old: old_horizon::HorizonDefinition) -> Result<new_horizon::HorizonDefinition> {
    let mut tree = old.cluster_definition.datomize(vec![]).protosize();
    for node in children(&mut children(&mut tree)?[1])? {
        let network = children(&mut children(node)?[6])?;
        if let Protos::Headed { head, body, .. } = &mut network[4] {
            if head.0 != "Some" {
                return Err("invalid old router option".into());
            }
            let radio = children(body)?;
            if radio.len() != 8 {
                return Err("invalid old RouterInterfaces arity".into());
            }
            radio.remove(0);
        }
    }
    let mut budget = Budget {
        remaining: 1_000_000,
        reader: ReaderBudget {
            remaining: 1_000_000,
        },
        depth: 0,
        maximum_depth: 1024,
    };
    Ok(new_horizon::HorizonDefinition {
        horizon_configuration: same!(old.horizon_configuration),
        cluster_definition: Potential::from(tree.textualize())
            .actualize(&mut budget)
            .map_err(|e| format!("invalid mapped Horizon: {e:?}"))?,
    })
}

pub fn configuration(
    old: old_signal::LojixNexusConfiguration,
) -> Result<new_signal::LojixNexusConfiguration> {
    Ok(new_signal::LojixNexusConfiguration {
        ordinary_socket_path: old.ordinary_socket_path,
        ordinary_socket_mode: old.ordinary_socket_mode,
        owner_socket_path: old.owner_socket_path,
        owner_socket_mode: old.owner_socket_mode,
        state_directory_path: old.state_directory_path,
        daemon_host: old.daemon_host,
        test_defaults_choice: match old.test_defaults_choice {
            old_signal::TestDefaultsChoice::NoTestDefaults => {
                new_signal::TestDefaultsChoice::NoTestDefaults
            }
            old_signal::TestDefaultsChoice::TestDefaults(d) => {
                new_signal::TestDefaultsChoice::TestDefaults(new_signal::TestDefaults {
                    cluster_name: same!(d.cluster_name),
                    node_name: same!(d.node_name),
                    test_mode: same!(d.test_mode),
                    flake_reference: same!(d.flake_reference),
                    nix_system: same!(d.nix_system),
                    deployment_output_selector: same!(d.deployment_output_selector),
                    horizon_definition_option: d
                        .horizon_definition_option
                        .map(horizon)
                        .transpose()?,
                })
            }
        },
    })
}

fn submission(
    old: old::runtime_model::DeploySubmission,
) -> Result<new::runtime_model::DeploySubmission> {
    use new::runtime_model as n;
    use old::runtime_model as o;
    Ok(match old {
        o::DeploySubmission::Host(d) => n::DeploySubmission::Host(n::HostDeployment {
            cluster_name: same!(d.cluster_name),
            node_name: same!(d.node_name),
            host_composition: same!(d.host_composition),
            proposal_source: same!(d.proposal_source),
            secrets_input: same!(d.secrets_input),
            flake_reference: same!(d.flake_reference),
            deployment_transport: same!(d.deployment_transport),
            deployment_input_mode: same!(d.deployment_input_mode),
            horizon_definition_option: d.horizon_definition_option.map(horizon).transpose()?,
            deployment_output_selector: same!(d.deployment_output_selector),
            activation_backend: same!(d.activation_backend),
            host_deploy_action: same!(d.host_deploy_action),
            source_revision_policy: same!(d.source_revision_policy),
            optional_nix_builder_spec: same!(d.optional_nix_builder_spec),
            extra_substituter_vector: same!(d.extra_substituter_vector),
        }),
        o::DeploySubmission::UserEnvironment(d) => {
            n::DeploySubmission::UserEnvironment(n::UserEnvironmentDeployment {
                cluster_name: same!(d.cluster_name),
                node_name: same!(d.node_name),
                user_name: same!(d.user_name),
                proposal_source: same!(d.proposal_source),
                secrets_input: same!(d.secrets_input),
                flake_reference: same!(d.flake_reference),
                deployment_transport: same!(d.deployment_transport),
                deployment_input_mode: same!(d.deployment_input_mode),
                horizon_definition_option: d.horizon_definition_option.map(horizon).transpose()?,
                deployment_output_selector: same!(d.deployment_output_selector),
                activation_backend: same!(d.activation_backend),
                user_environment_action: same!(d.user_environment_action),
                source_revision_policy: same!(d.source_revision_policy),
                optional_nix_builder_spec: same!(d.optional_nix_builder_spec),
                extra_substituter_vector: same!(d.extra_substituter_vector),
            })
        }
    })
}

pub fn job(old: old::runtime_model::DeployJob) -> Result<new::runtime_model::DeployJob> {
    use old::runtime_model::DeployJobPhase;
    if !matches!(
        old.deploy_job_phase,
        DeployJobPhase::Activated | DeployJobPhase::Failed
    ) {
        return Err("live deploy job requires completion before migration".into());
    }
    Ok(new::runtime_model::DeployJob {
        deployment_identifier: same!(old.deployment_identifier),
        generation_identifier: same!(old.generation_identifier),
        cluster_name: same!(old.cluster_name),
        node_name: same!(old.node_name),
        deploy_job_phase: same!(old.deploy_job_phase),
        optional_closure_path: same!(old.optional_closure_path),
        source_revision_policy: same!(old.source_revision_policy),
        flake_reference: same!(old.flake_reference),
        optional_flake_reference: same!(old.optional_flake_reference),
        resolved_revision: old.resolved_revision,
        deployment_transport: same!(old.deployment_transport),
        deployment_input_mode: same!(old.deployment_input_mode),
        deployment_output_selector: same!(old.deployment_output_selector),
        activation_backend: same!(old.activation_backend),
        optional_nix_builder_spec: same!(old.optional_nix_builder_spec),
        boot_once_unit: old.boot_once_unit,
        optional_generation_slot: same!(old.optional_generation_slot),
        persisted_flake_input_override_vector: same!(old.persisted_flake_input_override_vector),
        deploy_resume_stage: same!(old.deploy_resume_stage),
        optional_phase_receipt: same!(old.optional_phase_receipt),
        optional_deploy_submission: old.optional_deploy_submission.map(submission).transpose()?,
    })
}
