use std::{
    ffi::OsString,
    path::{Path, PathBuf},
};

use datom_codec::{Actualizing, Budget, Potential};
use lojix::client::SocketExchange;
use protos::ReaderBudget;
use signal::{ByteViewable, Restorable, Signal, Signalizable};

const SOCKET_ENV: &str = "LOJIX_OWNER_SOCKET";

#[derive(Debug)]
pub struct Client {
    input: meta_signal_lojix::Query,
}

pub trait Invocable {
    fn run_from_environment() -> lojix::Result<meta_signal_lojix::Response>
    where
        Self: Sized;
    fn from_arguments(arguments: impl IntoIterator<Item = OsString>) -> lojix::Result<Self>
    where
        Self: Sized;
    fn input(&self) -> &meta_signal_lojix::Query;
    fn run(self) -> lojix::Result<meta_signal_lojix::Response>
    where
        Self: Sized;
}

impl Invocable for Client {
    fn run_from_environment() -> lojix::Result<meta_signal_lojix::Response> {
        Self::from_arguments(std::env::args_os().skip(1))?.run()
    }
    fn from_arguments(arguments: impl IntoIterator<Item = OsString>) -> lojix::Result<Self> {
        let source = lojix::single_inline_datom_argument(arguments)?;
        let input = Potential::<meta_signal_lojix::ClientQuery>::from(source)
            .actualize(&mut budget())
            .map_err(|fault| lojix::Error::DatomRequestText(format!("{fault:?}")))?;
        Ok(Self {
            input: actualize(input)?,
        })
    }
    fn input(&self) -> &meta_signal_lojix::Query {
        &self.input
    }
    fn run(self) -> lojix::Result<meta_signal_lojix::Response> {
        let request = self
            .input
            .signalize()
            .map_err(|fault| lojix::Error::Wire(format!("{fault:?}")))?;
        let reply =
            SocketExchange::for_environment(SOCKET_ENV)?.exchange(request.bytes().to_vec())?;
        Signal::<meta_signal_lojix::Response>::from(reply)
            .restore()
            .map_err(|fault| lojix::Error::Wire(format!("{fault:?}")))
    }
}

fn actualize(input: meta_signal_lojix::ClientQuery) -> lojix::Result<meta_signal_lojix::Query> {
    use meta_signal_lojix::{ClientQuery, Query};
    Ok(match input {
        ClientQuery::Configure(value) => Query::Configure(value),
        ClientQuery::ReverseConfiguration => Query::ReverseConfiguration,
        ClientQuery::Retire(value) => Query::Retire(value),
        ClientQuery::Pin(value) => Query::Pin(value),
        ClientQuery::Test(value) => Query::Test(value),
        ClientQuery::Unpin(value) => Query::Unpin(value),
        ClientQuery::Deploy(value) => {
            let horizon_definition_option = match &value {
                meta_signal_lojix::DeploySubmission::Host(deployment) => actualize_horizon(
                    &deployment.deployment_input_mode,
                    &deployment.proposal_source,
                )?,
                meta_signal_lojix::DeploySubmission::UserEnvironment(deployment) => {
                    actualize_horizon(
                        &deployment.deployment_input_mode,
                        &deployment.proposal_source,
                    )?
                }
            };
            Query::Deploy(meta_signal_lojix::ActualizedDeploySubmission {
                deploy_submission: value,
                horizon_definition_option,
            })
        }
    })
}

fn actualize_horizon(
    mode: &signal_lojix::DeploymentInputMode,
    source: &signal_lojix::ProposalSource,
) -> lojix::Result<Option<horizon_lib::HorizonDefinition>> {
    match mode {
        signal_lojix::DeploymentInputMode::Direct => Ok(None),
        signal_lojix::DeploymentInputMode::Horizon => {
            let path = checked_horizon_path(source)?;
            let authored = std::fs::read_to_string(path)?;
            let definition = horizon_lib::decode(&authored).map_err(|_| {
                lojix::Error::DatomRequestText("proposal source is not a Horizon definition".into())
            })?;
            Ok(Some(definition))
        }
    }
}

fn checked_horizon_path(source: &str) -> lojix::Result<PathBuf> {
    const ARTIFACT: &str = "horizon-definition.datom";
    if source.is_empty() || source.chars().any(char::is_control) {
        return Err(lojix::Error::DatomRequestText(
            "proposal source is not a safe canonical Horizon artifact".into(),
        ));
    }
    let path = PathBuf::from(source);
    if !path.is_absolute()
        || path.file_name().and_then(|name| name.to_str()) != Some(ARTIFACT)
        || path.components().any(|component| {
            !matches!(
                component,
                std::path::Component::RootDir | std::path::Component::Normal(_)
            )
        })
    {
        return Err(lojix::Error::DatomRequestText(
            "proposal source is not a safe canonical Horizon artifact".into(),
        ));
    }
    let mut prefix = PathBuf::from(Path::new("/"));
    for component in path.components() {
        let std::path::Component::Normal(part) = component else {
            continue;
        };
        prefix.push(part);
        if std::fs::symlink_metadata(&prefix)?.file_type().is_symlink() {
            return Err(lojix::Error::DatomRequestText(
                "proposal source traverses a symbolic link".into(),
            ));
        }
    }
    if !std::fs::symlink_metadata(&path)?.file_type().is_file() {
        return Err(lojix::Error::DatomRequestText(
            "proposal source is not a regular file".into(),
        ));
    }
    Ok(path)
}
fn budget() -> Budget {
    Budget {
        remaining: 16_384,
        reader: ReaderBudget { remaining: 16_384 },
        depth: 0,
        maximum_depth: 16_384,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datom_codec::Datomizable;
    use protos::{Protosizable, Textualizable};
    #[test]
    fn decodes_canonical_inline_query() {
        let input = meta_signal_lojix::ClientQuery::Unpin(meta_signal_lojix::UnpinRequest {
            cluster_name: "cluster".into(),
            node_name: "node".into(),
            pin_label: "keep".into(),
        });
        let text = input.datomize(vec![]).protosize().textualize();
        assert_eq!(
            Client::from_arguments([OsString::from(text)])
                .unwrap()
                .input(),
            &meta_signal_lojix::Query::Unpin(meta_signal_lojix::UnpinRequest {
                cluster_name: "cluster".into(),
                node_name: "node".into(),
                pin_label: "keep".into(),
            })
        );
    }
    #[test]
    fn rejects_non_single_inline_arguments() {
        assert!(Client::from_arguments(Vec::<OsString>::new()).is_err());
        assert!(Client::from_arguments([OsString::from("--pretty")]).is_err());
    }

    fn horizon_definition() -> horizon_lib::HorizonDefinition {
        horizon_lib::HorizonDefinition {
            horizon_configuration: horizon_lib::HorizonConfiguration {
                generic_nodes: vec![],
                domain_configuration: horizon_lib::DomainConfiguration {
                    string: "internal.invalid".into(),
                    domain_name_vector: vec![],
                },
            },
            cluster_definition: horizon_lib::ClusterDefinition {
                cluster_name: "fixture-cluster".into(),
                cluster_nodes: vec![],
                generic_node_names: vec![],
                users: vec![],
                domains: vec![],
                cluster_trust: horizon_lib::ClusterTrust {
                    magnitude: horizon_lib::Magnitude::Zero,
                    cluster_trust_entry_vector: vec![],
                    node_trust_entry_vector: vec![],
                    user_trust_entry_vector: vec![],
                },
            },
        }
    }

    #[test]
    fn actualizes_horizon_file_before_constructing_wire_query() {
        let directory = tempfile::tempdir().expect("temporary authored input");
        let path = directory.path().join("horizon-definition.datom");
        let definition = horizon_definition();
        std::fs::write(
            &path,
            definition.clone().datomize(vec![]).protosize().textualize(),
        )
        .expect("write authored Horizon fixture");
        let client_query = meta_signal_lojix::ClientQuery::Deploy(
            meta_signal_lojix::DeploySubmission::Host(meta_signal_lojix::HostDeployment {
                cluster_name: "fixture-cluster".into(),
                node_name: "fixture-node".into(),
                host_composition: signal_lojix::HostComposition::BaseHost,
                proposal_source: path.display().to_string(),
                secrets_input: signal_lojix::SecretsInput::SecretsDirectory(
                    "fixture-secret-reference".into(),
                ),
                flake_reference: "github:fixture/system".into(),
                deployment_transport: signal_lojix::DeploymentTransport {
                    nix_store_uri: "ssh-ng://builder.invalid".into(),
                    ssh_destination: "root@node.invalid".into(),
                },
                deployment_input_mode: signal_lojix::DeploymentInputMode::Horizon,
                deployment_output_selector: signal_lojix::DeploymentOutputSelector {
                    flake_attribute: "checks.fixture".into(),
                },
                activation_backend: signal_lojix::ActivationBackend::NixosSystemdBootV1,
                host_deploy_action: signal_lojix::HostDeployAction::Realize,
                source_revision_policy: signal_lojix::SourceRevisionPolicy::ResolveAndRecord,
                nix_builder_spec_option: None,
                extra_substituter_vector: vec![],
            }),
        );
        let rendered = client_query.datomize(vec![]).protosize().textualize();
        let client = Client::from_arguments([OsString::from(rendered)]).expect("actualize client");
        let meta_signal_lojix::Query::Deploy(actualized) = client.input() else {
            panic!("expected actualized deployment")
        };
        assert_eq!(
            actualized.horizon_definition_option.as_ref(),
            Some(&definition)
        );
        assert!(matches!(
            &actualized.deploy_submission,
            meta_signal_lojix::DeploySubmission::Host(host)
                if matches!(host.secrets_input, signal_lojix::SecretsInput::SecretsDirectory(_))
        ));
    }
}
