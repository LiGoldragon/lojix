use horizon_lib::DatomDecoding;
use lojix::InlineDatomArguments as _;
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
    /// The decode budget this client actualizes its one inline Datom argument
    /// under. It belongs to the client, which is the thing that has a budget.
    fn budget() -> Budget
    where
        Self: Sized;
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
    fn budget() -> Budget {
        Budget {
            remaining: 16_384,
            reader: ReaderBudget { remaining: 16_384 },
            depth: 0,
            maximum_depth: 16_384,
        }
    }
    fn run_from_environment() -> lojix::Result<meta_signal_lojix::Response> {
        Self::from_arguments(std::env::args_os().skip(1))?.run()
    }
    fn from_arguments(arguments: impl IntoIterator<Item = OsString>) -> lojix::Result<Self> {
        let source = (arguments).single_inline_datom()?;
        let input = Potential::<meta_signal_lojix::ClientQuery>::from(source)
            .actualize(&mut <Self as Invocable>::budget())
            .map_err(|fault| lojix::Error::DatomRequestText(format!("{fault:?}")))?;
        Ok(Self {
            input: input.actualized()?,
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

/// A `ClientQuery` carries the *reference* to a Horizon proposal; the Nexus is
/// sent the actualized definition. Raising one to a wire `Query` is the client's
/// own verb, so it is homed on the query it raises.
trait ClientQueryActualizing {
    fn actualized(self) -> lojix::Result<meta_signal_lojix::Query>;
}

impl ClientQueryActualizing for meta_signal_lojix::ClientQuery {
    fn actualized(self) -> lojix::Result<meta_signal_lojix::Query> {
        use meta_signal_lojix::{ClientQuery, Query};
        Ok(match self {
            ClientQuery::Configure(value) => Query::Configure(value),
            ClientQuery::ReverseConfiguration => Query::ReverseConfiguration,
            ClientQuery::Retire(value) => Query::Retire(value),
            ClientQuery::Pin(value) => Query::Pin(value),
            ClientQuery::Test(value) => Query::Test(value),
            ClientQuery::Unpin(value) => Query::Unpin(value),
            ClientQuery::Deploy(value) => {
                let horizon_definition_option = match &value {
                    meta_signal_lojix::DeploySubmission::Host(deployment) => HorizonProposal {
                        mode: &deployment.deployment_input_mode,
                        source: &deployment.proposal_source,
                    }
                    .definition()?,
                    meta_signal_lojix::DeploySubmission::UserEnvironment(deployment) => {
                        HorizonProposal {
                            mode: &deployment.deployment_input_mode,
                            source: &deployment.proposal_source,
                        }
                        .definition()?
                    }
                };
                Query::Deploy(meta_signal_lojix::ActualizedDeploySubmission {
                    deploy_submission: value,
                    horizon_definition_option,
                })
            }
        })
    }
}

/// A deployment's claim about where its Horizon definition comes from: the
/// input mode paired with the source it names. The pair is the noun — neither
/// half decides on its own whether a definition is to be read, or from where.
struct HorizonProposal<'submission> {
    mode: &'submission signal_lojix::DeploymentInputMode,
    source: &'submission signal_lojix::ProposalSource,
}

/// Reading a proposal's definition off disk, and the path check that must
/// precede the read.
trait HorizonProposing {
    /// `None` for a direct deployment, which names no Horizon artifact.
    fn definition(&self) -> lojix::Result<Option<horizon_lib::HorizonDefinition>>;

    /// The artifact path, accepted only as an absolute, traversal-free,
    /// symlink-free regular file named `horizon-definition.datom`.
    fn checked_path(&self) -> lojix::Result<PathBuf>;
}

impl HorizonProposing for HorizonProposal<'_> {
    fn definition(&self) -> lojix::Result<Option<horizon_lib::HorizonDefinition>> {
        match self.mode {
            signal_lojix::DeploymentInputMode::Direct => Ok(None),
            signal_lojix::DeploymentInputMode::Horizon => {
                let authored = std::fs::read_to_string(self.checked_path()?)?;
                let definition =
                    horizon_lib::HorizonDefinition::decode(&authored).map_err(|_| {
                        lojix::Error::DatomRequestText(
                            "proposal source is not a Horizon definition".into(),
                        )
                    })?;
                Ok(Some(definition))
            }
        }
    }

    fn checked_path(&self) -> lojix::Result<PathBuf> {
        const ARTIFACT: &str = "horizon-definition.datom";
        let source: &str = self.source;
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
