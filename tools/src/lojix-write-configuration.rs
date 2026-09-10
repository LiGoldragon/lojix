//! `lojix-write-configuration` encodes one generated current Datom request into the daemon's rkyv startup archive.
use datom_codec::{Actualizing, Potential};
use lojix::ingress;
use lojix::{Error as LojixError, LegacyStartupConfiguration, TestDefaults, TestMode};
use std::path::{Path, PathBuf};
use thiserror::Error;
fn main() {
    if let Err(error) = ConfigurationWriterCli::from_environment().run() {
        eprintln!("lojix-write-configuration: {error}");
        std::process::exit(1);
    }
}
struct ConfigurationWriterCli;
impl ConfigurationWriterCli {
    fn from_environment() -> Self {
        Self
    }
    fn run(&self) -> Result<(), ConfigurationWriterError> {
        let request = Potential::<ingress::ConfigurationWriterInput>::from(self.source()?)
            .actualize(&mut <lojix::Ingress as lojix::Budgeted>::budget())
            .map_err(|fault| ConfigurationWriterError::Decode(format!("{fault:?}")))?;
        let ingress::ConfigurationWriterInput::ConfigurationWriteRequest(request) = request;
        let output_path = write_configuration(request)?;
        println!("ConfigurationWritten.{{ {} }}", output_path.display());
        Ok(())
    }
    fn source(&self) -> Result<String, ConfigurationWriterError> {
        lojix::single_inline_datom_argument(std::env::args_os().skip(1))
            .map_err(ConfigurationWriterError::Request)
    }
}
fn text(value: String) -> String {
    value
}
fn mode(value: i64) -> Result<u32, ConfigurationWriterError> {
    u32::try_from(value).map_err(|_| ConfigurationWriterError::InvalidMode(value))
}
fn write_configuration(
    ingress::ConfigurationWriteRequest {
        first_writer_path: ordinary_socket_path,
        first_writer_mode: ordinary_socket_mode,
        second_writer_path: owner_socket_path,
        second_writer_mode: owner_socket_mode,
        third_writer_path: state_directory_path,
        fourth_writer_path: store_path,
        writer_cluster: daemon_host,
        writer_test_defaults_choice: test_defaults,
        fifth_writer_path: output_path,
    }: ingress::ConfigurationWriteRequest,
) -> Result<PathBuf, ConfigurationWriterError> {
    let output_path = PathBuf::from(text(output_path));
    let configuration = LegacyStartupConfiguration {
        ordinary_socket_path: text(ordinary_socket_path),
        ordinary_socket_mode: mode(ordinary_socket_mode)?,
        owner_socket_path: text(owner_socket_path),
        owner_socket_mode: mode(owner_socket_mode)?,
        state_directory_path: text(state_directory_path),
        store_path: text(store_path),
        daemon_host: text(daemon_host),
        test_defaults: match test_defaults {
            ingress::WriterTestDefaultsChoice::NoTestDefaults => None,
            ingress::WriterTestDefaultsChoice::TestDefaults(ingress::WriterTestDefaults {
                first_writer_cluster: cluster,
                second_writer_cluster: default_vm_host,
                writer_test_mode: default_mode,
                third_writer_cluster: test_flake,
                fourth_writer_cluster: test_nix_system,
                fifth_writer_cluster: test_output_selector,
                writer_path: proposal_source,
            }) => Some(TestDefaults {
                cluster: text(cluster),
                default_vm_host: text(default_vm_host),
                default_mode: match default_mode {
                    ingress::WriterTestMode::Hermetic => TestMode::Hermetic,
                    ingress::WriterTestMode::Live => TestMode::Live,
                },
                test_flake: text(test_flake),
                test_nix_system: text(test_nix_system),
                test_output_selector: text(test_output_selector),
                horizon_definition: actualize_horizon_definition(&proposal_source)?,
            }),
        },
    };
    configuration
        .write_rkyv_file(&output_path)
        .map_err(ConfigurationWriterError::WriteConfiguration)?;
    Ok(output_path)
}

fn actualize_horizon_definition(
    source: &str,
) -> Result<Option<horizon_lib::HorizonDefinition>, ConfigurationWriterError> {
    if source.is_empty() {
        return Ok(None);
    }
    const ARTIFACT: &str = "horizon-definition.datom";
    let path = PathBuf::from(source);
    if source.chars().any(char::is_control)
        || !path.is_absolute()
        || path.file_name().and_then(|name| name.to_str()) != Some(ARTIFACT)
        || path.components().any(|component| {
            !matches!(
                component,
                std::path::Component::RootDir | std::path::Component::Normal(_)
            )
        })
    {
        return Err(ConfigurationWriterError::Horizon(
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
            return Err(ConfigurationWriterError::Horizon(
                "proposal source traverses a symbolic link".into(),
            ));
        }
    }
    if !std::fs::symlink_metadata(&path)?.file_type().is_file() {
        return Err(ConfigurationWriterError::Horizon(
            "proposal source is not a regular file".into(),
        ));
    }
    let authored = std::fs::read_to_string(path)?;
    horizon_lib::decode(&authored).map(Some).map_err(|_| {
        ConfigurationWriterError::Horizon("proposal source is not a Horizon definition".into())
    })
}
#[derive(Debug, Error)]
enum ConfigurationWriterError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("configuration request must be one inline Datom object: {0}")]
    Request(LojixError),
    #[error("configuration request Datom decode failed: {0}")]
    Decode(String),
    #[error("socket mode {0} is outside unsigned 32-bit range")]
    InvalidMode(i64),
    #[error("write configuration archive: {0}")]
    WriteConfiguration(lojix::Error),
    #[error("Horizon input error: {0}")]
    Horizon(String),
}
