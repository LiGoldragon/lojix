//! `lojix-write-configuration` encodes one generated current Datom request into the daemon's rkyv startup archive.
use datom_codec::{Actualizable, IncorporationBudget, Potential};
use lojix::{DaemonConfiguration, Error as LojixError, TestDefaults, TestMode};
use std::path::PathBuf;
use thiserror::Error;
use lojix::ingress;
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
            .actualize(IncorporationBudget::try_from(16_384).expect("static ingress budget"))
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
fn text(value: protos::Text) -> String {
    value.to_string()
}
fn mode(value: protos::Integer) -> Result<u32, ConfigurationWriterError> {
    u32::try_from(value).map_err(|_| ConfigurationWriterError::InvalidMode(value))
}
fn write_configuration(
    ingress::ConfigurationWriteRequest(
        ordinary_socket_path,
        ordinary_socket_mode,
        owner_socket_path,
        owner_socket_mode,
        state_directory_path,
        store_path,
        daemon_host,
        test_defaults,
        output_path,
    ): ingress::ConfigurationWriteRequest,
) -> Result<PathBuf, ConfigurationWriterError> {
    let output_path = PathBuf::from(text(output_path));
    let configuration = DaemonConfiguration {
        ordinary_socket_path: text(ordinary_socket_path),
        ordinary_socket_mode: mode(ordinary_socket_mode)?,
        owner_socket_path: text(owner_socket_path),
        owner_socket_mode: mode(owner_socket_mode)?,
        state_directory_path: text(state_directory_path),
        store_path: text(store_path),
        daemon_host: text(daemon_host),
        test_defaults: match test_defaults {
            ingress::WriterTestDefaultsChoice::NoTestDefaults => None,
            ingress::WriterTestDefaultsChoice::TestDefaults(ingress::WriterTestDefaults(
                cluster,
                default_vm_host,
                default_mode,
                test_flake,
                test_nix_system,
                test_output_selector,
                proposal_source,
            )) => Some(TestDefaults {
                cluster: text(cluster),
                default_vm_host: text(default_vm_host),
                default_mode: match default_mode {
                    ingress::WriterTestMode::Hermetic => TestMode::Hermetic,
                    ingress::WriterTestMode::Live => TestMode::Live,
                },
                test_flake: text(test_flake),
                test_nix_system: text(test_nix_system),
                test_output_selector: text(test_output_selector),
                proposal_source: text(proposal_source),
            }),
        },
    };
    configuration
        .write_rkyv_file(&output_path)
        .map_err(ConfigurationWriterError::WriteConfiguration)?;
    Ok(output_path)
}
#[derive(Debug, Error)]
enum ConfigurationWriterError {
    #[error("configuration request must be one inline Datom object: {0}")]
    Request(LojixError),
    #[error("configuration request Datom decode failed: {0}")]
    Decode(String),
    #[error("socket mode {0} is outside unsigned 32-bit range")]
    InvalidMode(i64),
    #[error("write configuration archive: {0}")]
    WriteConfiguration(lojix::Error),
}
