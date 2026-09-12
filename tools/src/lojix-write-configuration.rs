//! `lojix-write-configuration` encodes one generated current Datom request into the daemon's rkyv startup archive.
use datom_codec::{Actualizing, Potential};
use horizon_lib::DatomDecoding;
use lojix::InlineDatomArguments as _;
use lojix::ingress;
use lojix::{
    Error as LojixError, LegacyConfigurationArchivable as _, LegacyStartupConfiguration,
    TestDefaults, TestMode,
};
use std::path::{Path, PathBuf};
use thiserror::Error;
fn main() {
    if let Err(error) = ConfigurationWriterCli::from_environment().run() {
        eprintln!("lojix-write-configuration: {error}");
        std::process::exit(1);
    }
}
struct ConfigurationWriterCli;
trait ConfigurationWritable {
    fn from_environment() -> Self
    where
        Self: Sized;
    fn run(&self) -> Result<(), ConfigurationWriterError>;
    fn source(&self) -> Result<String, ConfigurationWriterError>;
}
impl ConfigurationWritable for ConfigurationWriterCli {
    fn from_environment() -> Self {
        Self
    }
    fn run(&self) -> Result<(), ConfigurationWriterError> {
        let request = Potential::<ingress::ConfigurationWriterInput>::from(self.source()?)
            .actualize(&mut <lojix::Ingress as lojix::Budgeted>::budget())
            .map_err(|fault| ConfigurationWriterError::Decode(format!("{fault:?}")))?;
        let ingress::ConfigurationWriterInput::ConfigurationWriteRequest(request) = request;
        let output_path = request.write()?;
        println!("ConfigurationWritten.{{ {} }}", output_path.display());
        Ok(())
    }
    fn source(&self) -> Result<String, ConfigurationWriterError> {
        (std::env::args_os().skip(1))
            .single_inline_datom()
            .map_err(ConfigurationWriterError::Request)
    }
}
/// A Unix socket permission mode as the configuration archive holds it. The
/// request carries it as a signed Datom integer, so narrowing is fallible and
/// the narrowing is the type's own.
struct SocketMode(u32);

impl TryFrom<i64> for SocketMode {
    type Error = ConfigurationWriterError;

    fn try_from(value: i64) -> Result<Self, Self::Error> {
        u32::try_from(value)
            .map(Self)
            .map_err(|_| ConfigurationWriterError::InvalidMode(value))
    }
}

/// Turning one decoded write request into the archive it asks for.
trait ConfigurationWriting {
    /// Write the startup archive and answer with the path written.
    fn write(self) -> Result<PathBuf, ConfigurationWriterError>;
}

impl ConfigurationWriting for ingress::ConfigurationWriteRequest {
    fn write(self) -> Result<PathBuf, ConfigurationWriterError> {
        let ingress::ConfigurationWriteRequest {
            first_writer_path: ordinary_socket_path,
            first_writer_mode: ordinary_socket_mode,
            second_writer_path: owner_socket_path,
            second_writer_mode: owner_socket_mode,
            third_writer_path: state_directory_path,
            fourth_writer_path: store_path,
            writer_cluster: daemon_host,
            writer_test_defaults_choice: test_defaults,
            fifth_writer_path: output_path,
        } = self;
        let output_path = PathBuf::from(output_path);
        let configuration = LegacyStartupConfiguration {
            ordinary_socket_path,
            ordinary_socket_mode: SocketMode::try_from(ordinary_socket_mode)?.0,
            owner_socket_path,
            owner_socket_mode: SocketMode::try_from(owner_socket_mode)?.0,
            state_directory_path,
            store_path,
            daemon_host,
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
                    cluster,
                    default_vm_host,
                    default_mode: match default_mode {
                        ingress::WriterTestMode::Hermetic => TestMode::Hermetic,
                        ingress::WriterTestMode::Live => TestMode::Live,
                    },
                    test_flake,
                    test_nix_system,
                    test_output_selector,
                    horizon_definition: HorizonArtifact(&proposal_source).definition()?,
                }),
            },
        };
        configuration
            .write_rkyv_file(&output_path)
            .map_err(ConfigurationWriterError::WriteConfiguration)?;
        Ok(output_path)
    }
}

/// The text a write request offers as the path of a Horizon definition file.
/// Empty means the request names none.
struct HorizonArtifact<'request>(&'request str);

/// Reading a named Horizon artifact, and the path check that must precede it.
trait HorizonArtifactReading {
    /// `None` when no artifact is named.
    fn definition(
        &self,
    ) -> Result<Option<horizon_lib::HorizonDefinition>, ConfigurationWriterError>;

    /// The artifact path, accepted only as an absolute, traversal-free,
    /// symlink-free regular file named `horizon-definition.datom`.
    fn checked_path(&self) -> Result<PathBuf, ConfigurationWriterError>;
}

impl HorizonArtifactReading for HorizonArtifact<'_> {
    fn definition(
        &self,
    ) -> Result<Option<horizon_lib::HorizonDefinition>, ConfigurationWriterError> {
        if self.0.is_empty() {
            return Ok(None);
        }
        let authored = std::fs::read_to_string(self.checked_path()?)?;
        horizon_lib::HorizonDefinition::decode(&authored)
            .map(Some)
            .map_err(|_| {
                ConfigurationWriterError::Horizon(
                    "proposal source is not a Horizon definition".into(),
                )
            })
    }

    fn checked_path(&self) -> Result<PathBuf, ConfigurationWriterError> {
        const ARTIFACT: &str = "horizon-definition.datom";
        let path = PathBuf::from(self.0);
        if self.0.chars().any(char::is_control)
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
        Ok(path)
    }
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
