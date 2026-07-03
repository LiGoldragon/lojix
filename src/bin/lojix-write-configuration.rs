//! lojix-write-configuration — the bootstrap/deploy tool that encodes a typed
//! NOTA configuration request into the binary rkyv startup file the daemon
//! consumes. The daemon takes only a pre-generated rkyv argument and never
//! parses NOTA, so this tool is the NOTA-to-binary boundary at deploy time.
//! Mirrors `spirit-write-configuration`. Per the daemon-binary-startup rule
//! and Spirit `ur16` (bootstrap-config and runtime-config share one
//! vocabulary; the meta `Configure` op carries the same shape at runtime).

use std::path::PathBuf;

use lojix::{DaemonConfiguration, TestDefaults, TestMode};
use nota::{NotaDecode, NotaDecodeError, NotaSource};
use thiserror::Error;
use triad_runtime::{ArgumentError, ComponentArgument, ComponentCommand};

fn main() {
    if let Err(error) = ConfigurationWriterCli::from_environment().run() {
        eprintln!("lojix-write-configuration: {error}");
        std::process::exit(1);
    }
}

struct ConfigurationWriterCli {
    command: ComponentCommand,
}

#[derive(Debug, Clone, PartialEq, Eq, NotaDecode)]
struct ConfigurationWriteRequest {
    ordinary_socket_path: WriterPath,
    ordinary_socket_mode: WriterMode,
    owner_socket_path: WriterPath,
    owner_socket_mode: WriterMode,
    state_directory_path: WriterPath,
    daemon_host: WriterCluster,
    test_defaults: WriterTestDefaults,
    output_path: WriterPath,
}

/// The test-op defaults authored as typed NOTA at deploy time and encoded into
/// the daemon's binary startup — the NOTA-to-binary boundary for report 54's
/// `TestDefaults` (never a flag, never a runtime `Configure`).
#[derive(Debug, Clone, PartialEq, Eq, NotaDecode)]
struct WriterTestDefaults {
    cluster: WriterCluster,
    default_vm_host: WriterCluster,
    default_mode: WriterTestMode,
    test_flake: WriterCluster,
    proposal_source: WriterPath,
}

#[derive(Debug, Clone, PartialEq, Eq, NotaDecode)]
struct WriterCluster(String);

#[derive(Debug, Clone, PartialEq, Eq, NotaDecode)]
enum WriterTestMode {
    Hermetic,
    Live,
}

#[derive(Debug, Clone, PartialEq, Eq, NotaDecode)]
enum ConfigurationWriterInput {
    ConfigurationWriteRequest(ConfigurationWriteRequest),
}

#[derive(Debug, Clone, PartialEq, Eq, NotaDecode)]
struct WriterPath(String);

#[derive(Debug, Clone, PartialEq, Eq, NotaDecode)]
struct WriterMode(u32);

impl ConfigurationWriterCli {
    fn from_environment() -> Self {
        Self {
            command: ComponentCommand::from_environment(),
        }
    }

    fn run(&self) -> Result<(), ConfigurationWriterError> {
        let text = self.source()?;
        let request = NotaSource::new(&text)
            .parse::<ConfigurationWriterInput>()?
            .into_request();
        let output_path = request.write()?;
        println!("(ConfigurationWritten [{}])", output_path.display());
        Ok(())
    }

    fn source(&self) -> Result<String, ConfigurationWriterError> {
        match self.command.nota_argument()? {
            ComponentArgument::InlineNota(argument) => Ok(argument.into_string()),
            ComponentArgument::NotaFile(file) => {
                let path = file.into_path();
                std::fs::read_to_string(&path)
                    .map_err(|source| ConfigurationWriterError::ReadNotaFile { path, source })
            }
            ComponentArgument::SignalFile(file) => {
                Err(ConfigurationWriterError::UnsupportedSignalFile {
                    path: file.into_path(),
                })
            }
        }
    }
}

impl ConfigurationWriterInput {
    fn into_request(self) -> ConfigurationWriteRequest {
        match self {
            Self::ConfigurationWriteRequest(request) => request,
        }
    }
}

impl ConfigurationWriteRequest {
    fn write(self) -> Result<PathBuf, ConfigurationWriterError> {
        let output_path = self.output_path.path_buf();
        let configuration = DaemonConfiguration {
            ordinary_socket_path: self.ordinary_socket_path.0,
            ordinary_socket_mode: self.ordinary_socket_mode.0,
            owner_socket_path: self.owner_socket_path.0,
            owner_socket_mode: self.owner_socket_mode.0,
            state_directory_path: self.state_directory_path.0,
            daemon_host: self.daemon_host.0,
            test_defaults: TestDefaults {
                cluster: self.test_defaults.cluster.0,
                default_vm_host: self.test_defaults.default_vm_host.0,
                default_mode: self.test_defaults.default_mode.into(),
                test_flake: self.test_defaults.test_flake.0,
                proposal_source: self.test_defaults.proposal_source.0,
            },
        };
        configuration
            .write_rkyv_file(&output_path)
            .map_err(ConfigurationWriterError::WriteConfiguration)?;
        Ok(output_path)
    }
}

impl WriterPath {
    fn path_buf(&self) -> PathBuf {
        PathBuf::from(&self.0)
    }
}

impl From<WriterTestMode> for TestMode {
    fn from(mode: WriterTestMode) -> Self {
        match mode {
            WriterTestMode::Hermetic => Self::Hermetic,
            WriterTestMode::Live => Self::Live,
        }
    }
}

#[derive(Debug, Error)]
enum ConfigurationWriterError {
    #[error(transparent)]
    Argument(#[from] ArgumentError),
    #[error("read NOTA file {}: {source}", path.display())]
    ReadNotaFile {
        path: PathBuf,
        source: std::io::Error,
    },
    #[error("signal-encoded configuration requests are not supported: {}", path.display())]
    UnsupportedSignalFile { path: PathBuf },
    #[error(transparent)]
    Decode(#[from] NotaDecodeError),
    #[error("write configuration archive: {0}")]
    WriteConfiguration(lojix::Error),
}
