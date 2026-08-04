//! lojix-write-configuration — the bootstrap/deploy tool that encodes a typed
//! DOTOS configuration request into the binary rkyv startup file the daemon
//! consumes. The daemon takes only a pre-generated rkyv argument and never
//! parses DOTOS, so this tool is the DOTOS-to-binary boundary at deploy time.
//! Mirrors `spirit-write-configuration`. Per the daemon-binary-startup rule
//! and Spirit `ur16` (bootstrap-config and runtime-config share one
//! vocabulary; the meta `Configure` op carries the same shape at runtime).

use std::path::PathBuf;

use dotos::{DotosDecode, DotosDecodeError, DotosSource};
use lojix::{DaemonConfiguration, TestDefaults, TestMode};
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

#[derive(Debug, Clone, PartialEq, Eq, DotosDecode)]
struct ConfigurationWriteRequest {
    ordinary_socket_path: WriterPath,
    ordinary_socket_mode: WriterMode,
    owner_socket_path: WriterPath,
    owner_socket_mode: WriterMode,
    state_directory_path: WriterPath,
    store_path: WriterPath,
    daemon_host: WriterCluster,
    effect_timeout_seconds: WriterSeconds,
    test_defaults: WriterTestDefaultsChoice,
    output_path: WriterPath,
}

/// Whether the daemon's binary startup bakes a test-op fixture at all. A
/// production node carries the bare `NoTestDefaults`: with no baked fixture the
/// daemon lowers to `None` and rejects a bare `(Check …)`/`(Run …)` with
/// `NoTestDefaults` instead of silently building a baked test cluster. Test
/// fixtures are supplied only by test code, never baked per-node into a
/// production module (the workspace deployment-independence discipline). A
/// test/dev daemon carries `(TestDefaults …)` with the explicit fixture.
#[derive(Debug, Clone, PartialEq, Eq, DotosDecode)]
enum WriterTestDefaultsChoice {
    NoTestDefaults,
    TestDefaults(WriterTestDefaults),
}

/// The test-op defaults authored as typed DOTOS at deploy time and encoded into
/// the daemon's binary startup — the DOTOS-to-binary boundary for report 54's
/// `TestDefaults` (never a flag, never a runtime `Configure`).
#[derive(Debug, Clone, PartialEq, Eq, DotosDecode)]
struct WriterTestDefaults {
    cluster: WriterCluster,
    default_vm_host: WriterCluster,
    default_mode: WriterTestMode,
    test_flake: WriterCluster,
    test_nix_system: WriterCluster,
    test_output_selector: WriterCluster,
    proposal_source: WriterPath,
}

#[derive(Debug, Clone, PartialEq, Eq, DotosDecode)]
struct WriterCluster(String);

#[derive(Debug, Clone, PartialEq, Eq, DotosDecode)]
enum WriterTestMode {
    Hermetic,
    Live,
}

#[derive(Debug, Clone, PartialEq, Eq, DotosDecode)]
enum ConfigurationWriterInput {
    ConfigurationWriteRequest(ConfigurationWriteRequest),
}

#[derive(Debug, Clone, PartialEq, Eq, DotosDecode)]
struct WriterPath(String);

#[derive(Debug, Clone, PartialEq, Eq, DotosDecode)]
struct WriterMode(u32);

#[derive(Debug, Clone, PartialEq, Eq, DotosDecode)]
struct WriterSeconds(u64);

impl ConfigurationWriterCli {
    fn from_environment() -> Self {
        Self {
            command: ComponentCommand::from_environment(),
        }
    }

    fn run(&self) -> Result<(), ConfigurationWriterError> {
        let text = self.source()?;
        let request = DotosSource::new(&text)
            .parse::<ConfigurationWriterInput>()?
            .into_request();
        let output_path = request.write()?;
        println!("(ConfigurationWritten [{}])", output_path.display());
        Ok(())
    }

    fn source(&self) -> Result<String, ConfigurationWriterError> {
        match self.command.dotos_argument()? {
            ComponentArgument::InlineDotos(argument) => Ok(argument.into_string()),
            ComponentArgument::DotosFile(file) => {
                let path = file.into_path();
                std::fs::read_to_string(&path)
                    .map_err(|source| ConfigurationWriterError::ReadDotosFile { path, source })
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
        if self.effect_timeout_seconds.0 == 0 {
            return Err(ConfigurationWriterError::ZeroEffectTimeout);
        }
        let output_path = self.output_path.path_buf();
        let configuration = DaemonConfiguration {
            ordinary_socket_path: self.ordinary_socket_path.0,
            ordinary_socket_mode: self.ordinary_socket_mode.0,
            owner_socket_path: self.owner_socket_path.0,
            owner_socket_mode: self.owner_socket_mode.0,
            state_directory_path: self.state_directory_path.0,
            store_path: self.store_path.0,
            daemon_host: self.daemon_host.0,
            effect_timeout_seconds: self.effect_timeout_seconds.0,
            test_defaults: self.test_defaults.into_config(),
        };
        configuration
            .write_rkyv_file(&output_path)
            .map_err(ConfigurationWriterError::WriteConfiguration)?;
        Ok(output_path)
    }
}

impl WriterTestDefaultsChoice {
    /// Lower the authored choice into the daemon's optional config default.
    /// `NoTestDefaults` becomes `None` — the production posture — so a bare
    /// `(Check …)`/`(Run …)` is rejected rather than resolved against a baked
    /// fixture.
    fn into_config(self) -> Option<TestDefaults> {
        match self {
            Self::NoTestDefaults => None,
            Self::TestDefaults(defaults) => Some(TestDefaults {
                cluster: defaults.cluster.0,
                default_vm_host: defaults.default_vm_host.0,
                default_mode: defaults.default_mode.into(),
                test_flake: defaults.test_flake.0,
                test_nix_system: defaults.test_nix_system.0,
                test_output_selector: defaults.test_output_selector.0,
                proposal_source: defaults.proposal_source.0,
            }),
        }
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
    #[error("read DOTOS file {}: {source}", path.display())]
    ReadDotosFile {
        path: PathBuf,
        source: std::io::Error,
    },
    #[error("signal-encoded configuration requests are not supported: {}", path.display())]
    UnsupportedSignalFile { path: PathBuf },
    #[error("effect timeout must be at least one second")]
    ZeroEffectTimeout,
    #[error(transparent)]
    Decode(#[from] DotosDecodeError),
    #[error("write configuration archive: {0}")]
    WriteConfiguration(lojix::Error),
}
