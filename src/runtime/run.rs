//! RunDaemon — owns daemon startup. Reads a single NOTA argument,
//! parses it into the schema-emitted `DaemonConfiguration`, spawns
//! the Engine, binds the socket, and runs the accept loop forever.

use std::path::PathBuf;

use kameo::actor::{ActorRef, Spawn};

use crate::error::{Error, Result};
use crate::generated::DaemonConfiguration;
use crate::runtime::authorization::AuthorizationPolicy;
use crate::runtime::engine::Engine;
use crate::runtime::socket::{ServeForever, SocketListener};
use crate::runtime::toolchain::ToolchainMode;

/// Daemon runner. State carries the parsed configuration plus the
/// spawned engine handle.
pub struct RunDaemon {
    configuration: DaemonConfiguration,
    engine: Option<Engine>,
    listener: Option<ActorRef<SocketListener>>,
}

impl RunDaemon {
    /// Build a runner from a single NOTA argument (inline text or path).
    pub async fn from_single_argument(argument: &str) -> Result<Self> {
        let configuration = DaemonConfiguration::parse_argument(argument)?;
        Ok(Self {
            configuration,
            engine: None,
            listener: None,
        })
    }

    pub fn configuration(&self) -> &DaemonConfiguration {
        &self.configuration
    }

    pub async fn start(&mut self) -> Result<()> {
        let database_path = PathBuf::from(self.configuration.sema_database_path.0.clone());
        let engine = Engine::spawn(
            database_path,
            self.configuration.toolchain.clone(),
            ToolchainMode::Sandbox,
            AuthorizationPolicy::AllowAll,
        )
        .await?;
        let socket_path = PathBuf::from(self.configuration.socket_path.0.clone());
        std::fs::create_dir_all(PathBuf::from(self.configuration.state_directory.0.clone()))?;
        std::fs::create_dir_all(PathBuf::from(
            self.configuration.gc_root_directory.0.clone(),
        ))?;
        let listener =
            SocketListener::spawn(SocketListener::new(socket_path, engine.root().clone()));
        self.engine = Some(engine);
        self.listener = Some(listener);
        Ok(())
    }

    pub async fn serve_forever(&self) -> Result<()> {
        let listener = self.listener.as_ref().ok_or(Error::Shutdown)?;
        listener
            .ask(ServeForever)
            .await
            .map_err(|error| Error::ActorSend(format!("serve_forever: {error}")))?;
        Ok(())
    }
}

impl DaemonConfiguration {
    /// Parse a daemon configuration from a single argument string.
    /// The argument is either inline NOTA (`(DaemonConfiguration ...)`) or
    /// a path to a NOTA file.
    pub fn parse_argument(argument: &str) -> Result<Self> {
        let source = if argument.trim_start().starts_with(['(', '[', '{']) {
            argument.to_owned()
        } else {
            std::fs::read_to_string(argument)
                .map_err(|error| Error::ConfigurationDecode(format!("read {argument}: {error}")))?
        };
        let configuration: Self =
            source
                .parse()
                .map_err(|error: crate::generated::NotaDecodeError| {
                    Error::ConfigurationDecode(error.to_string())
                })?;
        Ok(configuration)
    }
}

impl std::str::FromStr for DaemonConfiguration {
    type Err = crate::generated::NotaDecodeError;

    fn from_str(source: &str) -> std::result::Result<Self, Self::Err> {
        let document = nota_next::Document::parse(source)
            .map_err(|error| crate::generated::NotaDecodeError::Parse(error.to_string()))?;
        if document.holds_root_objects() != 1 {
            return Err(crate::generated::NotaDecodeError::ExpectedSingleRoot {
                found: document.holds_root_objects(),
            });
        }
        let root = document.root_object_at(0).expect("root count checked");
        Self::from_nota_block(root)
    }
}
