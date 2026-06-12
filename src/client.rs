//! The lojix CLI clients — text-to-Signal adapters for the daemon, one per
//! authority-tiered socket.
//!
//! Each client takes the single `ComponentArgument` (per the no-flags /
//! NOTA-only rule), decodes it into exactly one contract `Input` (a
//! signal-encoded file decoded directly; inline / NOTA-file text via the
//! optional `nota-text` feature), connects to its socket, and exchanges one
//! length-prefixed frame. Because each client parses only its own contract
//! there is no cross-tier classification step — the prior unified client's
//! audit-R7 short-header collision (meta `Deploy` == ordinary `Query` == 0x0)
//! is avoided structurally rather than disambiguated by rkyv layout. Socket
//! paths come from the environment (`LOJIX_ORDINARY_SOCKET` /
//! `LOJIX_OWNER_SOCKET`) — env vars are a NOTA host, not flags.

use std::os::unix::net::UnixStream;
use std::path::Path;

use triad_runtime::{ComponentArgument, ComponentCommand, FrameBody, LengthPrefixedCodec};

use crate::{Error, Result};

const ORDINARY_SOCKET_ENV: &str = "LOJIX_ORDINARY_SOCKET";
const OWNER_SOCKET_ENV: &str = "LOJIX_OWNER_SOCKET";
const DEFAULT_ORDINARY_SOCKET: &str = "/run/lojix/ordinary.sock";
const DEFAULT_OWNER_SOCKET: &str = "/run/lojix/owner.sock";

/// A length-prefixed framed exchange over one Unix socket: connect, write the
/// request frame, read the single reply frame. Tier-agnostic at the byte level;
/// each tier client owns the typed encode/decode around it.
pub struct SocketExchange {
    socket_path: String,
    codec: LengthPrefixedCodec,
}

impl SocketExchange {
    pub fn for_environment(environment_variable: &str, default_path: &str) -> Self {
        let socket_path =
            std::env::var(environment_variable).unwrap_or_else(|_| default_path.to_string());
        Self {
            socket_path,
            codec: LengthPrefixedCodec::default(),
        }
    }

    /// Exchange one request frame for one reply frame, returning the reply body
    /// bytes for the caller to decode against its contract.
    pub fn exchange(&self, request: Vec<u8>) -> Result<Vec<u8>> {
        let mut stream = UnixStream::connect(&self.socket_path)?;
        let frame = FrameBody::new(request);
        self.codec.write_body(&mut stream, &frame)?;
        let reply = self.codec.read_body(&mut stream)?;
        Ok(reply.bytes().to_vec())
    }
}

/// The ordinary-socket CLI client: speaks the peer-callable `signal-lojix`
/// contract (Query / WatchDeployments / WatchCacheRetention / Unwatch /
/// CheckHostKeyMaterial).
#[derive(Debug)]
pub struct OrdinaryClient {
    input: signal_lojix::schema::lib::Input,
}

impl OrdinaryClient {
    pub fn run_from_environment() -> Result<signal_lojix::schema::lib::Output> {
        let argument = ComponentCommand::from_environment().nota_argument()?;
        Self::from_argument(argument)?.run()
    }

    pub fn from_argument(argument: ComponentArgument) -> Result<Self> {
        let input = match argument {
            ComponentArgument::SignalFile(file) => Self::decode_signal_file(file.as_path())?,
            ComponentArgument::NotaFile(file) => {
                let path = file.as_path();
                if path.extension().and_then(|extension| extension.to_str()) == Some("nota") {
                    Self::decode_nota_text(&std::fs::read_to_string(path)?)?
                } else {
                    Self::decode_signal_file(path)?
                }
            }
            ComponentArgument::InlineNota(inline) => Self::decode_nota_text(inline.as_str())?,
        };
        Ok(Self { input })
    }

    pub fn input(&self) -> &signal_lojix::schema::lib::Input {
        &self.input
    }

    pub fn run(self) -> Result<signal_lojix::schema::lib::Output> {
        let exchange = SocketExchange::for_environment(ORDINARY_SOCKET_ENV, DEFAULT_ORDINARY_SOCKET);
        let reply = exchange.exchange(self.input.encode_signal_frame()?)?;
        let (_, output) = signal_lojix::schema::lib::Output::decode_signal_frame(&reply)?;
        Ok(output)
    }

    fn decode_signal_file(path: &Path) -> Result<signal_lojix::schema::lib::Input> {
        let bytes = std::fs::read(path)?;
        let (_, input) = signal_lojix::schema::lib::Input::decode_signal_frame(&bytes)?;
        Ok(input)
    }

    #[cfg(feature = "nota-text")]
    fn decode_nota_text(text: &str) -> Result<signal_lojix::schema::lib::Input> {
        text.parse::<signal_lojix::schema::lib::Input>()
            .map_err(|error| Error::NotaRequestText(error.to_string()))
    }

    #[cfg(not(feature = "nota-text"))]
    fn decode_nota_text(_text: &str) -> Result<signal_lojix::schema::lib::Input> {
        Err(Error::NotaTextUnsupported)
    }
}

/// The owner-only meta-socket CLI client: the privileged sibling of
/// `OrdinaryClient`. It speaks the `meta-signal-lojix` policy contract (Deploy /
/// Pin / Unpin / Retire) over the daemon's owner/meta socket.
#[derive(Debug)]
pub struct MetaClient {
    input: meta_signal_lojix::schema::lib::Input,
}

impl MetaClient {
    pub fn run_from_environment() -> Result<meta_signal_lojix::schema::lib::Output> {
        let argument = ComponentCommand::from_environment().nota_argument()?;
        Self::from_argument(argument)?.run()
    }

    pub fn from_argument(argument: ComponentArgument) -> Result<Self> {
        let input = match argument {
            ComponentArgument::SignalFile(file) => Self::decode_signal_file(file.as_path())?,
            ComponentArgument::NotaFile(file) => {
                let path = file.as_path();
                if path.extension().and_then(|extension| extension.to_str()) == Some("nota") {
                    Self::decode_nota_text(&std::fs::read_to_string(path)?)?
                } else {
                    Self::decode_signal_file(path)?
                }
            }
            ComponentArgument::InlineNota(inline) => Self::decode_nota_text(inline.as_str())?,
        };
        Ok(Self { input })
    }

    pub fn input(&self) -> &meta_signal_lojix::schema::lib::Input {
        &self.input
    }

    pub fn run(self) -> Result<meta_signal_lojix::schema::lib::Output> {
        let exchange = SocketExchange::for_environment(OWNER_SOCKET_ENV, DEFAULT_OWNER_SOCKET);
        let reply = exchange.exchange(self.input.encode_signal_frame()?)?;
        let (_, output) = meta_signal_lojix::schema::lib::Output::decode_signal_frame(&reply)?;
        Ok(output)
    }

    fn decode_signal_file(path: &Path) -> Result<meta_signal_lojix::schema::lib::Input> {
        let bytes = std::fs::read(path)?;
        let (_, input) = meta_signal_lojix::schema::lib::Input::decode_signal_frame(&bytes)?;
        Ok(input)
    }

    #[cfg(feature = "nota-text")]
    fn decode_nota_text(text: &str) -> Result<meta_signal_lojix::schema::lib::Input> {
        text.parse::<meta_signal_lojix::schema::lib::Input>()
            .map_err(|error| Error::NotaRequestText(error.to_string()))
    }

    #[cfg(not(feature = "nota-text"))]
    fn decode_nota_text(_text: &str) -> Result<meta_signal_lojix::schema::lib::Input> {
        Err(Error::NotaTextUnsupported)
    }
}
