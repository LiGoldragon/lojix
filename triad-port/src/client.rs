//! The lojix thin CLI client — text-to-Signal adapter for the daemon.
//!
//! Takes the single `ComponentArgument` (per the no-flags / NOTA-only rule),
//! resolves it to a contract `Input` (signal-encoded file decoded directly;
//! inline / NOTA-file text via the optional `nota-text` feature), classifies it
//! as ordinary or owner, connects to the matching authority-tiered socket, and
//! exchanges one length-prefixed frame. Socket paths come from the environment
//! (`LOJIX_ORDINARY_SOCKET` / `LOJIX_OWNER_SOCKET`) — env vars are a NOTA host,
//! not flags.

use std::os::unix::net::UnixStream;

use triad_runtime::{ComponentArgument, ComponentCommand, FrameBody, LengthPrefixedCodec};

use crate::{Error, Result};

const ORDINARY_SOCKET_ENV: &str = "LOJIX_ORDINARY_SOCKET";
const OWNER_SOCKET_ENV: &str = "LOJIX_OWNER_SOCKET";
const DEFAULT_ORDINARY_SOCKET: &str = "/run/lojix/ordinary.sock";
const DEFAULT_OWNER_SOCKET: &str = "/run/lojix/owner.sock";

/// A decoded request bound for one authority tier, plus the resolved reply
/// after the exchange. The request is one of the two contract `Input` unions.
#[derive(Debug, Clone)]
pub enum ClientRequest {
    Ordinary(signal_lojix::schema::lib::Input),
    Owner(meta_signal_lojix::schema::lib::Input),
}

/// A reply returned by the daemon, carrying its source tier so the CLI can
/// print the NOTA / debug form of the right contract `Output`.
#[derive(Debug, Clone)]
pub enum ClientReply {
    Ordinary(signal_lojix::schema::lib::Output),
    Owner(meta_signal_lojix::schema::lib::Output),
}

impl std::fmt::Display for ClientReply {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Ordinary(output) => write!(formatter, "{output:?}"),
            Self::Owner(output) => write!(formatter, "{output:?}"),
        }
    }
}

/// The CLI client noun: holds the resolved request and the length-prefixed
/// frame codec, and performs the socket exchange.
pub struct Client {
    request: ClientRequest,
    codec: LengthPrefixedCodec,
}

impl Client {
    pub fn run_from_environment() -> Result<ClientReply> {
        let command = ComponentCommand::from_environment();
        let argument = command.nota_argument()?;
        Self::from_argument(argument)?.exchange()
    }

    fn from_argument(argument: ComponentArgument) -> Result<Self> {
        let request = Self::resolve_request(argument)?;
        Ok(Self {
            request,
            codec: LengthPrefixedCodec::default(),
        })
    }

    fn resolve_request(argument: ComponentArgument) -> Result<ClientRequest> {
        match argument {
            ComponentArgument::SignalFile(file) => Self::decode_signal_file(file.as_path()),
            ComponentArgument::NotaFile(file) => Self::decode_signal_file(file.as_path()),
            ComponentArgument::InlineNota(_) => Err(Error::InlineNotaUnsupported),
        }
    }

    fn decode_signal_file(path: &std::path::Path) -> Result<ClientRequest> {
        let bytes = std::fs::read(path)?;
        // Try the owner contract first, then the ordinary contract. NOTE
        // (audit R7): the two contracts' short-header ordinals currently COLLIDE
        // (meta `Deploy` == ordinary `Query` == 0x0), so this disambiguation
        // relies on rkyv layout divergence, not a structural tier discriminator.
        // A tier bit in the short header is the proper fix (upstream of lojix).
        if let Ok((_, input)) =
            meta_signal_lojix::schema::lib::Input::decode_signal_frame(&bytes)
        {
            return Ok(ClientRequest::Owner(input));
        }
        let (_, input) = signal_lojix::schema::lib::Input::decode_signal_frame(&bytes)?;
        Ok(ClientRequest::Ordinary(input))
    }

    fn exchange(self) -> Result<ClientReply> {
        match self.request {
            ClientRequest::Ordinary(input) => {
                let socket = Self::socket_path(ORDINARY_SOCKET_ENV, DEFAULT_ORDINARY_SOCKET);
                let mut stream = UnixStream::connect(socket)?;
                let frame = FrameBody::new(input.encode_signal_frame()?);
                self.codec.write_body(&mut stream, &frame)?;
                let reply_body = self.codec.read_body(&mut stream)?;
                let (_, output) =
                    signal_lojix::schema::lib::Output::decode_signal_frame(reply_body.bytes())?;
                Ok(ClientReply::Ordinary(output))
            }
            ClientRequest::Owner(input) => {
                let socket = Self::socket_path(OWNER_SOCKET_ENV, DEFAULT_OWNER_SOCKET);
                let mut stream = UnixStream::connect(socket)?;
                let frame = FrameBody::new(input.encode_signal_frame()?);
                self.codec.write_body(&mut stream, &frame)?;
                let reply_body = self.codec.read_body(&mut stream)?;
                let (_, output) =
                    meta_signal_lojix::schema::lib::Output::decode_signal_frame(reply_body.bytes())?;
                Ok(ClientReply::Owner(output))
            }
        }
    }

    fn socket_path(env_var: &str, default: &str) -> String {
        std::env::var(env_var).unwrap_or_else(|_| default.to_string())
    }
}
