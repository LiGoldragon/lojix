//! The lojix CLI clients — text-to-Signal adapters for the daemon, one per
//! authority-tiered socket.
//!
//! Each client takes the single `ComponentArgument` (per the no-flags /
//! DOTOS-only rule), decodes it into exactly one contract `Input` (a
//! signal-encoded file decoded directly; inline / DOTOS-file text via the
//! optional `dotos-text` feature), connects to its socket, and exchanges one
//! length-prefixed frame. Because each client parses only its own contract
//! there is no cross-tier classification step — the prior unified client's
//! audit-R7 short-header collision (meta `Deploy` == ordinary `Query` == 0x0)
//! is avoided structurally rather than disambiguated by rkyv layout. Socket
//! paths come from the environment (`LOJIX_ORDINARY_SOCKET` /
//! `LOJIX_OWNER_SOCKET`) — env vars are a DOTOS host, not flags.

use std::os::unix::net::UnixStream;
use std::path::Path;

use signal_frame::{ExchangeIdentifier, ExchangeLane, LaneSequence, Reply, SessionEpoch, SubReply};
use triad_runtime::{ComponentArgument, ComponentCommand, FrameBody, LengthPrefixedCodec};

use crate::{Error, Result};

const ORDINARY_SOCKET_ENV: &str = "LOJIX_ORDINARY_SOCKET";
const OWNER_SOCKET_ENV: &str = "LOJIX_OWNER_SOCKET";

fn exchange_identifier() -> ExchangeIdentifier {
    ExchangeIdentifier::new(
        SessionEpoch::new(1),
        ExchangeLane::Connector,
        LaneSequence::first(),
    )
}

/// A length-prefixed framed exchange over one Unix socket: connect, write the
/// request frame, read the single reply frame. Tier-agnostic at the byte level;
/// each tier client owns the typed encode/decode around it.
pub struct SocketExchange {
    socket_path: String,
    codec: LengthPrefixedCodec,
}

impl SocketExchange {
    pub fn for_environment(environment_variable: &str) -> Result<Self> {
        let socket_path = std::env::var(environment_variable)
            .map_err(|_| Error::MissingRuntimeConfiguration(environment_variable.to_string()))?;
        if socket_path.is_empty() {
            return Err(Error::MissingRuntimeConfiguration(
                environment_variable.to_string(),
            ));
        }
        Ok(Self {
            socket_path,
            codec: LengthPrefixedCodec::default(),
        })
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
        let argument = ComponentCommand::from_environment().dotos_argument()?;
        Self::from_argument(argument)?.run()
    }

    pub fn from_argument(argument: ComponentArgument) -> Result<Self> {
        let input = match argument {
            ComponentArgument::SignalFile(file) => Self::decode_signal_file(file.as_path())?,
            ComponentArgument::DotosFile(file) => {
                Self::decode_dotos_text(&std::fs::read_to_string(file.as_path())?)?
            }
            ComponentArgument::InlineDotos(inline) => Self::decode_dotos_text(inline.as_str())?,
        };
        Ok(Self { input })
    }

    pub fn input(&self) -> &signal_lojix::schema::lib::Input {
        &self.input
    }

    pub fn run(self) -> Result<signal_lojix::schema::lib::Output> {
        let exchange = SocketExchange::for_environment(ORDINARY_SOCKET_ENV)?;
        let identifier = exchange_identifier();
        let reply = exchange.exchange(self.input.encode_request_frame(identifier)?)?;
        Self::decode_reply(&reply, identifier)
    }

    fn decode_signal_file(path: &Path) -> Result<signal_lojix::schema::lib::Input> {
        let bytes = std::fs::read(path)?;
        let (_, input) = signal_lojix::schema::lib::ContractMarker::decode_single_request(&bytes)?;
        Ok(input)
    }

    fn decode_reply(
        bytes: &[u8],
        expected_exchange: ExchangeIdentifier,
    ) -> Result<signal_lojix::schema::lib::Output> {
        let frame = signal_lojix::schema::lib::ContractMarker::decode_frame(bytes)?;
        match frame.into_body() {
            signal_lojix::schema::lib::FrameBody::Reply { exchange, reply }
                if exchange == expected_exchange =>
            {
                match reply {
                    Reply::Accepted { per_operation, .. } => match per_operation.into_head() {
                        SubReply::Ok(output)
                        | SubReply::Failed {
                            detail: Some(output),
                            ..
                        } => Ok(output),
                        SubReply::Invalidated
                        | SubReply::Skipped
                        | SubReply::Failed { detail: None, .. } => Err(Error::UnexpectedFrame),
                    },
                    Reply::Rejected { .. } => Err(Error::UnexpectedFrame),
                }
            }
            _ => Err(Error::UnexpectedFrame),
        }
    }

    #[cfg(feature = "dotos-text")]
    fn decode_dotos_text(text: &str) -> Result<signal_lojix::schema::lib::Input> {
        text.parse::<signal_lojix::schema::lib::Input>()
            .map_err(|error| Error::DotosRequestText(error.to_string()))
    }

    #[cfg(not(feature = "dotos-text"))]
    fn decode_dotos_text(_text: &str) -> Result<signal_lojix::schema::lib::Input> {
        Err(Error::DotosTextUnsupported)
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
        let argument = ComponentCommand::from_environment().dotos_argument()?;
        Self::from_argument(argument)?.run()
    }

    pub fn from_argument(argument: ComponentArgument) -> Result<Self> {
        let input = match argument {
            ComponentArgument::SignalFile(file) => Self::decode_signal_file(file.as_path())?,
            ComponentArgument::DotosFile(file) => {
                Self::decode_dotos_text(&std::fs::read_to_string(file.as_path())?)?
            }
            ComponentArgument::InlineDotos(inline) => Self::decode_dotos_text(inline.as_str())?,
        };
        Ok(Self { input })
    }

    pub fn input(&self) -> &meta_signal_lojix::schema::lib::Input {
        &self.input
    }

    pub fn run(self) -> Result<meta_signal_lojix::schema::lib::Output> {
        let exchange = SocketExchange::for_environment(OWNER_SOCKET_ENV)?;
        let identifier = exchange_identifier();
        let reply = exchange.exchange(self.input.encode_request_frame(identifier)?)?;
        Self::decode_reply(&reply, identifier)
    }

    fn decode_signal_file(path: &Path) -> Result<meta_signal_lojix::schema::lib::Input> {
        let bytes = std::fs::read(path)?;
        let (_, input) =
            meta_signal_lojix::schema::lib::ContractMarker::decode_single_request(&bytes)?;
        Ok(input)
    }

    fn decode_reply(
        bytes: &[u8],
        expected_exchange: ExchangeIdentifier,
    ) -> Result<meta_signal_lojix::schema::lib::Output> {
        let frame = meta_signal_lojix::schema::lib::ContractMarker::decode_frame(bytes)?;
        match frame.into_body() {
            meta_signal_lojix::schema::lib::FrameBody::Reply { exchange, reply }
                if exchange == expected_exchange =>
            {
                match reply {
                    Reply::Accepted { per_operation, .. } => match per_operation.into_head() {
                        SubReply::Ok(output)
                        | SubReply::Failed {
                            detail: Some(output),
                            ..
                        } => Ok(output),
                        SubReply::Invalidated
                        | SubReply::Skipped
                        | SubReply::Failed { detail: None, .. } => Err(Error::UnexpectedFrame),
                    },
                    Reply::Rejected { .. } => Err(Error::UnexpectedFrame),
                }
            }
            _ => Err(Error::UnexpectedFrame),
        }
    }

    #[cfg(feature = "dotos-text")]
    fn decode_dotos_text(text: &str) -> Result<meta_signal_lojix::schema::lib::Input> {
        text.parse::<meta_signal_lojix::schema::lib::Input>()
            .map_err(|error| Error::DotosRequestText(error.to_string()))
    }

    #[cfg(not(feature = "dotos-text"))]
    fn decode_dotos_text(_text: &str) -> Result<meta_signal_lojix::schema::lib::Input> {
        Err(Error::DotosTextUnsupported)
    }
}
