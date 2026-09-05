//! Typed Datom clients for the two Lojix authority-tier sockets.

use std::ffi::OsString;
use std::os::unix::net::UnixStream;

use datom_codec::{Actualizable, IncorporationBudget, Potential};
use signal_lojix::WireConversion;
use signal_frame::{
    BoundExchangeFrame, ExchangeFrameBody, ExchangeIdentifier, ExchangeLane, LaneSequence,
    Reply, SessionEpoch, SubReply,
};
use triad_runtime::{FrameBody, LengthPrefixedCodec};

use crate::{Error, Result, single_inline_datom_argument};

const ORDINARY_SOCKET_ENV: &str = "LOJIX_ORDINARY_SOCKET";
const OWNER_SOCKET_ENV: &str = "LOJIX_OWNER_SOCKET";

fn exchange_identifier() -> ExchangeIdentifier {
    ExchangeIdentifier::new(SessionEpoch::new(1), ExchangeLane::Connector, LaneSequence::first())
}

pub struct SocketExchange { socket_path: String, codec: LengthPrefixedCodec }

impl SocketExchange {
    pub fn for_environment(variable: &str) -> Result<Self> {
        let socket_path = std::env::var(variable)
            .map_err(|_| Error::MissingRuntimeConfiguration(variable.to_owned()))?;
        if socket_path.is_empty() { return Err(Error::MissingRuntimeConfiguration(variable.to_owned())); }
        Ok(Self { socket_path, codec: LengthPrefixedCodec::default() })
    }

    pub fn exchange(&self, request: Vec<u8>) -> Result<Vec<u8>> {
        let mut stream = UnixStream::connect(&self.socket_path)?;
        self.codec.write_body(&mut stream, &FrameBody::new(request))?;
        Ok(self.codec.read_body(&mut stream)?.bytes().to_vec())
    }
}

#[derive(Debug)]
pub struct OrdinaryClient { input: signal_lojix::Request }

impl OrdinaryClient {
    pub fn run_from_environment() -> Result<signal_lojix::Response> {
        Self::from_arguments(std::env::args_os().skip(1))?.run()
    }

    pub fn from_arguments(arguments: impl IntoIterator<Item = OsString>) -> Result<Self> {
        let input = Potential::<signal_lojix::Request>::from(single_inline_datom_argument(arguments)?)
            .actualize(IncorporationBudget::try_from(16_384).expect("positive request budget"))
            .map_err(|fault| Error::DatomRequestText(format!("{fault:?}")))?;
        Ok(Self { input })
    }

    pub fn input(&self) -> &signal_lojix::Request { &self.input }

    pub fn run(self) -> Result<signal_lojix::Response> {
        let exchange = exchange_identifier();
        let bytes = signal_lojix::encode_request(exchange, self.input)?;
        let reply = SocketExchange::for_environment(ORDINARY_SOCKET_ENV)?.exchange(bytes)?;
        let frame = BoundExchangeFrame::<signal_lojix::LojixWire, signal_lojix::RequestWire, signal_lojix::ResponseWire>::decode_length_prefixed(&reply)?;
        match frame.into_body() {
            ExchangeFrameBody::Reply { exchange: found, reply } if found == exchange => match reply {
                Reply::Accepted { per_operation, .. } => match per_operation.into_head() {
                    SubReply::Ok(wire) | SubReply::Failed { detail: Some(wire), .. } => {
                        signal_lojix::Response::try_from_wire(wire).map_err(|fault| Error::Wire(format!("{fault:?}")))
                    }
                    _ => Err(Error::UnexpectedFrame),
                },
                Reply::Rejected { .. } => Err(Error::SignalRequestRejected),
            },
            _ => Err(Error::UnexpectedFrame),
        }
    }
}

#[derive(Debug)]
pub struct MetaClient { input: meta_signal_lojix::Request }

impl MetaClient {
    pub fn run_from_environment() -> Result<meta_signal_lojix::Response> {
        Self::from_arguments(std::env::args_os().skip(1))?.run()
    }

    pub fn from_arguments(arguments: impl IntoIterator<Item = OsString>) -> Result<Self> {
        let input = Potential::<meta_signal_lojix::Request>::from(single_inline_datom_argument(arguments)?)
            .actualize(IncorporationBudget::try_from(16_384).expect("positive request budget"))
            .map_err(|fault| Error::DatomRequestText(format!("{fault:?}")))?;
        Ok(Self { input })
    }

    pub fn input(&self) -> &meta_signal_lojix::Request { &self.input }

    pub fn run(self) -> Result<meta_signal_lojix::Response> {
        use meta_signal_lojix::WireConversion;
        let exchange = exchange_identifier();
        let input = self.input;
        let route = match &input {
            meta_signal_lojix::Request::Retire(_) => 0,
            meta_signal_lojix::Request::Pin(_) => 1,
            meta_signal_lojix::Request::Deploy(_) => 2,
            meta_signal_lojix::Request::Test(_) => 3,
            meta_signal_lojix::Request::Unpin(_) => 4,
        };
        let request = BoundExchangeFrame::<meta_signal_lojix::MetaLojixWire, meta_signal_lojix::RequestWire, meta_signal_lojix::ResponseWire>::new(
            signal_frame::WireRoute::new(signal_frame::RootCode::new(0), signal_frame::VariantCode::new(route)),
            ExchangeFrameBody::Request { exchange, request: signal_frame::Request::from_payload(input.into_wire()) },
        ).encode_length_prefixed()?;
        let reply = SocketExchange::for_environment(OWNER_SOCKET_ENV)?.exchange(request)?;
        let frame = BoundExchangeFrame::<meta_signal_lojix::MetaLojixWire, meta_signal_lojix::RequestWire, meta_signal_lojix::ResponseWire>::decode_length_prefixed(&reply)?;
        match frame.into_body() {
            ExchangeFrameBody::Reply { exchange: found, reply } if found == exchange => match reply {
                Reply::Accepted { per_operation, .. } => match per_operation.into_head() {
                    SubReply::Ok(wire) | SubReply::Failed { detail: Some(wire), .. } => {
                        meta_signal_lojix::Response::try_from_wire(wire).map_err(|fault| Error::Wire(format!("{fault:?}")))
                    }
                    _ => Err(Error::UnexpectedFrame),
                },
                Reply::Rejected { .. } => Err(Error::SignalRequestRejected),
            },
            _ => Err(Error::UnexpectedFrame),
        }
    }
}
