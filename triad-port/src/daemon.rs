//! The lojix daemon loop — two authority-tiered unix sockets driving the
//! generated Nexus runner.
//!
//! Resolves the port-plan §4.4 blocker by binding two `ListenerSocket`s on one
//! `MultiListenerDaemon` (the runtime's two-socket primitive), each tagged by
//! its authority role. `handle_stream` decodes the length-prefixed wire frame
//! for the arriving role into a `SignalInput`, drives it through
//! `NexusEngine::execute` (which runs the `Runner` continuation loop — the
//! deploy pipeline included), and encodes the reply back. The schema engine is
//! the single source of routing truth; there is no inline request `Store`.

use std::fmt::{Display, Formatter};
use std::os::unix::net::UnixStream;

use triad_runtime::{
    FrameBody, LengthPrefixedCodec, ListenerSocket, MultiListenerDaemon, MultiListenerDaemonError,
    MultiListenerRuntime, RequestErrorLog, SocketMode,
};

use crate::schema::nexus::{self, NexusEngine};
use crate::schema_runtime::SchemaRuntime;
use crate::{DaemonConfiguration, Error, Result};

/// Which authority-tiered socket an arriving stream belongs to. Ordinary is the
/// peer-callable `signal-lojix` surface; Owner is the `meta-signal-lojix`
/// Deploy/Pin/Unpin/Retire surface. Used as the `MultiListenerRuntime::Listener`
/// tag so `handle_stream` decodes the correct contract.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ListenerRole {
    Ordinary,
    Owner,
}

impl Display for ListenerRole {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Ordinary => formatter.write_str("ordinary"),
            Self::Owner => formatter.write_str("owner"),
        }
    }
}

/// The lojix daemon: configuration plus the schema engine that decides every
/// arriving signal. Construct with [`Daemon::new`], then [`Daemon::run`] binds
/// both sockets and serves forever.
pub struct Daemon {
    configuration: DaemonConfiguration,
}

impl Daemon {
    pub fn new(configuration: DaemonConfiguration) -> Self {
        Self { configuration }
    }

    pub fn run(self) -> Result<()> {
        let configuration = self.configuration;
        let sockets = vec![
            ListenerSocket::new(ListenerRole::Ordinary, configuration.ordinary_socket_path.clone())
                .with_socket_mode(SocketMode::new(configuration.ordinary_socket_mode)),
            ListenerSocket::new(ListenerRole::Owner, configuration.owner_socket_path.clone())
                .with_socket_mode(SocketMode::new(configuration.owner_socket_mode)),
        ];
        let runtime = LojixRuntime::new();
        let request_error_log = RequestErrorLog::new("lojix-daemon");
        MultiListenerDaemon::new(sockets, runtime, request_error_log)
            .run()
            .map_err(Self::map_daemon_error)
    }

    fn map_daemon_error(error: MultiListenerDaemonError<Error, Error>) -> Error {
        match error {
            MultiListenerDaemonError::Listener(listener_error) => {
                Error::SignalFrame(triad_runtime::FrameError::Io(std::io::Error::other(
                    listener_error.to_string(),
                )))
            }
            MultiListenerDaemonError::Start(error) | MultiListenerDaemonError::Stop(error) => error,
        }
    }
}

/// The `MultiListenerRuntime` realization: holds the schema engine and the
/// length-prefixed frame codec. Each arriving stream is decoded, executed
/// through the engine, and replied to.
struct LojixRuntime {
    engine: SchemaRuntime,
    codec: LengthPrefixedCodec,
}

impl LojixRuntime {
    fn new() -> Self {
        Self {
            engine: SchemaRuntime::new(),
            codec: LengthPrefixedCodec::default(),
        }
    }

    fn handle_ordinary(&mut self, stream: &mut UnixStream) -> Result<()> {
        let body = self.codec.read_body(stream)?;
        let (_, input) =
            signal_lojix::schema::lib::Input::decode_signal_frame(body.bytes())?;
        let signal_input = nexus::SignalInput::OrdinaryInput(input);
        let output = self.execute(signal_input);
        let reply = Self::ordinary_reply(output)?;
        let reply_body = FrameBody::new(reply.encode_signal_frame()?);
        self.codec.write_body(stream, &reply_body)?;
        Ok(())
    }

    fn handle_owner(&mut self, stream: &mut UnixStream) -> Result<()> {
        let body = self.codec.read_body(stream)?;
        let (_, input) =
            meta_signal_lojix::schema::lib::Input::decode_signal_frame(body.bytes())?;
        let signal_input = nexus::SignalInput::MetaInput(input);
        let output = self.execute(signal_input);
        let reply = Self::meta_reply(output)?;
        let reply_body = FrameBody::new(reply.encode_signal_frame()?);
        self.codec.write_body(stream, &reply_body)?;
        Ok(())
    }

    fn execute(&mut self, signal_input: nexus::SignalInput) -> nexus::SignalOutput {
        let origin_route = nexus::OriginRoute(0);
        let work = nexus::NexusWork::SignalArrived(signal_input).with_origin_route(origin_route);
        let action = self.engine.execute(work).into_root();
        match action {
            nexus::NexusAction::ReplyToSignal(output) => output,
            // `execute` always terminates the runner with a reply; any other
            // action escaping the runner is a runtime invariant violation.
            _ => nexus::SignalOutput::OrdinaryOutput(
                signal_lojix::schema::lib::Output::QueryRejected(
                    signal_lojix::schema::lib::RejectedQuery {
                        query_rejection_reason:
                            signal_lojix::schema::lib::QueryRejectionReason::MalformedSelector,
                        database_marker: signal_lojix::schema::lib::DatabaseMarker {
                            commit_sequence: 0,
                            state_digest: 0,
                        },
                    },
                ),
            ),
        }
    }

    fn ordinary_reply(
        output: nexus::SignalOutput,
    ) -> Result<signal_lojix::schema::lib::Output> {
        match output {
            nexus::SignalOutput::OrdinaryOutput(output) => Ok(output),
            nexus::SignalOutput::MetaOutput(_) => Err(Error::UnexpectedFrame),
        }
    }

    fn meta_reply(
        output: nexus::SignalOutput,
    ) -> Result<meta_signal_lojix::schema::lib::Output> {
        match output {
            nexus::SignalOutput::MetaOutput(output) => Ok(output),
            nexus::SignalOutput::OrdinaryOutput(_) => Err(Error::UnexpectedFrame),
        }
    }
}

impl MultiListenerRuntime for LojixRuntime {
    type Listener = ListenerRole;
    type StartError = Error;
    type StopError = Error;
    type RequestError = Error;

    fn start(&mut self) -> Result<()> {
        Ok(())
    }

    fn stop(&mut self) -> Result<()> {
        Ok(())
    }

    fn handle_stream(
        &mut self,
        listener: Self::Listener,
        mut stream: UnixStream,
    ) -> Result<()> {
        match listener {
            ListenerRole::Ordinary => self.handle_ordinary(&mut stream),
            ListenerRole::Owner => self.handle_owner(&mut stream),
        }
    }
}
