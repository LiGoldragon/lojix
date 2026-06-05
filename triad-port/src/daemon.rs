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
use std::time::Duration;

use triad_runtime::{
    FrameBody, LengthPrefixedCodec, ListenerSocket, MaximumFrameLength, MultiListenerDaemon,
    MultiListenerDaemonError, MultiListenerRuntime, RequestErrorLog, SocketMode,
};

/// Maximum inbound request-frame body the daemon accepts (8 MiB). A lojix
/// request is a few hundred bytes; this bounds a hostile length prefix far
/// below the 4 GiB the u32-prefix codec default would pre-allocate (audit R1).
const MAXIMUM_REQUEST_FRAME_BYTES: usize = 8 * 1024 * 1024;

/// How long the daemon waits for a connected client to send its request frame
/// before dropping the stream — bounds the connect-and-never-send wedge of the
/// serial accept loop (audit R2). A legitimate client sends immediately.
const REQUEST_READ_TIMEOUT: Duration = Duration::from_secs(10);

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
        Self::validate_owner_socket_mode(configuration.owner_socket_mode)?;
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

    /// Refuse an owner-socket mode that grants any "other" access. The owner
    /// socket carries the privileged Deploy/Pin/Unpin/Retire surface and its
    /// authority rests entirely on the socket file mode (no peer-credential
    /// check yet — audit R3); a permissive mode from config would silently make
    /// that surface world-reachable.
    fn validate_owner_socket_mode(mode: u32) -> Result<()> {
        if mode & 0o007 != 0 {
            return Err(Error::InsecureOwnerSocketMode(mode));
        }
        Ok(())
    }
}

/// The `MultiListenerRuntime` realization: holds the schema engine and the
/// length-prefixed frame codec. Each arriving stream is decoded, executed
/// through the engine, and replied to.
///
/// INVARIANT (audit R6/P3): the `MultiListenerDaemon` accept loop is serial and
/// each `handle_stream` drives the engine to completion before the next request
/// is accepted — there is exactly one in-flight request at a time. The engine's
/// single-slot `active_deploy`/`active_operation` fields are safe ONLY under
/// this invariant; introducing concurrency (a worker pool / async effect plane)
/// requires replacing them with a keyed in-flight map first. The cost of this
/// model is that a long `nix` build blocks the OTHER socket for its duration —
/// the open concurrency-model decision in cloud-designer report 29.
struct LojixRuntime {
    engine: SchemaRuntime,
    codec: LengthPrefixedCodec,
}

impl LojixRuntime {
    fn new() -> Self {
        Self {
            engine: SchemaRuntime::new(),
            codec: LengthPrefixedCodec::new(MaximumFrameLength::new(MAXIMUM_REQUEST_FRAME_BYTES)),
        }
    }

    fn handle_ordinary(&mut self, stream: &mut UnixStream) -> Result<()> {
        stream.set_read_timeout(Some(REQUEST_READ_TIMEOUT))?;
        let body = self.codec.read_body(stream)?;
        let (_, input) =
            signal_lojix::schema::lib::Input::decode_signal_frame(body.bytes())?;
        let signal_input = nexus::SignalInput::OrdinaryInput(input);
        let output = self.execute(ListenerRole::Ordinary, signal_input);
        let reply = Self::ordinary_reply(output)?;
        let reply_body = FrameBody::new(reply.encode_signal_frame()?);
        self.codec.write_body(stream, &reply_body)?;
        Ok(())
    }

    fn handle_owner(&mut self, stream: &mut UnixStream) -> Result<()> {
        stream.set_read_timeout(Some(REQUEST_READ_TIMEOUT))?;
        let body = self.codec.read_body(stream)?;
        let (_, input) =
            meta_signal_lojix::schema::lib::Input::decode_signal_frame(body.bytes())?;
        let signal_input = nexus::SignalInput::MetaInput(input);
        let output = self.execute(ListenerRole::Owner, signal_input);
        let reply = Self::meta_reply(output)?;
        let reply_body = FrameBody::new(reply.encode_signal_frame()?);
        self.codec.write_body(stream, &reply_body)?;
        Ok(())
    }

    fn execute(
        &mut self,
        listener: ListenerRole,
        signal_input: nexus::SignalInput,
    ) -> nexus::SignalOutput {
        let origin_route = nexus::OriginRoute(0);
        let work = nexus::NexusWork::SignalArrived(signal_input).with_origin_route(origin_route);
        match self.engine.execute(work).into_root() {
            nexus::NexusAction::ReplyToSignal(output) => output,
            // `execute` always terminates the runner with a reply; any other
            // action escaping the runner is a runtime invariant violation. Reply
            // with a typed rejection on the SAME authority tier the request
            // arrived on (audit R4), so the client decodes a real reply rather
            // than an EOF mid-exchange.
            _ => Self::invariant_rejection(listener),
        }
    }

    /// A tier-correct typed rejection for the should-never-happen case where the
    /// runner ends on a non-reply action. Owner requests get a meta
    /// `DeployRejected(InternalError)`; ordinary requests an ordinary
    /// `QueryRejected` — never a cross-tier output the client cannot decode.
    fn invariant_rejection(listener: ListenerRole) -> nexus::SignalOutput {
        match listener {
            ListenerRole::Owner => nexus::SignalOutput::MetaOutput(
                meta_signal_lojix::schema::lib::Output::DeployRejected(
                    meta_signal_lojix::schema::lib::RejectedDeploy {
                        deploy_rejection_reason:
                            meta_signal_lojix::schema::lib::DeployRejectionReason::InternalError,
                        database_marker: meta_signal_lojix::schema::lib::DatabaseMarker {
                            commit_sequence: 0,
                            state_digest: 0,
                        },
                    },
                ),
            ),
            ListenerRole::Ordinary => nexus::SignalOutput::OrdinaryOutput(
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
