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
use std::sync::{Arc, Condvar, Mutex};
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
use crate::{DaemonConfiguration, Error, Result, Store};

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

/// Maximum number of requests served concurrently. Each accepted connection is
/// handled on its own worker thread so a long `nix` build never blocks the
/// accept loop or other connections (intent 2alg); this caps the live worker
/// count so a flood cannot exhaust resources.
const MAXIMUM_CONCURRENT_REQUESTS: usize = 64;

/// The `MultiListenerRuntime` realization. Owns the SHARED durable `Store` (the
/// concurrency point — locked only briefly per sema operation), the frame
/// codec, and the connection-permit pool. `handle_stream` acquires a permit and
/// spawns a [`RequestWorker`] thread, then returns immediately so the accept
/// loop stays responsive on BOTH sockets while deploys run (intent 2alg,
/// resolving audit 29's serial-model question). Per-request in-flight state
/// lives on the worker's own `SchemaRuntime`, never shared.
struct LojixRuntime {
    store: Arc<Store>,
    codec: LengthPrefixedCodec,
    permits: Arc<ConnectionPermits>,
}

impl LojixRuntime {
    fn new() -> Self {
        Self {
            store: Arc::new(Store::new()),
            codec: LengthPrefixedCodec::new(MaximumFrameLength::new(MAXIMUM_REQUEST_FRAME_BYTES)),
            permits: Arc::new(ConnectionPermits::new(MAXIMUM_CONCURRENT_REQUESTS)),
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

    fn handle_stream(&mut self, listener: Self::Listener, stream: UnixStream) -> Result<()> {
        // Acquire a concurrency permit (backpressure on the accept loop if at
        // the cap), then serve the request on its own thread and return
        // immediately. The accept loop is freed to poll both sockets while this
        // request — possibly a multi-minute `nix` build — runs (intent 2alg).
        let permit = self.permits.clone().acquire();
        let worker = RequestWorker {
            store: self.store.clone(),
            codec: self.codec,
        };
        std::thread::spawn(move || {
            // `_permit` is released when this worker thread exits.
            let _permit = permit;
            worker.serve(listener, stream);
        });
        Ok(())
    }
}

/// One request, served on its own thread. Builds a fresh per-request
/// `SchemaRuntime` over a clone of the shared `Store`, so the in-flight deploy
/// cursor is never shared across concurrent connections (intent 2alg).
struct RequestWorker {
    store: Arc<Store>,
    codec: LengthPrefixedCodec,
}

impl RequestWorker {
    fn serve(self, listener: ListenerRole, mut stream: UnixStream) {
        let result = match listener {
            ListenerRole::Ordinary => self.serve_ordinary(&mut stream),
            ListenerRole::Owner => self.serve_owner(&mut stream),
        };
        if let Err(error) = result {
            // In-band rejections are already written as typed replies; this
            // covers only IO / decode failures where no reply is possible.
            eprintln!("(RequestFailed [{listener} {error}])");
        }
    }

    fn serve_ordinary(&self, stream: &mut UnixStream) -> Result<()> {
        stream.set_read_timeout(Some(REQUEST_READ_TIMEOUT))?;
        let body = self.codec.read_body(stream)?;
        let (_, input) = signal_lojix::schema::lib::Input::decode_signal_frame(body.bytes())?;
        let output =
            self.execute(ListenerRole::Ordinary, nexus::SignalInput::OrdinaryInput(input));
        let reply = Self::ordinary_reply(output)?;
        self.codec
            .write_body(stream, &FrameBody::new(reply.encode_signal_frame()?))?;
        Ok(())
    }

    fn serve_owner(&self, stream: &mut UnixStream) -> Result<()> {
        stream.set_read_timeout(Some(REQUEST_READ_TIMEOUT))?;
        let body = self.codec.read_body(stream)?;
        let (_, input) = meta_signal_lojix::schema::lib::Input::decode_signal_frame(body.bytes())?;
        let output = self.execute(ListenerRole::Owner, nexus::SignalInput::MetaInput(input));
        let reply = Self::meta_reply(output)?;
        self.codec
            .write_body(stream, &FrameBody::new(reply.encode_signal_frame()?))?;
        Ok(())
    }

    /// Build a per-request engine over the shared `Store` and drive it. The
    /// engine's in-flight cursor is local to this call, so concurrent requests
    /// never corrupt each other's deploy state (intent 2alg).
    fn execute(
        &self,
        listener: ListenerRole,
        signal_input: nexus::SignalInput,
    ) -> nexus::SignalOutput {
        let mut engine = SchemaRuntime::with_store(self.store.clone());
        let work =
            nexus::NexusWork::SignalArrived(signal_input).with_origin_route(nexus::OriginRoute(0));
        match engine.execute(work).into_root() {
            nexus::NexusAction::ReplyToSignal(output) => output,
            // `execute` always terminates the runner with a reply; any other
            // action escaping is a runtime invariant violation. Reply with a
            // typed rejection on the SAME authority tier the request arrived on
            // (audit R4), so the client decodes a real reply, not an EOF.
            _ => Self::invariant_rejection(listener),
        }
    }

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

    fn ordinary_reply(output: nexus::SignalOutput) -> Result<signal_lojix::schema::lib::Output> {
        match output {
            nexus::SignalOutput::OrdinaryOutput(output) => Ok(output),
            nexus::SignalOutput::MetaOutput(_) => Err(Error::UnexpectedFrame),
        }
    }

    fn meta_reply(output: nexus::SignalOutput) -> Result<meta_signal_lojix::schema::lib::Output> {
        match output {
            nexus::SignalOutput::MetaOutput(output) => Ok(output),
            nexus::SignalOutput::OrdinaryOutput(_) => Err(Error::UnexpectedFrame),
        }
    }
}

/// A bounded pool of connection permits — a counting semaphore (`Mutex` +
/// `Condvar`) capping concurrently-served requests. `acquire` blocks (applying
/// backpressure to the accept loop) when the cap is reached; the returned
/// [`ConnectionPermit`] releases its slot on drop, when the worker thread exits.
struct ConnectionPermits {
    available: Mutex<usize>,
    released: Condvar,
}

impl ConnectionPermits {
    fn new(count: usize) -> Self {
        Self {
            available: Mutex::new(count),
            released: Condvar::new(),
        }
    }

    fn acquire(self: Arc<Self>) -> ConnectionPermit {
        let mut available = self
            .available
            .lock()
            .expect("connection-permit mutex poisoned");
        while *available == 0 {
            available = self
                .released
                .wait(available)
                .expect("connection-permit condvar poisoned");
        }
        *available -= 1;
        drop(available);
        ConnectionPermit { permits: self }
    }

    fn release(&self) {
        let mut available = self
            .available
            .lock()
            .expect("connection-permit mutex poisoned");
        *available += 1;
        self.released.notify_one();
    }
}

struct ConnectionPermit {
    permits: Arc<ConnectionPermits>,
}

impl Drop for ConnectionPermit {
    fn drop(&mut self) {
        self.permits.release();
    }
}
