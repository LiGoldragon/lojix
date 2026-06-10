//! The lojix daemon loop — two authority-tiered unix sockets driving the
//! generated Nexus runner.
//!
//! Resolves the port-plan §4.4 blocker by binding two `ListenerSocket`s on one
//! `ActorMultiListenerDaemon` (the runtime's two-socket primitive), each tagged by
//! its authority role. `handle_stream` decodes the length-prefixed wire frame
//! for the arriving role into a `SignalInput`, drives it through
//! awaits `NexusEngine::execute` (which runs the `Runner` continuation loop —
//! the deploy pipeline included), and encodes the reply back. The schema engine
//! is the single source of routing truth; there is no inline request `Store`.

use std::fmt::{Display, Formatter};
use std::sync::Arc;
use std::time::Duration;

use triad_runtime::{
    AcceptedConnection, ActorListenerSocket, ActorMultiConnectionRuntime, ActorMultiListenerDaemon,
    ActorMultiListenerDaemonError, FrameBody, LengthPrefixedCodec, MaximumFrameLength,
    RequestConcurrencyLimit, RequestErrorLog, SocketMode,
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
use crate::schema_runtime::{RuntimeConfiguration, SchemaRuntime};
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
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()?
            .block_on(self.run_async())
    }

    async fn run_async(self) -> Result<()> {
        let configuration = self.configuration;
        Self::validate_owner_socket_mode(configuration.owner_socket_mode)?;
        let sockets = vec![
            ActorListenerSocket::new(
                ListenerRole::Ordinary,
                configuration.ordinary_socket_path.clone(),
            )
            .with_socket_mode(SocketMode::new(configuration.ordinary_socket_mode)),
            ActorListenerSocket::new(ListenerRole::Owner, configuration.owner_socket_path.clone())
                .with_socket_mode(SocketMode::new(configuration.owner_socket_mode)),
        ];
        let runtime = LojixRuntime::new(RuntimeConfiguration::from_daemon_configuration(
            &configuration,
        ));
        let request_error_log = RequestErrorLog::new("lojix-daemon");
        ActorMultiListenerDaemon::new(sockets, runtime, request_error_log)
            .with_concurrency_limit(RequestConcurrencyLimit::new(MAXIMUM_CONCURRENT_REQUESTS))
            .run()
            .await
            .map_err(Self::map_daemon_error)
    }

    fn map_daemon_error(error: ActorMultiListenerDaemonError<Error>) -> Error {
        match error {
            ActorMultiListenerDaemonError::Listener(listener_error) => Error::SignalFrame(
                triad_runtime::FrameError::Io(std::io::Error::other(listener_error.to_string())),
            ),
            ActorMultiListenerDaemonError::Start(error)
            | ActorMultiListenerDaemonError::Stop(error) => error,
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

/// Maximum number of requests served concurrently per listener. Each accepted
/// connection is handled in an actor-runtime Tokio task, so a long owner deploy
/// never blocks ordinary socket admission (intent 2alg); this caps live work so
/// a flood cannot exhaust resources.
const MAXIMUM_CONCURRENT_REQUESTS: usize = 64;

/// The `ActorMultiConnectionRuntime` realization. Owns the SHARED durable `Store` (the
/// concurrency point — locked only briefly per sema operation), the frame
/// codec, and no local listener/thread machinery. The actor runtime admits
/// requests through per-listener gates and spawns each connection task, so BOTH
/// sockets remain responsive while deploys run (intent 2alg, resolving audit
/// 29's serial-model question). Per-request in-flight state lives on the
/// connection's own `SchemaRuntime`, never shared.
struct LojixRuntime {
    store: Arc<Store>,
    configuration: Arc<RuntimeConfiguration>,
    codec: LengthPrefixedCodec,
}

impl LojixRuntime {
    fn new(configuration: RuntimeConfiguration) -> Self {
        Self {
            store: Arc::new(Store::new()),
            configuration: Arc::new(configuration),
            codec: LengthPrefixedCodec::new(MaximumFrameLength::new(MAXIMUM_REQUEST_FRAME_BYTES)),
        }
    }
}

impl ActorMultiConnectionRuntime for LojixRuntime {
    type Listener = ListenerRole;
    type Error = Error;

    async fn start(&self) -> Result<()> {
        Ok(())
    }

    async fn stop(&self) -> Result<()> {
        Ok(())
    }

    async fn handle_connection(
        &self,
        listener: Self::Listener,
        connection: AcceptedConnection,
    ) -> Result<()> {
        let worker = RequestWorker {
            store: self.store.clone(),
            configuration: self.configuration.clone(),
            codec: self.codec,
        };
        worker.serve(listener, connection).await
    }
}

/// One request, served on its own actor-runtime task. Builds a fresh per-request
/// `SchemaRuntime` over a clone of the shared `Store`, so the in-flight deploy
/// cursor is never shared across concurrent connections (intent 2alg).
struct RequestWorker {
    store: Arc<Store>,
    configuration: Arc<RuntimeConfiguration>,
    codec: LengthPrefixedCodec,
}

impl RequestWorker {
    async fn serve(self, listener: ListenerRole, mut connection: AcceptedConnection) -> Result<()> {
        match listener {
            ListenerRole::Ordinary => self.serve_ordinary(&mut connection).await,
            ListenerRole::Owner => self.serve_owner(&mut connection).await,
        }
    }

    async fn serve_ordinary(&self, connection: &mut AcceptedConnection) -> Result<()> {
        let body = self.read_body(connection).await?;
        let (_, input) = signal_lojix::schema::lib::Input::decode_signal_frame(body.bytes())?;
        let output = self
            .execute_request(
                ListenerRole::Ordinary,
                nexus::SignalInput::OrdinaryInput(input),
            )
            .await;
        let reply = Self::ordinary_reply(output)?;
        self.codec
            .write_body_async(
                connection.stream_mut(),
                &FrameBody::new(reply.encode_signal_frame()?),
            )
            .await?;
        Ok(())
    }

    async fn serve_owner(&self, connection: &mut AcceptedConnection) -> Result<()> {
        let body = self.read_body(connection).await?;
        let (_, input) = meta_signal_lojix::schema::lib::Input::decode_signal_frame(body.bytes())?;
        let output = self
            .execute_request(ListenerRole::Owner, nexus::SignalInput::MetaInput(input))
            .await;
        let reply = Self::meta_reply(output)?;
        self.codec
            .write_body_async(
                connection.stream_mut(),
                &FrameBody::new(reply.encode_signal_frame()?),
            )
            .await?;
        Ok(())
    }

    async fn read_body(&self, connection: &mut AcceptedConnection) -> Result<FrameBody> {
        tokio::time::timeout(
            REQUEST_READ_TIMEOUT,
            self.codec.read_body_async(connection.stream_mut()),
        )
        .await
        .map_err(|_| Error::RequestReadTimedOut)?
        .map_err(Error::SignalFrame)
    }

    async fn execute_request(
        &self,
        listener: ListenerRole,
        signal_input: nexus::SignalInput,
    ) -> nexus::SignalOutput {
        Self::execute_with_store(
            self.store.clone(),
            self.configuration.clone(),
            listener,
            signal_input,
        )
        .await
    }

    /// Build a per-request engine over the shared `Store` and drive it. The
    /// generated runner is async, and child-process effects are awaited through
    /// Tokio process handles rather than hidden behind a blocking-pool bridge.
    /// The engine's in-flight cursor is local to this call, so concurrent
    /// requests never corrupt each other's deploy state (intent 2alg).
    async fn execute_with_store(
        store: Arc<Store>,
        configuration: Arc<RuntimeConfiguration>,
        listener: ListenerRole,
        signal_input: nexus::SignalInput,
    ) -> nexus::SignalOutput {
        let mut engine = SchemaRuntime::with_store_and_configuration(store, configuration);
        let work =
            nexus::NexusWork::SignalArrived(signal_input).with_origin_route(nexus::OriginRoute(0));
        match engine.execute(work).await.into_root() {
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
