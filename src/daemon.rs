//! The lojix daemon loop — two authority-tiered unix sockets driving the
//! generated Nexus runner.
//!
//! Resolves the port-plan §4.4 blocker by binding two `AsyncListenerSocket`s on
//! one `AsyncMultiListenerDaemon` (the runtime's two-socket primitive), each
//! tagged by its authority role. `handle_stream` decodes the length-prefixed wire frame
//! for the arriving role into a `SignalInput`, drives it through
//! awaits `NexusEngine::execute` (which runs the `Runner` continuation loop —
//! the deploy pipeline included), and encodes the reply back. The schema engine
//! is the single source of routing truth; there is no inline request `Store`.

use std::fmt::{Display, Formatter};
use std::sync::Arc;
use std::time::Duration;

use triad_runtime::{
    AcceptedConnection, AsyncListenerSocket, AsyncMultiConnectionRuntime, AsyncMultiListenerDaemon,
    AsyncMultiListenerDaemonError, ConnectionContext, FrameBody, LengthPrefixedCodec,
    MaximumFrameLength, PeerIdentity, RequestConcurrencyLimit, RequestErrorLog, SocketMode,
    UnixCredentials,
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
/// Deploy/Pin/Unpin/Retire surface. Used as the
/// `AsyncMultiConnectionRuntime::Listener` tag so `handle_stream` decodes the
/// correct contract.
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
            AsyncListenerSocket::new(
                ListenerRole::Ordinary,
                configuration.ordinary_socket_path.clone(),
            )
            .with_socket_mode(SocketMode::new(configuration.ordinary_socket_mode)),
            AsyncListenerSocket::new(ListenerRole::Owner, configuration.owner_socket_path.clone())
                .with_socket_mode(SocketMode::new(configuration.owner_socket_mode)),
        ];
        // Open the durable sema-engine store under the state directory,
        // parallel to the `generated-inputs` subdir. Opening doubles as the
        // self-resume: an existing `lojix.sema` resumes its persisted catalog,
        // commit sequence, and records (ur16). Construction is fallible and its
        // Result propagates through `run`'s existing Result.
        let state_database_path =
            std::path::PathBuf::from(&configuration.state_directory_path).join("lojix.sema");
        let runtime = LojixRuntime::new(
            RuntimeConfiguration::from_daemon_configuration(&configuration),
            state_database_path,
        )?;
        let request_error_log = RequestErrorLog::new("lojix-daemon");
        AsyncMultiListenerDaemon::new(sockets, runtime, request_error_log)
            .with_concurrency_limit(RequestConcurrencyLimit::new(MAXIMUM_CONCURRENT_REQUESTS))
            .run()
            .await
            .map_err(Self::map_daemon_error)
    }

    fn map_daemon_error(error: AsyncMultiListenerDaemonError<Error>) -> Error {
        match error {
            AsyncMultiListenerDaemonError::Listener(listener_error) => Error::SignalFrame(
                triad_runtime::FrameError::Io(std::io::Error::other(listener_error.to_string())),
            ),
            AsyncMultiListenerDaemonError::Start(error)
            | AsyncMultiListenerDaemonError::Stop(error) => error,
        }
    }

    /// Refuse an owner-socket mode that grants any "other" access. The owner
    /// socket carries the privileged Deploy/Pin/Unpin/Retire surface; a
    /// permissive mode from config would silently make that surface
    /// world-reachable before the per-connection peer credential check runs.
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

/// The `AsyncMultiConnectionRuntime` realization. Owns the SHARED durable
/// `Store` (the concurrency point — locked only briefly per sema operation), the frame
/// codec, and no local listener/thread machinery. The actor runtime admits
/// requests through per-listener gates and spawns each connection task, so BOTH
/// sockets remain responsive while deploys run (intent 2alg, resolving audit
/// 29's serial-model question). Per-request in-flight state lives on the
/// connection's own `SchemaRuntime`, never shared.
struct LojixRuntime {
    store: Arc<Store>,
    configuration: Arc<RuntimeConfiguration>,
    codec: LengthPrefixedCodec,
    owner_authority: OwnerPeerAuthority,
}

impl LojixRuntime {
    fn new(
        configuration: RuntimeConfiguration,
        state_database_path: std::path::PathBuf,
    ) -> Result<Self> {
        Ok(Self {
            store: Arc::new(Store::open(state_database_path)?),
            configuration: Arc::new(configuration),
            codec: LengthPrefixedCodec::new(MaximumFrameLength::new(MAXIMUM_REQUEST_FRAME_BYTES)),
            owner_authority: OwnerPeerAuthority::current_process(),
        })
    }
}

/// The uid/gid policy for privileged owner-socket connections.
///
/// The kernel supplies each accepted Unix-stream peer credential through
/// `triad-runtime`; Lojix admits owner-socket requests only from the same
/// effective uid/gid that launched the daemon. TCP peers carry no Unix
/// credentials and are refused at this privileged surface. The daemon is
/// cluster-operator-owned, so deploy authority stays with that operator account
/// instead of trusting payload claims or socket mode alone.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct OwnerPeerAuthority {
    user_id: u32,
    group_id: u32,
}

impl OwnerPeerAuthority {
    pub const fn new(user_id: u32, group_id: u32) -> Self {
        Self { user_id, group_id }
    }

    pub fn current_process() -> Self {
        Self::new(
            rustix::process::geteuid().as_raw(),
            rustix::process::getegid().as_raw(),
        )
    }

    pub fn authorize(&self, context: &ConnectionContext) -> Result<()> {
        match context.peer() {
            PeerIdentity::Unix(credentials) => self.authorize_unix_credentials(credentials),
            PeerIdentity::Tcp(address) => Err(Error::UnauthorizedOwnerTcpPeer {
                peer_address: address.to_string(),
            }),
        }
    }

    fn authorize_unix_credentials(&self, credentials: &UnixCredentials) -> Result<()> {
        if credentials.user_id() == self.user_id && credentials.group_id() == self.group_id {
            return Ok(());
        }
        Err(Error::UnauthorizedOwnerPeer {
            peer_user_id: credentials.user_id(),
            peer_group_id: credentials.group_id(),
            daemon_user_id: self.user_id,
            daemon_group_id: self.group_id,
        })
    }
}

impl AsyncMultiConnectionRuntime for LojixRuntime {
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
            owner_authority: self.owner_authority,
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
    owner_authority: OwnerPeerAuthority,
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
        self.owner_authority.authorize(connection.context())?;
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
        let work = nexus::NexusWork::SignalArrived(signal_input)
            .with_origin_route(nexus::OriginRoute::new(0));
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
                    meta_signal_lojix::schema::lib::DeployRejected::new(
                        meta_signal_lojix::schema::lib::RejectedDeploy {
                            deploy_rejection_reason:
                                meta_signal_lojix::schema::lib::DeployRejectionReason::InternalError,
                            database_marker: meta_signal_lojix::schema::lib::DatabaseMarker {
                                commit_sequence: signal_lojix::schema::lib::CommitSequence::new(0),
                                state_digest: signal_lojix::schema::lib::StateDigest::new(0),
                            },
                        },
                    ),
                ),
            ),
            ListenerRole::Ordinary => nexus::SignalOutput::OrdinaryOutput(
                signal_lojix::schema::lib::Output::QueryRejected(
                    signal_lojix::schema::lib::QueryRejected::new(
                        signal_lojix::schema::lib::RejectedQuery {
                            query_rejection_reason:
                                signal_lojix::schema::lib::QueryRejectionReason::MalformedSelector,
                            database_marker: signal_lojix::schema::lib::DatabaseMarker {
                                commit_sequence: signal_lojix::schema::lib::CommitSequence::new(0),
                                state_digest: signal_lojix::schema::lib::StateDigest::new(0),
                            },
                        },
                    ),
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
