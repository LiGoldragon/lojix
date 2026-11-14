//! The lojix daemon loop — two authority-tiered unix sockets, each input
//! answered through its contract's generated dispatch.
//!
//! Two `AsyncListenerSocket`s are bound on one `AsyncMultiListenerDaemon` (the
//! runtime's two-socket primitive), each tagged by its authority role. A
//! connection's length-prefixed frame is restored as its contract's `Input`
//! and answered with `input.answer(&answerer)`; the output is the whole reply.
//! A deploy or test admitted there runs on its own actor. A deployment watch
//! keeps its connection and streams each matching phase event beside the
//! dispatch.

use crate::Payload;
use crate::answering::{Answerer, Answers as _};
use crate::schema_runtime::{
    DaemonRuntime as _, DeployDriving as _, DeployJobResuming as _, DetachedActivation as _,
    DetachedObserving as _, RuntimeCore as _, TestDriving as _,
};
use crate::{
    DeploymentLedger as _, DurableStore as _, GenerationLedger as _, NexusPersistable as _,
};
use std::fmt::{Display, Formatter};
use std::io::Write;
use std::sync::Arc;
use std::time::Duration;

use kameo::actor::{Actor, ActorRef, Spawn};
use kameo::error::Infallible;
use kameo::message::{Context, Message};
use meta_signal_lojix::Answerable as _;
use signal::{AsyncFrameReading, AsyncFrameWriting, ByteViewable, FrameBody, FrameCapacity};
use signal_lojix::Answerable as _;
use triad_runtime::{
    AcceptedConnection, AsyncListenerSocket, AsyncMultiConnectionRuntime, AsyncMultiListenerDaemon,
    AsyncMultiListenerDaemonError, ConnectionContext, PeerIdentity, RequestConcurrencyLimit,
    RequestErrorLog, SocketMode, UnixCredentials,
};

// The inbound request-frame body cap comes from the shared Signal frame
// capacity (8 MiB). A lojix request is a few hundred bytes; the cap bounds a
// hostile length prefix far below the 4 GiB a bare u32 prefix could
// pre-allocate (audit R1).

/// How long the daemon waits for a connected client to send its request frame
/// before dropping the stream — bounds the connect-and-never-send wedge of the
/// serial accept loop (audit R2). A legitimate client sends immediately.
const REQUEST_READ_TIMEOUT: Duration = Duration::from_secs(10);

/// Reading one meta-socket frame body as the request it archives.
trait MetaRequestFraming {
    fn meta_request(&self) -> Result<meta_signal_lojix::Input>;
}

impl MetaRequestFraming for [u8] {
    fn meta_request(&self) -> Result<meta_signal_lojix::Input> {
        use signal::{Restorable, Signal};
        Signal::<meta_signal_lojix::Input>::from(self.to_vec())
            .restore()
            .map_err(|fault| Error::Wire(format!("{fault:?}")))
    }
}

/// Writing one meta-socket reply as the frame body that carries it.
trait MetaResponseFraming {
    fn meta_frame(self) -> Result<Vec<u8>>;
}

impl MetaResponseFraming for meta_signal_lojix::Output {
    fn meta_frame(self) -> Result<Vec<u8>> {
        use signal::{ByteViewable, Signalizable};
        self.signalize()
            .map(|signal| signal.bytes().to_vec())
            .map_err(|fault| Error::Wire(format!("{fault:?}")))
    }
}

/// Writing one ordinary-socket reply as the frame body that carries it.
trait OrdinaryResponseFraming {
    fn ordinary_frame(self) -> Result<Vec<u8>>;
}

impl OrdinaryResponseFraming for signal_lojix::Output {
    fn ordinary_frame(self) -> Result<Vec<u8>> {
        use signal::{ByteViewable, Signalizable};
        self.signalize()
            .map(|signal| signal.bytes().to_vec())
            .map_err(|fault| Error::Wire(format!("{fault:?}")))
    }
}

/// What woke a deployment watch's connection.
enum WatchTurn {
    /// A matching phase event, or `None` once an `Unwatch` ended the watch.
    Observed(Option<memory::DeploymentPhaseEvent>),
    PeerClosed,
    PeerSpoke,
}

use crate::deployment_watch::{
    DeploymentWatchStream, StreamsDeploymentEvents as _, WatchesDeployments as _,
};
use crate::runtime_model as memory;
use crate::schema_runtime::{RuntimeConfiguration, SchemaRuntime};
use crate::{Error, LojixNexusConfigurable as _, NexusConfiguration, Result, Store};

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
/// arriving signal. [`EnvironmentConstructible`] discovers its stable state;
/// [`Runnable`] then binds both sockets and serves forever.
pub struct Daemon {
    configuration: NexusConfiguration,
    state_database_path: std::path::PathBuf,
}

/// Capability to discover the stable Lojix Memory and desired configuration.
pub trait EnvironmentConstructible {
    fn from_environment() -> Result<Self>
    where
        Self: Sized;
}

/// Capability to run a configured Nexus until process termination.
pub trait Runnable {
    fn run(self) -> Result<()>;
}

impl Runnable for Daemon {
    fn run(self) -> Result<()> {
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()?
            .block_on(run_daemon(self))
    }
}

/// The one event a supervisor, an operator or a test may wait on to know that a
/// Nexus is serving: both listeners bound, both started, nothing yet accepted.
/// It is announced once, on standard output, in the same parenthesised form the
/// offline tools use for their terminals.
///
/// A Nexus announces this because the alternative is a clock. A deadline that
/// holds on a developer's machine is a lie on a loaded builder, and a waiter
/// that believes it reports a startup failure that did not happen.
pub trait NexusReadiness {
    /// The head of the announcement. A waiter matches on this and reads the
    /// bound socket paths that follow.
    const READY: &'static str = "(LojixNexusReady";

    /// Say, once, that both listeners are bound and serving.
    fn announce_readiness(&self) -> Result<()>;
}

impl NexusReadiness for NexusConfiguration {
    fn announce_readiness(&self) -> Result<()> {
        let mut standard_output = std::io::stdout().lock();
        writeln!(
            standard_output,
            "{} {} {})",
            Self::READY,
            self.ordinary_socket_path.0,
            self.owner_socket_path.0
        )?;
        standard_output.flush()?;
        Ok(())
    }
}

async fn run_daemon(daemon_configuration: Daemon) -> Result<()> {
    let configuration = daemon_configuration.configuration;
    configuration.validate()?;
    let ordinary_socket_mode =
        u32::try_from(configuration.ordinary_socket_mode.0).map_err(|_| {
            Error::InvalidNexusConfiguration("ordinary socket mode is outside u32".into())
        })?;
    let owner_socket_mode = u32::try_from(configuration.owner_socket_mode.0)
        .map_err(|_| Error::InvalidNexusConfiguration("meta socket mode is outside u32".into()))?;
    for path in [
        &configuration.ordinary_socket_path.0,
        &configuration.owner_socket_path.0,
    ] {
        if let Some(parent) = std::path::Path::new(path).parent() {
            std::fs::create_dir_all(parent)?;
        }
    }
    std::fs::create_dir_all(&configuration.state_directory_path.0)?;
    let sockets = vec![
        AsyncListenerSocket::new(
            ListenerRole::Ordinary,
            configuration.ordinary_socket_path.0.clone(),
        )
        .with_socket_mode(SocketMode::new(ordinary_socket_mode)),
        AsyncListenerSocket::new(
            ListenerRole::Owner,
            configuration.owner_socket_path.0.clone(),
        )
        .with_socket_mode(SocketMode::new(owner_socket_mode)),
    ];
    let runtime = LojixRuntime::new(
        RuntimeConfiguration::from(&configuration),
        daemon_configuration.state_database_path,
    )
    .await?;
    let request_error_log = RequestErrorLog::new("lojix-nexus");
    let daemon = AsyncMultiListenerDaemon::new(sockets, runtime, request_error_log)
        .with_concurrency_limit(RequestConcurrencyLimit::new(MAXIMUM_CONCURRENT_REQUESTS))
        .bind()
        .await
        .map_err(|error| Error::from(AsyncMultiListenerDaemonError::Listener(error)))?;
    daemon
        .start()
        .await
        .map_err(|error| Error::from(AsyncMultiListenerDaemonError::Start(error)))?;
    // Both listeners are bound and started and nothing has been accepted yet:
    // this is the moment a waiter is entitled to know about, so the Nexus says
    // so rather than leaving a supervisor or a test to guess it from a clock.
    configuration.announce_readiness()?;

    let mut terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
    let mut interrupt = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt())?;
    loop {
        tokio::select! {
            _ = terminate.recv() => break,
            _ = interrupt.recv() => break,
            result = daemon.serve_next_connection_at(0) => {
                result.map_err(|error| Error::from(AsyncMultiListenerDaemonError::Listener(error)))?;
            }
            result = daemon.serve_next_connection_at(1) => {
                result.map_err(|error| Error::from(AsyncMultiListenerDaemonError::Listener(error)))?;
            }
        }
    }
    daemon
        .stop()
        .await
        .map_err(|error| Error::from(AsyncMultiListenerDaemonError::Stop(error)))
}

impl From<AsyncMultiListenerDaemonError<Error>> for Error {
    fn from(error: AsyncMultiListenerDaemonError<Error>) -> Self {
        match error {
            AsyncMultiListenerDaemonError::Listener(listener_error) => Self::SignalFrame(
                signal::FrameError::Io(std::io::Error::other(listener_error.to_string())),
            ),
            AsyncMultiListenerDaemonError::Start(error)
            | AsyncMultiListenerDaemonError::Stop(error) => error,
        }
    }
}

impl EnvironmentConstructible for Daemon {
    fn from_environment() -> Result<Self> {
        let default_configuration = NexusConfiguration::built_in();
        let state_database_path = NexusConfiguration::discover_store_path()?;
        if let Some(parent) = state_database_path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let store =
            Store::open_with_default_configuration(&state_database_path, default_configuration)?;
        let configuration = store.nexus_configuration_state()?.desired_configuration;
        Ok(Self {
            configuration,
            state_database_path,
        })
    }
}

/// Maximum number of requests served concurrently per listener. Each accepted
/// connection is handled in an actor-runtime Tokio task, so a long owner deploy
/// never blocks ordinary socket admission (intent 2alg); this caps live work so
/// a flood cannot exhaust resources.
const MAXIMUM_CONCURRENT_REQUESTS: usize = 64;

/// Maximum number of deploy pipelines running concurrently on the daemon-owned
/// deploy-job executor (up9q). Decoupled from `MAXIMUM_CONCURRENT_REQUESTS`:
/// the per-connection request permit now covers only the short submit-reply, so
/// a long-running deploy no longer holds a request permit for its whole run.
/// This separate cap bounds concurrent pipelines; over it, a `Deploy` is
/// refused with the typed `DeploymentInFlight` rejection rather than queued
/// unbounded. A deploy is a heavyweight nix build + activation, so the bound is
/// small relative to the request cap.
const MAXIMUM_CONCURRENT_DEPLOYS: usize = 8;

/// The `AsyncMultiConnectionRuntime` realization. Owns the SHARED durable
/// `Store` (the concurrency point — locked only briefly per memory operation), the frame
/// codec, and no local listener/thread machinery. The actor runtime admits
/// requests through per-listener gates and spawns each connection task, so BOTH
/// sockets remain responsive while deploys run (intent 2alg, resolving audit
/// 29's serial-model question). Per-request in-flight state lives on the
/// connection's own `SchemaRuntime`, never shared.
struct LojixRuntime {
    store: Arc<Store>,
    configuration: Arc<RuntimeConfiguration>,
    capacity: FrameCapacity,
    owner_authority: OwnerPeerAuthority,
    /// The daemon-owned deploy-job and test-job executors. Their `ActorRef`s
    /// live here on the runtime (daemon-lifetime), NOT on a connection task,
    /// so an admitted job's pipeline outlives the owner connection that
    /// submitted it (up9q): a dropped client ends only its own answer.
    executors: JobExecutors,
}

trait RuntimeConstructible {
    async fn new(
        configuration: RuntimeConfiguration,
        state_database_path: std::path::PathBuf,
    ) -> Result<Self>
    where
        Self: Sized;
}

impl RuntimeConstructible for LojixRuntime {
    async fn new(
        configuration: RuntimeConfiguration,
        state_database_path: std::path::PathBuf,
    ) -> Result<Self> {
        let store = Arc::new(Store::open(state_database_path)?);
        let configuration = Arc::new(configuration);
        let deploy_slots = JobSlots::with_capacity(MAXIMUM_CONCURRENT_DEPLOYS);
        let deploy_jobs =
            DeployJobs::start(store.clone(), configuration.clone(), deploy_slots.clone()).await;
        // Read any persisted in-flight deploy-job rows and reconcile them on
        // start (up9q durable resume scaffolding). A clean shutdown leaves no
        // rows; rows present mean a deploy was in flight when the daemon
        // stopped, so the actor decides per row whether to poll the activation
        // unit, re-drive the pipeline, or drop a stale terminal row. A startup
        // reconcile failure is non-fatal: the durable rows remain for the next
        // start, and the daemon still serves new requests.
        let _ = deploy_jobs.ask(ReconcilePersistedJobs).await;
        let test_slots = JobSlots::with_capacity(MAXIMUM_CONCURRENT_DEPLOYS);
        let test_jobs = TestJobs::start().await;
        Ok(Self {
            store,
            configuration,
            capacity: FrameCapacity::default(),
            owner_authority: OwnerPeerAuthority::current_process(),
            executors: JobExecutors {
                deploy_jobs,
                deploy_slots,
                test_jobs,
                test_slots,
            },
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

/// Capability to establish and enforce the privileged peer identity.
pub trait PeerAuthorizable {
    fn new(user_id: u32, group_id: u32) -> Self
    where
        Self: Sized;
    fn current_process() -> Self
    where
        Self: Sized;
    fn authorize(&self, context: &ConnectionContext) -> Result<()>;
    fn authorize_unix_credentials(&self, credentials: &UnixCredentials) -> Result<()>;
}

impl PeerAuthorizable for OwnerPeerAuthority {
    fn new(user_id: u32, group_id: u32) -> Self {
        Self { user_id, group_id }
    }

    fn current_process() -> Self {
        Self::new(
            rustix::process::geteuid().as_raw(),
            rustix::process::getegid().as_raw(),
        )
    }

    fn authorize(&self, context: &ConnectionContext) -> Result<()> {
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
            capacity: self.capacity,
            owner_authority: self.owner_authority,
            executors: self.executors.clone(),
        };
        worker.serve(listener, connection).await
    }
}

/// One request, served on its own actor-runtime task and answered by an
/// answerer over a clone of the shared `Store`, so an admitted job's cursor
/// is never shared across concurrent connections (intent 2alg).
struct RequestWorker {
    store: Arc<Store>,
    configuration: Arc<RuntimeConfiguration>,
    capacity: FrameCapacity,
    owner_authority: OwnerPeerAuthority,
    /// The job executors an admitted `Deploy` or `Test` is handed to; its
    /// pipeline runs on the actor, not this connection task.
    executors: JobExecutors,
}

trait RequestServable {
    async fn serve(self, listener: ListenerRole, connection: AcceptedConnection) -> Result<()>;
    async fn serve_ordinary(&self, connection: &mut AcceptedConnection) -> Result<()>;
    async fn serve_deployment_watch(
        &self,
        connection: &mut AcceptedConnection,
        watch: DeploymentWatchStream,
    ) -> Result<()>;
    async fn serve_owner(&self, connection: &mut AcceptedConnection) -> Result<()>;
    async fn read_body(&self, connection: &mut AcceptedConnection) -> Result<FrameBody>;
    /// The answerer for this request: the shared store, and the executors an
    /// admitted job is handed to.
    fn answerer(&self) -> Answerer;
}

impl RequestServable for RequestWorker {
    async fn serve(self, listener: ListenerRole, mut connection: AcceptedConnection) -> Result<()> {
        match listener {
            ListenerRole::Ordinary => self.serve_ordinary(&mut connection).await,
            ListenerRole::Owner => self.serve_owner(&mut connection).await,
        }
    }

    async fn serve_ordinary(&self, connection: &mut AcceptedConnection) -> Result<()> {
        let body = self.read_body(connection).await?;
        use signal::{Restorable, Signal};
        let input = Signal::<signal_lojix::Input>::from(body.bytes().to_vec())
            .restore()
            .map_err(|fault| Error::Wire(format!("{fault:?}")))?;
        let reply = input.answer(&self.answerer());
        // A deployment watch the answer opened is claimed before anything else
        // can fail, so a watcher is never left registered with no connection:
        // the claimed stream removes it when dropped.
        let watch = match &reply {
            signal_lojix::Output::DeploymentsWatched(opened) => self
                .store
                .deployment_watchers()
                .claim(*opened.subscription_token.payload()),
            _ => None,
        };
        connection
            .stream_mut()
            .write_frame(&FrameBody::from(reply.ordinary_frame()?), self.capacity)
            .await?;
        // An ordinary input closes after its one reply; a deployment watch
        // keeps its connection and writes each matching event.
        match watch {
            Some(watch) => self.serve_deployment_watch(connection, watch).await,
            None => Ok(()),
        }
    }

    /// Write each committed phase event the watch matches as one
    /// `DeploymentPhaseObserved` frame, until the peer closes or an `Unwatch`
    /// ends the watch. Either way the watch is dropped, which removes its
    /// watcher, the end is logged, and the connection closes. The peer sends
    /// nothing after its request; reading it only tells when it goes.
    async fn serve_deployment_watch(
        &self,
        connection: &mut AcceptedConnection,
        mut watch: DeploymentWatchStream,
    ) -> Result<()> {
        use tokio::io::AsyncReadExt;
        let subscription_token = watch.subscription_token();
        let (mut peer, mut writer) = connection.stream_mut().split();
        let mut unread = [0_u8; 256];
        let end = loop {
            let turn = tokio::select! {
                event = watch.next_event() => WatchTurn::Observed(event),
                read = peer.read(&mut unread) => match read {
                    Ok(0) | Err(_) => WatchTurn::PeerClosed,
                    Ok(_) => WatchTurn::PeerSpoke,
                },
            };
            match turn {
                WatchTurn::Observed(Some(event)) => {
                    let frame =
                        signal_lojix::Output::DeploymentPhaseObserved(event).ordinary_frame()?;
                    if writer
                        .write_frame(&FrameBody::from(frame), self.capacity)
                        .await
                        .is_err()
                    {
                        break "PeerClosed";
                    }
                }
                WatchTurn::Observed(None) => break "Unwatched",
                WatchTurn::PeerClosed => break "PeerClosed",
                WatchTurn::PeerSpoke => {}
            }
        };
        drop(watch);
        eprintln!("lojix-nexus: DeploymentWatchEnded.{{ {subscription_token} {end} }}");
        Ok(())
    }

    async fn serve_owner(&self, connection: &mut AcceptedConnection) -> Result<()> {
        self.owner_authority.authorize(connection.context())?;
        let body = self.read_body(connection).await?;
        let input = body.bytes().meta_request()?;
        // A `Deploy` or `Test` is admitted in the answer and handed to its
        // actor, which owns the pipeline; this task only writes the answer.
        let frame = input.answer(&self.answerer()).meta_frame()?;
        connection
            .stream_mut()
            .write_frame(&FrameBody::from(frame), self.capacity)
            .await?;
        Ok(())
    }

    async fn read_body(&self, connection: &mut AcceptedConnection) -> Result<FrameBody> {
        tokio::time::timeout(
            REQUEST_READ_TIMEOUT,
            connection.stream_mut().read_frame(self.capacity),
        )
        .await
        .map_err(|_| Error::RequestReadTimedOut)?
        .map_err(Error::SignalFrame)
    }

    fn answerer(&self) -> Answerer {
        Answerer::over(
            self.store.clone(),
            self.configuration.clone(),
            self.executors.clone(),
        )
    }
}

/// A count of jobs an executor runs at once. An admission reserves a slot
/// before it writes anything; the slot travels with the job to its actor and
/// is freed when the job's pipeline ends, however it ends.
#[derive(Clone)]
pub struct JobSlots(Arc<tokio::sync::Semaphore>);

/// One reserved slot, held by the pipeline it was reserved for; dropping it
/// frees the slot.
pub struct JobSlot {
    _permit: tokio::sync::OwnedSemaphorePermit,
}

/// Reserving a slot without waiting: an executor at its cap refuses at once.
pub trait JobSlotReserving {
    fn with_capacity(capacity: usize) -> Self;
    fn reserve(&self) -> Option<JobSlot>;
}

impl JobSlotReserving for JobSlots {
    fn with_capacity(capacity: usize) -> Self {
        Self(Arc::new(tokio::sync::Semaphore::new(capacity)))
    }

    fn reserve(&self) -> Option<JobSlot> {
        self.0
            .clone()
            .try_acquire_owned()
            .ok()
            .map(|permit| JobSlot { _permit: permit })
    }
}

/// The daemon's two job executors as an admission sees them: the slots it
/// reserves before admitting, and the actors that run what it admitted.
#[derive(Clone)]
pub struct JobExecutors {
    pub deploy_jobs: ActorRef<DeployJobs>,
    pub deploy_slots: JobSlots,
    pub test_jobs: ActorRef<TestJobs>,
    pub test_slots: JobSlots,
}

/// Admitting a job against its executor's cap, and handing an admitted job's
/// engine to the actor that runs it.
pub trait JobLaunching {
    fn reserve_deploy(&self) -> Option<JobSlot>;
    fn reserve_test(&self) -> Option<JobSlot>;
    fn launch_deploy(&self, engine: SchemaRuntime, slot: JobSlot);
    fn launch_test(&self, engine: SchemaRuntime, slot: JobSlot);
}

impl JobLaunching for JobExecutors {
    fn reserve_deploy(&self) -> Option<JobSlot> {
        self.deploy_slots.reserve()
    }

    fn reserve_test(&self) -> Option<JobSlot> {
        self.test_slots.reserve()
    }

    /// The deployment is already durable, its job row at `Submitted`. Should
    /// the actor be gone, the daemon is stopping, and the next start resumes
    /// the row.
    fn launch_deploy(&self, engine: SchemaRuntime, slot: JobSlot) {
        if self
            .deploy_jobs
            .tell(LaunchDeploy { engine, slot })
            .try_send()
            .is_err()
        {
            eprintln!("lojix-nexus: DeployNotLaunched (the deploy-job actor is gone)");
        }
    }

    fn launch_test(&self, engine: SchemaRuntime, slot: JobSlot) {
        if self
            .test_jobs
            .tell(LaunchTest { engine, slot })
            .try_send()
            .is_err()
        {
            eprintln!("lojix-nexus: TestNotLaunched (the test-job actor is gone)");
        }
    }
}

/// The daemon-owned deploy-job executor (up9q surface a). A kameo actor whose
/// `ActorRef` lives on `LojixRuntime` for the daemon's whole lifetime, NOT on
/// any connection task. Each deploy admitted by an answer is handed here with
/// its seeded engine and its slot, and the actor launches the deploy pipeline
/// as an independent runtime task (decoupled from the owner connection), so a
/// dropped client cannot cancel an in-flight deploy. On start it reconciles
/// the job rows a previous run left.
pub struct DeployJobs {
    store: Arc<Store>,
    configuration: Arc<RuntimeConfiguration>,
    /// The deploy slots this executor shares with admission; a resumed job
    /// takes one as an admitted job does.
    slots: JobSlots,
}

trait DeployProcessable {
    async fn start(
        store: Arc<Store>,
        configuration: Arc<RuntimeConfiguration>,
        slots: JobSlots,
    ) -> ActorRef<DeployJobs>;
    fn launch_pipeline(&self, engine: SchemaRuntime, slot: JobSlot);
    async fn reconcile_persisted_jobs(&mut self);
    fn daemon_host_system_closure() -> Option<String>;
    fn persist_reconciled_self_switch(
        &self,
        deployment_identifier: i64,
        generation: crate::runtime_model::LiveGeneration,
        root: crate::runtime_model::GcRoot,
    );
}

impl DeployProcessable for DeployJobs {
    async fn start(
        store: Arc<Store>,
        configuration: Arc<RuntimeConfiguration>,
        slots: JobSlots,
    ) -> ActorRef<Self> {
        let actor = Self::spawn_with_mailbox(
            Self {
                store,
                configuration,
                slots,
            },
            kameo::mailbox::unbounded(),
        );
        actor.wait_for_startup().await;
        actor
    }

    /// Launch one admitted deploy's pipeline as an independent daemon runtime
    /// task and return immediately. The task owns the seeded engine and drives
    /// the full effect chain to completion, holding its slot until then.
    /// Because the task is spawned on the runtime (not the owner connection's
    /// task), dropping the connection never cancels it. THIS is the
    /// decoupling.
    fn launch_pipeline(&self, mut engine: SchemaRuntime, slot: JobSlot) {
        tokio::spawn(async move {
            let terminal = engine.drive_submitted_deploy().await;
            let _ = terminal;
            eprintln!("lojix deploy pipeline reached a terminal state");
            drop(slot);
        });
    }

    /// Read persisted in-flight deploy-job rows on start and decide each one's
    /// reconcile action (up9q durable resume). A row present means a deploy was
    /// mid-flight at the last shutdown. The typed [`crate::schema_runtime`]
    /// `DeployJobResumption` verdict (poll the activation unit, re-drive the
    /// pipeline, or drop a stale terminal row) is computed per row here; the
    /// LIVE continuation behind each verdict is proven on a real target at S5,
    /// so this start path computes and (for terminal rows) clears, leaving live
    /// resumption to S5. Pre-activation rows are dropped here so they do not
    /// wedge the cap; they are re-submittable by the operator.
    async fn reconcile_persisted_jobs(&mut self) {
        let Ok(jobs) = self.store.deploy_jobs() else {
            return;
        };
        let daemon_host = self.configuration.daemon_host();
        for job in jobs {
            let deployment_identifier = *job.deployment_identifier.payload();
            if let Some(unit) = job.detached_test_activation_unit(daemon_host) {
                // Test activation is deliberately non-persistent. Its retained
                // PID-1 receipt, not /nix/var/nix/profiles/system, is the only
                // authority for the successor's terminal outcome.
                // Subscribe before the first GetUnit lookup. systemd-run's
                // accepted transient can still be in PID 1's registration
                // window while the candidate has already replaced us.
                let Ok(observer) = unit.prepare_observer().await else {
                    // An observer transport error is not a terminal activation
                    // outcome. Leave the durable Activating row for recovery.
                    continue;
                };
                match observer.initial_outcome().await {
                    Ok(Some(crate::schema_runtime::DetachedActivationOutcome::Succeeded)) => {
                        if self
                            .store
                            .terminalize_deployment(
                                deployment_identifier,
                                memory::DeploymentTerminal::Succeeded,
                            )
                            .is_ok()
                        {
                            unit.clean().await;
                        }
                    }
                    Ok(
                        outcome @ Some(
                            crate::schema_runtime::DetachedActivationOutcome::Failed(_)
                            | crate::schema_runtime::DetachedActivationOutcome::Missing,
                        ),
                    ) => {
                        // PID 1 either reported the failure or no longer
                        // holds the unit; either way the unit's journal tail
                        // is what the activation itself printed.
                        let observed = match outcome {
                            Some(crate::schema_runtime::DetachedActivationOutcome::Failed(
                                observed,
                            )) => observed,
                            _ => unit.vanished(),
                        };
                        let observed = unit
                            .journalled(observed, self.configuration.effect_execution())
                            .await;
                        let failure_evidence_option =
                            Some(crate::schema_runtime::Witnessing::witness(&observed));
                        if self
                            .store
                            .terminalize_deployment(
                                deployment_identifier,
                                memory::DeploymentTerminal::Failed(memory::DeploymentFailure {
                                    deployment_failure_stage:
                                        memory::DeploymentFailureStage::Activate,
                                    deployment_terminal_reason:
                                        memory::DeploymentTerminalReason::ActivationFailed,
                                    failure_evidence_option,
                                }),
                            )
                            .is_ok()
                        {
                            unit.clean().await;
                        }
                    }
                    Ok(None | Some(crate::schema_runtime::DetachedActivationOutcome::Running)) => {
                        // A registration window (None) or a unit still running.
                        // The exact observer owns both the queued UnitNew and
                        // the subscribe-before-read terminal-state watcher.
                        observer.observe(
                            self.store.clone(),
                            deployment_identifier,
                            self.configuration.effect_execution().clone(),
                        );
                    }
                    Err(_) => {
                        // Same as an observer interruption: no invented
                        // terminal result from a failed systemd observation.
                    }
                }
                continue;
            }
            match job.resumption() {
                // A detached self-switch (bead primary-7u8p): a persisted host
                // `ActivateNow` through `NixosSystemdBootV1` targeting the
                // daemon's OWN host restarts the daemon inside the switch, so
                // the terminal activation write never committed. Only that exact
                // persisted submission can take this repair, and only a
                // COMPLETED self-switch has set the live system profile to this
                // job's closure. `self_switch_activation_record` requires both
                // predicates before it considers that exact profile match; a
                // racy unrelated restart during SetBootProfile, boot-once, test,
                // or user-environment activation therefore falls through to the
                // S5 arm. Recording the generation the interrupted pipeline
                // could not also advances the live-set-scanned id allocator,
                // ending the id reuse.
                crate::schema_runtime::DeployJobResumption::PollActivationUnit { unit: None }
                    if job.node_name == *daemon_host =>
                {
                    let system_closure = Self::daemon_host_system_closure();
                    match job.self_switch_activation_record(system_closure.as_deref()) {
                        Some((generation, root)) => {
                            self.persist_reconciled_self_switch(
                                deployment_identifier,
                                generation,
                                root,
                            );
                        }
                        None => {
                            // Not a completed host self-switch (the live system
                            // profile does not resolve to this closure): leave the
                            // row for the S5 unit-poll path rather than recording
                            // it as a Current complete-host generation.
                        }
                    }
                }
                crate::schema_runtime::DeployJobResumption::PollActivationUnit { .. } => {
                    // S5: poll `journalctl -u <unit>` and adopt the unit's
                    // outcome rather than re-activating. Until then leave the
                    // row so the operator sees the in-flight activation.
                }
                crate::schema_runtime::DeployJobResumption::RestartPipeline => {
                    let Some(slot) = self.slots.reserve() else {
                        continue;
                    };
                    let mut engine = SchemaRuntime::with_store_and_configuration(
                        self.store.clone(),
                        self.configuration.clone(),
                    );
                    match engine.resume_deploy_job(job) {
                        Ok(true) => {
                            self.launch_pipeline(engine, slot);
                        }
                        Ok(false) => {
                            let _ = self.store.retract_deploy_job(deployment_identifier);
                        }
                        Err(_) => {
                            // Preserve the original durable cursor for the next
                            // start; a failed reconstruction must not erase an
                            // accepted deployment.
                        }
                    }
                }
                crate::schema_runtime::DeployJobResumption::AlreadyTerminal => {
                    // Terminal correlation is durable; the stale convenience
                    // cursor can be retired without losing the public outcome.
                    let _ = self.store.retract_deploy_job(deployment_identifier);
                }
            }
        }
    }

    /// The store path the daemon host's live system profile currently resolves
    /// to (`/nix/var/nix/profiles/system` -> the running system closure), or
    /// `None` when it cannot be read. A completed host self-switch set exactly
    /// this to the deploy's closure (`nix-env --set`) before the switch
    /// restarted the daemon, so a reconcile uses it as the tight witness that an
    /// interrupted `Activating` job really finished as a host switch (bead
    /// primary-7u8p).
    fn daemon_host_system_closure() -> Option<String> {
        std::fs::canonicalize("/nix/var/nix/profiles/system")
            .ok()
            .map(|path| path.display().to_string())
    }

    /// Persist a reconciled self-switch generation idempotently and fail-safe.
    /// A recovered generation is not a completed deployment until its public
    /// terminal-success transition has been journalled and acknowledged. The
    /// terminal transition retires the private cursor in that acknowledgement
    /// commit, so neither an activation-store failure nor a terminal-write
    /// failure can erase the only durable retry witness.
    fn persist_reconciled_self_switch(
        &self,
        deployment_identifier: i64,
        generation: crate::runtime_model::LiveGeneration,
        root: crate::runtime_model::GcRoot,
    ) {
        let already_recorded = self
            .store
            .records::<crate::runtime_model::GcRoot>()
            .map(|roots| {
                roots.iter().any(|existing| {
                    existing.generation_identifier == generation.generation_identifier
                })
            })
            .unwrap_or(false);
        if !already_recorded && self.store.record_activation(generation, root).is_err() {
            return;
        }
        let _ = self
            .store
            .terminalize_deployment(deployment_identifier, memory::DeploymentTerminal::Succeeded);
    }
}

impl Actor for DeployJobs {
    type Args = Self;
    type Error = Infallible;

    async fn on_start(
        jobs: Self::Args,
        _actor_reference: ActorRef<Self>,
    ) -> std::result::Result<Self, Self::Error> {
        Ok(jobs)
    }
}

/// An admitted deploy, handed to the executor with the engine that holds its
/// cursor and the slot reserved for it.
pub struct LaunchDeploy {
    engine: SchemaRuntime,
    slot: JobSlot,
}

impl Message<LaunchDeploy> for DeployJobs {
    type Reply = ();

    async fn handle(
        &mut self,
        message: LaunchDeploy,
        _context: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.launch_pipeline(message.engine, message.slot);
    }
}

/// Read and reconcile persisted in-flight deploy-job rows on daemon start.
pub struct ReconcilePersistedJobs;

impl Message<ReconcilePersistedJobs> for DeployJobs {
    type Reply = ();

    async fn handle(
        &mut self,
        _message: ReconcilePersistedJobs,
        _context: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.reconcile_persisted_jobs().await;
    }
}

/// The daemon-owned TEST-job executor (Unit 2b, the test analogue of
/// [`DeployJobs`]). A kameo actor whose `ActorRef` lives on `LojixRuntime` for
/// the daemon's whole lifetime, NOT on any connection task. Each test admitted
/// by an answer is handed here with its seeded engine and its slot, and the
/// actor launches the dispatch pipeline — the real hermetic `nix build` of
/// `vm-<node>`, or the gated live cycle — as an independent runtime task, so a
/// dropped client cannot cancel an in-flight test. The pipeline rewrites the
/// durable row to a terminal `Passed`/`Failed`, read over `QueryTestRuns`.
pub struct TestJobs;

trait TestProcessable {
    async fn start() -> ActorRef<TestJobs>;
    fn launch_pipeline(&self, engine: SchemaRuntime, slot: JobSlot);
}

impl TestProcessable for TestJobs {
    async fn start() -> ActorRef<Self> {
        let actor = Self::spawn_with_mailbox(Self, kameo::mailbox::unbounded());
        actor.wait_for_startup().await;
        actor
    }

    /// Launch one admitted test's dispatch pipeline as an independent daemon
    /// runtime task and return immediately. The task owns the seeded engine,
    /// drives the real dispatch (`drive_submitted_test`) to its terminal
    /// outcome, and holds its slot until then. Because the task is spawned on
    /// the runtime (not the owner connection's task), dropping the connection
    /// never cancels it — the decoupling.
    fn launch_pipeline(&self, mut engine: SchemaRuntime, slot: JobSlot) {
        tokio::spawn(async move {
            let terminal = engine.drive_submitted_test().await;
            let _ = terminal;
            eprintln!("lojix test pipeline reached a terminal state");
            drop(slot);
        });
    }
}

impl Actor for TestJobs {
    type Args = Self;
    type Error = Infallible;

    async fn on_start(
        jobs: Self::Args,
        _actor_reference: ActorRef<Self>,
    ) -> std::result::Result<Self, Self::Error> {
        Ok(jobs)
    }
}

/// An admitted test, handed to the executor with the engine that holds its
/// cursor and the slot reserved for it.
pub struct LaunchTest {
    engine: SchemaRuntime,
    slot: JobSlot,
}

impl Message<LaunchTest> for TestJobs {
    type Reply = ();

    async fn handle(
        &mut self,
        message: LaunchTest,
        _context: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.launch_pipeline(message.engine, message.slot);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime_model as ordinary;
    use crate::{EventHistory as _, IdentifierAllocating as _};

    const CLOSURE: &str = "/nix/store/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-self-switch";
    const REVISION: &str = "0123456789abcdef0123456789abcdef01234567";

    fn self_switch_submission() -> memory::ActualizedDeploySubmission {
        memory::ActualizedDeploySubmission {
            horizon_definition_option: None,
            deploy_submission: memory::DeploySubmission::Host(memory::HostDeployment {
                cluster_name: ordinary::ClusterName::new("fixture-cluster".into()),
                node_name: ordinary::NodeName::new("fixture-daemon".into()),
                host_composition: ordinary::HostComposition::CompleteHost,
                proposal_source: ordinary::ProposalSource::new("/dev/null".into()),
                secrets_input: memory::SecretsInput::NoSecrets,
                flake_reference: ordinary::FlakeReference::new(format!(
                    "github:LiGoldragon/CriomOS?rev={REVISION}"
                )),
                deployment_transport: memory::DeploymentTransport {
                    nix_store_uri: memory::NixStoreUri::new("ssh-ng://fixture-copy.invalid".into()),
                    ssh_destination: memory::SshDestination::new(
                        "root@fixture-daemon.invalid".into(),
                    ),
                },
                deployment_input_mode: memory::DeploymentInputMode::Direct,
                deployment_output_selector: memory::DeploymentOutputSelector::new(
                    memory::FlakeAttribute::new(
                        "nixosConfigurations.fixture-daemon.config.system.build.toplevel"
                            .to_string(),
                    ),
                ),
                activation_backend: memory::ActivationBackend::NixosSystemdBootV1,
                host_deploy_action: ordinary::HostDeployAction::ActivateNow,
                source_revision_policy: ordinary::SourceRevisionPolicy::RequireImmutable,
                nix_builder_spec_option: None,
                extra_substituter_vector: Vec::new(),
            }),
        }
    }

    fn self_switch_job() -> memory::DeployJob {
        memory::DeployJob {
            deployment_identifier: ordinary::DeploymentIdentifier::new(0),
            generation_identifier: ordinary::GenerationIdentifier::new(0),
            cluster_name: ordinary::ClusterName::new("fixture-cluster".into()),
            node_name: ordinary::NodeName::new("fixture-daemon".into()),
            deploy_job_phase: memory::DeployJobPhase::Submitted,
            closure_path_option: Some(ordinary::ClosurePath::new(CLOSURE.into())),
            source_revision_policy: ordinary::SourceRevisionPolicy::RequireImmutable,
            flake_reference: ordinary::FlakeReference::new(format!(
                "github:LiGoldragon/CriomOS?rev={REVISION}"
            )),
            flake_reference_option: Some(ordinary::FlakeReference::new(format!(
                "github:LiGoldragon/CriomOS?rev={REVISION}"
            ))),
            resolved_revision_option: Some(crate::runtime_model::ResolvedRevision(
                REVISION.to_string(),
            )),
            deployment_transport: memory::DeploymentTransport {
                nix_store_uri: memory::NixStoreUri::new("ssh-ng://fixture-copy.invalid".into()),
                ssh_destination: memory::SshDestination::new("root@fixture-daemon.invalid".into()),
            },
            deployment_input_mode: memory::DeploymentInputMode::Direct,
            deployment_output_selector: memory::DeploymentOutputSelector::new(
                memory::FlakeAttribute::new(
                    "nixosConfigurations.fixture-daemon.config.system.build.toplevel".to_string(),
                ),
            ),
            activation_backend: memory::ActivationBackend::NixosSystemdBootV1,
            nix_builder_spec_option: None,
            boot_once_unit_option: None,
            generation_slot_option: Some(ordinary::GenerationSlot::Current),
            persisted_flake_input_override_vector: Vec::new(),
            deploy_resume_stage: memory::DeployResumeStage::ResolveFlakeAuth,
            phase_receipt_option: None,
            actualized_deploy_submission_option: Some(self_switch_submission()),
        }
    }

    fn self_switch_identity() -> ordinary::DeploymentRequestIdentity {
        ordinary::DeploymentRequestIdentity {
            deployment_environment: ordinary::DeploymentEnvironment::HostEnvironment,
            cluster_name: ordinary::ClusterName::new("fixture-cluster".into()),
            node_name: ordinary::NodeName::new("fixture-daemon".into()),
            generation_artifact: ordinary::GenerationArtifact::CompleteHost,
            requested_deployment_action: ordinary::RequestedDeploymentAction::Host(
                ordinary::HostDeployAction::ActivateNow,
            ),
            activation_effect: ordinary::ActivationEffect::LiveActivation,
            source_revision_policy: ordinary::SourceRevisionPolicy::RequireImmutable,
            immutable_revision_option: Some(ordinary::ImmutableRevision::new(REVISION.into())),
        }
    }

    #[test]
    fn successor_reconciles_self_switch_as_current_and_public_terminal_success() {
        let directory = tempfile::tempdir().expect("temporary store directory");
        let store =
            Arc::new(Store::open(directory.path().join("lojix.memory")).expect("open store"));
        let mut job = self_switch_job();
        let record = store
            .allocate_deployment_record(self_switch_identity(), job.clone())
            .expect("admit interrupted self-switch");
        let deployment_identifier = *record.deployment_identifier.payload();
        job.deployment_identifier = record.deployment_identifier;
        job.generation_identifier = record.generation_identifier;

        let copying_position = store
            .allocate_event_log_position()
            .expect("reserve copying event position");
        job.deploy_job_phase = memory::DeployJobPhase::Copying;
        job.deploy_resume_stage = memory::DeployResumeStage::ActivateGeneration;
        store
            .advance_deployment_phase(
                deployment_identifier,
                ordinary::DeploymentPhase::Copying,
                job.clone(),
                ordinary::DeploymentPhaseEvent {
                    deployment_identifier: record.deployment_identifier,
                    generation_identifier: record.generation_identifier,
                    cluster_name: ordinary::ClusterName::new("fixture-cluster".into()),
                    node_name: ordinary::NodeName::new("fixture-daemon".into()),
                    deployment_phase: ordinary::DeploymentPhase::Copying,
                    event_log_position: ordinary::EventLogPosition::new(copying_position),
                    transition_marker: ordinary::TransitionMarker {
                        database_marker: ordinary::DatabaseMarker {
                            commit_sequence: ordinary::CommitSequence::new(0),
                            state_digest: ordinary::StateDigest::new(0),
                        },
                    },
                    immutable_revision_option: Some(ordinary::ImmutableRevision::new(
                        REVISION.into(),
                    )),
                    deployment_terminal_option: None,
                },
            )
            .expect("persist copying boundary");
        job.deploy_job_phase = memory::DeployJobPhase::Activating;
        job.deploy_resume_stage = memory::DeployResumeStage::RecordGenerationActivated;
        store
            .upsert_deploy_job(job.clone())
            .expect("persist activating cursor");

        let (generation, root) = job
            .self_switch_activation_record(Some(CLOSURE))
            .expect("matching live system profile proves the switch completed");
        let successor = DeployJobs {
            store: store.clone(),
            configuration: Arc::new(RuntimeConfiguration::test_default()),
            slots: JobSlots::with_capacity(1),
        };
        successor.persist_reconciled_self_switch(deployment_identifier, generation, root);

        assert!(
            store
                .records::<crate::runtime_model::LiveGeneration>()
                .expect("current live generation")
                .iter()
                .any(|generation| {
                    *generation.deployment_identifier.payload() == deployment_identifier
                        && matches!(
                            generation.generation_slot,
                            ordinary::GenerationSlot::Current
                        )
                })
        );
        let record = store
            .deployment_records()
            .expect("public deployment record")
            .into_iter()
            .find(|record| *record.deployment_identifier.payload() == deployment_identifier)
            .expect("reconciled deployment record");
        assert!(matches!(
            record.deployment_phase,
            ordinary::DeploymentPhase::Completed
        ));
        assert!(matches!(
            record.deployment_terminal_option,
            Some(ordinary::DeploymentTerminal::Succeeded)
        ));
        assert!(store.event_log_in_range(0, i64::MAX).expect("terminal event").iter().any(
            |entry| matches!(
                &entry.logged_event,
                ordinary::LoggedEvent::Deployment(event)
                    if *event.deployment_identifier.payload() == deployment_identifier
                        && matches!(event.deployment_phase, ordinary::DeploymentPhase::Completed)
                        && matches!(
                            event.deployment_terminal_option,
                            Some(ordinary::DeploymentTerminal::Succeeded)
                        )
            )
        ));
        assert!(store.deploy_jobs().expect("reconciled cursor").is_empty());
    }
}
