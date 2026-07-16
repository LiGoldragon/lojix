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

use kameo::actor::{Actor, ActorRef, Spawn};
use kameo::error::Infallible;
use kameo::message::{Context, Message};
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
use crate::schema_runtime::{
    DeploySubmissionOutcome, RuntimeConfiguration, SchemaRuntime, TestSubmissionOutcome,
};
use crate::{DaemonConfiguration, Error, Result, Store};
use meta_signal_lojix::schema::lib as meta;

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
        )
        .await?;
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
    /// The daemon-owned deploy-job executor. Its `ActorRef` lives here on the
    /// runtime (daemon-lifetime), NOT on a connection task, so an admitted
    /// deploy's pipeline outlives the owner connection that submitted it
    /// (up9q): a dropped client kills only the short submit-reply task.
    deploy_jobs: ActorRef<DeployJobs>,
    /// The daemon-owned test-job executor (Unit 2b). Same decoupling property
    /// as `deploy_jobs`: an admitted test's dispatch pipeline (the real
    /// hermetic `nix build`, or the gated live cycle) outlives the owner
    /// connection that submitted it.
    test_jobs: ActorRef<TestJobs>,
}

impl LojixRuntime {
    async fn new(
        configuration: RuntimeConfiguration,
        state_database_path: std::path::PathBuf,
    ) -> Result<Self> {
        let store = Arc::new(Store::open(state_database_path)?);
        let configuration = Arc::new(configuration);
        let deploy_jobs = DeployJobs::start(
            store.clone(),
            configuration.clone(),
            MAXIMUM_CONCURRENT_DEPLOYS,
        )
        .await;
        // Read any persisted in-flight deploy-job rows and reconcile them on
        // start (up9q durable resume scaffolding). A clean shutdown leaves no
        // rows; rows present mean a deploy was in flight when the daemon
        // stopped, so the actor decides per row whether to poll the activation
        // unit, re-drive the pipeline, or drop a stale terminal row. A startup
        // reconcile failure is non-fatal: the durable rows remain for the next
        // start, and the daemon still serves new requests.
        match deploy_jobs.ask(ReconcilePersistedJobs).await {
            Ok(RecoveryAdmission::Recovered) => {}
            Ok(RecoveryAdmission::Rejected(message)) => {
                return Err(Error::RecoveryAdmission(message));
            }
            Err(error) => return Err(Error::RecoveryAdmission(error.to_string())),
        }
        let test_jobs = TestJobs::start(
            store.clone(),
            configuration.clone(),
            MAXIMUM_CONCURRENT_DEPLOYS,
        )
        .await;
        Ok(Self {
            store,
            configuration,
            codec: LengthPrefixedCodec::new(MaximumFrameLength::new(MAXIMUM_REQUEST_FRAME_BYTES)),
            owner_authority: OwnerPeerAuthority::current_process(),
            deploy_jobs,
            test_jobs,
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
            deploy_jobs: self.deploy_jobs.clone(),
            test_jobs: self.test_jobs.clone(),
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
    /// The daemon-owned deploy-job executor's handle. A `Deploy` request hands
    /// the submission here and replies the accepted handle; the pipeline runs
    /// on the actor, not this connection task.
    deploy_jobs: ActorRef<DeployJobs>,
    /// The daemon-owned test-job executor's handle (Unit 2b). A `Test` request
    /// hands the submission here and replies the accepted handle; the dispatch
    /// runs on the actor, not this connection task.
    test_jobs: ActorRef<TestJobs>,
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
        // A `Deploy` decouples from this connection task: the deploy-job actor
        // owns the pipeline, this task only submits and replies the accepted
        // handle. Pin/Unpin/Retire are fast single writes and stay synchronous
        // on this task (up9q surface a — only Deploy decouples).
        let reply = match input {
            meta::Input::Deploy(request) => self.submit_deploy(request.into_payload()).await,
            // A `Test` decouples from this connection task exactly like a
            // `Deploy` (Unit 2b): the test-job actor owns the dispatch pipeline
            // (the real `nix build` of the hermetic check, or the gated live
            // cycle), this task only submits and replies the accepted handle.
            meta::Input::Test(request) => self.submit_test(request.into_payload()).await,
            other => {
                let output = self
                    .execute_request(ListenerRole::Owner, nexus::SignalInput::MetaInput(other))
                    .await;
                Self::meta_reply(output)?
            }
        };
        self.codec
            .write_body_async(
                connection.stream_mut(),
                &FrameBody::new(reply.encode_signal_frame()?),
            )
            .await?;
        Ok(())
    }

    /// Submit a `Deploy` to the daemon-owned deploy-job actor and return the
    /// immediate wire reply (up9q surface a). The actor checks the deploy-job
    /// cap, runs the synchronous submit (issue identifier, persist the
    /// `Submitted` job row), spawns the pipeline on the daemon runtime, and
    /// replies the `DeployHandle` handle — all before any pipeline effect
    /// runs. When the cap is full it replies `DeploymentInFlight`. This task's
    /// only remaining work is writing this reply frame; dropping it (a client
    /// disconnect) cannot cancel the spawned pipeline.
    async fn submit_deploy(&self, request: meta::DeployRequest) -> meta::Output {
        match self.deploy_jobs.ask(AdmitDeploy { request }).await {
            Ok(DeployAdmission::Accepted(accepted)) => {
                meta::Output::DeployAccepted(meta::DeployAccepted::new(accepted))
            }
            Ok(DeployAdmission::Rejected(rejected)) => {
                meta::Output::DeployRejected(meta::DeployRejected::new(rejected))
            }
            // The deploy-job actor is daemon-lifetime; a send error means the
            // runtime is tearing down. Reply a typed internal rejection rather
            // than dropping the connection without a frame.
            Err(_) => {
                meta::Output::DeployRejected(meta::DeployRejected::new(meta::RejectedDeploy {
                    deploy_rejection_reason: meta::DeployRejectionReason::InternalError,
                    database_marker: Self::zero_marker(),
                }))
            }
        }
    }

    /// Submit a `Test` to the daemon-owned test-job actor and return the
    /// immediate wire reply (Unit 2b, mirroring `submit_deploy`). The actor runs
    /// the synchronous submit (lower + validate + persist the Pending row),
    /// spawns the dispatch pipeline (the real hermetic `nix build`, or the gated
    /// live cycle) on the daemon runtime, and replies the `AcceptedTest` handle
    /// — all before the build runs. Dropping this task (a client disconnect)
    /// cannot cancel the spawned pipeline; the result lands durably and is read
    /// over the ordinary `(ByTestRun …)` query.
    async fn submit_test(&self, request: meta::TestRequest) -> meta::Output {
        match self.test_jobs.ask(AdmitTest { request }).await {
            Ok(TestAdmission::Accepted(accepted)) => {
                meta::Output::Tested(meta::Tested::new(accepted))
            }
            Ok(TestAdmission::Rejected(rejected)) => {
                meta::Output::TestRejected(meta::TestRejected::new(rejected))
            }
            Err(_) => meta::Output::TestRejected(meta::TestRejected::new(meta::RejectedTest {
                test_rejection_reason: meta::TestRejectionReason::InternalError,
                database_marker: Self::zero_marker(),
            })),
        }
    }

    fn zero_marker() -> meta::DatabaseMarker {
        meta::DatabaseMarker {
            commit_sequence: signal_lojix::schema::lib::CommitSequence::new(0),
            state_digest: signal_lojix::schema::lib::StateDigest::new(0),
        }
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

/// The daemon-owned deploy-job executor (up9q surface a). A kameo actor whose
/// `ActorRef` lives on [`LojixRuntime`] for the daemon's whole lifetime, NOT on
/// any connection task. It owns the deploy-job admission cap and the in-flight
/// count; on each accepted `Deploy` it runs the synchronous submit and then
/// launches the deploy pipeline as an independent runtime task (decoupled from
/// the owner connection), so a dropped client cannot cancel an in-flight
/// deploy. The per-deploy pipeline task is a daemon-owned `tokio::spawn` the
/// actor tracks via its count; a later iteration can promote it to a supervised
/// child actor without changing the decoupling property this actor guarantees.
pub struct DeployJobs {
    store: Arc<Store>,
    configuration: Arc<RuntimeConfiguration>,
    /// Maximum concurrent deploy pipelines. Over it, `AdmitDeploy` replies
    /// `DeploymentInFlight` (a real `DeployRejectionReason`).
    cap: usize,
    /// Pipelines currently running. Incremented on admit, decremented when the
    /// pipeline task reports `DeployCompleted`. Single-writer (the actor), so
    /// the cap check and the increment are atomic with no shared lock.
    active_count: usize,
}

impl DeployJobs {
    pub async fn start(
        store: Arc<Store>,
        configuration: Arc<RuntimeConfiguration>,
        cap: usize,
    ) -> ActorRef<Self> {
        let actor = Self::spawn(Self {
            store,
            configuration,
            cap,
            active_count: 0,
        });
        actor.wait_for_startup().await;
        actor
    }

    fn at_capacity(&self) -> bool {
        self.active_count >= self.cap
    }

    fn deployment_in_flight_rejection(&self) -> meta::RejectedDeploy {
        let commit_sequence = self.store.commit_sequence().unwrap_or(0);
        meta::RejectedDeploy {
            deploy_rejection_reason: meta::DeployRejectionReason::DeploymentInFlight,
            database_marker: meta::DatabaseMarker {
                commit_sequence: signal_lojix::schema::lib::CommitSequence::new(commit_sequence),
                state_digest: signal_lojix::schema::lib::StateDigest::new(commit_sequence),
            },
        }
    }

    /// Launch one admitted deploy's pipeline as an independent daemon runtime
    /// task and return immediately. The task owns the seeded engine and drives
    /// the full effect chain to completion, then reports `DeployCompleted` so
    /// the actor frees a cap slot. Because the task is spawned on the runtime
    /// (not the owner connection's task), dropping the connection — or the
    /// short submit-reply task — never cancels it. THIS is the decoupling.
    fn launch_pipeline(&self, mut engine: SchemaRuntime, jobs: ActorRef<DeployJobs>) {
        tokio::spawn(async move {
            let terminal = engine.drive_submitted_deploy().await;
            eprintln!("lojix deploy pipeline terminal output: {terminal:?}");
            // Free the cap slot. `tell` is safe: `DeployCompleted` has an
            // infallible `()` reply, so it cannot crash the actor.
            let _ = jobs.tell(DeployCompleted).await;
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
    fn reconcile_persisted_jobs(&mut self, jobs: ActorRef<DeployJobs>) -> RecoveryAdmission {
        let persisted = match self.store.deploy_jobs() {
            Ok(jobs) => jobs,
            Err(error) => return RecoveryAdmission::Rejected(error.to_string()),
        };
        let daemon_host = self.configuration.daemon_host();
        for job in persisted {
            let deployment_identifier = *job.deployment_identifier.payload();
            match job.resumption() {
                crate::schema_runtime::DeployJobResumption::PollActivationUnit { unit: None }
                    if job.node_name == *daemon_host =>
                {
                    let system_closure = Self::daemon_host_system_closure();
                    if let Some((generation, root)) =
                        job.self_switch_activation_record(system_closure.as_deref())
                    {
                        if let Err(error) = self.persist_reconciled_self_switch(
                            deployment_identifier,
                            generation,
                            root,
                        ) {
                            return RecoveryAdmission::Rejected(error.to_string());
                        }
                    } else if let Err(error) = self.resolve_unrestartable_job(job) {
                        return RecoveryAdmission::Rejected(error.to_string());
                    }
                }
                crate::schema_runtime::DeployJobResumption::RestartPipeline => {
                    if self.at_capacity() {
                        return RecoveryAdmission::Rejected(
                            "persisted deploy jobs exceed recovery capacity".to_string(),
                        );
                    }
                    let engine = SchemaRuntime::from_recovered_deploy_job(
                        self.store.clone(),
                        self.configuration.clone(),
                        job,
                    );
                    self.active_count += 1;
                    self.launch_pipeline(engine, jobs.clone());
                }
                crate::schema_runtime::DeployJobResumption::PollActivationUnit { .. } => {
                    if let Err(error) = self.resolve_unrestartable_job(job) {
                        return RecoveryAdmission::Rejected(error.to_string());
                    }
                }
                crate::schema_runtime::DeployJobResumption::AlreadyTerminal => {}
            }
        }
        RecoveryAdmission::Recovered
    }

    /// An activation whose durable cursor cannot prove a safe continuation is
    /// retained as an explicit Failed resolution, never reactivated or dropped.
    fn resolve_unrestartable_job(&self, mut job: crate::schema::sema::DeployJob) -> Result<()> {
        job.phase = crate::schema::sema::DeployJobPhase::Failed;
        self.store.upsert_deploy_job(job)
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

    /// Persist a reconciled self-switch generation idempotently and fail-safe
    /// (bead primary-7u8p audit). If a prior reconcile already recorded this
    /// generation (its gc-root is present) but crashed before retracting the job
    /// row, just drop the row. Otherwise record, and retract the resume cursor
    /// ONLY on a successful write: a genuine store failure keeps the row so the
    /// next restart retries, rather than silently dropping the generation and
    /// reviving the lost-generation / id-reuse bug this fix closes.
    fn persist_reconciled_self_switch(
        &self,
        deployment_identifier: u64,
        generation: crate::schema::sema::LiveGeneration,
        root: crate::schema::sema::GcRoot,
    ) -> Result<()> {
        let already_recorded = self
            .store
            .gc_roots()
            .map(|roots| {
                roots.iter().any(|existing| {
                    existing.generation_identifier == generation.generation_identifier
                })
            })
            .unwrap_or(false);
        if already_recorded {
            self.store.retract_deploy_job(deployment_identifier)?;
            return Ok(());
        }
        self.store.record_activation(generation, root)?;
        self.store.retract_deploy_job(deployment_identifier)?;
        Ok(())
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

/// Submit a `Deploy` to the executor: check the cap, run the synchronous
/// submit, and on accept launch the pipeline. The reply is the immediate
/// admission verdict.
pub struct AdmitDeploy {
    pub request: meta::DeployRequest,
}

/// The immediate admission verdict for an `AdmitDeploy` — the wire reply the
/// daemon sends the owner connection before any pipeline effect runs.
#[derive(Debug, Clone, PartialEq, Eq, kameo::Reply)]
pub enum DeployAdmission {
    Accepted(meta::DeployHandle),
    Rejected(meta::RejectedDeploy),
}

impl Message<AdmitDeploy> for DeployJobs {
    type Reply = DeployAdmission;

    async fn handle(
        &mut self,
        message: AdmitDeploy,
        context: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        if self.at_capacity() {
            return DeployAdmission::Rejected(self.deployment_in_flight_rejection());
        }
        let mut engine = SchemaRuntime::with_store_and_configuration(
            self.store.clone(),
            self.configuration.clone(),
        );
        match engine.submit_deploy(message.request) {
            DeploySubmissionOutcome::Accepted(accepted) => {
                self.active_count += 1;
                self.launch_pipeline(engine, context.actor_ref().clone());
                DeployAdmission::Accepted(accepted)
            }
            DeploySubmissionOutcome::Rejected(rejected) => DeployAdmission::Rejected(rejected),
        }
    }
}

/// One deploy pipeline finished (success or failure). Frees a cap slot.
pub struct DeployCompleted;

impl Message<DeployCompleted> for DeployJobs {
    type Reply = ();

    async fn handle(
        &mut self,
        _message: DeployCompleted,
        _context: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.active_count = self.active_count.saturating_sub(1);
    }
}

/// Startup recovery verdict. A rejected recovery prevents listener construction.
#[derive(Debug, Clone, PartialEq, Eq, kameo::Reply)]
pub enum RecoveryAdmission {
    Recovered,
    Rejected(String),
}

/// Read and reconcile persisted in-flight deploy-job rows on daemon start.
pub struct ReconcilePersistedJobs;

impl Message<ReconcilePersistedJobs> for DeployJobs {
    type Reply = RecoveryAdmission;

    async fn handle(
        &mut self,
        _message: ReconcilePersistedJobs,
        context: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.reconcile_persisted_jobs(context.actor_ref().clone())
    }
}

/// The daemon-owned TEST-job executor (Unit 2b, the test analogue of
/// [`DeployJobs`]). A kameo actor whose `ActorRef` lives on [`LojixRuntime`] for
/// the daemon's whole lifetime, NOT on any connection task. On each accepted
/// `Test` it runs the synchronous submit (lower + validate + persist the
/// Pending row), then launches the dispatch pipeline — the real hermetic
/// `nix build` of `vm-<node>`, or the gated live cycle — as an independent
/// runtime task, so a dropped client cannot cancel an in-flight test. The
/// pipeline rewrites the durable row to a terminal `Passed`/`Failed`, read over
/// the ordinary `(ByTestRun …)` query.
pub struct TestJobs {
    store: Arc<Store>,
    configuration: Arc<RuntimeConfiguration>,
    cap: usize,
    active_count: usize,
}

impl TestJobs {
    pub async fn start(
        store: Arc<Store>,
        configuration: Arc<RuntimeConfiguration>,
        cap: usize,
    ) -> ActorRef<Self> {
        let actor = Self::spawn(Self {
            store,
            configuration,
            cap,
            active_count: 0,
        });
        actor.wait_for_startup().await;
        actor
    }

    fn at_capacity(&self) -> bool {
        self.active_count >= self.cap
    }

    fn substrate_unavailable_rejection(&self) -> meta::RejectedTest {
        let commit_sequence = self.store.commit_sequence().unwrap_or(0);
        meta::RejectedTest {
            test_rejection_reason: meta::TestRejectionReason::SubstrateUnavailable,
            database_marker: meta::DatabaseMarker {
                commit_sequence: signal_lojix::schema::lib::CommitSequence::new(commit_sequence),
                state_digest: signal_lojix::schema::lib::StateDigest::new(commit_sequence),
            },
        }
    }

    /// Launch one admitted test's dispatch pipeline as an independent daemon
    /// runtime task and return immediately. The task owns the seeded engine and
    /// drives the real dispatch (`drive_submitted_test`) to its terminal
    /// outcome, then reports `TestCompleted` so the actor frees a cap slot.
    /// Because the task is spawned on the runtime (not the owner connection's
    /// task), dropping the connection never cancels it — the decoupling.
    fn launch_pipeline(&self, mut engine: SchemaRuntime, jobs: ActorRef<TestJobs>) {
        tokio::spawn(async move {
            let terminal = engine.drive_submitted_test().await;
            eprintln!("lojix test pipeline terminal output: {terminal:?}");
            let _ = jobs.tell(TestCompleted).await;
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

/// Submit a `Test` to the executor: check the cap, run the synchronous submit,
/// and on accept launch the dispatch pipeline. The reply is the immediate
/// admission verdict the daemon sends the owner connection.
pub struct AdmitTest {
    pub request: meta::TestRequest,
}

/// The immediate admission verdict for an `AdmitTest` — the wire reply the
/// daemon sends before any dispatch effect runs.
#[derive(Debug, Clone, PartialEq, Eq, kameo::Reply)]
pub enum TestAdmission {
    Accepted(meta::AcceptedTest),
    Rejected(meta::RejectedTest),
}

impl Message<AdmitTest> for TestJobs {
    type Reply = TestAdmission;

    async fn handle(
        &mut self,
        message: AdmitTest,
        context: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        if self.at_capacity() {
            return TestAdmission::Rejected(self.substrate_unavailable_rejection());
        }
        let mut engine = SchemaRuntime::with_store_and_configuration(
            self.store.clone(),
            self.configuration.clone(),
        );
        match engine.submit_test(message.request).await {
            TestSubmissionOutcome::Accepted(accepted) => {
                self.active_count += 1;
                self.launch_pipeline(engine, context.actor_ref().clone());
                TestAdmission::Accepted(accepted)
            }
            TestSubmissionOutcome::Rejected(rejected) => TestAdmission::Rejected(rejected),
        }
    }
}

/// One test dispatch finished (success or failure). Frees a cap slot.
pub struct TestCompleted;

impl Message<TestCompleted> for TestJobs {
    type Reply = ();

    async fn handle(
        &mut self,
        _message: TestCompleted,
        _context: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.active_count = self.active_count.saturating_sub(1);
    }
}
