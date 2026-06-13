use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};

use kameo::Actor;
use kameo::actor::{ActorRef, Spawn};
use kameo::error::Infallible;
use kameo::message::{Context, Message};
use nix::unistd::{Group, chown};
use signal_frame::{
    ExchangeIdentifier, ExchangeLane, LaneSequence, NonEmpty, Reply as CoreReply, SessionEpoch,
    StreamEventIdentifier, StreamingFrameBody, SubReply, SubscriptionTokenInner,
};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadHalf, WriteHalf};
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::mpsc::{UnboundedReceiver, unbounded_channel};

use crate::error::{Error, Result};
use crate::runtime::{
    OpenDeploymentObservationStream, RuntimeConfiguration, RuntimeRequest, RuntimeRoot,
};
use crate::wire;

pub const DEFAULT_SOCKET_PATH: &str = "/run/lojix/daemon.sock";
const DEFAULT_SOCKET_MODE: u32 = 0o600;

pub struct SocketAddress {
    path: PathBuf,
}

impl SocketAddress {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self { path: path.into() }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }
}

pub struct Connection<IoStream> {
    stream: IoStream,
}

impl<IoStream> Connection<IoStream> {
    pub fn new(stream: IoStream) -> Self {
        Self { stream }
    }

    pub fn into_inner(self) -> IoStream {
        self.stream
    }
}

impl<IoStream> Connection<IoStream>
where
    IoStream: AsyncWrite + Unpin,
{
    pub async fn write_frame(&mut self, frame: &wire::Frame) -> Result<()> {
        let bytes = frame.encode_length_prefixed()?;
        self.stream.write_all(&bytes).await?;
        self.stream.flush().await?;
        Ok(())
    }
}

impl<IoStream> Connection<IoStream>
where
    IoStream: AsyncRead + Unpin,
{
    pub async fn read_frame(&mut self) -> Result<wire::Frame> {
        let mut length_bytes = [0_u8; 4];
        self.stream.read_exact(&mut length_bytes).await?;
        let length = u32::from_be_bytes(length_bytes) as usize;
        let mut payload = vec![0_u8; length];
        self.stream.read_exact(&mut payload).await?;

        let mut framed = Vec::with_capacity(4 + length);
        framed.extend_from_slice(&length_bytes);
        framed.extend_from_slice(&payload);
        Ok(wire::Frame::decode_length_prefixed(&framed)?)
    }
}

pub struct SocketServer {
    address: SocketAddress,
    socket_mode: u32,
    socket_group: Option<wire::UnixGroup>,
    state_directory: Option<PathBuf>,
    gc_root_directory: Option<PathBuf>,
    runtime_configuration: RuntimeConfiguration,
}

impl SocketServer {
    pub fn new(address: SocketAddress) -> Self {
        Self {
            address,
            socket_mode: DEFAULT_SOCKET_MODE,
            socket_group: None,
            state_directory: None,
            gc_root_directory: None,
            runtime_configuration: RuntimeConfiguration::for_in_process_tests(),
        }
    }

    pub fn from_configuration(configuration: wire::LojixDaemonConfiguration) -> Self {
        let runtime_configuration = RuntimeConfiguration::from_daemon_configuration(&configuration);
        Self {
            address: SocketAddress::new(configuration.daemon_socket_path.as_str()),
            socket_mode: configuration.daemon_socket_mode.into_u32(),
            socket_group: configuration.daemon_socket_group.clone(),
            state_directory: Some(runtime_configuration.state_directory().to_path_buf()),
            gc_root_directory: Some(runtime_configuration.gc_root_directory().to_path_buf()),
            runtime_configuration,
        }
    }

    pub async fn serve_forever(self) -> Result<()> {
        let root = RuntimeRoot::spawn(RuntimeRoot::try_with_configuration(
            self.runtime_configuration.clone(),
        )?);
        let listener = self.bind_listener()?;
        loop {
            let (stream, _) = listener.accept().await?;
            Self::spawn_connection(stream, root.clone()).await?;
        }
    }

    fn bind_listener(&self) -> Result<UnixListener> {
        self.prepare_runtime_directories()?;
        if let Some(parent) = self.address.path().parent() {
            std::fs::create_dir_all(parent)?;
        }
        match std::fs::remove_file(self.address.path()) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
        let listener = UnixListener::bind(self.address.path())?;
        std::fs::set_permissions(
            self.address.path(),
            std::fs::Permissions::from_mode(self.socket_mode),
        )?;
        self.apply_socket_group()?;
        Ok(listener)
    }

    fn prepare_runtime_directories(&self) -> Result<()> {
        if let Some(state_directory) = &self.state_directory {
            std::fs::create_dir_all(state_directory)?;
        }
        if let Some(gc_root_directory) = &self.gc_root_directory {
            std::fs::create_dir_all(gc_root_directory)?;
        }
        Ok(())
    }

    fn apply_socket_group(&self) -> Result<()> {
        let Some(group) = &self.socket_group else {
            return Ok(());
        };
        let unix_group = Group::from_name(group.as_str())?
            .ok_or_else(|| Error::UnknownUnixGroup(group.as_str().to_owned()))?;
        chown(self.address.path(), None, Some(unix_group.gid))?;
        Ok(())
    }

    async fn spawn_connection(stream: UnixStream, root: ActorRef<RuntimeRoot>) -> Result<()> {
        let actor = ConnectionActor::spawn(ConnectionActor::new(Connection::new(stream), root));
        actor
            .tell(HandleConnection)
            .send()
            .await
            .map_err(|_| Error::ConnectionActorStopped)
    }

    pub async fn handle_stream<IoStream>(
        mut connection: Connection<IoStream>,
        root: ActorRef<RuntimeRoot>,
    ) -> Result<()>
    where
        IoStream: AsyncRead + AsyncWrite + Unpin,
    {
        let frame = connection.read_frame().await?;
        let (exchange, request) = match frame.into_body() {
            StreamingFrameBody::Request { exchange, request } => (exchange, request),
            _ => return Err(Error::ExpectedRequestFrame),
        };
        match SocketRequest::from_channel_request(request) {
            SocketRequest::OneShot(operations) => {
                let reply = RuntimeDispatch::new(root, operations).into_reply().await?;
                let frame = wire::Frame::new(StreamingFrameBody::Reply { exchange, reply });
                connection.write_frame(&frame).await
            }
            SocketRequest::DeploymentObservationStream(subscription) => {
                DeploymentObservationStreamConnection::new(connection, root, exchange, subscription)
                    .run()
                    .await
            }
        }
    }
}

struct ConnectionActor {
    connection: Option<Connection<UnixStream>>,
    root: ActorRef<RuntimeRoot>,
}

impl ConnectionActor {
    fn new(connection: Connection<UnixStream>, root: ActorRef<RuntimeRoot>) -> Self {
        Self {
            connection: Some(connection),
            root,
        }
    }
}

impl Actor for ConnectionActor {
    type Args = Self;
    type Error = Infallible;

    async fn on_start(
        arguments: Self::Args,
        _actor_ref: ActorRef<Self>,
    ) -> std::result::Result<Self, Self::Error> {
        Ok(arguments)
    }
}

struct HandleConnection;

impl Message<HandleConnection> for ConnectionActor {
    type Reply = ();

    async fn handle(
        &mut self,
        _message: HandleConnection,
        context: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        if let Some(connection) = self.connection.take() {
            let _ = SocketServer::handle_stream(connection, self.root.clone()).await;
        }
        context.stop();
    }
}

struct RuntimeDispatch {
    root: ActorRef<RuntimeRoot>,
    operations: NonEmpty<wire::Operation>,
}

impl RuntimeDispatch {
    fn new(root: ActorRef<RuntimeRoot>, operations: NonEmpty<wire::Operation>) -> Self {
        Self { root, operations }
    }

    async fn into_reply(self) -> Result<signal_frame::Reply<wire::LojixReply>> {
        let mut replies = Vec::new();
        for request in self.operations {
            let payload = self
                .root
                .ask(RuntimeRequest { request })
                .await
                .map_err(|_| Error::RuntimeActorStopped)?;
            replies.push(SubReply::Ok(payload));
        }

        let per_operation =
            NonEmpty::try_from_vec(replies).map_err(|_| Error::ExpectedSingleReplyPayload)?;
        Ok(CoreReply::committed(per_operation))
    }
}

enum SocketRequest {
    OneShot(NonEmpty<wire::Operation>),
    DeploymentObservationStream(wire::WatchDeployments),
}

impl SocketRequest {
    fn from_channel_request(request: signal_frame::Request<wire::Operation>) -> Self {
        let (head, tail) = request.payloads.into_head_and_tail();
        if tail.is_empty()
            && let wire::Operation::WatchDeployments(subscription) = head
        {
            return Self::DeploymentObservationStream(subscription);
        }
        Self::OneShot(NonEmpty::from_head_and_tail(head, tail))
    }
}

struct DeploymentObservationStreamConnection<IoStream> {
    reader: Connection<ReadHalf<IoStream>>,
    writer: Connection<WriteHalf<IoStream>>,
    root: ActorRef<RuntimeRoot>,
    exchange: ExchangeIdentifier,
    subscription: wire::WatchDeployments,
    next_event_sequence: LaneSequence,
}

impl<IoStream> DeploymentObservationStreamConnection<IoStream>
where
    IoStream: AsyncRead + AsyncWrite + Unpin,
{
    fn new(
        connection: Connection<IoStream>,
        root: ActorRef<RuntimeRoot>,
        exchange: ExchangeIdentifier,
        subscription: wire::WatchDeployments,
    ) -> Self {
        let (reader, writer) = tokio::io::split(connection.into_inner());
        Self {
            reader: Connection::new(reader),
            writer: Connection::new(writer),
            root,
            exchange,
            subscription,
            next_event_sequence: LaneSequence::first(),
        }
    }

    async fn run(mut self) -> Result<()> {
        let (sender, events) = unbounded_channel();
        let reply = self
            .root
            .ask(OpenDeploymentObservationStream {
                subscription: self.subscription.clone(),
                sender,
            })
            .await
            .map_err(|_| Error::RuntimeActorStopped)?;
        let Some(token) = deployment_observation_token(&reply) else {
            self.write_reply(self.exchange, completed_single_reply(reply))
                .await?;
            return Ok(());
        };
        self.write_reply(self.exchange, completed_single_reply(reply))
            .await?;
        self.run_open_stream(token, events).await
    }

    async fn run_open_stream(
        &mut self,
        token: wire::DeploymentObservationToken,
        mut events: UnboundedReceiver<wire::DeploymentObservation>,
    ) -> Result<()> {
        loop {
            tokio::select! {
                observation = events.recv() => {
                    let Some(observation) = observation else {
                        self.close_subscription(token.clone()).await?;
                        return Ok(());
                    };
                    if let Err(error) = self.write_deployment_observation_event(&token, observation).await {
                        let _ = self.close_subscription(token.clone()).await;
                        return Err(error);
                    }
                }
                frame = self.reader.read_frame() => {
                    let frame = match frame {
                        Ok(frame) => frame,
                        Err(error) => {
                            let _ = self.close_subscription(token.clone()).await;
                            if is_unexpected_eof(&error) {
                                return Ok(());
                            }
                            return Err(error);
                        }
                    };
                    let close_current = self.handle_client_frame(frame, &token).await?;
                    if close_current {
                        return Ok(());
                    }
                }
            }
        }
    }

    async fn handle_client_frame(
        &mut self,
        frame: wire::Frame,
        current_token: &wire::DeploymentObservationToken,
    ) -> Result<bool> {
        let (exchange, request) = match frame.into_body() {
            StreamingFrameBody::Request { exchange, request } => (exchange, request),
            _ => return Err(Error::ExpectedRequestFrame),
        };
        let close_current = request_closes_deployment_observation(&request, current_token);
        match SocketRequest::from_channel_request(request) {
            SocketRequest::OneShot(operations) => {
                let reply = RuntimeDispatch::new(self.root.clone(), operations)
                    .into_reply()
                    .await?;
                self.write_reply(exchange, reply).await?;
                Ok(close_current)
            }
            SocketRequest::DeploymentObservationStream(_) => {
                let reply = CoreReply::rejected(signal_frame::RequestRejectionReason::Internal);
                self.write_reply(exchange, reply).await?;
                Ok(false)
            }
        }
    }

    async fn close_subscription(&self, token: wire::DeploymentObservationToken) -> Result<()> {
        let request = wire::Operation::UnwatchDeployments(token);
        self.root
            .ask(RuntimeRequest { request })
            .await
            .map_err(|_| Error::RuntimeActorStopped)?;
        Ok(())
    }

    async fn write_reply(
        &mut self,
        exchange: ExchangeIdentifier,
        reply: signal_frame::Reply<wire::LojixReply>,
    ) -> Result<()> {
        let frame = wire::Frame::new(StreamingFrameBody::Reply { exchange, reply });
        self.writer.write_frame(&frame).await
    }

    async fn write_deployment_observation_event(
        &mut self,
        token: &wire::DeploymentObservationToken,
        observation: wire::DeploymentObservation,
    ) -> Result<()> {
        let frame = wire::Frame::new(StreamingFrameBody::SubscriptionEvent {
            event_identifier: self.next_event_identifier(),
            token: SubscriptionTokenInner::new(token.value()),
            event: wire::LojixEvent::DeploymentObservation(observation),
        });
        self.writer.write_frame(&frame).await
    }

    fn next_event_identifier(&mut self) -> StreamEventIdentifier {
        let identifier = StreamEventIdentifier::new(
            self.exchange.session_epoch,
            ExchangeLane::Acceptor,
            self.next_event_sequence,
        );
        self.next_event_sequence = self.next_event_sequence.next();
        identifier
    }
}

fn is_unexpected_eof(error: &Error) -> bool {
    matches!(error, Error::Io(io_error) if io_error.kind() == std::io::ErrorKind::UnexpectedEof)
}

fn deployment_observation_token(
    reply: &wire::LojixReply,
) -> Option<wire::DeploymentObservationToken> {
    let wire::LojixReply::DeploymentObservationSubscriptionOpened(opened) = reply else {
        return None;
    };
    Some(opened.token.clone())
}

fn completed_single_reply(payload: wire::LojixReply) -> signal_frame::Reply<wire::LojixReply> {
    CoreReply::committed(NonEmpty::single(SubReply::Ok(payload)))
}

fn request_closes_deployment_observation(
    request: &signal_frame::Request<wire::Operation>,
    current_token: &wire::DeploymentObservationToken,
) -> bool {
    let operations = request.payloads();
    if !operations.tail().is_empty() {
        return false;
    }
    matches!(
        operations.head(),
        wire::Operation::UnwatchDeployments(token) if token == current_token
    )
}

pub struct ExchangeIdentity {
    value: ExchangeIdentifier,
}

impl ExchangeIdentity {
    pub fn first_connector_exchange() -> Self {
        Self {
            value: ExchangeIdentifier::new(
                SessionEpoch::new(1),
                ExchangeLane::Connector,
                LaneSequence::first(),
            ),
        }
    }

    pub fn value(&self) -> ExchangeIdentifier {
        self.value
    }
}

impl From<UnixStream> for Connection<UnixStream> {
    fn from(stream: UnixStream) -> Self {
        Self::new(stream)
    }
}
