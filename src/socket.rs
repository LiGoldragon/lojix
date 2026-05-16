use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};

use kameo::Actor;
use kameo::actor::{ActorRef, Spawn};
use kameo::error::Infallible;
use kameo::message::{Context, Message};
use nix::unistd::{Group, chown};
use signal_core::{
    ExchangeIdentifier, ExchangeLane, LaneSequence, NonEmpty, Reply as CoreReply, SessionEpoch,
    SubReply,
};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::{UnixListener, UnixStream};

use crate::error::{Error, Result};
use crate::runtime::{RuntimeConfiguration, RuntimeRequest, RuntimeRoot};
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
}

impl<IoStream> Connection<IoStream>
where
    IoStream: AsyncRead + AsyncWrite + Unpin,
{
    pub async fn write_frame(&mut self, frame: &wire::LojixFrame) -> Result<()> {
        let bytes = frame.encode_length_prefixed()?;
        self.stream.write_all(&bytes).await?;
        self.stream.flush().await?;
        Ok(())
    }

    pub async fn read_frame(&mut self) -> Result<wire::LojixFrame> {
        let mut length_bytes = [0_u8; 4];
        self.stream.read_exact(&mut length_bytes).await?;
        let length = u32::from_be_bytes(length_bytes) as usize;
        let mut payload = vec![0_u8; length];
        self.stream.read_exact(&mut payload).await?;

        let mut framed = Vec::with_capacity(4 + length);
        framed.extend_from_slice(&length_bytes);
        framed.extend_from_slice(&payload);
        Ok(wire::LojixFrame::decode_length_prefixed(&framed)?)
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
        let root = RuntimeRoot::spawn(RuntimeRoot::with_configuration(
            self.runtime_configuration.clone(),
        ));
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
            wire::LojixFrameBody::Request { exchange, request } => (exchange, request),
            _ => return Err(Error::ExpectedRequestFrame),
        };
        let reply = RuntimeDispatch::new(root, request).into_reply().await?;
        let frame = wire::LojixFrame::new(wire::LojixFrameBody::Reply { exchange, reply });
        connection.write_frame(&frame).await
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
    request: wire::LojixChannelRequest,
}

impl RuntimeDispatch {
    fn new(root: ActorRef<RuntimeRoot>, request: wire::LojixChannelRequest) -> Self {
        Self { root, request }
    }

    async fn into_reply(self) -> Result<wire::LojixChannelReply> {
        let checked = match self.request.into_checked() {
            Ok(checked) => checked,
            Err((reason, _request)) => return Ok(CoreReply::rejected(reason)),
        };

        let mut replies = Vec::new();
        for operation in checked.operations {
            let verb = operation.verb;
            let payload = self
                .root
                .ask(RuntimeRequest {
                    request: operation.payload,
                })
                .await
                .map_err(|_| Error::RuntimeActorStopped)?;
            replies.push(SubReply::Ok { verb, payload });
        }

        let per_operation =
            NonEmpty::try_from_vec(replies).map_err(|_| Error::ExpectedSingleReplyPayload)?;
        Ok(CoreReply::completed(per_operation))
    }
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
