//! SocketListener — owns the Unix-socket accept loop and per-connection
//! frame exchange.
//!
//! Per `skills/component-triad.md` §"Runtime triad - Signal":
//! the SocketListener is the daemon's external surface. It reads
//! length-prefixed signal frames, decodes `Input` via the
//! schema-emitted `Input::decode_signal_frame`, dispatches through
//! the Engine, and writes back the framed `Output`. The SocketListener's
//! State carries the bound UnixListener handle plus the engine ref.

use kameo::Actor;
use kameo::actor::ActorRef;
use kameo::error::Infallible;
use kameo::message::{Context, Message};
use std::path::PathBuf;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{UnixListener, UnixStream};

use crate::error::{Error, Result};
use crate::generated::{Input, Output};
use crate::runtime::root::{DispatchInput, LojixRoot};

const LENGTH_PREFIX_BYTE_COUNT: usize = 4;

pub struct SocketListener {
    socket_path: PathBuf,
    listener: Option<UnixListener>,
    root: ActorRef<LojixRoot>,
}

impl SocketListener {
    pub fn new(socket_path: PathBuf, root: ActorRef<LojixRoot>) -> Self {
        Self {
            socket_path,
            listener: None,
            root,
        }
    }

    pub fn socket_path(&self) -> &std::path::Path {
        &self.socket_path
    }

    pub fn root(&self) -> &ActorRef<LojixRoot> {
        &self.root
    }

    pub fn bind(&mut self) -> Result<()> {
        if let Some(parent) = self.socket_path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let _ = std::fs::remove_file(&self.socket_path);
        let listener = UnixListener::bind(&self.socket_path).map_err(|error| {
            Error::SocketBusy(format!("{} ({error})", self.socket_path.display()))
        })?;
        self.listener = Some(listener);
        Ok(())
    }
}

impl Actor for SocketListener {
    type Args = Self;
    type Error = Infallible;

    async fn on_start(
        mut state: Self::Args,
        _actor_ref: ActorRef<Self>,
    ) -> std::result::Result<Self, Self::Error> {
        // Bind on start; binding errors propagate via Accept.
        let _ = state.bind();
        Ok(state)
    }
}

/// Accept one inbound connection, read one input frame, dispatch, write one output frame.
/// Returns the output (for tests) on success.
pub struct AcceptOnce;

impl Message<AcceptOnce> for SocketListener {
    type Reply = Result<Output>;

    async fn handle(
        &mut self,
        _message: AcceptOnce,
        _context: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        let listener = self
            .listener
            .as_ref()
            .ok_or_else(|| Error::SocketBusy(self.socket_path.display().to_string()))?;
        let (stream, _addr) = listener.accept().await?;
        let connection = SocketConnection::new(stream, self.root.clone());
        connection.serve_once().await
    }
}

/// Run the accept loop forever; serves connections until the listener errors.
pub struct ServeForever;

impl Message<ServeForever> for SocketListener {
    type Reply = Result<()>;

    async fn handle(
        &mut self,
        _message: ServeForever,
        _context: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        let listener = self
            .listener
            .as_ref()
            .ok_or_else(|| Error::SocketBusy(self.socket_path.display().to_string()))?;
        loop {
            match listener.accept().await {
                Ok((stream, _addr)) => {
                    let connection = SocketConnection::new(stream, self.root.clone());
                    let _ = connection.serve_once().await;
                }
                Err(error) => {
                    return Err(Error::Io(error));
                }
            }
        }
    }
}

/// Per-connection frame handler. Stateful — owns the stream for one
/// request/response exchange.
pub struct SocketConnection {
    stream: UnixStream,
    root: ActorRef<LojixRoot>,
}

impl SocketConnection {
    pub fn new(stream: UnixStream, root: ActorRef<LojixRoot>) -> Self {
        Self { stream, root }
    }

    pub async fn serve_once(mut self) -> Result<Output> {
        let frame = self.read_frame().await?;
        let (_route, input) = Input::decode_signal_frame(&frame)?;
        let output = self
            .root
            .ask(DispatchInput(input))
            .await
            .map_err(|error| Error::ActorSend(format!("connection: {error}")))?;
        let reply = output.encode_signal_frame()?;
        self.write_frame(&reply).await?;
        Ok(output)
    }

    async fn read_frame(&mut self) -> Result<Vec<u8>> {
        let mut length_bytes = [0_u8; LENGTH_PREFIX_BYTE_COUNT];
        self.stream.read_exact(&mut length_bytes).await?;
        let length = u32::from_be_bytes(length_bytes) as usize;
        let mut frame = vec![0_u8; length];
        self.stream.read_exact(&mut frame).await?;
        Ok(frame)
    }

    async fn write_frame(&mut self, frame: &[u8]) -> Result<()> {
        let length =
            u32::try_from(frame.len()).map_err(|_| Error::FrameTooLarge { found: frame.len() })?;
        self.stream.write_all(&length.to_be_bytes()).await?;
        self.stream.write_all(frame).await?;
        self.stream.flush().await?;
        Ok(())
    }
}
