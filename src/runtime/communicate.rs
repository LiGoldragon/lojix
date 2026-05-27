//! `Communicate` trait — the abstract wire interface.
//!
//! Per psyche record 935 + report `/390 §"Communicate trait"`, the
//! wire surface for sending a typed request and receiving a typed
//! response is a trait. The concrete impl for lojix-next is
//! `UnixSocketCommunicate`, which encodes the schema-emitted `Input`
//! into a length-prefixed signal frame, writes it to a Unix socket,
//! reads the response frame, and decodes the `Output`.
//!
//! Iteration-2 decision: the trait lives inside `lojix-next` for the
//! pilot. Promoting to `signal-frame` (or a dedicated abstract crate)
//! is iteration-3 work — the trait shape is now load-bearing in this
//! crate's tests, but the full schema-derived signal-frame rewrite
//! is the substrate work that earns the move.

use std::path::{Path, PathBuf};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::UnixStream;

use crate::error::Error;
use crate::generated::{Input, Output};

const LENGTH_PREFIX_BYTE_COUNT: usize = 4;

/// Abstract wire interface. `send_request` carries one typed
/// request, waits for the response, and returns the typed reply.
/// Errors are crate-typed (per `skills/rust/errors.md`). The
/// returned future is `Send` so the trait can be driven from any
/// tokio runtime configuration.
#[allow(async_fn_in_trait)]
pub trait Communicate {
    type Input;
    type Output;
    type TransportError: std::error::Error + Send + Sync + 'static;

    async fn send_request(
        &mut self,
        input: Self::Input,
    ) -> Result<Self::Output, Self::TransportError>;
}

/// Concrete `Communicate` impl over a Unix socket. State carries the
/// socket path; each `send_request` opens a fresh connection,
/// exchanges one frame pair, and returns. The connection-per-request
/// shape mirrors the daemon's `SocketConnection::serve_once`
/// contract on the other side.
pub struct UnixSocketCommunicate {
    socket_path: PathBuf,
}

impl UnixSocketCommunicate {
    pub fn new(socket_path: impl Into<PathBuf>) -> Self {
        Self {
            socket_path: socket_path.into(),
        }
    }

    pub fn socket_path(&self) -> &Path {
        &self.socket_path
    }
}

impl Communicate for UnixSocketCommunicate {
    type Input = Input;
    type Output = Output;
    type TransportError = Error;

    async fn send_request(
        &mut self,
        input: Self::Input,
    ) -> Result<Self::Output, Self::TransportError> {
        let frame = input.encode_signal_frame()?;
        let mut stream = UnixStream::connect(&self.socket_path).await?;
        let mut exchange = SignalFrameExchange::new(&mut stream);
        exchange.write_frame(&frame).await?;
        let reply_frame = exchange.read_frame().await?;
        let (_route, output) = Output::decode_signal_frame(&reply_frame)?;
        Ok(output)
    }
}

/// One length-prefixed frame exchange over a Unix socket. The
/// State borrows the stream; methods on this type read and write
/// length-prefixed frames — the verbs belong on the exchange noun,
/// not on the trait.
pub struct SignalFrameExchange<'stream> {
    stream: &'stream mut UnixStream,
}

impl<'stream> SignalFrameExchange<'stream> {
    pub fn new(stream: &'stream mut UnixStream) -> Self {
        Self { stream }
    }

    pub async fn read_frame(&mut self) -> Result<Vec<u8>, Error> {
        let mut length_bytes = [0_u8; LENGTH_PREFIX_BYTE_COUNT];
        self.stream.read_exact(&mut length_bytes).await?;
        let length = u32::from_be_bytes(length_bytes) as usize;
        let mut frame = vec![0_u8; length];
        self.stream.read_exact(&mut frame).await?;
        Ok(frame)
    }

    pub async fn write_frame(&mut self, frame: &[u8]) -> Result<(), Error> {
        let length =
            u32::try_from(frame.len()).map_err(|_| Error::FrameTooLarge { found: frame.len() })?;
        self.stream.write_all(&length.to_be_bytes()).await?;
        self.stream.write_all(frame).await?;
        self.stream.flush().await?;
        Ok(())
    }
}
