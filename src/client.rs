//! Datom-free socket exchange shared by the separate client crates.

use std::io::{ErrorKind, Read};
use std::os::unix::net::UnixStream;

use signal::{
    FRAME_PREFIX_BYTES, FrameBody, FrameCapacity, FramePrefix, FrameWriting, LengthDeclaring,
};

use crate::{Error, Result};

pub struct SocketExchange {
    socket_path: String,
    capacity: FrameCapacity,
}

/// One Nexus socket, named by the environment variable that carries its path
/// or by the path itself. A request is one frame out; its replies are every
/// frame back until the Nexus closes the connection. The client holds no
/// knowledge of how many replies an input has: the Nexus, which knows the
/// types, closes after the one reply of an ordinary input and keeps a watch
/// open. Nothing here interprets the bytes: the frame is the whole contract at
/// this seam.
pub trait NexusSocket: Sized {
    fn for_environment(variable: &str) -> Result<Self>;

    fn at_path(socket_path: String) -> Self;

    /// Send `request` and hand each reply frame body to `each`, in order,
    /// until the Nexus closes the connection between frames.
    fn exchange(&self, request: Vec<u8>, each: &mut dyn FnMut(Vec<u8>) -> Result<()>)
    -> Result<()>;
}

/// Reading frames from a stream that may close between them.
trait ClosingFrameReading {
    /// The next frame, or `None` when the peer closed before its first byte.
    /// A close inside a frame is an error.
    fn next_frame(&mut self, capacity: FrameCapacity) -> Result<Option<FrameBody>>;
}

impl ClosingFrameReading for UnixStream {
    fn next_frame(&mut self, capacity: FrameCapacity) -> Result<Option<FrameBody>> {
        let mut prefix = [0_u8; FRAME_PREFIX_BYTES];
        let mut filled = 0;
        while filled < FRAME_PREFIX_BYTES {
            match self.read(&mut prefix[filled..]) {
                Ok(0) if filled == 0 => return Ok(None),
                Ok(0) => return Err(Error::Io(ErrorKind::UnexpectedEof.into())),
                Ok(read) => filled += read,
                Err(error) if error.kind() == ErrorKind::Interrupted => {}
                Err(error) => return Err(Error::Io(error)),
            }
        }
        let length = FramePrefix::from(prefix).admitted_length(capacity)?;
        let mut body = vec![0_u8; length];
        self.read_exact(&mut body)?;
        Ok(Some(FrameBody::from(body)))
    }
}

impl NexusSocket for SocketExchange {
    fn for_environment(variable: &str) -> Result<Self> {
        let socket_path = std::env::var(variable)
            .map_err(|_| Error::MissingRuntimeConfiguration(variable.to_owned()))?;
        if socket_path.is_empty() {
            return Err(Error::MissingRuntimeConfiguration(variable.to_owned()));
        }
        Ok(Self::at_path(socket_path))
    }

    fn at_path(socket_path: String) -> Self {
        Self {
            socket_path,
            capacity: FrameCapacity::default(),
        }
    }

    fn exchange(
        &self,
        request: Vec<u8>,
        each: &mut dyn FnMut(Vec<u8>) -> Result<()>,
    ) -> Result<()> {
        let mut stream = UnixStream::connect(&self.socket_path)?;
        stream.write_frame(&FrameBody::from(request), self.capacity)?;
        while let Some(reply) = stream.next_frame(self.capacity)? {
            each(reply.into())?;
        }
        Ok(())
    }
}
