//! Datom-free socket exchange shared by the separate client crates.

use std::os::unix::net::UnixStream;

use signal::{ByteViewable, FrameBody, FrameCapacity, FrameReading, FrameWriting};

use crate::{Error, Result};

pub struct SocketExchange {
    socket_path: String,
    capacity: FrameCapacity,
}

/// One Nexus socket, named by the environment variable that carries its path,
/// used for exactly one request and its one reply. Nothing here interprets the
/// bytes: the frame is the whole contract at this seam.
pub trait NexusSocket: Sized {
    fn for_environment(variable: &str) -> Result<Self>;

    fn exchange(&self, request: Vec<u8>) -> Result<Vec<u8>>;
}

impl NexusSocket for SocketExchange {
    fn for_environment(variable: &str) -> Result<Self> {
        let socket_path = std::env::var(variable)
            .map_err(|_| Error::MissingRuntimeConfiguration(variable.to_owned()))?;
        if socket_path.is_empty() {
            return Err(Error::MissingRuntimeConfiguration(variable.to_owned()));
        }
        Ok(Self {
            socket_path,
            capacity: FrameCapacity::default(),
        })
    }

    fn exchange(&self, request: Vec<u8>) -> Result<Vec<u8>> {
        let mut stream = UnixStream::connect(&self.socket_path)?;
        stream.write_frame(&FrameBody::from(request), self.capacity)?;
        Ok(stream.read_frame(self.capacity)?.bytes().to_vec())
    }
}
