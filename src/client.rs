//! Datom-free socket exchange shared by the separate client crates.

use std::os::unix::net::UnixStream;

use signal::{ByteViewable, FrameBody, FrameCapacity, FrameReading, FrameWriting};

use crate::{Error, Result};

pub struct SocketExchange {
    socket_path: String,
    capacity: FrameCapacity,
}

impl SocketExchange {
    pub fn for_environment(variable: &str) -> Result<Self> {
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

    pub fn exchange(&self, request: Vec<u8>) -> Result<Vec<u8>> {
        let mut stream = UnixStream::connect(&self.socket_path)?;
        stream.write_frame(&FrameBody::from(request), self.capacity)?;
        Ok(stream.read_frame(self.capacity)?.bytes().to_vec())
    }
}
