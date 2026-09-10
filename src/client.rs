//! Datom-free socket exchange shared by the separate client crates.

use std::os::unix::net::UnixStream;

use triad_runtime::{FrameBody, LengthPrefixedCodec};

use crate::{Error, Result};

pub struct SocketExchange {
    socket_path: String,
    codec: LengthPrefixedCodec,
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
            codec: LengthPrefixedCodec::default(),
        })
    }

    pub fn exchange(&self, request: Vec<u8>) -> Result<Vec<u8>> {
        let mut stream = UnixStream::connect(&self.socket_path)?;
        self.codec
            .write_body(&mut stream, &FrameBody::new(request))?;
        Ok(self.codec.read_body(&mut stream)?.bytes().to_vec())
    }
}
