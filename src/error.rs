use thiserror::Error;

use crate::generated::{NotaDecodeError, SignalFrameError};

#[derive(Debug, Error)]
pub enum Error {
    #[error("input decode failed: {0}")]
    InputDecode(NotaDecodeError),

    #[error("configuration decode failed: {0}")]
    ConfigurationDecode(String),

    #[error("signal frame error: {0}")]
    SignalFrame(SignalFrameError),

    #[error("io: {0}")]
    Io(#[from] std::io::Error),

    #[error("frame too large for u32 prefix: {found} bytes")]
    FrameTooLarge { found: usize },

    #[error("daemon socket already in use at {0}")]
    SocketBusy(String),

    #[error("build command failed: {0}")]
    BuildFailed(String),

    #[error("activation command failed: {0}")]
    ActivationFailed(String),

    #[error("copy command failed: {0}")]
    CopyFailed(String),

    #[error("gc pin failed: {0}")]
    GcPinFailed(String),

    #[error("actor send: {0}")]
    ActorSend(String),

    #[error("sema-engine error: {0}")]
    Sema(#[from] sema_engine::Error),

    #[error("daemon shutdown")]
    Shutdown,
}

impl From<NotaDecodeError> for Error {
    fn from(value: NotaDecodeError) -> Self {
        Self::InputDecode(value)
    }
}

impl From<SignalFrameError> for Error {
    fn from(value: SignalFrameError) -> Self {
        Self::SignalFrame(value)
    }
}

pub type Result<Value> = std::result::Result<Value, Error>;
