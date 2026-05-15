use thiserror::Error;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Error)]
pub enum Error {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    #[error("frame error: {0}")]
    Frame(#[from] signal_core::FrameError),

    #[error("nota error: {0}")]
    Nota(#[from] nota_codec::Error),

    #[error("expected a request frame")]
    ExpectedRequestFrame,

    #[error("expected a reply frame")]
    ExpectedReplyFrame,

    #[error("reply exchange identifier did not match the request")]
    ReplyExchangeMismatch,

    #[error("daemon rejected request before execution: {0}")]
    RequestRejected(signal_core::RequestRejectionReason),

    #[error("daemon reply did not contain exactly one successful payload")]
    ExpectedSingleReplyPayload,

    #[error("runtime actor stopped before replying")]
    RuntimeActorStopped,

    #[error("connection actor stopped before accepting work")]
    ConnectionActorStopped,
}
