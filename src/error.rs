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

    #[error("horizon nota error: {0}")]
    HorizonNota(#[from] horizon_nota_codec::Error),

    #[error("configuration error: {0}")]
    Configuration(#[from] nota_config::Error),

    #[error("signal-lojix boundary error: {0}")]
    SignalLojix(#[from] signal_lojix::Error),

    #[error("horizon error: {0}")]
    Horizon(#[from] horizon_lib::Error),

    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("process {program} failed with exit {status}: {stderr}")]
    ProcessFailed {
        program: String,
        status: i32,
        stderr: String,
    },

    #[error("deployment rejected: {0}")]
    DeploymentRejected(String),

    #[error("unix group does not exist: {0}")]
    UnknownUnixGroup(String),

    #[error("failed to apply unix socket ownership: {0}")]
    UnixOwnership(#[from] nix::errno::Errno),

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
