//! lojix-daemon — long-lived deploy orchestrator entry point.
//!
//! The first runtime slice binds the socket and routes requests through
//! a Kameo `RuntimeRoot`. Effect-bearing deploy/cache actors land behind
//! that root; until then those operations fail closed with typed replies.

use std::process::ExitCode;

#[tokio::main]
async fn main() -> ExitCode {
    let server = lojix::SocketServer::new(lojix::SocketAddress::from_environment());
    match server.serve_forever().await {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("lojix-daemon: {error}");
            ExitCode::FAILURE
        }
    }
}
