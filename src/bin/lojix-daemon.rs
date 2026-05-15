//! lojix-daemon — long-lived deploy orchestrator entry point.
//!
//! The first runtime slice binds the socket and routes requests through
//! a Kameo `RuntimeRoot`. Effect-bearing deploy/cache actors land behind
//! that root; until then those operations fail closed with typed replies.

use std::process::ExitCode;

use nota_config::ConfigurationSource;

#[tokio::main]
async fn main() -> ExitCode {
    let configuration = match ConfigurationSource::from_argv()
        .and_then(|source| source.decode::<lojix::wire::LojixDaemonConfiguration>())
    {
        Ok(configuration) => configuration,
        Err(error) => {
            eprintln!("lojix-daemon: {error}");
            return ExitCode::FAILURE;
        }
    };

    let server = lojix::SocketServer::from_configuration(configuration);
    match server.serve_forever().await {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("lojix-daemon: {error}");
            ExitCode::FAILURE
        }
    }
}
