//! Lojix Nexus entry. It takes no arguments, discovers its stable Sema from
//! the standard state base, loads desired configuration, and serves both
//! authority-tiered sockets.

use lojix::daemon::EnvironmentConstructible as _;
use lojix::daemon::Runnable as _;

fn main() {
    match run() {
        Ok(()) => {}
        Err(error) => {
            eprintln!("lojix-nexus: {error}");
            std::process::exit(2);
        }
    }
}

fn run() -> lojix::Result<()> {
    if std::env::args_os().nth(1).is_some() {
        return Err(lojix::Error::UnexpectedNexusArguments);
    }
    lojix::daemon::Daemon::from_environment()?.run()
}
