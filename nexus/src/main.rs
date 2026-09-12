//! Lojix Nexus entry. It takes no arguments, discovers its stable Sema from
//! the standard state base, loads desired configuration, and serves both
//! authority-tiered sockets.

use lojix::daemon::EnvironmentConstructible as _;

fn main() {
    // The argument guard and the serve call are the whole of this entry point,
    // so they stay in `fn main` rather than becoming a floating verb: a Nexus
    // takes no arguments, and everything it then does is `Daemon`'s.
    let served = if std::env::args_os().nth(1).is_some() {
        Err(lojix::Error::UnexpectedNexusArguments)
    } else {
        lojix::daemon::Daemon::from_environment()
            .and_then(<lojix::daemon::Daemon as lojix::daemon::Runnable>::run)
    };
    if let Err(error) = served {
        eprintln!("lojix-nexus: {error}");
        std::process::exit(2);
    }
}
