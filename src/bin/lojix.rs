//! lojix thin CLI entry — the daemon's first client. Takes exactly one NOTA
//! argument (inline NOTA / NOTA file / signal-encoded file via the runtime's
//! `ComponentArgument`), classifies it as an ordinary or owner Signal request,
//! sends it on the matching authority-tiered socket, and prints the reply.
//!
//! NOTA is the only argument language and there are no flags: the single
//! argument is the request. A signal-encoded (rkyv) file is decoded directly;
//! inline / NOTA-file text decoding rides the optional `nota-text` feature.

use lojix::client::Client;

fn main() {
    match Client::run_from_environment() {
        Ok(reply) => println!("{reply}"),
        Err(error) => {
            eprintln!("(CliRejected [{error}])");
            std::process::exit(2);
        }
    }
}
