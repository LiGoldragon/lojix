//! lojix CLI — the ordinary-socket client. Takes exactly one NOTA argument
//! (inline NOTA / NOTA file / signal-encoded file), decodes it as a
//! `signal-lojix` peer request, exchanges it on the ordinary socket, and prints
//! the reply. No flags: the single argument is the request. Its owner-only
//! sibling is `meta-lojix`.

use lojix::client::OrdinaryClient;

fn main() {
    match OrdinaryClient::run_from_environment() {
        Ok(output) => {
            #[cfg(feature = "nota-text")]
            println!("{output}");
            #[cfg(not(feature = "nota-text"))]
            println!("{output:?}");
        }
        Err(error) => {
            eprintln!("(CliRejected [{error}])");
            std::process::exit(2);
        }
    }
}
