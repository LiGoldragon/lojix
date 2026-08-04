//! lojix CLI — the ordinary-socket client. Takes exactly one inline DOTOS/NOTA
//! object, decodes it as a
//! `signal-lojix` peer request, exchanges it on the ordinary socket, and prints
//! the reply. No flags: the single argument is the request. Its owner-only
//! sibling is `meta-lojix`.

use lojix::client::OrdinaryClient;

fn main() {
    match OrdinaryClient::run_from_environment() {
        Ok(output) => {
            #[cfg(feature = "dotos-text")]
            println!("{output}");
            #[cfg(not(feature = "dotos-text"))]
            println!("{output:?}");
        }
        Err(error) => {
            eprintln!("(CliRejected [{error}])");
            std::process::exit(2);
        }
    }
}
