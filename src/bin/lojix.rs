//! lojix CLI — the ordinary-socket client. Takes exactly one inline Datom
//! object, decodes it as a
//! `signal-lojix` peer request, exchanges it on the ordinary socket, and prints
//! the reply. No flags: the single argument is the request. Its owner-only
//! sibling is `meta-lojix`.

use datom_codec::{Conceivable, Textualizable};
use lojix::client::OrdinaryClient;

fn main() {
    match OrdinaryClient::run_from_environment() {
        Ok(output) => {
            let (_, datom) = output.conceive().expect("generated response is Datom");
            println!("{}", datom.textualize());
        }
        Err(error) => {
            eprintln!("(CliRejected [{error}])");
            std::process::exit(2);
        }
    }
}
