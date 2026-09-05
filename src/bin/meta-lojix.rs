//! meta-lojix CLI — the owner-only meta-socket client, the privileged sibling
//! of `lojix`. Takes exactly one inline Datom object, decodes it as a
//! `meta-signal-lojix` policy request (Deploy / Pin / Unpin / Retire / Test),
//! exchanges it on the owner/meta socket, and prints the typed reply. A
//! `DeployAccepted` reply is admission, not terminal deploy success. Mirrors `lojix`
//! but typed on the meta contract. Per Spirit `ssk2` (two CLIs, one per socket)
//! and the `meta-` naming rule `8bwo`.

use datom_codec::{Conceivable, Textualizable};
use lojix::client::MetaClient;

fn main() {
    match MetaClient::run_from_environment() {
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
