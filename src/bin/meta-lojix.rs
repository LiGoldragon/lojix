//! meta-lojix CLI — the owner-only meta-socket client, the privileged sibling
//! of `lojix`. Takes exactly one inline DOTOS/NOTA object, decodes it as a
//! `meta-signal-lojix` policy request (Deploy / Pin / Unpin / Retire / Test),
//! exchanges it on the owner/meta socket, and prints the typed reply. A
//! `DeployAccepted` reply is admission, not terminal deploy success. Mirrors `lojix`
//! but typed on the meta contract. Per Spirit `ssk2` (two CLIs, one per socket)
//! and the `meta-` naming rule `8bwo`.

use lojix::client::MetaClient;

fn main() {
    match MetaClient::run_from_environment() {
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
