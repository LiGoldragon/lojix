//! meta-lojix CLI — the owner-only meta-socket client, the privileged sibling
//! of `lojix`. Takes exactly one NOTA argument, decodes it as a
//! `meta-signal-lojix` policy request (Deploy / Pin / Unpin / Retire),
//! exchanges it on the owner/meta socket, and prints the reply. Mirrors `lojix`
//! but typed on the meta contract. Per Spirit `ssk2` (two CLIs, one per socket)
//! and the `meta-` naming rule `8bwo`.

use lojix::client::MetaClient;

fn main() {
    match MetaClient::run_from_environment() {
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
