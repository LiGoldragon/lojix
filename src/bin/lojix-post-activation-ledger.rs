//! Read-only deployment evidence CLI.

use lojix::post_activation_ledger::{LedgerArguments, PostActivationLedger};

fn main() {
    let result = LedgerArguments::from_environment()
        .map(PostActivationLedger::new)
        .and_then(|ledger| ledger.run());
    match result {
        Ok(ledger) => {
            print!("{}", ledger.render());
            if !ledger.is_healthy() {
                std::process::exit(1);
            }
        }
        Err(error) => {
            eprintln!("(LedgerRejected [{error}])");
            std::process::exit(2);
        }
    }
}
