//! Idempotent pre-start Lojix store migration CLI.

use lojix::reconstruction::StoreMigrationCommand;

fn main() {
    match StoreMigrationCommand::from_environment().and_then(|command| command.run()) {
        Ok(outcome) => println!("{outcome}"),
        Err(error) => {
            eprintln!("(StoreMigrationRejected [{error}])");
            std::process::exit(2);
        }
    }
}
