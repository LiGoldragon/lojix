//! Explicit, inline-DOTOS, path-scoped Lojix v4 store reset CLI.

use lojix::reconstruction::StoreResetCommand;

fn main() {
    match StoreResetCommand::from_environment().and_then(|command| command.run()) {
        Ok(outcome) => println!("{outcome}"),
        Err(error) => {
            eprintln!("(StoreResetRejected [{error}])");
            std::process::exit(2);
        }
    }
}
