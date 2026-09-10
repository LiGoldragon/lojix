//! Read-only Lojix store inspection CLI.

use lojix::inspection::StoreInspectionCommand;

fn main() {
    match StoreInspectionCommand::from_environment() {
        Ok(command) => print!("{}", command.run()),
        Err(error) => {
            eprintln!("(CliRejected [{error}])");
            std::process::exit(2);
        }
    }
}
