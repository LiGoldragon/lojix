//! Maintained, daemon-free bootstrap entry point.
//!
//! The binary accepts one inline `BootstrapRun` DOTOS object.  It never opens
//! a Lojix socket or reuses daemon state: its v4 journal is private to the
//! supplied journal parent and its terminal evidence is durable at the
//! supplied evidence path.

fn main() {
    match lojix::bootstrap::run_from_environment() {
        Ok(terminal) => {
            println!(
                "(BootstrapTerminal.{{{} {}}})",
                terminal.request_id, terminal.status
            );
            if terminal.status == "Failed" {
                std::process::exit(2);
            }
        }
        Err(error) => {
            eprintln!("(BootstrapRejected [{}])", error.redacted());
            std::process::exit(2);
        }
    }
}
