//! Maintained, daemon-free bootstrap entry point.
//!
//! The binary accepts one inline `BootstrapRun` Datom object. It never opens
//! a Lojix socket or reuses daemon state: its resumable journal is private to the
//! supplied journal parent and its terminal evidence is durable at the
//! supplied evidence path.

use lojix::Named as _;
use lojix::bootstrap::Redacting as _;
use lojix::bootstrap::{BootstrapInvocation as _, BootstrapRun};
use lojix::bootstrap_journal::BootstrapEvidenceStatus;

fn main() {
    match BootstrapRun::run_from_environment() {
        Ok(status) => {
            println!("(BootstrapTerminal.{})", status.as_str());
            if status == BootstrapEvidenceStatus::Failed {
                std::process::exit(2);
            }
        }
        Err(error) => {
            eprintln!("(BootstrapRejected [{}])", error.redacted());
            std::process::exit(2);
        }
    }
}
