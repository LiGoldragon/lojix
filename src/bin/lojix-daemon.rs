//! lojix-daemon — long-lived deploy orchestrator entry point.
//!
//! Today this is a placeholder that exits cleanly. Subsequent commits
//! wire up the supervised actor runtime per `ARCHITECTURE.md`:
//! `RuntimeRoot` → (`LiveSetActor`, `GcRootActor`, `EventLogActor`,
//! `ContainerLifecycleActor`, socket accept loop). The socket lives at
//! `/run/lojix/daemon.sock` (mode `0660`, cluster-operator group).

use std::process::ExitCode;

#[tokio::main]
async fn main() -> ExitCode {
    eprintln!(
        "lojix-daemon: scaffold ({}). \
         Actor runtime + socket accept loop land in subsequent commits.",
        env!("CARGO_PKG_VERSION")
    );
    ExitCode::SUCCESS
}
