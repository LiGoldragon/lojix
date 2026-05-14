//! lojix — thin CLI client for `lojix-daemon`.
//!
//! Today this is a placeholder that exits cleanly. Subsequent commits
//! wire up the one-record-in / one-record-out shape per
//! `ARCHITECTURE.md`: read one Nota request from stdin (or argv),
//! open `/run/lojix/daemon.sock`, send a `signal-core` frame carrying
//! a `signal_lojix::Request`, await the matching `signal_lojix::Reply`,
//! print it as Nota.

use std::process::ExitCode;

#[tokio::main]
async fn main() -> ExitCode {
    eprintln!(
        "lojix: scaffold ({}). \
         One-nota-record-in / one-nota-record-out lands in subsequent commits.",
        env!("CARGO_PKG_VERSION")
    );
    ExitCode::SUCCESS
}
