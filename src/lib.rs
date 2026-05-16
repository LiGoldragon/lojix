//! lojix — long-lived deploy orchestrator daemon + thin CLI client.
//!
//! This crate ships two binaries and a library:
//!
//! - `lojix-daemon` — supervised actor runtime that owns the live
//!   generation set, GC roots tree, and deploy event log; binds the
//!   `/run/lojix/daemon.sock` Unix socket and accepts `signal-lojix`
//!   requests, emits `signal-lojix` replies and observations.
//! - `lojix` — thin CLI client. Reads one Nota request, forwards it
//!   as a `signal-core` frame to the daemon, prints one Nota reply.
//!
//! ## Substrates
//!
//! - **Storage**: `sema-engine` (typed database engine over `sema`
//!   storage kernel). One redb file owned by the daemon binary.
//! - **Wire**: `signal-core` frames carrying `signal-lojix`
//!   request/reply records.
//! - **Text projection**: `nota-codec` for the CLI's nota-in /
//!   nota-out boundary.
//!
//! The current runtime slice owns the Signal socket path, typed
//! startup configuration, and a Kameo `RuntimeRoot` that accepts
//! build-only deployment submissions. Build jobs run in per-deployment
//! actors; realized outputs are pinned as GC roots before success is
//! reported; local builds and activation actions are rejected before
//! any external tool runs. Both production binaries read typed
//! `signal-lojix` startup configuration records through `nota-config`;
//! environment variables are not a production control-plane channel.

pub mod client;
pub mod deploy;
pub mod error;
pub mod process;
pub mod runtime;
pub mod socket;

// Re-export the wire vocabulary so dependents (the binaries below)
// reach `Request`, `Reply`, and the typed records via `lojix::wire`.
pub use signal_lojix as wire;

pub use client::Client;
pub use error::{Error, Result};
pub use runtime::{RuntimeConfiguration, RuntimeRequest, RuntimeRoot};
pub use socket::{Connection, DEFAULT_SOCKET_PATH, SocketAddress, SocketServer};
