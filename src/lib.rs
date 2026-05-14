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
//! ## Status
//!
//! Scaffolding. The library + binary entry points compile and link
//! against the substrate dependencies. Actor implementations
//! (`LiveSetActor`, `GcRootActor`, `EventLogActor`,
//! `ContainerLifecycleActor`, socket accept loop, supervisor root)
//! land in subsequent commits per the architecture in
//! `ARCHITECTURE.md`.

// Re-export the wire vocabulary so dependents (the binaries below)
// reach `Request`, `Reply`, and the typed records via `lojix::wire`.
pub use signal_lojix as wire;
