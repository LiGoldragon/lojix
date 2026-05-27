//! Hand-written Kameo actor topology over the schema-emitted nouns.
//!
//! Every actor's `State` carries data that names what the actor IS.
//! Methods on schema-emitted types live in `codec.rs` (Input/Output
//! framing, lower/raise between Input/SemaCommand/SemaResponse/Output).
//! Each plane gets its own actor file; the root composes them.

pub mod activator;
pub mod authorization;
pub mod builder;
pub mod codec;
pub mod communicate;
pub mod copier;
pub mod engine;
pub mod gc_root;
pub mod nexus;
pub mod observation;
pub mod root;
pub mod run;
pub mod socket;
pub mod store;
pub mod toolchain;
pub mod trace;

pub use activator::Activator;
pub use authorization::{AuthorizationGate, AuthorizationPolicy};
pub use builder::Builder;
pub use communicate::{Communicate, UnixSocketCommunicate};
pub use copier::ClosureCopier;
pub use engine::Engine;
pub use gc_root::GcRootPinner;
pub use nexus::{NexusActorRefs, NexusHooks, NexusMailKeeper};
pub use observation::ObservationFan;
pub use root::LojixRoot;
pub use run::RunDaemon;
pub use socket::SocketListener;
pub use store::Store;
pub use toolchain::{ProcessToolchain, ToolchainMode};
pub use trace::{TraceLog, TraceWitness};
