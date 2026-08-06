# lojix — skills

The new deploy stack: one crate, two binaries (`lojix-daemon` long-lived
orchestrator + `lojix` thin CLI client). The live crate is at the
repo root.

## Repo intent

This repo is the **implementation home** for the daemon-based deploy
stack. The architectural decisions are settled (per `ARCHITECTURE.md`);
what remains is implementation and validation.

The library half (`lojix`) holds shared types, handwritten
Nexus/SEMA runtime code, the actor-native socket shell, and
request/reply plumbing. The two binaries (`lojix-daemon`, `lojix`) are
thin entry points.

## Required reading when implementation starts

**Workspace baseline** (every Rust crate the workspace ships reads
these):

- `~/primary/skills/actor-systems.md` — Kameo discipline; no
  zero-state holders.
- `~/primary/skills/push-not-pull.md` — producers push, consumers
  subscribe. Hard rule for every observation-emitting surface.
- `~/primary/skills/rust-discipline.md` — methods on types, domain
  newtypes, one-object-in/out, error enums.
- `~/primary/skills/rust/storage-and-wire.md` — sema-engine + signal-core
  defaults; rkyv archives between Rust components; redb tables for
  durable state.
- `~/primary/skills/contract-repo.md` — how to consume
  `signal-lojix` and `meta-signal-lojix`.
- `~/primary/skills/nix-discipline.md` — flake-input forms and
  remote-build-safe Nix smoke testing.
- `~/primary/skills/testing.md` — Nix-backed pure / stateful / chained
  test surfaces.

## Storage and wire defaults

- **Storage:** handwritten runtime nouns in SEMA tables over one durable, configured
  exact store path. The startup archive carries that path to the daemon and
  reset service; no component derives `lojix.sema`. The reset service supplies
  its archive through `LOJIX_CONFIGURATION` and calls only pathless inline
  `(ResetStore)`: it recreates recognised pre-v4 Lojix stores, reports v4 as
  `AlreadyCurrent`, and never selects a caller-named file.
- **Wire:** `signal-frame` records from `signal-lojix` and
  `meta-signal-lojix`. Length-prefixed rkyv archives over two Unix
  sockets. Don't invent parallel framing or envelope mechanisms.
- **Horizon materialization:** an explicit `DeploymentInputMode::Horizon`
  selects proposal projection. The daemon projects the request's cluster proposal
  through `horizon-rs`, writes generated flake inputs under its state
  directory, hashes them with Nix, and passes typed override inputs to
  eval. This is a Nexus `MaterializeHorizon` effect, not inline
  side-channel logic.

## Related repos

- `signal-lojix` — ordinary peer-callable wire contract.
- `meta-signal-lojix` — owner/meta policy wire contract.
- `signal-frame` — wire kernel; the substrate both contracts build on.
- `sema-engine` — typed database engine; depend on this rather than
  `sema` directly.
- `horizon-rs` — cluster proposal projection; read-only per request.
- deployment-specific cluster proposal sources (DOTOS records read by
  horizon-rs).
- `clavifaber` — per-host key material; separate component.
- `meta-signal-lojix` — owner/meta deploy and retention mutation
  contract consumed by `meta-lojix` and `lojix-daemon`.

## Status (2026-08-04)

- The actor-native daemon socket shell is implemented at the repo root.
- The handwritten Nexus runner and effect hooks are async;
  the daemon awaits `NexusEngine::execute` directly and does not wrap
  engine execution in `spawn_blocking`.
- Production host and user-environment deploy requests enter the
  materialization path; activating actions enter the copy/activate
  pipeline instead of being rejected as unsupported.
- A live ignored smoke exercises local `nix flake metadata` + `nix eval`
  through generated Horizon inputs without building a closure.
- The flake provides build, test, format, clippy, and startup-boundary checks.
