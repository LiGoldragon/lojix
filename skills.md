# lojix — skills

The new deploy stack: one crate, two binaries (`lojix-daemon` long-lived
orchestrator + `lojix` thin CLI client). The live crate is under
`triad-port/`.

## Repo intent

This repo is the **implementation home** for the new deploy stack that
replaces today's monolithic `lojix-cli`. The architectural decisions
are settled (per `ARCHITECTURE.md`); what remains is implementation.

The library half (`lojix`) holds shared types, schema-derived
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
  remote-build-safe Nix smoke testing. This repo does not yet have a
  flake check surface.
- `~/primary/skills/testing.md` — Nix-backed pure / stateful / chained
  test surfaces.

## Storage and wire defaults

- **Storage:** schema-derived SEMA table nouns over an in-memory
  shared store today. Redb/sema-engine durability is the next storage
  cutover.
- **Wire:** `signal-frame` records from `signal-lojix` and
  `meta-signal-lojix`. Length-prefixed rkyv archives over two Unix
  sockets. Don't invent parallel framing or envelope mechanisms.

## Related repos

- `signal-lojix` — ordinary peer-callable wire contract.
- `meta-signal-lojix` — owner/meta policy wire contract.
- `signal-frame` — wire kernel; the substrate both contracts build on.
- `sema-engine` — typed database engine; depend on this rather than
  `sema` directly.
- `horizon-rs` — cluster proposal projection; read-only per request.
- `goldragon` — cluster proposal source (Nota records read by
  horizon-rs).
- `clavifaber` — per-host key material; separate component.
- `lojix-cli` — legacy monolithic orchestrator. Stays at the current
  schema for the duration of the horizon re-engineering arc; retires
  after CriomOS migrates to consume this daemon's projection.

## Status (2026-06-07)

- The actor-native daemon socket shell is implemented in `triad-port/`.
- Live Nix smoke tests exercise a self-contained CriomOS test-cluster
  build with `max-jobs = 0` to avoid local laptop builds.
- There is no Nix flake check surface yet.
