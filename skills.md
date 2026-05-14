# lojix — skills

The new deploy stack: one crate, two binaries (`lojix-daemon` long-lived
orchestrator + `lojix` thin CLI client). Implementation lands on the
`horizon-re-engineering` feature branch.

---

## Repo intent

This repo is the **implementation home** for the new deploy stack that
replaces today's monolithic `lojix-cli`. The architectural decisions
are settled (per `ARCHITECTURE.md`); what remains is implementation.

The library half (`lojix`) holds shared types, Kameo actor
implementations, and request/reply plumbing. The two binaries
(`lojix-daemon`, `lojix`) are thin entry points.

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
- `~/primary/skills/contract-repo.md` — how to consume `signal-lojix`.
- `~/primary/skills/nix-discipline.md` — flake-input forms,
  `nix flake check` as canonical pre-commit runner.
- `~/primary/skills/testing.md` — Nix-backed pure / stateful / chained
  test surfaces.
- `~/primary/skills/feature-development.md` — worktree-based feature
  branches; this repo's first arc lands on `horizon-re-engineering`.

## Storage and wire defaults

- **Storage:** `sema-engine` (the full typed database engine library
  over `sema` and `signal-core`). One redb file owned by the
  `lojix-daemon` binary. Don't reach for `sema` directly unless the
  engine doesn't expose the surface you need (rare).
- **Wire:** `signal-core` frames carrying `signal-lojix` records.
  Length-prefixed rkyv archives over the Unix socket. Don't invent
  parallel framing or envelope mechanisms.

## Related repos

- `signal-lojix` — typed wire contract (records this stack
  produces/consumes).
- `signal-core` — wire kernel; the substrate signal-lojix builds on.
- `sema-engine` — typed database engine; depend on this rather than
  `sema` directly.
- `horizon-rs` — cluster proposal projection; read-only per request.
- `goldragon` — cluster proposal source (Nota records read by
  horizon-rs).
- `clavifaber` — per-host key material; separate component.
- `lojix-cli` — legacy monolithic orchestrator. Stays at the current
  schema for the duration of the horizon re-engineering arc; retires
  after CriomOS migrates to consume this daemon's projection.

## Status (2026-05-14)

- Repo recently renamed from `lojix-daemon`. GitHub redirect from old
  name in place.
- First commits land on the `horizon-re-engineering` branch (see
  `~/primary/skills/feature-development.md` for the worktree-based
  branch convention).
- The `signal-lojix` contract crate (`github:LiGoldragon/signal-lojix`)
  evolves in parallel.
