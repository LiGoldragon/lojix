# lojix-daemon — skills

The repo is currently a skeleton; this file will fill out when
implementation starts. Until then:

## Status

- No code lands here until implementation kicks off. The canonical
  specification is in `ARCHITECTURE.md` and in
  `~/primary/reports/system-assistant/04-dedicated-cloud-host-plan-second-revision.md`
  §P5.

## Required reading when implementation starts

- `~/primary/skills/actor-systems.md` — Kameo discipline; no
  zero-state holders.
- `~/primary/skills/push-not-pull.md` — producers push, consumers
  subscribe.
- `~/primary/skills/rust-discipline.md` — methods on types, domain
  newtypes, one-object-in/out, error enums.
- `~/primary/skills/rust/storage-and-wire.md` — sema-db patterns.
- `~/primary/skills/contract-repo.md` — how to consume `signal-lojix`.
- `~/primary/skills/nix-discipline.md` — flake-input forms,
  `nix flake check` as canonical pre-commit runner.
- `~/primary/skills/testing.md` — Nix-backed pure / stateful / chained
  test surfaces.

## Related repos

- `signal-lojix` — wire contract this daemon implements.
- `lojix-cli` — today's monolithic orchestrator; becomes a thin client
  over this daemon post-cutover.
- `horizon-rs` — cluster proposal projection (read-only per request).
- `goldragon` — proposal source.
- `clavifaber` — per-host key material (separate component).
