You MUST read [~/primary/repos/lore/AGENTS.md](../../../lore/AGENTS.md) — the canonical agent contract.

# lojix — agent carve-outs

- **Status: active horizon-re-engineering implementation (2026-05-16).**
  This repo now has a Rust crate, flake, `lojix-daemon` binary, thin
  `lojix` CLI binary, socket/runtime tests, and binary-level Nix
  integration witnesses. The user has explicitly green-lit continued
  implementation on this branch.

- **Future infrastructure, active branch.** Per the active-repository
  map, this daemon is the new deploy implementation surface. Do not
  assume current cluster deploys flow through it until the production
  deploy stack consumes this branch.

- **CLI has one peer.** The human-facing `lojix` binary talks only to
  `lojix-daemon` over Signal frames on the configured Unix socket. It
  never reads Horizon inputs, invokes Nix, opens sema state, or talks to
  another runtime peer.

- **Typed configuration boundary.** Production binaries read typed
  `signal-lojix` configuration records through `nota-config` argv
  sources. Environment variables are not a production control-plane
  channel.

- **Spec.** `ARCHITECTURE.md` is the local source of truth.
  Cross-references:
  - `~/primary/reports/system-assistant/04-dedicated-cloud-host-plan-second-revision.md`
    §P5 — the broader implementation plan.
  - `signal-lojix` at `github:LiGoldragon/signal-lojix` — wire contract.

- **Actor discipline.** Every stateful plane is a Kameo actor with
  declared mailbox, message protocol, and supervision.
  No zero-state holders per
  `~/primary/skills/actor-systems.md` §"Zero-sized actors are not
  actors".

- **Push, not poll.** Per `~/primary/skills/push-not-pull.md`. The
  daemon emits observations; consumers subscribe.
