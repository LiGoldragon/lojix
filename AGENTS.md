You MUST read [~/primary/repos/lore/AGENTS.md](../../../lore/AGENTS.md) — the canonical agent contract.

# lojix-daemon — agent carve-outs

- **Status: skeleton (2026-05-13).** No `Cargo.toml`, no `src/`, no
  `flake.nix`, no `skills.md` body beyond a stub. The repo exists to
  lock the namespace and host the architecture spec. Do not begin
  implementation here without explicit direction from the user.

- **Future infrastructure.** Per
  `~/primary/protocols/active-repositories.md` §"Replacement Stack
  (Future Infrastructure)", this daemon replaces the implementation
  surface of today's `lojix-cli` once shipped. Do not assume current
  cluster deploys flow through it.

- **Spec.** `ARCHITECTURE.md` is the local source of truth.
  Cross-reference: `signal-lojix` at `github:LiGoldragon/signal-lojix`
  — wire contract.

- **Actor discipline.** When implementation lands, every plane is a
  Kameo actor with declared mailbox, message protocol, and supervision.
  No zero-state holders per
  `~/primary/skills/actor-systems.md` §"Zero-sized actors are not
  actors".

- **Push, not poll.** Per `~/primary/skills/push-not-pull.md`. The
  daemon emits observations; consumers subscribe.
