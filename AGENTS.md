You MUST read [~/primary/repos/lore/AGENTS.md](../../../lore/AGENTS.md) — the canonical agent contract.

# lojix-daemon — agent carve-outs

- **Status: schema-deep pilot.** This worktree contains the runnable
  `lojix-next` prototype: schema-emitted signal types, internal SEMA
  and actor mailbox nouns, Kameo runtime actors, and Nix-backed
  witness tests. Continue implementation only through the designed
  planes described in `ARCHITECTURE.md`; when a plane is missing,
  develop that plane instead of bypassing it.

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
