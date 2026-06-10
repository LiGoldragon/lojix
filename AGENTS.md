You MUST read [~/primary/repos/lore/AGENTS.md](../../../lore/AGENTS.md) — the canonical agent contract.

# lojix — agent carve-outs

- **Status: implemented triad-port crate (2026-06-10).** The live
  Rust crate is under `triad-port/` and ships `lojix-daemon` plus the
  thin `lojix` CLI. Production System/Home build requests without
  `build_attribute` materialize Horizon-derived flake inputs before
  `nix eval`; activating deploys still reject until copy/activate is
  target-safe. There is no repo flake yet; use the cargo suite under
  `triad-port/` until a Nix check surface lands.

- **Future infrastructure.** Per
  `~/primary/protocols/active-repositories.md` §"Replacement Stack
  (Future Infrastructure)", this daemon replaces the implementation
  surface of today's `lojix-cli` once shipped. Do not assume current
  cluster deploys flow through it.

- **Spec.** `INTENT.md` first, then `ARCHITECTURE.md`. Cross-reference:
  `signal-lojix` and `meta-signal-lojix` for the two wire contracts.

- **Actor discipline.** The daemon socket shell uses
  `triad-runtime`'s actor-native multi-listener. Generated Nexus
  execution and effect hooks are async; do not reintroduce
  blocking-pool bridges around the engine. No zero-state holders per
  `~/primary/skills/actor-systems.md`
  §"Zero-sized actors are not actors".

- **Push, not poll.** Per `~/primary/skills/push-not-pull.md`. The
  daemon emits observations; consumers subscribe.
