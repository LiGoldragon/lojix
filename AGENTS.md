# lojix — agent carve-outs

- **Status: implemented crate (2026-06-10).** The live
  Rust crate is at the repo root and ships `lojix-daemon` plus the
  thin `lojix` CLI. Production host and user-environment build requests without
  `build_attribute` materialize Horizon-derived flake inputs before
  `nix eval`; activating deploys enter the copy/activate pipeline. The
  maintained flake exposes the root cargo suite plus the daemon-free
  `lojix-bootstrap` package/app for an explicitly authorized one-shot v4
  bootstrap.

- **Future infrastructure.** Per
  the active-repositories protocol §"Replacement Stack
  (Future Infrastructure)", this daemon is the direct typed deploy implementation surface. Do not
  assume current cluster deploys flow through it.

- **Spec.** `INTENT.md` first, then `ARCHITECTURE.md`. Cross-reference:
  `signal-lojix` and `meta-signal-lojix` for the two wire contracts.

- **Non-ideal operations.** For driving today's deliberately hacky
  SSH-based deployment — build-on-target, root-mediated user-environment
  activation, and deploying a different user on a different node — see
  `NON_IDEAL_AGENTS.md`.

- **Actor discipline.** The daemon socket shell uses
  `triad-runtime`'s actor-native multi-listener. Generated Nexus
  execution and effect hooks are async; do not reintroduce
  blocking-pool bridges around the engine. No zero-state holders per
  the actor-system discipline
  §"Zero-sized actors are not actors".

- **Push, not poll.** Per the push-not-pull discipline. The
  daemon emits observations; consumers subscribe.
