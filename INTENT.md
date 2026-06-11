# INTENT — lojix

*What the psyche has explicitly intended for this project.
Synthesised from psyche statements and applicable workspace
constraints; not embellished. `ARCHITECTURE.md` says what lojix
IS; this file says what the psyche wants it to BE.*

## Purpose

`lojix` is the new deploy stack: one crate shipping a long-lived
deploy orchestrator daemon (`lojix-daemon`) plus a thin CLI client
(`lojix`) that speaks the daemon over a Unix socket. It is the
cluster-operator-owned authority for "what generation is running on
every node right now," GC-roots retention, and the deploy event
log. It replaces the implementation surface of the monolithic
`lojix-cli`; the legacy CLI stays at its current schema and retires
after CriomOS migrates to consume this daemon's projection.

## Constraints

- **One crate, two binaries; the CLI is the daemon's thin first
  client.** `lojix-daemon` (long-lived orchestrator) and `lojix`
  (CLI) per the binary-naming rule. The CLI reads one NOTA request
  per invocation, forwards it as a `signal-lojix` frame, and prints
  one reply or streams events. Per `primary/skills/component-triad.md`
  and `primary/AGENTS.md` §"Binary naming".
- **The wire vocabularies are `signal-lojix` and
  `meta-signal-lojix`; the wire kernel is `signal-frame`.** Every
  external operation is a typed Signal variant — no untyped escape
  hatch on the wire. This crate consumes schema-derived contract
  output; it does not invent parallel framing.
- **SEMA tables are daemon-owned.** The live generation set, GC
  roots, event log, and container-lifecycle records are modeled as
  schema-derived SEMA tables. The current implementation is
  in-memory behind a shared store; durable redb/sema-engine backing
  remains the next storage cutover.
- **The daemon starts from binary rkyv configuration only.** Launch
  tooling encodes typed configuration before exec; `lojix-daemon`
  rejects inline NOTA and `.nota` startup files and never parses
  textual configuration.
- **The daemon binds two authority-tiered Unix sockets.** The
  ordinary socket serves peer-callable reads/subscriptions; the
  owner/meta socket serves deploy and retention mutations. The
  owner socket mode must not grant other-access.
- **Push, never poll.** Subscribers register; the daemon pushes
  `DeploymentObservation` and `CacheRetentionObservation` events as
  they occur, bridged downstream of the commit so delta delivery
  cannot roll back the write transaction. Per
  `primary/skills/push-not-pull.md`.
- **Operator intent is sovereign.** The daemon never initiates
  deploys on its own — every deploy starts from a received
  `DeploymentSubmission`. It records what happens in response to
  typed requests; it does not invent its own schedule.
- **Cluster-operator-owned, not per-host.** A single instance per
  operator workstation or shared deploy host, not running on every
  cluster node.

## Stack discipline

- Actor-native runtime surfaces are data-bearing nouns; no zero-state
  holders; daemon-internal actor messages stay inside the crate.
  Generated Nexus execution and child-process effects are async, not
  isolated behind blocking-pool bridges. Per `primary/skills/actor-systems.md`.
- Full English words; no crate-name prefix on types. Per
  `primary/skills/naming.md`.

## Scope — today, not eventually

lojix sits on today's substrate (Rust on Linux, Signal over Unix
sockets, schema-derived Nexus/SEMA tables, direct Nix invocations).
It is a realization step toward the Sema-on-Sema future, built
rightly for today's deploy need. Per `primary/ESSENCE.md` §"Today and
eventually".

*Source statements live in Spirit intent records and the project's
`ARCHITECTURE.md`. The wire contract intent stays in
`signal-lojix/INTENT.md`. Workspace-shape intent stays in
`primary/INTENT.md` and the named skills above.*
