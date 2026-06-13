# INTENT — lojix

*What the psyche has explicitly intended for this project.
Synthesised from psyche statements and applicable workspace
constraints; not embellished. `ARCHITECTURE.md` says what lojix
IS; this file says what the psyche wants it to BE.*

## Purpose

`lojix` is the new deploy stack and the next production logics: one
crate shipping a long-lived deploy orchestrator daemon (`lojix-daemon`)
plus **two CLIs, one per socket** (`lojix` on the ordinary socket,
`meta-lojix` on the meta socket) and a `lojix-write-configuration`
bootstrap tool that encodes typed configuration into the daemon's
binary startup. It is the cluster-operator-owned authority for "what
generation is running on every node right now," GC-roots retention, and
the deploy event log. The goalpost is feature parity with the
monolithic `lojix-cli` and cutover so the cluster runs on the
daemon-based stack; the legacy CLI stays at its current schema and
retires once CriomOS consumes this daemon's projection.

## Constraints

- **One crate; one daemon, two CLIs, one config-writer.** The crate
  ships `lojix-daemon` (the long-lived orchestrator) plus **two CLI
  clients, one per socket** — `lojix` on the ordinary socket
  (`signal-lojix`: Query / Watch / Unwatch / CheckHostKeyMaterial) and
  `meta-lojix` on the meta socket (`meta-signal-lojix`: Deploy / Pin /
  Unpin / Retire / Configure) — and `lojix-write-configuration`, the
  deploy/bootstrap tool that encodes typed NOTA configuration into the
  daemon's binary rkyv startup. Each CLI reads one NOTA request per
  invocation, forwards it as a frame on its socket's contract, and
  prints the reply. This mirrors the shipped spirit precedent
  (`spirit` / `meta-spirit` / `spirit-daemon` /
  `spirit-write-configuration`). Per Spirit `ssk2` (two CLIs, one per
  socket), the meta-naming rule `8bwo`, and
  `primary/skills/component-triad.md`.
- **The wire vocabularies are `signal-lojix` and
  `meta-signal-lojix`; the wire kernel is `signal-frame`.** Every
  external operation is a typed Signal variant — no untyped escape
  hatch on the wire. This crate consumes schema-derived contract
  output; it does not invent parallel framing.
- **SEMA tables are daemon-owned and durable.** The live generation
  set, GC roots, event log, and container-lifecycle records are
  schema-derived SEMA tables backed by a durable `sema-engine` store
  (`<state-directory>/lojix.sema`) — one keyed row per element, not one
  blob per table. Opening the engine resumes the persisted catalog,
  commit sequence, and records, so daemon state and restart-safe
  identifier issuance survive a process restart (Spirit `oh9l`,
  `ur16`). Atomic version-controlled backup is the named follow-on.
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

## Production cutover charter

The active goalpost: finalize lojix as the next production logics and
move the cluster onto the daemon-based stack. Parity with `lojix-cli`
is the bar (Spirit `tvbn` — reach parity, then switch per node and
retire the dual stacks):

- **Full-OS deploy.** The daemon deploys a complete OS generation
  (System and Home), not just eval/build — copy and activate must be
  target-safe.
- **Survives SSH disconnect.** A durable deploy is owned by a job
  actor that owns the external process and persists job state; process
  lifetime is decoupled from the request stream, so a dropped client
  does not abort the deploy. Per Spirit `up9q` (and the `lojix-cli`
  `systemd-run --collect` transient-unit reference it ports).
- **Every operation described in schema types.** No untyped escape
  hatch on any operation; both contracts already type the full
  operation surface.
- **Durable-first state.** Build the sema-engine / redb durable
  backing (live set, GC roots, event log) with self-resume on restart
  *before* the first cutover, not on in-memory state. Per Spirit
  `oh9l`.
- **Validated end-to-end against the full routed microVM.** The
  cutover-validation deploy lands a full OS on a routed microVM with
  its own Criome domain and reachable IP, surviving SSH disconnect.
  Per Spirit `se72`.

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
