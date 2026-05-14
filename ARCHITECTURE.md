# lojix — architecture

`lojix` is the new deploy stack: one crate that ships a long-lived
deploy orchestrator daemon (`lojix-daemon`) plus a thin CLI client
(`lojix`) that speaks the daemon over a Unix socket.

> **Status (2026-05-14):** in-development — repo recently renamed from
> `lojix-daemon`. First implementation lands on the
> `horizon-re-engineering` feature branch alongside the parallel
> horizon schema refactor. Today's `lojix-cli` (separate repo) stays
> at the current schema for the duration; retires after CriomOS
> migrates to consume this daemon's projection.

> **Scope (today vs eventually).** This stack sits on today's substrate
> — Rust on Linux, `signal-core` over a Unix socket, `sema-engine`
> for durable state, direct nix invocations. It is a realization step
> toward the Sema-on-Sema future per `~/primary/ESSENCE.md` §"Today
> and eventually".

## 0 · Crate shape

One crate, two binaries (per `~/primary/AGENTS.md` §"Binary naming —
`-daemon` suffix"):

```
Cargo.toml:
  [lib] name = "lojix"
  [[bin]] name = "lojix-daemon"   # long-lived orchestrator
  [[bin]] name = "lojix"          # thin CLI client
```

The library half (`lojix`) holds the shared types, request/reply
plumbing, and the daemon actor implementations. The two binaries
are thin entry points: `lojix-daemon` brings up the actor supervisor
and binds the socket; `lojix` opens the socket, sends one
`signal-lojix` request, and prints one reply.

## 1 · Owned surface

- **`/run/lojix/daemon.sock`** — Unix socket binding (mode `0660`,
  cluster-operator group). Receives `signal-lojix` requests; emits
  `signal-lojix` replies and observations.
- **Live generation set** — `BTreeMap<(ClusterName, NodeName, Kind),
  Generation>` persisted via `sema-engine`. Source of truth for
  "what's running on every node right now."
- **GC roots tree** —
  `/nix/var/nix/gcroots/criomos/<cluster>/<node>/<kind>/<generation>`
  symlink layout per
  `~/primary/reports/system-assistant/04-dedicated-cloud-host-plan-second-revision.md`
  §P4. Two-phase deletion respecting narinfo TTL.
- **Deploy event log** — append-only log of typed events
  (`BuildRealized`, `CachePublished`, `ActivationSucceeded`,
  `GenerationRetired`, `ContainerStarted`, `ContainerStopped`).
  Subscribers consume via `signal-lojix` `DeploymentObservation` /
  `CacheRetentionObservation`.
- **Container lifecycle observation** — systemd dbus subscriptions
  for `containers.<name>.service` transitions; mirrors into the
  event log.
- **Thin CLI** — `lojix` binary reads a single Nota request (per the
  one-record operator-surface discipline that already lives in
  `lojix-cli/skills.md`), forwards it as a `signal-lojix` frame to
  the daemon, prints the reply.

## 2 · Not owned

- **Wire contract types** — `signal-lojix` owns the typed records
  (DeploymentSubmission/Accepted/Rejected/Observation,
  CacheRetentionRequest/Accepted/Rejected/Observation,
  GenerationQuery/Listing). This crate consumes them.
- **Wire transport primitives** — `signal-core` owns frames,
  envelopes, channel macro. This crate uses signal-core types for
  every inter-component byte; it does not invent a parallel framing.
- **Cluster proposal source** — `goldragon` (read per request via
  horizon-rs).
- **Per-host key material** — `clavifaber` (this stack is
  cluster-side, not per-host).
- **Cluster trust runtime** — separate component (today missing; see
  `~/primary/reports/system-specialist/118-criomos-state-and-sandbox-audit.md`
  §"Cluster-trust runtime is still missing").

## 3 · Code map (planned)

```
src/
  lib.rs                # module entry; types + handlers
  bin/
    lojix-daemon.rs     # daemon entry: socket bind, supervisor root
    lojix.rs            # CLI: read one nota, send, print one nota
  daemon/
    live_set.rs         # LiveSetActor: BTreeMap<...> via sema-engine
    gc_roots.rs         # GcRootActor: /nix/var/nix/gcroots/criomos/...
    events.rs           # EventLogActor: append-only typed events
    container.rs        # ContainerLifecycleActor: systemd dbus observer
    socket.rs           # accept loop; signal-core frame decode/encode
    supervisor.rs       # Kameo supervisor wiring
  client/
    mod.rs              # CLI's request/reply handling
```

Each daemon actor is a Kameo actor per `~/primary/skills/actor-systems.md`.
No zero-state holders.

## 4 · Storage and wire

- **Storage:** `sema-engine` (the typed database engine library;
  see `~/primary/skills/rust/storage-and-wire.md` §"The sema-engine
  pattern"). One redb file owned by the daemon; tables for live set,
  GC roots, event log, container lifecycle records.
- **Wire:** `signal-core` frames carrying `signal-lojix` records.
  Length-prefixed rkyv archives over the Unix socket.

## 5 · Invariants

- The daemon does not initiate deploys on its own. It receives
  requests (`DeploymentSubmission`) and records what happens.
  Operator intent comes from outside.
- Every external operation is a typed `signal-lojix` request.
  Daemon-internal actor messages stay internal.
- Push, never poll. Subscribers register; the daemon pushes
  `DeploymentObservation` and `CacheRetentionObservation` as events
  occur. See `~/primary/skills/push-not-pull.md`.
- The daemon is cluster-operator-owned, not per-host. A single
  instance per operator workstation (or per shared deploy host); not
  running on every cluster node.
- One Nota record in, one Nota record out at the socket boundary
  (matches the operator surface discipline today's `lojix-cli/skills.md`
  established).

## 6 · Cross-cutting context

- Workspace `~/primary/ESSENCE.md` is upstream of every rule.
- `signal-lojix` at `github:LiGoldragon/signal-lojix` is the wire
  vocabulary.
- `signal-core` at `github:LiGoldragon/signal-core` is the wire
  kernel.
- `sema-engine` at `github:LiGoldragon/sema-engine` is the typed
  database engine.
- `horizon-rs` at `github:LiGoldragon/horizon-rs` is the projection
  of cluster proposals; this stack reads horizon per request, never
  edits it.
- `lojix-cli` at `github:LiGoldragon/lojix-cli` is the legacy
  monolithic orchestrator; stays at the current schema for the
  duration of the horizon re-engineering arc; retires after
  CriomOS migrates to consume this daemon's projection.
