# lojix — architecture

`lojix` is the new deploy stack: one crate that ships a long-lived
deploy orchestrator daemon (`lojix-daemon`) plus a thin CLI client
(`lojix`) that speaks the daemon over a Unix socket.

> **Status (2026-05-15):** in-development — repo recently renamed
> from `lojix-daemon` (2026-05-14). First implementation lands on
> the `horizon-re-engineering` feature branch alongside the parallel
> horizon schema refactor. Today's `lojix-cli` (separate repo) stays
> at the current schema for the duration; retires after CriomOS
> migrates to consume this daemon's projection.

> **Scope (today vs eventually).** This stack sits on today's
> substrate — Rust on Linux, `signal-core` over a Unix socket,
> `sema-engine` for durable state, direct nix invocations. It is a
> realization step toward the Sema-on-Sema future per
> `~/primary/ESSENCE.md` §"Today and eventually".

## 0 · Crate shape

One crate, two binaries (per `~/primary/AGENTS.md` §"Binary naming —
`-daemon` suffix"):

```
Cargo.toml:
  [lib] name = "lojix"
  [[bin]] name = "lojix-daemon"   # long-lived orchestrator
  [[bin]] name = "lojix"          # thin CLI client
```

The library half (`lojix`) holds the shared types, the daemon's
actor implementations, and the CLI's request/reply plumbing. The
two binaries are thin entry points: `lojix-daemon` brings up the
actor supervisor and binds the socket; `lojix` opens the socket,
sends one `signal-lojix` request, and prints one reply or
streams subscription events.

## 1 · Owned surface

- **`/run/lojix/daemon.sock`** — Unix socket binding (mode `0660`,
  cluster-operator group). Receives `signal-lojix` requests; emits
  `signal-lojix` replies and observation events.
- **Live generation set** — `BTreeMap<(ClusterName, NodeName, Kind),
  Generation>` persisted via `sema-engine`. Source of truth for
  "what's running on every node right now."
- **GC roots tree** —
  `/nix/var/nix/gcroots/criomos/<cluster>/<node>/<kind>/<generation>` →
  `<store-path>` symlinks. Per-`<kind>` slots: `current` (active
  top-level), `boot-pending` (closure on `system.profile` not yet
  activated), `rollback/<n>` (last N rolled-back generations,
  default 4), `pinned/<label>` (operator-pinned releases),
  `recent/<timestamp>` (short-grace builds protecting freshly-built
  closures from cache eviction). Closure introspection via
  `nix path-info -r`; do not reimplement Nix's reachability graph.
  Two-phase deletion respecting narinfo TTL.
- **Deploy event log** — append-only log of typed events
  (`BuildRealized`, `CachePublished`, `ActivationSucceeded`,
  `GenerationRetired`, `ContainerStarted`, `ContainerStopped`).
  Subscribers consume via `signal-lojix` `DeploymentObservation` and
  `CacheRetentionObservation`, bridged through `sema-engine`'s
  `SubscriptionSink` trait.
- **Container lifecycle observation** — systemd dbus subscriptions
  for `containers.<name>.service` transitions; mirrors into the
  event log.
- **Thin CLI** — `lojix` binary reads a single NOTA request per the
  one-record operator-surface discipline, forwards it as a
  `signal-lojix` frame to the daemon, and prints the reply or
  streams events.

## 2 · Not owned

- **Wire vocabulary** — `signal-lojix` owns the typed records and
  the `signal_channel!` declaration that fixes the channel's verbs,
  events, and stream relations. This crate consumes the macro
  output.
- **Wire kernel** — `signal-core` owns `StreamingFrame`,
  `ExchangeIdentifier`, `StreamEventIdentifier`, the verb spine,
  and the channel-macro engine. This crate uses signal-core types
  for every inter-component byte and does not invent parallel
  framing.
- **Storage kernel** — `sema-engine` owns table registration, Signal
  verb execution (`assert`, `mutate`, `retract`, `commit`, `match`,
  `validate`, `subscribe`), the commit log, snapshot identity, and
  the subscription-delivery primitive. `sema` (the storage kernel
  beneath it) owns redb/rkyv mechanics. This crate consumes both
  through `sema-engine`'s public surface.
- **Cluster proposal source** — `goldragon` (read per request via
  horizon-rs).
- **Per-host key material** — `clavifaber` (this stack is
  cluster-side, not per-host).
- **Cluster trust runtime** — separate component (today missing).
  Horizon carries policy and fingerprints; ClaviFaber emits local
  public material; a separate runtime distributes that public
  material across the cluster.

## 3 · Code map (planned)

```
src/
  lib.rs                # module entry; types + handlers
  bin/
    lojix-daemon.rs     # daemon entry: socket bind, supervisor root
    lojix.rs            # CLI: read one NOTA, send, print one NOTA
  daemon/
    live_set.rs         # LiveSetActor: BTreeMap<...> via sema-engine
    gc_roots.rs         # GcRootActor: /nix/var/nix/gcroots/criomos/...
    events.rs           # EventLogActor: append-only typed events
    container.rs        # ContainerLifecycleActor: systemd dbus observer
    subscriptions.rs    # SubscriptionSink bridge: sema-engine deltas
                        # → signal-lojix DeploymentObservation /
                        # CacheRetentionObservation events on the wire
    socket.rs           # accept loop; signal-core frame decode/encode
    supervisor.rs       # Kameo supervisor wiring
  client/
    mod.rs              # CLI's request/reply handling
```

Each daemon actor is a Kameo actor per
`~/primary/skills/actor-systems.md`. No zero-state holders.

## 4 · Storage and wire

- **Storage:** `sema-engine` (the typed database engine library;
  see `~/primary/skills/rust/storage-and-wire.md` §"The sema-engine
  pattern"). One redb file owned by the daemon. Tables for live
  set, GC roots, event log, and container-lifecycle records are
  registered through `Engine::register_table` at startup.
- **Wire:** `signal-core` frames carrying `signal-lojix` records.
  Length-prefixed rkyv archives over the Unix socket. Because the
  daemon emits subscription events, the channel uses
  `StreamingFrame` / `StreamingFrameBody`.

## 5 · Constraints

- The daemon binds exactly one Unix socket at
  `/run/lojix/daemon.sock` with mode `0660` and the
  cluster-operator group.
- The CLI sends one NOTA-encoded `signal-lojix` request per
  invocation and prints one NOTA-encoded reply (or streams events
  until the subscription closes).
- Every external operation is a typed `signal-lojix` variant;
  there is no untyped escape hatch on the wire.
- The daemon never initiates deploys on its own — every deploy
  starts from a received `DeploymentSubmission`.
- The daemon opens its `sema-engine` handle through
  `Engine::open(EngineOpen::new(path, SchemaVersion))` at startup
  and registers every table family before serving requests.
- Subscription events ride on the acceptor's outbound lane via
  `StreamingFrameBody::SubscriptionEvent`; the daemon mints each
  event's `StreamEventIdentifier` from the lane's monotonic
  `LaneSequence`.
- The daemon's subscription bridge is downstream of the commit:
  `sema-engine` delta delivery cannot roll back the write
  transaction.
- Daemon-internal actor messages stay inside the crate; only
  `signal-lojix` records cross the socket.

## 6 · Invariants

- Push, never poll. Subscribers register; the daemon pushes
  `DeploymentObservation` and `CacheRetentionObservation` events as
  they occur. See `~/primary/skills/push-not-pull.md`.
- The daemon is cluster-operator-owned, not per-host. A single
  instance per operator workstation (or per shared deploy host);
  not running on every cluster node.
- Operator intent is sovereign — the daemon records what happens
  in response to typed requests; it does not invent its own
  schedule.

## 7 · Cross-cutting context

- Workspace `~/primary/ESSENCE.md` is upstream of every rule.
- `signal-lojix` at `github:LiGoldragon/signal-lojix` is the wire
  vocabulary; the daemon's external boundary is exactly that
  channel.
- `signal-core` at `github:LiGoldragon/signal-core` is the wire
  kernel.
- `sema-engine` at `github:LiGoldragon/sema-engine` is the typed
  database engine.
- `sema` at `github:LiGoldragon/sema` is the storage kernel beneath
  `sema-engine`; this crate depends on it only transitively.
- `horizon-rs` at `github:LiGoldragon/horizon-rs` is the projection
  of cluster proposals; this stack reads horizon per request, never
  edits it.
- `lojix-cli` at `github:LiGoldragon/lojix-cli` is the legacy
  monolithic orchestrator; stays at the current schema for the
  duration of the horizon re-engineering arc; retires after
  CriomOS migrates to consume this daemon's projection. It does
  not become a client of this daemon.
