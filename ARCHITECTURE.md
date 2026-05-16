# lojix — architecture

`lojix` is the new deploy stack: one crate that ships a long-lived
deploy orchestrator daemon (`lojix-daemon`) plus a thin CLI client
(`lojix`) that speaks the daemon over a Unix socket.

> **Status (2026-05-16):** in-development — repo recently renamed from
> `lojix-daemon`. The `horizon-re-engineering` branch now has the first
> socket/client/runtime slice against the current `signal-core` streaming
> channel macro, typed daemon/CLI configuration, and the first
> build-only deploy actor slice. Today's `lojix-cli` (separate repo)
> stays at the current schema for the duration; retires after CriomOS
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

- **`/run/lojix/daemon.sock`** — Unix socket binding. Receives
  `signal-core` frames carrying `signal-lojix` requests; emits
  matching replies. Production service wiring owns the final mode and
  group.
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
  Subscribers consume via `signal-lojix` streams opened by
  `DeploymentObservationSubscription` /
  `CacheRetentionObservationSubscription`.
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

## 3 · Code map

```
src/
  lib.rs                # module entry; public exports
  client.rs             # thin client: one Nota request -> one Nota reply
  deploy.rs             # deployment/event-log actors + build-only request path
  error.rs              # crate-owned typed errors
  process.rs            # typed external-process invocations/toolchain
  runtime.rs            # Kameo RuntimeRoot + first message handler
  socket.rs             # listener + per-connection actors + frames
  bin/
    lojix-daemon.rs     # daemon entry: socket bind, supervisor root
    lojix.rs            # CLI: read one nota, send, print one nota
```

Next implementation slices add the sema-backed durable actors:

```
src/daemon/
  live_set.rs           # LiveSetActor: BTreeMap<...> via sema-engine
  gc_roots.rs           # GcRootActor: /nix/var/nix/gcroots/criomos/...
  events.rs             # EventLogActor: sema-backed append-only typed events
  container.rs          # ContainerLifecycleActor: systemd dbus observer
  supervisor.rs         # Kameo supervisor wiring
```

Each daemon actor is a Kameo actor per
`~/primary/skills/actor-systems.md`. No zero-state holders.

## 4 · Storage and wire

- **Storage:** `sema-engine` (the typed database engine library;
  see `~/primary/skills/rust/storage-and-wire.md` §"The sema-engine
  pattern"). One redb file owned by the daemon; tables for live set,
  GC roots, event log, container lifecycle records.
- **Wire:** `signal-core` frames carrying `signal-lojix` records.
  Length-prefixed rkyv archives over the Unix socket. Streaming
  observations are modeled as `signal-core` stream kinds, not ad hoc
  reply variants.

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
- One Nota record in, one Nota record out at the socket boundary.

## 6 · Constraints

Each constraint below becomes a test (per ESSENCE §"Constraints
become tests"). The new stack is "ready to replace the old one"
when every constraint has a green witness *and* the test cluster
exercises the deploy path through `nspawn-dune-on-prometheus`-style
end-to-end smoke against a controller-hosting node.

**Crate shape**
- C1. `lojix` is one crate with two binaries: `lojix-daemon` and
  `lojix`. No `-cli` suffix on the CLI binary.
- C2. The library `lojix` re-exports the wire vocabulary as
  `lojix::wire` (= `signal_lojix`).
- C3. Storage is `sema-engine`. Wire is `signal-core` carrying
  `signal-lojix` records. No second actor framework, no second
  database engine.

**Wire boundary**
- C4. `lojix-daemon` binds the socket named by
  `LojixDaemonConfiguration.daemon_socket_path` at startup; binds
  nowhere else. Production service wiring passes
  `/run/lojix/daemon.sock` and the cluster-operator group.
- C5. The socket carries only `signal-core`-framed
  `signal-lojix::Request` / `signal-lojix::Reply` payloads.
- C6. `lojix` reads `LojixCliConfiguration` from argv position 0,
  opens the configured socket, sends one Nota request read from argv
  position 1+ or stdin, prints one Nota reply, exits.
- C7. Frame decode rejects short prefixes, mismatched lengths, and
  bytecheck failures with typed errors (delegated to `signal-core`).
- C7a. The flake has a binary-level Nix witness that starts the
  installed `lojix-daemon`, drives the installed `lojix` client over a
  private Unix socket using typed `nota-config` files, checks socket
  mode, argv and stdin request modes, opens an observation subscription,
  and proves a stalled raw socket client does not block a second CLI
  request.

**Configuration boundary**
- C7b. Production binaries read control-plane configuration through
  `nota_config::ConfigurationSource`; environment variables are not a
  production socket/configuration channel. Witness:
  `tests/configuration_boundary.rs`.
- C7c. `LojixDaemonConfiguration` is data-bearing: the daemon applies
  the configured socket mode, optional socket group, state directory,
  GC-root directory, operator identity, owned cluster, and peer daemon
  bindings at startup.
- C7d. `LojixCliConfiguration` is control-plane only. Deploy plans,
  generation queries, and cache-retention mutations remain data-plane
  `Request` records and are never embedded in CLI configuration.

**Actor topology**
- C8. `RuntimeRoot` is a Kameo `Actor` with state carrying child
  refs (`LiveSetActor`, `GcRootActor`, `EventLogActor`,
  `ContainerLifecycleActor`, `SocketAcceptor`). No ZST root.
- C9. Each daemon-internal plane is its own Kameo actor with a
  named state field; no `State = ()` actors.
- C10. Failure policy: each supervisor has typed
  `RestartPolicy::Permanent` for sema-backed actors (LiveSet,
  EventLog) and `RestartPolicy::Never` for transient actors
  (per-connection handlers).
- C11. No `Arc<Mutex<T>>` between actors. State has one owner.
- C12. No detached `tokio::spawn` in production code. Long-running
  work is a supervised actor or `DelegatedReply<R>` for short reply
  deferral. The socket listener spawns one Kameo actor per accepted
  connection so a stalled client cannot hold the listener.

**Durable state**
- C13. The live generation set lives in a sema-engine table
  (`Generation { generation, cluster, node, kind, store_path,
  state }`); reconstructed on restart from sema, not from memory.
- C14. The deploy event log is append-only via sema-engine
  `Assert`; subscribers receive deltas through sema-engine
  `Subscribe` (push-not-poll).
- C15. GC roots are filesystem state at
  `/nix/var/nix/gcroots/criomos/<cluster>/<node>/<kind>/<generation>`
  with per-`<kind>` slots (`current`, `boot-pending`,
  `rollback/<n>`, `pinned/<label>`, `recent/<timestamp>`); the
  daemon never queries them via polling — its in-memory + sema
  view is the source of truth.

**Deploy pipeline**
- C16. `DeploymentSubmission` triggers the projection-then-build
  pipeline: read horizon-rs in-process, project the requested
  cluster/node, build the toplevel via `nix build` with the
  projected horizon as override-input, copy the closure to the
  target node, activate per the requested `SystemAction`.
  Current implemented slice accepts only build-only submissions,
  rejects local builds and activation actions before any external
  tool runs, stages generated Horizon/System/Deployment inputs to the
  remote builder, and records `Submitted` / `Building` / `Built` /
  `Failed` observations.
  While this branch is under construction, deploy-facing examples and
  tests target the matching `horizon-re-engineering` branches of
  `CriomOS`, `goldragon`, and `horizon-rs`; default-branch examples are
  not valid witnesses for this arc.
- C17. Each pipeline phase emits a `DeploymentObservation` event
  (`Submitted`, `Building`, `Built`, `Copying`, `Activating`,
  `Succeeded` / `Failed`); subscribers see them live.
  Current implemented slice exposes the in-memory event log through
  subscription-open snapshots; live pushed stream frames remain part
  of the next stream-delivery slice.
- C18. Activation failure rolls back the GC root for that kind
  (the failed generation does not become `current`).

**Cache retention**
- C19. `CacheRetentionRequest::PinGeneration` adds the generation
  to `pinned/<label>`; `UnpinGeneration` removes it; `RetireGeneration`
  removes the generation's GC roots and emits a `Retired`
  observation. All transitions are committed through sema-engine's
  commit boundary so subscribers see one `SnapshotId` per request.

**Generation queries**
- C20. `GenerationQuery` returns the live set filtered by the
  query's optional `cluster`/`node`/`kind`. Read via sema-engine
  `Match`; never blocks the mailbox on disk reads (the snapshot
  is in memory).

**Test discipline**
- C21. Every constraint above has at least one test that fails
  if the constraint is violated. (Topology / trace / forbidden-edge
  / no-blocking / no-zst-actor families per
  `~/primary/skills/actor-systems.md` §"Test actor density".)
- C22. End-to-end smoke: a synthetic deploy on the test cluster
  (atlas eval + dune nspawn boot) succeeds and emits the expected
  `DeploymentObservation` event sequence.
- C23. `goldragon`'s `datom.nota` projects + builds + activates
  through the daemon for `prometheus` (the production-shape
  smoke).

**Cutover**
- C24. After every constraint is green, `lojix-cli` is retired:
  `CriomOS-home/flake.lock` and `CriomOS/flake.lock` no longer
  pin `lojix-cli`; `lojix` and `lojix-daemon` are the only
  cluster-deploy surfaces; the legacy `horizon-rs` `main` branch
  closes the gap with `horizon-re-engineering`.

## 7 · Cross-cutting context

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
