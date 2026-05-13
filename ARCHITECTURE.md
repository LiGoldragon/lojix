# lojix-daemon — architecture

*Long-lived deploy orchestrator daemon for cluster Nix operations.*

> **Status (2026-05-13):** Skeleton. Documentation only. No
> `Cargo.toml`, no `src/`, no `flake.nix`. Implementation lands per
> `~/primary/reports/system-assistant/04-dedicated-cloud-host-plan-second-revision.md`
> §P5. See `protocols/active-repositories.md` §"Replacement Stack
> (Future Infrastructure)" in the primary workspace.

## 0 · TL;DR

`lojix-daemon` is the long-lived owner of cluster deploy state. It
receives typed deploy requests over a Unix socket (`signal-lojix`
records), executes the build/copy/activate pipeline, observes the
resulting cluster state, and maintains the durable substrate needed for
cache retention and container lifecycle visibility.

Today's `lojix-cli` is one-shot: each invocation projects horizon,
builds, copies, activates, exits. After cutover, `lojix-cli` becomes a
thin client; this daemon owns persistent state.

> **Scope (eventual vs today).** This daemon sits on today's stack — Rust
> on Linux, `signal-core` over a Unix socket, `sema-db` for durable
> state, direct nix invocations. It is a realization step toward the
> Sema-on-Sema future per `~/primary/ESSENCE.md` §"Today and
> eventually".

## 1 · Owned Surface

- **`/run/lojix/daemon.sock`** — Unix socket binding (mode 0660,
  cluster-operator group). Receives `signal-lojix` requests; emits
  `signal-lojix` replies and observations.
- **Live generation set** — `BTreeMap<(ClusterName, NodeName, Kind),
  Generation>` persisted via `sema-db`. Source of truth for "what's
  running on every node right now."
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
- **Container lifecycle observation** — systemd dbus subscriptions for
  `containers.<name>.service` transitions; mirrors into the event log.

## 2 · Not Owned

- **Argv parsing** — `lojix-cli` owns the CLI text surface.
- **Wire contract** — `signal-lojix` owns the typed records.
- **Nix build/copy/activate primitives** — invoked but the
  orchestration shape comes from today's lojix-cli core; eventually
  shared via `lojix-core` (per plan 04 §5.1).
- **Cluster proposal source** — `goldragon` (read per request).
- **Per-host key material** — `clavifaber` (this daemon is
  cluster-side, not per-host).
- **Cluster trust runtime** — separate component (today missing; see
  `~/primary/reports/system-specialist/118-criomos-state-and-sandbox-audit.md`
  §"Cluster-trust runtime is still missing").

## 3 · Code Map (Planned)

```
src/
  main.rs           # daemon entry: socket bind, supervisor root
  live_set.rs       # LiveSetActor: BTreeMap<...> in sema-db
  gc_roots.rs       # GcRootActor: /nix/var/nix/gcroots/criomos/...
  events.rs         # EventLogActor: append-only typed events
  container.rs      # ContainerLifecycleActor: systemd dbus observer
  socket.rs         # accept loop; signal-lojix frame decode/encode
```

Each actor is a Kameo actor per `~/primary/skills/actor-systems.md`.
No zero-state holders (per
`~/primary/skills/actor-systems.md` §"Zero-sized actors are not
actors").

## 4 · Invariants

- The daemon does not initiate deploys on its own. It receives requests
  (`DeploymentSubmission`) and records what happens. Operator intent
  comes from outside.
- Every external operation is a typed `signal-lojix` request.
  Daemon-internal actor messages stay internal.
- Push, never poll. Subscribers register; the daemon pushes
  `DeploymentObservation` and `CacheRetentionObservation` as events
  occur. See `~/primary/skills/push-not-pull.md`.
- The daemon is cluster-operator-owned, not per-host. A single instance
  per operator workstation (or per shared deploy host); not running on
  every cluster node.
- One Nota record in, one Nota record out at the socket boundary
  (matches the lojix-cli operator surface discipline per
  `lojix-cli/skills.md`).

## 5 · Cross-Cutting Context

- Workspace `~/primary/ESSENCE.md` is upstream of every rule.
- `signal-lojix` at `github:LiGoldragon/signal-lojix` is the wire
  vocabulary.
- `horizon-rs` is the projection of cluster proposals; this daemon
  reads horizon per request, never edits it.
- `sema-db` is the storage substrate (redb + rkyv + typed slots).
- Today's `lojix-cli` at `github:LiGoldragon/lojix-cli` is the
  monolithic orchestrator whose implementation surface this daemon
  eventually replaces.
