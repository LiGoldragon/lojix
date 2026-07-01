# lojix — architecture

`lojix` is the new deploy stack: one crate that ships a long-lived
deploy orchestrator daemon (`lojix-daemon`) plus a thin CLI client
(`lojix`) that speaks the daemon over a Unix socket.

> **Status (2026-06-10):** implemented Rust crate at the repo root.
> The daemon uses the actor-native `triad-runtime` multi-listener for
> two authority-tiered sockets and awaits the generated async Nexus
> runner directly; child-process effects use `tokio::process`. Production
> System/Home build requests without `build_attribute` now enter a
> Horizon materialization effect that projects the cluster proposal with
> `horizon-rs`, writes generated `horizon` / `system` / `deployment`
> flake inputs under daemon state, and passes content-addressed
> `--override-input` values into `nix eval`. Activating deploys still
> reject until copy/activate is target-safe.

> **Build-on-target (2026-06-19, report 150).** A deploy whose target
> node is NOT the daemon host realizes the node's closure in the TARGET
> node's own store over `ssh-ng://root@<node>.<cluster>.criome`
> (`nix build --eval-store auto --store <uri> <attr>^*`): evaluation
> stays local and cheap while realization — and any model-bearing NAR —
> happens on the target, so a node's closure never transits the daemon
> host (Spirit `ufjd` / `0a9p` / `lc28`). When the target node IS the
> daemon host (e.g. deploying ouranos from the ouranos-hosted daemon)
> the build stays `Local`; an explicit operator `builder` still wins.
> The daemon's own host is named by `DaemonConfiguration::daemon_host`.
> A target-store build leaves the closure already on the target, so the
> copy step is a no-op and only the `BootOnce` activation runs there.
> The `BootOnce` transient-unit name is the deterministic
> `lojix-boot-once-deploy-<deployment-identifier>` — the same string the
> durable resume cursor persists — so a daemon crash inside the BootOnce
> window reconciles by polling that exact unit.

> **Scope (today vs eventually).** This stack sits on today's
> substrate — Rust on Linux, `signal-core` over a Unix socket,
> `sema-engine` for durable state, direct nix invocations. It is a
> realization step toward the Sema-on-Sema future per
> `~/primary/ARCHITECTURE.md` §"Workspace vision and intent".

## 0.5 · Direction

`lojix` is the new production deploy stack — the daemon-based successor to the monolithic `lojix-cli`. The active goalpost is feature parity with `lojix-cli` and then production cutover so the cluster runs on `lojix-daemon`; the legacy CLI stays at its current schema and retires once CriomOS migrates to consume this daemon's projection.

The production cutover bar is specific: full-OS deploy (System and Home, not eval/build only), deploys that survive SSH disconnect (job actor decoupled from the request stream so a dropped client does not abort the deploy), every operation described in schema types with no untyped escape hatch, durable-first state built and self-resuming before the first cutover, and end-to-end validation against a full routed microVM with its own Criome domain and reachable IP (Spirit `se72`).

This stack sits on today's substrate as a realization step toward the Sema-on-Sema future — "Today, not eventually." See §7 for the detailed direction bullets governing testing/deployment discipline, typed Nix interface, ergonomic test authoring, credential custody, and GitHub-auth.

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

- **Ordinary Unix socket** — peer-callable `signal-lojix` reads,
  watches, and unwatch calls.
- **Owner/meta Unix socket** — `meta-signal-lojix` deploy and
  retention mutations. Owner socket modes granting any "other" access
  are refused at startup, and each owner connection must present
  kernel-vouched peer credentials matching the daemon process uid/gid.
  The two-contract authority split places `Deploy`/`Pin`/`Unpin`/`Retire`
  as owner-only policy in `meta-signal-lojix` — a deploy mutates the live
  cluster and can break the router, the strongest owner-socket case —
  while `Query`, the `WatchDeployments`/`WatchCacheRetention`
  subscriptions, and `Unwatch` are peer-callable on the ordinary
  `signal-lojix`. The policy contract is born `meta-signal-lojix`, never
  `owner-signal-lojix` (Spirit `vudl`). Until cutover the meta contract is
  carried as a local path-dependency package inside the `lojix` tree
  (mirroring the cloud stopgap); the standalone repo is created at cutover.
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

## 3 · Code map

```
src/
  lib.rs                # shared state, configuration, error type
  daemon.rs             # actor-native two-socket daemon shell
  client.rs             # thin CLI socket exchange
  schema_runtime.rs     # async hand-written engine over generated schema nouns
  schema/               # checked-in generated Nexus/SEMA artifacts
  bin/
    lojix-daemon.rs     # daemon entry
    lojix.rs            # CLI entry
```

Each daemon actor is a Kameo actor per
`~/primary/skills/actor-systems.md`. No zero-state holders.

## 4 · Storage and wire

- **Storage:** schema-derived SEMA tables over a durable `sema-engine`
  store — a `*.sema` file under `<state-directory>/lojix.sema`. The four
  record families (live set, gc-roots, event log, container lifecycle)
  are keyed rows, one per element; `Engine::open` resumes the persisted
  catalog, commit sequence, and records on restart, so daemon state
  survives a process stop with no replay code. The identifier counters
  (generation, deployment, event-log position) derive from the persisted
  rows, so they no longer reset to zero on restart. Atomic
  version-controlled backup is the named follow-on.
- **Wire:** `signal-frame` records carrying `signal-lojix` on the
  ordinary socket and `meta-signal-lojix` on the owner/meta socket.
  Length-prefixed rkyv archives over Unix sockets.
- **Generated Nix inputs:** production deploys materialize projected
  Horizon data into tiny flake inputs under
  `<state-directory>/generated-inputs/<cluster>/<node>/<shape>/`.
  `nix hash path --type sha256 --sri` supplies the narHash for each
  override.
- **Substituter resolution (provisional):** for the cutover, resolving
  a node name to its Yggdrasil cache URL and public key moves into the
  daemon — the daemon gains horizon-read for substituters and the wire
  reverts to carrying bare node names instead of pre-resolved
  url/public-key pairs. This is a provisional for-now choice; the code
  must be marked must-be-replaced-by-better-design (Spirit `lc28`).

## 5 · Constraints

- The daemon binds two Unix sockets from its binary rkyv startup
  configuration: ordinary and owner/meta. Inline NOTA and `.nota` files
  are rejected at daemon startup; launch tooling must encode
  configuration before exec. The owner/meta socket refuses any mode with
  "other" access and admits only same-uid/gid owner peers.
- The CLI sends one NOTA-encoded `signal-lojix` request per
  invocation and prints one NOTA-encoded reply (or streams events
  until the subscription closes).
- Every external operation is a typed `signal-lojix` variant;
  there is no untyped escape hatch on the wire.
- The daemon never initiates deploys on its own — every deploy
  starts from a received `DeploymentSubmission`.
- The daemon serves connections concurrently, bounded by a permit
  cap, and never blocks on in-progress nix builds. Deploy state is
  per-request: each connection owns its own pipeline cursor. The
  durable `sema-engine` Store is the only shared point and is locked
  only briefly per sema op; long nix effects hold no global lock
  (Spirit `2alg`).
- The daemon's shared store is durable `sema-engine` backing; cross-table
  atomicity (an activation writes the live-set row then the gc-root row as
  two sequential keyed asserts) awaits a multi-table commit enhancement in
  `sema-engine`. The keyed asserts are fail-safe — a duplicate key errors
  rather than clobbering — but a crash mid-activation leaves a torn write
  with no reopen reconciliation, tracked as the follow-on.
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

## 7 · Direction

- **Testing and deployment are one function.** Both build an OS or
  cluster closure and bring it up on a target; they differ only in
  containment. The triad exposes both faces: ordinary non-meta signal
  targets contained throwaway resources (hermetic VMs, sandboxes,
  on-demand `VmHost` guests, ephemeral cloud droplets it provisions
  and reaps) so a broken run kills only the contained target, while
  meta signal is privileged production deploy to real nodes. The
  ordinary-vs-meta split is the safety boundary, enforced by typed
  contained-vs-production targets, not a runtime flag (Spirit `mq5s`).
- **Safe typed interface is the default for nix work.** Practical
  build, test, and deploy invocations describe the intended operation,
  required capabilities, containment level, and builder policy in
  Lojix language rather than hand-writing raw nix commands. Lojix
  verifies an eligible remote worker exists, schedules jobs across
  builder capabilities, and emits the corresponding nix invocation
  only once the safety and placement constraints are satisfied
  (Spirit `75pw`).
- **Ergonomic test authoring is first-class.** The public
  test-authoring interface carries schema shorthands and
  well-developed option setting and querying so authoring a cluster
  test — for example a criome, spirit, and router cluster test on the
  ordinary contained-testing interface — is ergonomic rather than
  verbose. Ease of use is a requirement of the interface, not an
  afterthought (Spirit `vfgk`).
- **Production credentials custodied through criome.** As part of
  production cutover, the deploy daemon's operational credentials and
  unattended machine identity are custodied and authenticated through
  criome rather than borrowing the operator's logged-in session
  (GPG/SSH agent). This builds on agents holding cryptographic
  identity via criome (Spirit `h03z`).
- **`lojix-daemon` owns GitHub-authenticated flake input resolution.**
  The GitHub API rate-limit stale-activation failure is a deploy-path
  problem owned by `lojix-daemon`, not a package problem: an
  authenticated wrapper around the nix invocation fetches the GitHub
  API key from the secret store and injects it into the nix call
  (via `NIX_CONFIG` access-tokens). A small Rust library encapsulates
  the secret-fetch and auth-injection so the rest of `lojix-daemon`
  never handles the token directly. The credential value and its
  store path stay out of source, logs, and the nix store (Spirit
  `2qhw`).

## 8 · Cross-cutting context

- Workspace `~/primary/ARCHITECTURE.md` §"Workspace vision and intent" is upstream of every rule.
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
