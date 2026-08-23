# lojix — architecture

`lojix` is the new deploy stack: one crate that ships a long-lived
deploy orchestrator daemon (`lojix-daemon`) plus a thin CLI client
(`lojix`) that speaks the daemon over a Unix socket.

> **Status (2026-08-04):** implemented Rust crate at the repo root.
> The daemon uses the actor-native `triad-runtime` multi-listener for
> two authority-tiered sockets and awaits the handwritten async Nexus
> runner directly; child-process effects use `tokio::process`. Production
> host and user-environment build requests select their materialization mode
> explicitly. `Horizon` enters a materialization effect that projects the proposal with
> `horizon-rs`, writes generated `horizon` / `system` / `deployment`
> flake inputs under daemon state, and passes content-addressed
> `--override-input` values into `nix eval`. Activating deploys use an
> explicitly requested activation backend and request-owned transport.

> **Request-owned deployment routing (v4).** Every request carries the exact
> `nix_store_uri`, exact `ssh_destination`, output selector, input mode,
> activation backend, and optional Nix builder specification. Lojix validates
> and snapshots those private values before admission; it never forms a domain,
> login, output attribute, builder-file location, or target route from cluster
> and node names. Nix copy uses `nix_store_uri` verbatim; remote activation uses
> `ssh_destination` verbatim, including an explicitly requested `root` login.
> The daemon evaluates locally and a supplied builder specification is passed to
> Nix verbatim through `--builders`; it does not use `/etc/nix/machines`.
> The `BootOnce` transient-unit name is the deterministic
> `lojix-boot-once-deploy-<deployment-identifier>` — the same string the
> durable resume cursor persists — so a daemon crash inside the BootOnce
> window reconciles by polling that exact unit.

> **Scope (today vs eventually).** This stack sits on today's
> substrate — Rust on Linux, `signal-core` over a Unix socket,
> `sema-engine` for durable state, direct nix invocations. It is a
> realization step toward the Sema-on-Sema future per
> the workspace architecture's "Workspace vision and intent" section.

## 0.6 · Direction

`lojix` is the production deploy stack: a daemon-based orchestrator with direct typed ordinary and owner/meta contracts. The active goalpost is production cutover so the cluster runs on `lojix-daemon` and all consumers use the direct contracts without compatibility translation layers or aliases.

The production cutover bar is specific: complete-host and user-environment deploys (not eval/build only), deploys that survive SSH disconnect (job actor decoupled from the request stream so a dropped client does not abort the deploy), every operation described in schema types with no untyped escape hatch, durable-first state built and self-resuming before the first cutover, and end-to-end validation against a full routed microVM with its own Criome domain and reachable IP (Spirit `se72`).

This stack sits on today's substrate as a realization step toward the Sema-on-Sema future — "Today, not eventually." See §7 for the detailed direction bullets governing testing/deployment discipline, typed Nix interface, ergonomic test authoring, credential custody, and GitHub-auth.

## 0 · Crate shape

One crate, daemon/client binaries, and one maintained flake bootstrap app (per the workspace agent instructions' "Binary naming
— `-daemon` suffix" rule):

```
Cargo.toml:
  [lib] name = "lojix"
  [[bin]] name = "lojix-daemon"   # long-lived orchestrator
  [[bin]] name = "lojix"          # thin CLI client
  [[bin]] name = "lojix-bootstrap" # daemon-free, explicit bootstrap
```

The library half (`lojix`) holds the shared types, the daemon's
actor implementations, and the CLI's request/reply plumbing. The
daemon/client binaries are thin entry points: `lojix-daemon` brings up the
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
  `owner-signal-lojix` (Spirit `vudl`). `meta-signal-lojix` is the maintained
  standalone dependency; Lojix does not carry a local compatibility contract.
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
- **Deploy event log** — typed historical events
  (`BuildRealized`, `CachePublished`, `ActivationSucceeded`,
  `GenerationRetired`, `ContainerStarted`, `ContainerStopped`). An explicit
  `EventLogRetention` policy bounds retained event and matching container
  rows without touching live generations, GC roots, or deploy-resume jobs. The
  versioned store also persists a finite 4,096-entry raw-history policy and
  invokes checkpoint-backed maintenance after each event append, so ordinary
  operation is neither disabled nor unbounded. On each retention pass, a
  verified local checkpoint compacts the corresponding versioned log; lojix
  has no configured mirror, so it creates no mirror-outbox replay rows. The
  bounded query window is not merely an in-memory or materialized-row
  projection.
  Subscribers consume via `signal-lojix` `DeploymentObservation` and
  `CacheRetentionObservation`, bridged through `sema-engine`'s
  `SubscriptionSink` trait.
- **Container lifecycle observation** — systemd dbus subscriptions
  for `containers.<name>.service` transitions; mirrors into the
  event log.
- **Thin CLIs** — `lojix` and `meta-lojix` each read exactly one inline
  DOTOS/NOTA object per the one-record operator-surface discipline; they reject
  raw paths, DOTOS files, signal files, and flags. Each forwards its object as a
  `signal-lojix` frame to the daemon, and prints the reply or
  streams events.
- **Maintained bootstrap app** — `lojix-bootstrap` is a flake package/app,
  not a daemon client. It accepts exactly one inline private `BootstrapRun`
  DOTOS object. The request explicitly supplies input mode, builder, optional
  hermetic test, local-or-remote BootOnce backend, journal parent, GC-root
  path, and terminal-evidence path. It creates a private v5 journal and
  configuration beneath that journal parent. The journal binds a request hash,
  immutable flake reference, closure, root receipt, and ordered intent/receipt/
  outcome records; an interrupted invocation reconciles the exact next stage.
  Remote BootOnce launches a deterministic target transient systemd unit with
  no-block dispatch and polls that exact unit, so losing the initiating SSH
  connection cannot interrupt profile or EFI work. Evidence is private and
  atomic; finalized journals are retained until cleanup can be proven through
  inode-bound directory handles. Remote policy supplies private identity and
  known-host files, strict host checking, and an OpenSSH closure path shared
  with Nix copy, with no ambient SSH config or agent. `BuildOnly` has no activation or transport field and
  therefore cannot activate. No daemon socket, daemon configuration, old
  journal/store, route, host, user, or path default participates.
  This is the explicitly authorized daemon-free crossing for incompatible
  daemon/client protocol or persisted schema generations: it does not assume
  compatibility or invent a migration path. A run is complete only when its
  durable terminal-evidence record is persisted alongside its journal; request
  admission, build completion, or a disconnected client is not terminal truth.

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
- **Cluster proposal source** — a request-supplied, validated `.dotos` path
  (read only for an explicitly selected Horizon materialization).
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
  bootstrap.rs          # daemon-free explicit resumable v5 bootstrap pipeline
  runtime_model.rs      # hand-written durable and operational nouns
  runtime_flow.rs       # hand-written Nexus effects, work, and runner logic
  schema_runtime.rs     # async hand-written Nexus decision engine
  bin/
    lojix-daemon.rs     # daemon entry
    lojix.rs            # CLI entry
    lojix-bootstrap.rs  # maintained flake bootstrap entry
```

Each daemon actor is a Kameo actor per
the workspace actor-systems doctrine. No zero-state holders.

## 4 · Storage and wire

- **Storage:** handwritten runtime nouns in SEMA tables over a durable `sema-engine`
  store — one exact configured absolute file shared by daemon startup and the
  reset service. The ten
  record families (live set, gc-roots, event log, container lifecycle,
  deploy job, test run, deployment record, identifier allocation, deployment
  outbox, pending transition intent, and legacy-event quarantine) are keyed
  rows, one per element; `Engine::open`
  resumes the persisted catalog, commit sequence, and records on restart, so
  daemon state survives a process stop with no replay code. The identifier
  counters (generation, deployment, event-log position) derive from the
  persisted rows, so they no longer reset to zero on restart. Storage schema 4
  adds the exact private deployment-routing snapshot and refuses every earlier
  schema. Each deploy admission, phase, and
  terminal update atomically writes its durable record/job mutation plus an
  intent; its marker is bound from that exact versioned commit, then
  dispatch/journal/local acknowledgement proceeds in order. The runtime never
  advances to a later effect before acknowledgement. Retention can compact an
  acknowledged event and outbox together, while the acknowledged intent keeps
  restart from reconstructing or re-delivering that historical transition.
  There is no migration or legacy resume path. With the daemon stopped, the
  manually started `lojix-reset-store` accepts only inline `(ResetStore)`. Its
  service-owned `LOJIX_CONFIGURATION` archive supplies the exact store path;
  an existing archive and primary must be absolute, regular, and non-symlinked.
  A recognised v2/v3 Lojix catalog is removed and recreated as v4, while a v4
  catalog returns `AlreadyCurrent` without any data deletion. Protocol
  sidecars are derived only from a proven pre-v4 primary; the reset never
  selects a Spirit store.
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
  configuration: ordinary and owner/meta. Inline DOTOS and `.dotos` files
  are rejected at daemon startup; launch tooling must encode
  configuration before exec. The owner/meta socket refuses any mode with
  "other" access and admits only same-uid/gid owner peers.
- `lojix-write-configuration` is the launch-only DOTOS boundary: it accepts
  exactly one inline `ConfigurationWriteRequest` object (never a file, raw
  path, or flag) and writes the rkyv signal file from the ordered socket/mode, state-directory,
  daemon-host, test-default, and output-path request. Test defaults
  include their exact Nix system and output selector. Production
  writes `NoTestDefaults`; the daemon receives only the resulting signal file.
- A deploy proposal source, when `DeploymentInputMode::Horizon` requires one,
  is an existing, direct, regular absolute `.dotos`
  file with no traversal, symlink, control, or credential-shaped path and a
  valid cluster-proposal parse. A closure is usable by an effect or a fresh
  durable v4 row only as a canonical immutable Nix store-item root. Public
  adapters redact every other path and never project raw proposal sources,
  flake references, or daemon error text.
- The startup configuration carries the test-op defaults as an OPTIONAL
  fixture: `DaemonConfiguration.test_defaults` is `Option<TestDefaults>` and the
  writer's `WriterTestDefaultsChoice` is `NoTestDefaults` (production)
  or `(TestDefaults …)` (test/dev). A production node bakes `NoTestDefaults` →
  `None`, so a bare `(Check …)`/`(Run …)` is rejected with `NoTestDefaults`
  rather than resolving against a per-node baked test cluster. Test fixtures are
  supplied only by test code (the workspace deployment-independence discipline).
- Each public CLI sends one inline DOTOS/NOTA object for its own contract per
  invocation, never reads a caller-selected request file, and prints one
  DOTOS-encoded reply (or streams events until the subscription closes).
- `lojix-inspect-store` is read-only and accepts exactly one inline
  `(InspectStore <path>)` object. It rejects raw paths, request files, flags,
  and extra arguments.
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
- The daemon's shared store uses `sema-engine` multi-table atomic commits for
  activation (live generation, GC root, and allocation) and for every deploy
  transition (correlation state/job plus intent, then acknowledgement and any
  terminal job retraction). A crash therefore cannot expose one half of those
  coupled facts; reopen finishes only pending intent delivery.
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
  they occur. See the workspace push-not-pull doctrine.
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
  authenticated execution environment for the nix invocation fetches the GitHub
  API key from the secret store and injects it into the nix call
  (via `NIX_CONFIG` access-tokens). A small Rust library encapsulates
  the secret-fetch and auth-injection so the rest of `lojix-daemon`
  never handles the token directly. The credential value and its
  store path stay out of source, logs, and the nix store (Spirit
  `2qhw`).

## 8 · Cross-cutting context

- The workspace architecture's "Workspace vision and intent" section is upstream of every rule.
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
- Deploy clients use direct typed `signal-lojix` and
  `meta-signal-lojix` records. Schema changes update consumers rather
  than adding compatibility translation.
