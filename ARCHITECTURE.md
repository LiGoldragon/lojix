# ARCHITECTURE — lojix-next (schema-deep pilot)

## Purpose

`lojix-next` is the schema-deep rewrite of the new lojix-horizon
logics on `nota-next` + `schema-next` + `schema-rust-next` +
`kameo` 0.20. It is the runnable proof that every typed noun the
daemon touches — wire `Input`/`Output`, SEMA `SemaCommand` /
`SemaResponse`, internal actor mailboxes `ActorRequest` /
`ActorReply`, payload records, daemon configuration — comes from
one authored `schema/lojix.schema`.

Built on the spirit-next precedent at one level of depth higher.

## Pipeline

```text
schema/lojix.schema
  -> build.rs
  -> schema-next::SchemaEngine
  -> schema-next::MacroRegistry  (SchemaStructDefinition + SchemaStructFields +
                                  SchemaEnumDefinition + SchemaEnumVariants)
  -> Asschema
  -> schema-rust-next::RustEmitter
  -> src/generated.rs            (Input, Output as roots; SemaCommand,
                                  SemaResponse, ActorRequest, ActorReply,
                                  DaemonConfiguration, payload records,
                                  newtypes, leaf enums in namespace)
  -> src/runtime/*               (Kameo actors; methods on emitted nouns;
                                  no ZST actors; State carries data that
                                  names what the actor IS)
```

## Runtime triad

Per `~/primary/skills/component-triad.md` §"Runtime triad":

### Signal layer

- **CLI** `src/bin/lojix-next.rs` — reads one NOTA argument,
  parses as schema-emitted `Input`, frames via
  `Input::encode_signal_frame`, sends over Unix socket, decodes
  `Output` reply, prints `Output::to_nota()`.
- **Daemon** `src/bin/lojix-next-daemon.rs` — reads one NOTA
  argument (parsed as schema-emitted `DaemonConfiguration`),
  starts the `RunDaemon` runner.
- **`SocketListener` actor** (`src/runtime/socket.rs`) — owns the
  bound `tokio::net::UnixListener`. Per connection, reads
  length-prefixed signal frame, decodes via
  `Input::decode_signal_frame`, dispatches through `LojixRoot`,
  writes back framed `Output`.

### Executor layer

- **`LojixRoot` actor** (`src/runtime/root.rs`) — runtime root; State
  carries the typed `LojixChildSet` (10 child actor refs).
- **`OperationDispatcher` actor** (`src/runtime/dispatcher.rs`) — the
  executor; drives `Input` through the full deploy pipeline by
  asking downstream actors in sequence. Emits `TraceWitness` events
  for every plane it touches.
- **Method placement** on schema-emitted nouns
  (`src/runtime/codec.rs`):
  - `Input::lower_to_sema_command(self) -> Lowered`
  - `DeploymentRequest::into_plan_record(self) -> PlanRecord`
  - `HelpQuery::into_help_reply(self) -> HelpReply`
  - `SemaResponse::into_output(self) -> Output`
  - `GenerationSelector::target_deployment(&self) -> &DeploymentIdentifier`
- **Method placement** on source-staging nouns
  (`src/runtime/source_stager.rs`):
  - `PlanRecord::source_digest(&self) -> SourceDigest`
  - `SourceRecord::from_plan(&PlanRecord) -> SourceRecord`
  - `SourceRecord::artifact_text(&self) -> String`
- **Method placement on `DeploymentRequest`**
  (`src/runtime/authorization.rs`):
  - `DeploymentRequest::authorize(&self, policy: &AuthorizationPolicy) -> CriomeAuthorization`

### SEMA layer

- **`Store` actor** (`src/runtime/store.rs`) — single-writer for the
  SEMA layer. State carries the typed registries: plans, builds,
  staged sources, copies, generations, observations, and four
  identifier counters. In-memory for the pilot; redb-backed in the
  follow-on.
- `Store::apply(SemaCommand) -> SemaResponse` is the single mutator
  path.

## Deep actor topology

| Plane | Actor noun | Inbound | State |
|---|---|---|---|
| Runtime root | `LojixRoot` | lifecycle, dispatch | `LojixChildSet` |
| Signal accept | `SocketListener` | raw bytes | `Option<UnixListener>` |
| Input executor | `OperationDispatcher` | `Input` (via `Dispatch`) | `ActorReferenceSet` |
| Authorization | `AuthorizationGate` | `AuthorizeMessage` | `AuthorizationPolicy` |
| Source staging | `SourceStager` | `ActorRequest::StageSources` | source artifact directory, most recent `SourceRecord` |
| Build execution | `Builder` | `DriveBuild` | `ProcessToolchain`, `Option<SourceRecord>` |
| Closure copy | `ClosureCopier` | `DriveCopy` | `ProcessToolchain`, `CopyQueue` |
| Activation | `Activator` | `DriveActivation` | `ProcessToolchain`, `ActiveGeneration` |
| GC root pin | `GcRootPinner` | `DrivePin` | `ProcessToolchain`, `PinnedSet` |
| Generation ledger | `Store` | `Apply(SemaCommand)` | typed registries |
| Observation fan | `ObservationFan` | `BroadcastObservation`, `Subscribe` | `SubscriberSet`, most recent |
| Trace log | `TraceLog` | `RecordWitness`, `Snapshot` | `Vec<TraceWitness>` |

**No ZST actors.** Every actor's `State` carries data — see
`tests/actor_topology.rs::lojix_next_no_zst_actors`.

**Trace witnesses required.** Every plane emits a typed
`TraceWitness` event; tests assert the pipeline ran through every
named plane.

## Schema-next limit encountered

The current `schema-next` (HEAD `d340433f`) enforces **exactly four
positional root objects** in `schema/<name>.schema`: imports `{}`,
`(Input ...)`, `(Output ...)`, namespace `{}`. Only `Input` and
`Output` get signal-frame infrastructure (route enum, short header,
encode/decode). Other root-shaped enums (`SemaCommand`,
`SemaResponse`, `ActorRequest`, `ActorReply`) live in the namespace —
they still get rkyv derives, `from_nota_block`, `to_nota`, but no
signal-frame plumbing. This is fine for the pilot because those
types are internal (Store actor, internal actor mailboxes); they
don't cross a process boundary in the runtime triad.

If a future iteration needs SemaCommand to be process-boundary
typed (e.g., a sema-engine daemon split out of the lojix daemon),
schema-next would need to grow N root enums or an alternative
mechanism for namespace-defined enum signal-framing.

## Build-step assertion

`build.rs` asserts that schema-next macro pairs
`SchemaStructDefinition + SchemaStructFields` and
`SchemaEnumDefinition + SchemaEnumVariants` both fired — the build
fails if the registry wasn't exercised. This catches future
schema-next refactors that bypass the registry.

## Sandbox-OS witness

Two tests in the family carry the sandbox-OS witness load:

- `lojix_next_build_only_pipeline_on_sandbox` — spawns the actual
  `lojix-next-daemon` binary, sends a NOTA `Submit` via the
  `lojix-next` CLI, the daemon drives the full pipeline through
  every actor, writes a `GenerationRecord`, and the CLI's
  subsequent `Query` returns the snapshot. The sandbox
  `ProcessToolchain` shells out to `echo` (build/copy/activate),
  which is enough to exercise every actor edge and prove
  `nix flake check` passes inside the Nix sandbox.
- `lojix_next_activation_on_nspawn_sandbox` — in-process activation
  witness; the activation command is `nspawn-sandbox-activate` and
  the observation stream is asserted end-to-end (`Activating Complete`
  + `Observed Complete`).
- `lojix_next_submit_stages_sources_before_build` — in-process
  source-staging witness; the submit path must ask `SourceStager`,
  commit `SemaCommand::RecordSource`, and write an inspectable source
  artifact under the daemon state directory before `Builder` runs.

A real `systemd-nspawn`-against-`nspawn-dune-on-prometheus` test
requires root + cgroup access which the Nix flake check sandbox
doesn't grant. Wiring the real nspawn pipeline is the operator's
job when amalgamating per
`~/primary/skills/double-implementation-strategy.md`.

## Known limits

- Storage is in-memory (a `Vec` per record family in `Store`); the
  redb backend lands next.
- Source staging writes a local source artifact under the daemon state
  directory. ARCA-backed content-addressed propagation is still a
  future designed component, not implemented in this pilot.
- Schema-next does not yet express vectors. `SemaResponse`
  surfaces one record at a time
  (`GenerationLedgerEntry(GenerationRecord)` not
  `GenerationLedger(Vec<GenerationRecord>)`) — same workaround as
  spirit-next's `Observed(Entry)`.
- Schema diff/upgrade machinery absent (same as spirit-next).
- No `owner-signal-lojix` companion contract yet; the pilot ships
  the ordinary signal surface only.
- The CriomOS test-cluster's actual `nspawn-dune-on-prometheus`
  flake is not pulled in (would require Nix-sandbox-incompatible
  privileges).

## See also

- `~/primary/reports/system-designer/35-schema-deep-new-logics/1-vision-schema-deep-new-logics.md`
- `~/primary/reports/system-designer/35-schema-deep-new-logics/2-...md`
  (this pilot's implementation report)
- `/git/github.com/LiGoldragon/spirit-next/ARCHITECTURE.md`
- `/git/github.com/LiGoldragon/schema-next/ARCHITECTURE.md`
- `~/primary/skills/component-triad.md` §"Runtime triad"
- `~/primary/skills/actor-systems.md`
- `~/primary/skills/abstractions.md` §"Schema-emitted nouns"
- `~/primary/skills/rust-discipline.md`
