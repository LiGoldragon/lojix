# ARCHITECTURE — lojix-next (schema-deep pilot)

## Purpose

`lojix-next` is the schema-deep rewrite of the new lojix-horizon
logics on `nota-next` + `schema-next` + `schema-rust-next` +
`kameo` 0.20 + `sema-engine`. It is the runnable proof that every
typed noun the daemon touches — wire `Input`/`Output`, SEMA
`SemaCommand` / `SemaResponse`, internal actor mailboxes
`ActorRequest` / `ActorReply`, payload records, `DatabaseMarker`
reply stamps, mail-lifecycle states, daemon configuration — comes
from one authored `schema/lojix.schema`.

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
  -> $OUT_DIR/lojix_next_generated.rs
                                 (Input, Output as roots; SemaCommand,
                                  SemaResponse, ActorRequest, ActorReply,
                                  DaemonConfiguration, DatabaseMarker,
                                  MailLifecycle, AcceptedReply, RejectedReply,
                                  ObservedReply, SnapshotReply, HelpAnswerReply,
                                  payload records, newtypes, leaf enums in
                                  namespace; PLUS the schema-rust-next-emitted
                                  Nexus mail lifecycle surfaces:
                                  MessageIdentifier, MessageSent, NexusMail,
                                  MessageProcessed, MessageSentHook,
                                  MessageProcessedHook, InputNexus, OutputNexus)
  -> src/runtime/*               (Kameo actors; methods on emitted nouns;
                                  no ZST actors; State carries data that
                                  names what the actor IS)
```

## Runtime triad — Signal / Nexus / SEMA

Per psyche record 964 + `~/primary/skills/component-triad.md`
§"Runtime triad", the daemon has three execution centers. Iteration
2 (per psyche records 963-970) names them explicitly and shapes the
Nexus as the mail keeper between Signal ingress and SEMA state
handling.

### Signal layer

- **CLI** `src/bin/lojix-next.rs` — reads one NOTA argument,
  parses as schema-emitted `Input`, frames via
  `Input::encode_signal_frame`, sends over Unix socket, decodes
  `Output` reply, prints `Output::to_nota()`. Iteration 2 adds the
  alternative path: a Rust client may use `UnixSocketCommunicate`
  (an impl of the new `Communicate` trait) for typed round trips.
- **Daemon** `src/bin/lojix-next-daemon.rs` — reads one NOTA
  argument (parsed as schema-emitted `DaemonConfiguration`),
  starts the `RunDaemon` runner.
- **`SocketListener` actor** (`src/runtime/socket.rs`) — owns the
  bound `tokio::net::UnixListener`. Per connection, reads
  length-prefixed signal frame, decodes via
  `Input::decode_signal_frame`, dispatches through `LojixRoot`,
  writes back framed `Output`.
- **`Communicate` trait** (`src/runtime/communicate.rs`) — abstract
  wire interface. `UnixSocketCommunicate` is the concrete impl for
  this pilot; the schema-emitted `Input` / `Output` types are the
  associated types. Iteration-2 decision: the trait lives in
  lojix-next while signal-frame's schema-derived rewrite is still
  in flight. Promoting the trait to `signal-frame` (or a dedicated
  abstract crate) is iteration-3 work.

### Nexus mail keeper layer

- **`LojixRoot` actor** (`src/runtime/root.rs`) — runtime root; State
  carries the typed `LojixChildSet` (9 child actor refs including
  the Nexus).
- **`NexusMailKeeper` actor** (`src/runtime/nexus.rs`) — the mail
  keeper. State carries:
  - `NexusActorRefs` — refs to authorization / builder / copier /
    activator / gc_root / store / fan / trace.
  - `NexusHooks` — push-style lifecycle hooks (schema-emitted
    `MessageSentHook` + `MessageProcessedHook<Output>`).
  - In-flight `Vec<MailEntry>` + completed `Vec<MailEntry>`.
  - `next_identifier: u64` (allocates `MessageIdentifier`s).
  - Each `Input` becomes a `MailEntry` with typed
    `MailLifecycle::{Sent, Queued, Processing, Replied, Failed}`
    (schema-emitted enum). The lifecycle path is preserved in the
    entry's `lifecycle_path` Vec so tests can assert the order.
  - When SEMA reply arrives, NexusMailKeeper stamps the response
    with a `DatabaseMarker` (current commit-counter + state-hash)
    and translates to the matching Output reply variant
    (`AcceptedReply` / `SnapshotReply` / `ObservedReply` /
    `RejectedReply` / `HelpAnswerReply`).
- **Method placement** on schema-emitted nouns
  (`src/runtime/codec.rs`):
  - `Input::lower_to_sema_command(self) -> Lowered`
  - `DeploymentRequest::into_plan_record(self) -> PlanRecord`
  - `HelpQuery::into_help_reply(self) -> HelpReply`
  - `SemaResponse::into_output(self, marker: DatabaseMarker) -> Output`
  - `ForwardOnlyReply::stamped(self, marker: DatabaseMarker) -> Output`
  - `Output::database_marker(&self) -> &DatabaseMarker`
  - `GenerationSelector::target_deployment(&self) -> &DeploymentIdentifier`
- **Method placement on `DeploymentRequest`**
  (`src/runtime/authorization.rs`):
  - `DeploymentRequest::authorize(&self, policy: &AuthorizationPolicy) -> CriomeAuthorization`

### SEMA layer

- **`Store` actor** (`src/runtime/store.rs`) — single-writer for the
  SEMA layer, **backed by `sema-engine`**. State carries:
  - `sema_engine::Engine` handle (redb-backed durable storage).
  - One `TableReference<RecordValue>` per record family
    (`plans_table`, `builds_table`, `copies_table`,
    `activations_table`, `observations_table`,
    `generations_table`, `counters_table`).
  - The `database_path: PathBuf` for diagnostics.
- `Store::apply(SemaCommand) -> SemaResponse` is the only mutator
  path. The schema-emitted records implement `EngineRecord` directly
  (no parallel mirrors): `record_key` lives on each schema-emitted
  noun. The counters (deployment / generation / command /
  observation) are persisted as `CounterRow` records in their own
  sema-engine table — they survive daemon restart.
- `Store::current_database_marker() -> DatabaseMarker` builds the
  marker from `Engine::current_commit_sequence()` +
  `Engine::latest_snapshot()`, hashed with Blake3.

## Deep actor topology

| Plane | Actor noun | Inbound | State |
|---|---|---|---|
| Runtime root | `LojixRoot` | lifecycle, dispatch | `LojixChildSet` |
| Signal accept | `SocketListener` | raw bytes | `Option<UnixListener>` |
| Mail keeper / executor | `NexusMailKeeper` | `Input` (via `DispatchMail`) | `NexusActorRefs`, `NexusHooks`, in-flight + completed `MailEntry` |
| Authorization | `AuthorizationGate` | `AuthorizeMessage` | `AuthorizationPolicy` |
| Build execution | `Builder` | `DriveBuild` | `ProcessToolchain`, `Option<PlanRecord>` |
| Closure copy | `ClosureCopier` | `DriveCopy` | `ProcessToolchain`, `CopyQueue` |
| Activation | `Activator` | `DriveActivation` | `ProcessToolchain`, `ActiveGeneration` |
| GC root pin | `GcRootPinner` | `DrivePin` | `ProcessToolchain`, `PinnedSet` |
| Sema engine | `Store` | `Apply(SemaCommand)`, `CurrentDatabaseMarker` | `sema_engine::Engine`, typed `TableReference`s |
| Observation fan | `ObservationFan` | `BroadcastObservation`, `Subscribe` | `SubscriberSet`, most recent |
| Trace log | `TraceLog` | `RecordWitness`, `Snapshot` | `Vec<TraceWitness>` |

**No ZST actors.** Every actor's `State` carries data — see
`tests/actor_topology.rs::lojix_next_no_zst_actors`.

**Trace witnesses required.** Every plane emits a typed
`TraceWitness` event. Iteration 2 adds the `MailSent` /
`MailQueued` / `MailProcessing` / `MailReplied` variants tagged
with `MessageIdentifier` so tests can match a specific in-flight
mail.

## DatabaseMarker — every reply is stamped

Per psyche record 935 (referenced via `/390`), every Output reply
variant carries a `DatabaseMarker` record:

```
DatabaseMarker [TransactionCounter StateHash]
TransactionCounter [Integer]
StateHash [Text]
```

`TransactionCounter` is sema-engine's monotonic `CommitSequence`
value. `StateHash` is a Blake3 digest of `(commit_sequence,
latest_snapshot)` — distinguishes reads (no advance) from writes
(advance + change). Nexus asks the Store
(`CurrentDatabaseMarker`) immediately before stamping each reply.

## Schema-next limit

The schema-next macro registry still enforces exactly four
positional root objects in `schema/<name>.schema`. Iteration 2
adds new namespace records (DatabaseMarker, AcceptedReply,
RejectedReply, ObservedReply, SnapshotReply, HelpAnswerReply,
MailLifecycle, MessageCounter, TransactionCounter, StateHash,
SemaDatabasePath) inside the existing namespace block; no change
to the root structure was required.

## Build-step assertion

`build.rs` asserts that schema-next macro pairs
`SchemaStructDefinition + SchemaStructFields` and
`SchemaEnumDefinition + SchemaEnumVariants` both fired — the build
fails if the registry wasn't exercised. This catches future
schema-next refactors that bypass the registry.

## Iteration-2 witness tests

The deliverable's six new witnesses, beyond the ten of iteration 1:

| # | Test | Witnesses |
|---|---|---|
| 11 | `lojix_next_nexus_is_mail_keeper` | MailEntry exists during SEMA round-trip; lifecycle path Sent -> Queued -> Processing -> Replied |
| 12 | `lojix_next_message_lifecycle_hooks_fire` | Attached `MessageSentHook` fires synchronously with the right correlation id |
| 13 | `lojix_next_sema_engine_durable_across_restart` | Stop and reopen the engine; plans + generations persist |
| 14 | `lojix_next_communicate_trait_round_trip` | `UnixSocketCommunicate` does a full Input → Output round trip |
| 15 | `lojix_next_database_marker_in_every_reply` | Every Output variant carries a DatabaseMarker; counter is monotonic |
| 16 | `lojix_next_database_marker_state_hash_changes_on_write` | State hash stable across reads; changes on writes |

## Sandbox-OS witness

Two tests carry the sandbox-OS witness load (unchanged from
iteration 1):

- `lojix_next_build_only_pipeline_on_sandbox` — spawns the
  `lojix-next-daemon` binary, sends a NOTA `Submit` via the
  `lojix-next` CLI; the daemon drives the full pipeline (now
  through `NexusMailKeeper` + sema-engine) and the CLI's subsequent
  `Query` returns the snapshot stamped with a DatabaseMarker.
- `lojix_next_activation_on_nspawn_sandbox` — in-process activation
  witness; the activation command is `nspawn-sandbox-activate` and
  the observation stream is asserted end-to-end.

A real `systemd-nspawn`-against-`nspawn-dune-on-prometheus` test
requires root + cgroup access which the Nix flake check sandbox
doesn't grant. Wiring the real nspawn pipeline is the operator's
job when amalgamating per
`~/primary/skills/double-implementation-strategy.md`.

## Known limits (iteration 2)

- Schema-next does not yet express vectors. `SemaResponse`
  surfaces one record at a time (same as iteration 1).
- Schema diff/upgrade machinery absent (same as spirit-next +
  iteration 1).
- No `owner-signal-lojix` companion contract yet; pilot ships the
  ordinary signal surface only.
- `Communicate` trait lives inside lojix-next; promotion to
  signal-frame is iteration-3 work.
- `NexusMailKeeper` lives inside lojix-next; if other components
  need the same primitive (likely), iteration 3 promotes to a
  shared `persona-mail` crate per `/390 §"Mail state manager"`.
- The CriomOS test-cluster's actual `nspawn-dune-on-prometheus`
  flake is not pulled in (would require Nix-sandbox-incompatible
  privileges).

## See also

- `~/primary/reports/system-designer/35-schema-deep-new-logics/` —
  iteration 1 (the baseline /35).
- `~/primary/reports/system-designer/37-prototype-schema-deep-iteration-2-nexus-mail-sema-engine-2026-05-27/` —
  this iteration's frame, target mapping, implementation report.
- `~/primary/reports/designer/389-schema-macros-canonical-direction.md`
  — schema language layer.
- `~/primary/reports/designer/390-wire-runtime-canonical-direction.md`
  — Communicate trait + mail manager + DatabaseMarker design.
- `~/primary/reports/designer/392-vision-schema-driven-stack-canonical.md`
  — the workspace vision; 8-component fullness criterion.
- `/git/github.com/LiGoldragon/spirit-next/ARCHITECTURE.md`
- `/git/github.com/LiGoldragon/schema-next/ARCHITECTURE.md`
- `/git/github.com/LiGoldragon/sema-engine/ARCHITECTURE.md`
- `~/primary/skills/component-triad.md` §"Runtime triad"
- `~/primary/skills/actor-systems.md`
- `~/primary/skills/abstractions.md` §"Schema-emitted nouns"
- `~/primary/skills/rust-discipline.md`
