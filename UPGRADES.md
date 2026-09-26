# Upgrades

# 8.0.0 to 8.1.0

A deploy behaviour change with no wire change and no store change: the
signal-lojix and meta-signal-lojix contracts, the store schema (v5) and the
`DeployResumeStage` set are unchanged, and no migration runs.

## A node other than the daemon host realizes in its own store

Before 8.1.0 every deployment evaluated and built on the daemon host and then
copied the closure to the target, so every Realize or activating deploy of a
remote node staged the whole closure (model weights included) in the daemon
host's store, and nothing rooted it there.

Now, when the deployed node is not the daemon host, the Build step does three
things, in order, all idempotent:

1. `nix copy --derivation --to <nix_store_uri> <drv>` — the locally evaluated
   derivation closure goes to the target.
2. `nix build --no-link --print-out-paths --store <nix_store_uri> <drv>^*` —
   the target's daemon builds or substitutes the outputs with its own
   settings and caches.
3. `ssh <ssh_destination> nix-store --add-root <root> --realise <out>` — a GC
   root on the target at
   `/nix/var/nix/gcroots/lojix/<daemon-host>/generation-<id>` for a `root`
   login, or `$HOME/.local/state/lojix/gcroots/<daemon-host>/generation-<id>`
   (an indirect root) for any other login.

Evaluation stays on the daemon host: it never runs against an ssh-ng store
(the 0.3.9 deadlock). The closure path Lojix records (job cursor, live
generation, GC root row) is the output as it exists in the target's store.

The copy stage stays in the pipeline but becomes
`nix path-info --store <nix_store_uri> <out>`: a presence check, not a
transfer. A missing output fails the copy stage with that command as evidence.

A resume from `NixBuild` reruns the whole triple.

When the deployed node is the daemon host, nothing changes.

## The builder field applies only to the daemon host

`optional_nix_builder_spec` (for example `Some.@/etc/nix/machines`) still
offloads a daemon-host build through `--builders`. For any other node it is
ignored, because the target daemon builds with its own settings. Each such
build logs one line on standard error:

```
lojix-nexus: BuilderIgnored.{ <node> TargetStore «<builder-spec>» }
```

and each successful target-store build logs:

```
lojix-nexus: TargetStoreRealized.{ <node> <nix_store_uri> <output> <gc-root> }
```

## What the operator needs

- The SSH login in `ssh_destination` must be able to run `nix-store` on the
  target (a NixOS target has it on the login PATH). The ssh-ng login in
  `nix_store_uri` must be a trusted user of the target daemon for
  `nix copy --derivation` and for forwarded substituter options.
- Retiring a generation does not yet remove its target-side root; remove
  `/nix/var/nix/gcroots/lojix/<daemon-host>/generation-<id>` by hand when the
  closure should become collectable.

## Library consumers

`BuildTarget` gained `TargetStore(TargetStoreBuild)`. `CopyClosureCommand`
gained `closure_origin: ClosureOrigin`. `DaemonRuntime` gained
`with_daemon_host`. The pipeline's `build_target`, `nix_eval_command`,
`nix_build_command` and `copy_closure_command` take the daemon host.


# 7.0.0 to 8.0.0

This major version rides the horizon-rs 0.12.0 → 0.13.0 bump, which changes the
archived layout of `HorizonDefinition`. The repin itself (horizon-rs,
signal-lojix, meta-signal-lojix and the wire break it carries) lands in its own
commit on top of this entry and extends it. The store schema stays v5.

## Rows that no longer decode are quarantined, never served

Two families can hold a `HorizonDefinition`: `deploy-job` (inside
`optional_deploy_submission`) and `nexus-configuration` (inside the test
defaults). Before this release the startup gate decoded every row of every
family and refused the whole store on the first failure, so one in-flight
deploy-job row archived by 7.0.0 would have left Nexus 8 crash-looping on a v5
store that `lojix-reset-store` does not reset.

Now each open reads every family row by row, before anything reads a family
whole. A row that decodes stays. Any other row is moved, in one atomic commit
with its retraction, into the new `quarantined-row` family
(`QuarantinedRowFamily`, schema hash `[13; 32]`) as a `QuarantinedRow` holding
its table, its stored key, its original archive bytes and the decode error. No
earlier layout is carried forward: a quarantined row is evidence only. The
Nexus logs one line per row, on standard error, from the open that moved it:

```
lojix-nexus: RowQuarantined.{ deploy-job 19 «<decode error>» }
```

**The configuration row is rebuilt.** When the `nexus-configuration` row is the
one set aside, the Nexus seeds the row again from its built-in configuration —
the same value a fresh store receives — and logs
`lojix-nexus: NexusConfigurationRebuilt.{ nexus-configuration BuiltIn }`. The
Nexus reads no startup archive (the `lojix-write-configuration` archive belongs
only to the reset unit), so the built-in configuration is the one source it
has. A layer that an ordinary or meta `Configure` had added is lost with the
row; its bytes stay in quarantine, and ordinary `Configure` is open again on the
rebuilt row.

**What a quarantined deploy job means.** The deployment it tracked does not
resume. Its deployment record and events stay as they were.

**Inspection.** `lojix-inspect-store` now covers every family, including
`deployment-outbox`, `pending-transition-intent`, `nexus-configuration` and
`quarantined-row`. A family with undecodable rows reports
`decode-failed undecodable_row_count=<n> row_count=<m> [<first error>]` rather
than stopping at the first, and the report ends with
`Quarantine quarantined_row_count=<n> undecodable_row_count=<m>`: the rows
already set aside, and the rows the next open will set aside. Run it on a copy
of the live store before the Nexus 8 restart to know both in advance.

**Consumers.** `TableInspectionStatus::DecodeFailed` gained
`undecodable_row_count` and `row_count`. `Store` now reports its opening through
`lojix::quarantine::OpenedStore`, and the quarantine through
`lojix::quarantine::RowQuarantine`.

## The Horizon 0.13.0 repin and its wire break

This release consumes `horizon-rs` 0.13.0 at
`a3ddaf8685b920093a2328b85ba350a04e11477a`, `signal-lojix` 6.0.0 at
`cd164896311af9849e2ddf1cdbdd35b5feedcdd1`, and `meta-signal-lojix` 7.0.0 at
`c0f883c5cc428ce94ffa109fa20a5a28f7902481`, in the root and in every member
manifest; `flake.nix` pins the same `horizon-rs` revision for the fixture
composer. `ethos-zero` and `datom-codec` are unchanged.

Horizon 0.13.0 changes the archived layout of `HorizonDefinition`:
`NodeCapability::TailnetClient` carries a `SecretReference`,
`NodeCapability::TailnetController` carries the certificate authority and the
TLS certificate and key references, `NodeCapability` gains `UsbDownlink`, and
`RouterInterfaces` gains an eighth field, `country_code`. Both socket contracts
embed the definition, so a 7.0.0 client and an 8.0.0 Nexus (or the reverse)
cannot talk: the clients, the Nexus and both Signal contracts deploy together.
The store schema stays v5. A `deploy-job` row archived by 7.0.0 with a Horizon
definition no longer decodes and is quarantined on the first open;
`tests/horizon_layout_fixture.rs` proves it on a store written at the 0.12
layout.

# 6.0.0 to 7.0.0

This release consumes `horizon-rs` `ee8d6f8d27eb6e200504807971ffdd26aaca7ed1`,
`signal-lojix` 5.0.0 at `f7866bf013499503d21491bcdaadaf88ea6c810c`, and
`meta-signal-lojix` 6.0.0 at `b500561f0ce813917366997581412b7a79aebc99`.
Those two Signal revisions change the socket request contract and must be
deployed together with Lojix 7.0.0. A mixed producer/consumer pair is
incompatible. This major version records that wire incompatibility. It is not
evidence for a deployed socket round trip, Realize, or activation.

# 5.0.0 to 6.0.0

**Every method lojix calls now lives in a trait, and `checks/no-inherent-methods.sh`
is a Nix check.** The wire is unchanged: `signal-lojix` 5.0.0 and
`meta-signal-lojix` 6.0.0 are the same pins, the store schema is still v4, and
no request or reply gained or lost a word. What changed is the Rust surface.

## The Nexus announces its readiness

After both listeners are bound and started and before anything is accepted, the
Nexus now writes one line to its standard output:

```
(LojixNexusReady /run/lojix/ordinary.sock /run/lojix/meta.sock)
```

`daemon::NexusReadiness` owns it: `READY` is the head a waiter matches on, and
the two bound socket paths follow. Nothing else about startup changed, and the
line goes to standard output, never to a socket — the wire is still pure signal.

**Why.** `nexus/tests/daemon_configuration.rs` waited on a clock: a five-second
deadline polled every ten milliseconds until a connect succeeded. That deadline
holds on a developer's machine and lies on a loaded remote builder, where the
same test failed while passing locally. The test now waits on the announcement,
and on the child's standard output closing — which is what a Nexus that dies
before readiness does, so a real startup failure is reported at once instead of
after a deadline. The only clock left is a 300-second backstop that exists so a
wedged Nexus cannot take the harness down, and the success path never reaches
it: the same test went from timing out at 300.00s to passing in 0.26s.

**A supervisor should use it.** A systemd unit for `lojix-nexus` can treat this
line as the readiness event rather than assuming the socket appears at some
point after the process starts.

## Bring the traits into scope

Nothing was renamed and no signature moved except where this file says so, but
a trait method needs its trait in scope. A consumer of `lojix` adds the traits
it uses:

```rust
use lojix::{DurableStore, DeploymentLedger, GenerationLedger, EventHistory,
            IdentifierAllocating, TransitionJournal, TestRunLedger,
            NexusPersistable, Payload, Named};
use lojix::schema_runtime::{RuntimeCore, DeployDriving, TestDriving};
use lojix::client::NexusSocket;
use lojix::daemon::NexusReadiness;
```

## The store is read by record kind, not by method name

Eleven readers — `live_generations`, `gc_roots`, `event_log_entries`,
`container_lifecycle_records`, `deployment_records`, `identifier_allocations`,
`deployment_outbox`, `pending_transition_intents`, `test_runs`,
`nexus_configuration_records` and their private twins — were eleven spellings
of one question. They are replaced by one generic reader on `DurableStore`:

```rust
store.records::<LiveGeneration>()?      // was store.live_generations()
store.records::<GcRoot>()?              // was store.gc_roots()
store.records::<DeploymentRecord>()?    // was store.deployment_records()
store.records::<StoredTestRun>()?       // was store.test_runs()
```

`deploy_jobs()` remains its own verb on `DeploymentLedger`, because reading a
deploy job validates its persisted closure path.

The same trait, `LojixRecord`, now carries each family's table name, family
name, schema hash and operator-facing role. The thirty-three module constants
that used to spell those three lists in parallel are gone, and so are the
eleven `register_table` blocks, the eleven startup validators and the eight
`TableInspectionTarget` constants in `inspection.rs`.

One message changed with it: the startup-gate stage for a single-row family is
now `validating identifier-allocation rows` and `validating nexus-configuration
rows`, plural like every other family. `Error::StoreStartupCompatibility`'s
`stage` field is a `String` rather than a `&'static str`.

## Newtypes answer `Payload`

Every `flow_newtype!`, `flow_text!`, `runtime_newtype!` and `runtime_text!`
type — and `OriginRoute` and `EventLogRetention` — implements
`lojix::Payload` instead of carrying its own `new`/`payload`/`into_payload`
triple. `new` now takes the carried value exactly; a text newtype built from a
`&str` uses `From`:

```rust
ClusterName::from("alpha")             // was ClusterName::new("alpha")
ClusterName::new(name_string)          // unchanged, with `Payload` in scope
```

`EventLogRetention::default_policy()` becomes `EventLogRetention::default()`
and `maximum_entries()` becomes `*retention.payload()`.

## Inspection reports are data

`StoreInspection` and `TableInspection` expose their fields directly; their
accessor methods are gone. `inspection.path()` becomes `inspection.path`,
`table.status()` becomes `table.status`, and so on. `table_named` remains, on
`InspectedTables`. `StoreInspector::new(path)` becomes
`StoreInspector { path }`, and `inspect()` is on `StoreInspecting`.

`StoreInspectionCommand` and `StoreResetCommand` are two shapes of one kind and
now share `lojix::OfflineCommand` (`from_environment`, `from_arguments`, `run`).
`StoreResetCommand::from_arguments_with_configuration` moved to
`reconstruction::StoreResetting`.

## What was deleted as dead

- **The effect barrier.** `EffectBarrier`, `RuntimeConfiguration::test_with_effect_barrier`
  and the `await` in front of the pipeline's first effect were the up9b
  decoupling witness. Nothing constructed a barrier any more — the test that
  did is gone — so production carried an always-`None` option and a branch that
  was never taken. All of it is removed.
- `impl NexusPersistable for Store` was five methods that forwarded to five
  identically named inherent methods. The bodies moved into the trait impl.
- `Nexus::root()`, `StoreInspection::catalog()`, `StoreInspection::tables()`
  and `TableInspection::role()` had no callers.
- `NexusWork::sema_write_completed`, `sema_read_completed`, `effect_completed`
  and `NexusAction::reply_to_signal` wrapped one enum variant each; the variant
  is the constructor.

## Conversions are conversions

`JournalStage::from_effect`/`effect`, `EffectResult::flake_resolved` and its
three siblings, `SchemaRuntime::marker`/`sema_marker`,
`SchemaRuntime::configuration_receipt`/`configuration_rejection` and
`SchemaRuntime::reply_meta` are `From` impls now.

## Nexus Core is seven questions, not ninety-seven methods

`impl SchemaRuntime` held 101 methods. Twenty-four of them were verbs of their
arguments and moved there: `RejectionVocabulary` on `RejectionReason`,
`TerminalReason` on `DeployRejectionReason`, `FailureStaging` on `EffectStage`,
`PhaseLifecycle` on `DeploymentPhase`, `ActivationSlot` on `ActivationEffect`,
`TestRunSelecting` on `TestRunLookup`, `GenerationSelecting` on `Selection`,
and `DeployAdmission` on `DeployRequest` — a deploy request now judges itself.
The rest are `RuntimeCore`, `SignalDeciding`, `DeployDriving`, `TestDriving`,
`SemaApplying`, `SemaObserving` and `EffectRunning`.

Seven effect types — `NixCommand`, `ClosureCopy`, `Activation`,
`HostActivation`, `UserEnvironmentActivation`, `HermeticCheck` and
`HorizonMaterialization` — share one `Effect` trait. `HorizonMaterialization::run`
takes the `EffectExecution` explicitly like every other effect rather than
reaching into its own configuration for one.

# 4.0.1 to 5.0.0

Repins `signal-lojix` 5.0.0 (`4271b5ced31ea02f11f29b602301832e83cfe6c2`) and
`meta-signal-lojix` 6.0.0 (`35deec4ef0a6d023f2f49464515075779cbb4973`). Both
are wire-breaking; see their UPGRADES.md.

**No production `unreachable!()` remains in the crate.** There were fourteen
(`src/lib.rs` 1, `src/schema_runtime.rs` 13); six more sit in `#[cfg(test)]`
modules and are left, because a test that reaches an impossible branch aborts
one test. Each was decided from the code: either the state it asserted was genuinely
unrepresentable, in which case the types were changed so the site had nothing
left to say, or it was reachable, in which case it now answers the peer.

*Made unrepresentable, site removed.*

- `Store::terminalize_deployment` and `Store::begin_terminal_transition` no
  longer take a `DeploymentLifecycle`. Every caller passed the one that agrees
  with the `DeploymentTerminal` beside it; the new `TerminalOutcome` trait
  reads the lifecycle, the deployment phase and the deploy-job phase from the
  terminal itself. A terminal and a lifecycle that disagree is now
  inexpressible, and `Store::terminal_phase`/`terminal_job_phase` are gone
  with it. **Consumers drop the middle argument.**
- `DeployResumeStage` bears `ResumePoint`: `recorded_phase()` and
  `deploy_stage()`. The resume path had a `matches!` guard and a match that
  had to agree about which three stages have a phase receipt; now it reads the
  one answer.
- `HostActivation::runs_detached_self_activation() -> bool` becomes
  `detached_self_activation() -> Option<DetachedSelfActivation>`. The handoff
  script is total over that type, so there is no action left without one.
- `DetachedActivationOutcome::PendingRegistration` is deleted. `classify`
  never produced it; only the pre-GetUnit lookup did, so
  `DetachedActivationObserver::initial_outcome` now returns
  `Option<DetachedActivationOutcome>` and the registration window is the
  absence of an outcome rather than one of its values.
- `drive_submitted_deploy` folds its `FinishDeployment` special case into the
  one match over the resume stage, dropping a duplicated cursor lookup.
- `SchemaRuntime::fail_pipeline` and `finish_deploy_pipeline` take the deploy
  cursor from their caller, which already holds it.
- `SchemaRuntime::deploy_rejection` takes the `DeploymentIdentifier` its
  caller holds, so a rejection with no deployment to name cannot be written.

*Reachable, now answered.*

- `meta-signal-lojix` gains `DeployRefused.RefusedDeploy`. Three refusals
  name no deployment — the continuation budget exhausted, a completion
  arriving with no correlated cursor, a durable write failing before the
  record exists. Each previously aborted the daemon or fabricated a record.
- Every durable-write failure inside a sema-apply handler now returns
  `WriteRejected(DurableWriteFailed)` instead of aborting; the cause goes to
  the daemon journal.
- `DeploySubmissionOutcome` and `DeployAdmission` each gain a third variant,
  `Refused`, carrying `RefusedDeploy`. Allocating a rejection is itself a
  durable write, and `reject_submission` ended in an `expect`: any deploy
  submitted while the store was unwritable aborted the daemon on the most
  ordinary path a peer has. **Consumers matching either enum add the arm.**
- `SchemaRuntime::reject_active_or_meta` reads the deploy cursor **before**
  clearing it. Clearing first meant every write rejection during a deploy
  aborted the daemon on the following `expect`.
- The four deploy preflights in the engine's own meta routing allocated no
  correlation record and then tried to terminalize one; they now go through
  `reject_submission`, as the synchronous submit already did. The four checks
  live in one `submission_rejection`, so the two paths cannot drift.

**`DeploymentTerminalReason::ClosureCopyFailed`** replaces
`BuilderUnreachable` for a failed closure copy. `nix copy` engages no builder.
`BuilderUnreachable` is no longer produced anywhere; it stays on the wire for
a build-stage failure that is one day classified that way.

**`DeploymentPhaseEvent` is unchanged, deliberately.** The discarded
`_detail: Option<String>` on `DeployPipeline::phase_event` is removed rather
than carried onto the wire: all four callers of `record_phase` passed `None`,
so there was no detail to lose. What would have been phase detail — a stage's
stderr, the command it ran — already rides the terminal record's
`FailureEvidence`, which the event log carries through
`optional_deployment_terminal`. Widening the event would have added an
always-absent field to every phase event in the durable log.

## 4.0.1 — the producer chain settles, and the VM fixture is produced not written

### The repin

Every workspace manifest moves to the final producer heads: `protos` 0.30.1
(`171b21f65337983ab624b7b906397a4f1f92c5a3`), `datom-codec` 0.26.3
(`627db67f2655efd9f786864009955005fd8ab2ad`), `ethos-zero` 8.0.1
(`de3d9928b156f2e1a92d060b7817af201abfdbef`), `signal` 3.0.2
(`8f9a0deb701cebbea518679548df4a795affc918`), `horizon-lib` 0.10.1
(`40d04d2504fee619e9b2b2564b8a769a3a9d6049`), `signal-lojix` 4.1.1
(`5c94485c84d20d5b1496d867a2b40f2d908a02e3`), `meta-signal-lojix` 5.1.1
(`2fdc7742eef200ac3ac3057792f2fa4f4bad9f39`). No wire type and no Rust surface
changed. `Cargo.lock` carries exactly one revision of each of our crates.

### The VM fixture no longer restates the Horizon schema

`checks.same-host-test-activation` carried its Horizon definition as a literal
string in `flake.nix`. That string had gone stale against the `horizon-lib`
this workspace itself pins: its `NodeDefinition` had ten fields where eleven
are required, and `HorizonDefinition::decode` refuses it with
`Arity { expected: 11, found: 10 }`. The check stayed green regardless,
because the deployment it drives selects `Direct` input mode, and a `Direct`
deployment never reads its proposal source — `actualize_horizon` returns
`None` for it in the meta client, and `proposal_source_rejection` requires
exactly that. The fixture proved nothing and said it proved something.

It is now produced by the real producer. `checks/horizon/` holds an authored
`HorizonConfiguration` and `ClusterDefinition`; the pinned horizon-rs
`horizon-compose` turns them into `horizon-definition.datom` while the check
builds, and the test script reads that file back through lojix's own Horizon
reader (`lojix-write-configuration` with a `TestDefaults` naming it) before
the deployment runs. A fixture that has drifted from the pinned schema now
fails at build time, named, instead of booting a guest that ignores it.

Nothing in the package changed. A consumer repins the revision.


## 4.0.0 — behavior is homed on the thing it belongs to

### What changed

lojix enforced neither of the two trait laws. `checks/no-free-functions.sh`
now does, as a Nix check: production Rust carries no module-level free
function except `fn main`. It scans the library and all four workspace
members with each file's `#[cfg(test)]` items removed, so the large
in-file test modules in `src/lib.rs` and `src/schema_runtime.rs` are not
production source and are not read as such.

One hundred and ten free functions were rehomed to get there. Most were
private, and this entry lists only what a consumer outside this workspace
can see: the public Rust surface of the `lojix` library, its two client
crates, and the offline tools. No wire contract changed — `signal-lojix`
and `meta-signal-lojix` are untouched, and a 3.0.0 client talks to a 4.0.0
Nexus unchanged.

Two duplications were the reason for two of the new types. The predicate
"is this a canonical `/nix/store` item root?" had three identical copies,
and "does this text name credential material?" had four. They are now
`NixStorePath`, `InspectedText` and `PercentEncodedText` in
`src/inspected_text.rs`, with the verbs on `StoreItemShape` and
`CredentialBearing`. And `mod ordinary`/`mod meta` in the schema runtime
held eighteen zero-sized `pub struct X; impl X { pub fn new(p: P) -> P { p } }`
shims — a namespace pretending to be a thing, whose every one of forty-two
call sites was the identity function. They are gone.

### The moves a consumer applies

Each is mechanical. Import the named trait (`use lojix::…Trait as _;`) and
rewrite the call.

| Was | Is | Trait to import |
| --- | --- | --- |
| `lojix::single_inline_datom_argument(arguments)` | `arguments.single_inline_datom()` | `lojix::InlineDatomArguments` |
| `lojix::bootstrap::run_from_environment()` | `BootstrapRun::run_from_environment()` | `lojix::bootstrap::BootstrapInvocation` |
| `lojix::bootstrap::decode_single_inline(arguments)` | `BootstrapRun::decode_single_inline(arguments)` | `lojix::bootstrap::BootstrapInvocation` |
| `lojix::bootstrap::run_with_executor(request, &mut executor)` | `request.run_with_executor(&mut executor)` | `lojix::bootstrap::BootstrapInvocation` |
| `lojix::bootstrap::run_with_executor_and_crash(request, &mut executor, &mut crash)` | `request.run_with_executor_and_crash(&mut executor, &mut crash)` | `lojix::bootstrap::BootstrapInvocation` |

`InlineDatomArguments` has a blanket implementation for every
`IntoIterator<Item = OsString>`, so the receiver is whatever the argument
expression already was.

### Additions, which break nothing

- `lojix::LojixNexusConfigurable` gains a provided `runtime_directory()`.
  Existing implementations compile unchanged.
- `lojix_client::Invocable` and `meta_lojix_client::Invocable` gain a
  provided `budget() -> Budget`. Existing implementations compile
  unchanged.
- `impl From<u64> for lojix::runtime_model::StateMarker`: a commit sequence
  is the whole of a state marker, since the digest is derived from it.
- `impl From<nexus::AsyncMultiListenerDaemonError<lojix::Error>> for lojix::Error`.
- `lojix::adapters::Raisable` is now implemented for
  `ConfigurationReceipt` and `ConfigurationRejection`.

### What is not done

`checks/no-inherent-methods.sh` is written and runnable but is **not**
wired into `flake.nix`, because it does not pass. See the check's own
output for the current sites.

## 3.0.0 — a failed deployment says what failed

### What changed

A `DeploymentFailure` now carries `Option<FailureEvidence>`: the bounded,
redacted text the failing stage printed and, when a subprocess ran, that
process's program, arguments and exit code. It reaches an operator three
ways — the `DeployTerminal` reply, `Query.ByDeployment`, and
`Query.ByEventLog`, which carries the same terminal in its journalled
transition.

Until now every stage failure collapsed into one of eleven generic reasons and
the captured stderr was printed to the journal and dropped. `Eval` and `Build`
both reported `FlakeReferenceMalformed`, which was true of neither; both now
have their own reason, `EvaluationFailed` and `BuildFailed`.

The detail is redacted at the producer: any line containing a credential term
is dropped, and `detail_truncated` says so. Only the last 4 KiB survive — a
Nexus keeps the evidence a retry needs, not a log stream.

`Query.ByDeployment` also answers with the generation that deployment
produced. It previously matched deployment records but no generation at all,
so the question an operator asks after a deployment — what did it put on the
node — had no answer.

`CheckHostKeyMaterial` is gone from the ordinary contract, with its whole
vocabulary. It was a stub reporting an empty mismatch vector for every node,
with no effect behind it and an adapter that discarded the vector regardless.
A security check that always answers "no mismatch" can only mislead. If it is
wanted it returns as an effect-backed verb with its own pipeline stage,
comparing Horizon's published `ssh_public_key` and Yggdrasil key view against
the live host.

### Reconciling a partial failure before a retry

This is the point of the evidence, so read it before retrying anything.

A Lojix terminal is a statement about the **ledger**, not about the target. A
`Failed` at `Activate` means the activation step reported failure; it does not
mean the target is unchanged. The user-environment pipeline sets the profile
before it activates, so a deployment that fails at `Activate` has already
advanced the target's Home Manager profile generation. A host deployment that
fails after `SetBootProfile` has already written a boot entry. The failed
deployment does not enter the live set, so Lojix's live-set query and the
target's actual state disagree — correctly, and on purpose.

Before retrying:

1. Read the evidence. `Query.ByDeployment` on the failed identifier gives the
   command and exit status. The named command tells you which step ran last.
2. Establish the target's real state independently — `readlink -f` the profile
   or `/run/current-system` on the node. Lojix's ledger cannot tell you this
   and does not claim to.
3. If the profile advanced but activation failed, the target holds a generation
   Lojix does not list as live. Either activate that generation on the target,
   or roll the profile back, before submitting a new deployment. Submitting on
   top of an unreconciled partial advance means the next failure has two causes
   and the evidence cannot separate them.
4. Only then resubmit. Lojix allocates a new deployment identifier; the failed
   one stays failed, with its evidence, permanently.

## Horizon 0.9 fixed location

Lojix now decodes Horizon 0.9 definitions, whose nodes end with an optional
declared fixed location. Deployment continues to accept only the composed
`horizon-definition.datom` artifact. A fixed location is authored cluster data
and does not represent a device-derived position measurement.

## 1.0.1 — trait-borne runtime operations

The Nexus runner, peer authority, request worker, deploy/test actors, historical
archive access, and configuration-writer CLI expose their operational methods
through qualifier-named traits. Nexus startup failures are plain diagnostics.
Package and wire behavior are unchanged from 1.0.0.

## 1.0.0 — zero-argument Nexus and separate clients

Lojix now ships `lojix-nexus`, `lojix`, and `lojix-meta` from separate Nexus,
ordinary-client, and meta-client packages. The Nexus discovers its stable Sema,
persists desired configuration plus the meta-Configure marker, and has no Datom
dependency in its package dependency graph; Datom-enabled maintenance programs
live in the separate `lojix-offline-tools` package. Its old startup archive is accepted
only by `lojix-migrate-configuration`, which migrates an exact pre-Nexus v5
store on a byte copy and leaves the source unchanged.

The older upgrade notes below describe their named historical releases. Their
startup-archive instructions do not apply to 1.0.0.

## 0.22.0 — final Protos and Datom composition chain

Lojix 0.22.0 regenerates its private ingress schema as an Ethos Library over
the final `String` and `Integer` data model. Every inline maintenance and
bootstrap request now traverses the bounded Protos → Datom → corporate-value
chain, including canonical bare dotted paths and colon URLs.

## 0.21.0 — generated Datom and materialized Horizon definition

Lojix 0.21.0 replaces the retired text/DOTOS ingress and legacy schema roots
with generated current Datom contracts. Ordinary and owner requests now travel
only in bounded binary Signal frames; the private inspect, reset, bootstrap,
and configuration-writer commands accept their generated typed Datom roots.

For Horizon deployment, provide the externally composed public
`horizon-definition.datom` rather than a `ClusterProposal` document. The
definition must contain the selected cluster node and is projected by Lojix;
the public artifact contains no secret values or secret paths. Every deploy
also supplies `SecretsInput`: use `NoSecrets` for no secret authority, or an
existing absolute non-symlink `SecretsDirectory` owned by the caller. Lojix no
longer derives a sibling secrets directory from the public artifact.


### Durable-store cutover

The generated `SecretsInput` is persisted in each in-flight `DeployJob`, so
0.21.0 opens a v5 store and deliberately refuses a v4 store before any job is
decoded or resumed. It does not migrate historical deploy jobs, event history,
or secret authority. In particular, it never substitutes `NoSecrets` for a
v4 job and never resumes that job under changed meaning.

For a non-destructive cutover, stop `lojix-nexus`; retain its v4 primary store
at its existing configured absolute path; then generate the next daemon startup
archive with a distinct, new absolute `store_path` (for example, a `.v5`
sibling). The typed production writer form is:

```text
lojix-write-configuration 'ConfigurationWriteRequest.{ <ordinary-socket> <ordinary-mode> <owner-socket> <owner-mode> <state-directory> <new-v5-store-path> <daemon-host> NoTestDefaults <new-startup-archive> }'
```

Update the service to use that new generated archive, then start the daemon
only with it. The fresh path is initialized as v5 while the v4 store remains
unchanged. Inspect that retained v4 path read-only with the compatible Lojix
0.20.3 inspector at `46585a2c8303bffe885b1722bfebd97d5353ca17`, which uses its
legacy Dotos ingress:

```text
lojix-inspect-store '(InspectStore <absolute-v4-store-path>)'
```

The v5 `lojix-inspect-store` uses `InspectStore.{ <path> }` and decodes v5
record types; it is not the compatible reader for retained v4 jobs/history. Do
not copy a v4 database into the new v5 path and do not point the v5 daemon at
the old path.

`lojix-reset-store` remains a separate, explicit discard option for a stopped
daemon: it may replace a recognized v2/v3/v4 Lojix store with an empty v5
store. That action discards the old store's jobs and history; archive or retain
the original v4 path first if those records are needed for inspection.

## 0.20.1 — query lookup wire shape

Lojix 0.20.1 preserves the public one-field product shape of
`Query.ByDeployment` and `Query.ByGeneration` while decoding both selectors
into the runtime model. Querying an unknown identifier now completes with the
ordinary typed empty `Queried` reply instead of ending the client exchange at
the wire boundary.

## 0.20.0 — canonical ClusterProposal artifact

Lojix 0.20.0 accepts a Horizon proposal only from a direct regular
`proposal.datom` artifact, whose content is embodied through
`Text<ClusterProposal>`. It no longer accepts a legacy `.dotos` proposal
source. Deploy the matching `goldragon/proposal.datom` and the Horizon 0.5
dependency together, then submit the normal immutable Lojix deployment request
with that exact canonical source path. Observe the returned deployment through
the ordinary Lojix client until its terminal record is `Succeeded`; admission
does not establish the upgrade.

# 1.0.1 to 2.0.0

Lojix takes its wire framing and its Signal kinds from the `signal`
repository. Nothing on the Lojix wire changed: `triad-runtime`'s
`LengthPrefixedCodec` and `signal`'s frame both write a four-byte
big-endian length prefix, so a 1.0.1 client and a 2.0.0 Nexus still
understand each other's bytes.

Three things do change for a consumer of the `lojix` library:

1. `lojix::Error::SignalFrame` now wraps `signal::FrameError` instead of
   `triad_runtime::FrameError`.
2. `lojix::client::SocketExchange` caps a response body at 8 MiB. It
   previously accepted `LengthPrefixedCodec::default()`, which admits a
   `u32::MAX` prefix — the daemon already capped requests at 8 MiB, and the
   client now matches it.
3. `Signal<T>`, `Signalizable`, `ByteViewable`, and `Restorable` come from
   `signal`, not from `signal-lojix` or `meta-signal-lojix`:

   ```rust
   -use signal_lojix::{ByteViewable, Restorable, Signal, Signalizable};
   +use signal::{ByteViewable, Restorable, Signal, Signalizable};
   ```

Deploy by rebuilding; the Nexus and both CLIs may be rolled independently
because the wire is unchanged.

Pins moved to `signal-lojix` 2.0.0, `meta-signal-lojix` 3.0.1, and
`signal` 2.0.0.

`triad-runtime` is still a dependency for the actor listener runtime; only
its frame codec is no longer used here. Its streaming path is untouched.
