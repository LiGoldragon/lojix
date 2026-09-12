# Upgrades

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
