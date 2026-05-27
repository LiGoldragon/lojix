# Intent — lojix-next (schema-deep iteration 2)

`lojix-next` (on the `schema-deep-iteration-2` branch) is the
runnable proof that the schema-derived stack works at the
lojix-component scale, with the iteration-2 deepening: Nexus is
the mail keeper, sema-engine is the durable state plane, every
reply is stamped with a DatabaseMarker, and the `Communicate`
trait abstracts the wire surface.

## Psyche intent — distilled

This INTENT.md is a per-repo synthesis of Spirit records
863-980 that bear on the lojix-horizon prototype work. It is
not the canonical intent log; that lives in Spirit (per
`~/primary/skills/intent-log.md`) and in records-by-id below.

*Iteration discipline is mine + implement + audit + develop.*
Per Spirit records 971-974 + 980: every prototype iteration
mines existing intent + reports + prototypes for working
solutions, implements a fully-working prototype, then audits
against the 8-component fullness criterion. When a designed
component is too incomplete to use, the prototype work develops
that component further rather than bypasses it.

*The runtime triad is Signal / Nexus / SEMA.* Per record 964:
the daemon has three execution centers. Signal is wire /
communication. Nexus is execution + mail keeper + translator —
when Nexus holds the mail, the mail is in BEING-PROCESSED
state. SEMA is durable state. The flow is `Signal IN -> Nexus
accepts -> Nexus translates to SEMA query -> SEMA produces
state change + reply -> Nexus receives reply + stamps
DatabaseMarker -> Signal OUT`.

*The mail mechanism is universal.* Per records 935 + 963-970:
every Signal root that enters Nexus becomes a
`NexusMail<Payload>` with a `MessageIdentifier`. Lifecycle hooks
fire push-style (not poll) on every state transition (Sent ->
Queued -> Processing -> Replied). The `MessageSent` event is the
schema-emitted hook surface; observers attach
`MessageSentHook` / `MessageProcessedHook<Output>` impls.

*Every reply carries a DatabaseMarker.* Per record 935 + `/390`:
the marker is `(TransactionCounter, StateHash)`. Counter is
sema-engine's monotonic `CommitSequence`. Hash is Blake3 of
`(counter, latest_snapshot)`. Reads leave the marker stable;
writes advance both fields. Nexus stamps the marker
immediately before sending the reply back through Signal.

*Schema-emitted types are the nouns.* Per record 882 +
`~/primary/skills/rust/methods.md`: every Rust function lives
on a non-zero-sized data-bearing type or trait impl. The
`record_key` method for sema-engine lives on each schema-emitted
record family (PlanRecord, BuildRecord, etc.) — no parallel
hand-written mirrors.

*Sema-engine is the durable state plane.* Per records 948-949:
the Store actor backs onto `sema-engine::Engine`, which sits on
redb. Per-family `TableReference` typed surfaces, monotonic
commit-sequence + snapshot identifiers, durable across restart.
The schema-emitted SemaCommand still names the in-process
protocol; sema-engine carries the rkyv-on-redb persistence.

*Designer lanes don't push to main.* Per psyche 2026-05-24
(record 515): the schema-deep-iteration-2 branch lives on a
worktree under `~/wt/`; operator integrates to main when
amalgamating.

## Spirit record references

The records that bear directly on this iteration:

| ID | Kind | Topic | Summary |
|---|---|---|---|
| 882 | Maximum | rust-discipline | Every Rust function is a method on a non-zero-sized data-bearing type or trait impl |
| 884 | Maximum | rust-discipline | Rust authoring must read `skills/rust-discipline.md` first |
| 909-910 | Maximum | schema-emission | Emit schema-derived Rust into `src/schema/`; methods on non-ZST nouns |
| 935 | Maximum | wire-runtime | Communicate trait + signal-frame + mail + DatabaseMarker (commit + state-hash on every reply) |
| 944 | Maximum | repo-intent | Per-repo INTENT + ARCHITECTURE continuously manifested as part of work |
| 948-949 | Maximum | sema-engine | Sema-engine is durable single-writer state; redb-backed |
| 950 | Maximum | schema-upgrade | Schema upgrade traits live on emitted types when the record family changes |
| 963 | Maximum | mail-mechanism | Universal mail mechanism with on_sent push hook |
| 964 | Maximum | runtime-triad | Three schema types ↔ three execution centers (Signal / Nexus / SEMA) |
| 965 | Maximum | runtime-triad | Nexus is execution + IO + UI surface; mail keeper is the fundamental role |
| 966-970 | Maximum | nexus-mail-keeper | Nexus IS the mail keeper; BEING-PROCESSED while held; lifecycle hooks fire push-style |
| 971-974 | Maximum | prototype-iteration | Mine + implement + audit + develop; partial mocks are the failure mode |
| 980 | Maximum | prototype-iteration | Same methodology, captured by this lane (redundant follow-on) |

## Boundary of this repo

`lojix-next` IS the in-process daemon + CLI for the lojix-horizon
component. It does not own:

- The Signal layer's substrate (`signal-frame` owns frame
  encoding; lojix-next uses the schema-rust-next-emitted
  `encode_signal_frame` / `decode_signal_frame` methods).
- The SEMA engine substrate (`sema-engine` owns redb-backed
  persistence; lojix-next opens an `Engine` and registers
  record-family tables).
- The schema language substrate (`schema-next` owns the macro
  registry and `Asschema` lowering; `schema-rust-next` owns Rust
  emission).
- The cross-component mail primitive (iteration 3 may extract
  `NexusMailKeeper` to a shared `persona-mail` crate per `/390
  §"Mail state manager"`; this iteration keeps it inside
  lojix-next).
- The Communicate trait's permanent home (iteration 3 may
  promote to `signal-frame` or a dedicated abstract crate).

## See also

- `ARCHITECTURE.md` — the structural shape of this iteration.
- `~/primary/INTENT.md` — workspace intent prose.
- `~/primary/AGENTS.md` — workspace compact contract.
- `~/primary/reports/system-designer/37-prototype-schema-deep-iteration-2-nexus-mail-sema-engine-2026-05-27/`
  — this iteration's frame, target mapping, and implementation
  report.
