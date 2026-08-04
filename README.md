# lojix

Daemon-based deploy stack for CriomOS hosts and user environments. This
crate ships the long-lived orchestrator plus thin CLI clients for the two
authority surfaces.

- **`lojix-daemon`** — owns durable deploy state, the live generation
  set, GC roots, deployment event log, test-run state, and activation
  pipeline. It binds ordinary and owner/meta Unix sockets.
- **`lojix`** — ordinary socket client for typed queries, watches,
  unwatch requests, and host-key-material checks.
- **`meta-lojix`** — owner/meta socket client for typed deploy, pin,
  unpin, retire, and test requests. A `DeployAccepted` reply is an
  admission handle, not proof that build/copy/activation finished. Use
  ordinary event-log or generation queries for status.

Storage lives in `sema-engine`; wire framing uses `signal-frame`. The
ordinary contract repo is `signal-lojix`; the owner/meta contract repo is
`meta-signal-lojix`.

## Deploy and input boundaries

Every accepted deploy phase is a durable correlation record plus a pending
transition intent. Its public event receives the marker of that exact source
commit, is journalled through the local outbox, and is acknowledged before a
later effect is eligible. Restart completes unacknowledged intents; compacted
acknowledged history is never replayed.

Deploy admission reads a proposal only from an existing, non-symlink absolute
regular `.dotos` file that parses as a cluster proposal. Resolver output,
effect output, persisted resume cursors, live generations, and GC roots may
use a closure only when it is a canonical immutable `/nix/store/<hash>-<name>`
root. Public replies expose no raw proposal source, flake reference, local
error text, or noncanonical path.

## Store migration

Run the packaged migrator as a service pre-start step, while the daemon is
stopped:

```sh
lojix-migrate-store /var/lib/lojix/lojix.sema
```

The command is idempotent for a missing store or schema 3. For schema 2 it
opens the source read-only with the frozen v2 vocabulary, preserves a unique
byte-identical `.schema-pre-v3.backup`, reconstructs and reopens a schema-3
staging store, and atomically replaces the canonical path only after
validation. The transient staging sidecars are `.schema-v3.pending` and
`.schema-v3.pending.owner`. A schema-3 retry accepts the permanent backup by
itself, but never ignores either sidecar: it removes only an owner marker left
after replacement when there is no staging file and that marker is a regular
schema-2 hard link with the same inode, bytes, and metadata as the permanent
backup. Every other sidecar combination remains untouched and stops startup.
Legacy deployment events are private quarantined evidence, legacy deploy jobs
are non-resumable, legacy `Current` claims are demoted to history, and only a
canonical legacy closure is projected publicly. Schema 1 is
intentionally refused rather than decoded with an incompatible layout.
Corrupt rows, unknown tables, relation mismatches, or a conflicting backup stop
the migration without changing the canonical store.

## Daemon startup configuration

`lojix-write-configuration` is the only DOTOS-to-startup boundary. It accepts
one `ConfigurationWriteRequest` (inline or `.dotos` file) containing, in
order, ordinary socket and mode, owner socket and mode, state directory,
daemon host, effect timeout, `NoTestDefaults` or `TestDefaults`, and output
path; it writes the rkyv startup archive. Production uses `NoTestDefaults`.
`lojix-daemon` accepts only that generated signal/rkyv file, never inline
DOTOS or a `.dotos` startup argument. Service activation must therefore run
the migrator while the daemon is stopped, invoke the writer, then pass its
output path to the daemon.

## Related

- `signal-lojix` — ordinary peer-callable wire contract.
- `meta-signal-lojix` — owner/meta policy wire contract.
- `signal-frame` — wire kernel that both contracts build on.
- `sema-engine` — typed database engine library used for durable state.
- `horizon-rs` — cluster-proposal projection (read-only per request).
- `goldragon` — cluster proposal source.
- `clavifaber` — per-host key material (separate component).

## License

License of Non-Authority.
