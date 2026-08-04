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

## Store reset

Lojix v4 deliberately refuses older Lojix schemas. There is no row migration
or legacy resume path. After stopping `lojix-daemon`, CriomOS may manually
start its dedicated reset unit:

```sh
systemctl start lojix-reset-store.service
```

The unit supplies its generated startup archive through
`LOJIX_CONFIGURATION`; the binary accepts exactly one pathless inline
`(ResetStore)` object and never accepts a store path, file, or flag form. The
archive must be a regular non-symlink file and its configured store must be an
existing, absolute, traversal-free regular non-symlink file. A recognised v2
or v3 Lojix catalog is removed and recreated as v4; an already-current v4
store returns `AlreadyCurrent` without deleting any data. Only then are the
pre-v4 protocol sidecars
(`.schema-pre-v3.backup`, `.schema-v3.pending`, and
`.schema-v3.pending.owner`) mechanically derived and removed. It never
selects, follows, or modifies a Spirit database.

## Daemon startup configuration

`lojix-write-configuration` is the only DOTOS-to-startup boundary. It accepts
exactly one inline `ConfigurationWriteRequest` object—never a `.dotos` file,
raw path, or flag—containing, in
order, ordinary socket and mode, owner socket and mode, state directory, exact
store path, daemon host, effect timeout, `NoTestDefaults` or `TestDefaults`,
and output path; it writes the rkyv startup archive. Production uses
`NoTestDefaults`.
`lojix-daemon` accepts only that generated signal/rkyv file, never inline
DOTOS or a `.dotos` startup argument. Service activation invokes the writer
and passes its output path to the daemon; reset is a separate, manually
started service that must not run while the daemon is active.

`lojix-inspect-store` is read-only and likewise accepts exactly one inline
`(InspectStore <path>)` object, never a raw path, file, flag, or extra
argument.

## Related

- `signal-lojix` — ordinary peer-callable wire contract.
- `meta-signal-lojix` — owner/meta policy wire contract.
- `signal-frame` — wire kernel that both contracts build on.
- `sema-engine` — typed database engine library used for durable state.
- `horizon-rs` — cluster-proposal projection (read-only per request).
- a deployment-specific cluster proposal source, supplied by each request.
- `clavifaber` — per-host key material (separate component).

## License

License of Non-Authority.
