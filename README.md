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

## Store migration

Run the packaged migrator as a service pre-start step, while the daemon is
stopped:

```sh
lojix-migrate-store /var/lib/lojix/lojix.sema
```

The command is idempotent for a missing store or schema 2. For schema 1 it
validates the source read-only, preserves
`lojix.sema.schema-v1.backup` byte-for-byte, reconstructs and reopens a schema-2
staging store, and atomically replaces the canonical path only after validation.
Corrupt rows, unknown tables, relation mismatches, or a conflicting backup stop
the migration without changing the canonical store.

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
