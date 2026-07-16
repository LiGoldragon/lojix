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

## Schema-one store reconstruction

A schema-one `*.sema` store is never opened by the schema-two daemon. Preserve
it first, inspect it read-only, then reconstruct into a **new, nonexistent**
destination path:

```sh
cp --reflink=auto /path/lojix.sema /safe-backup/lojix-schema-one.sema
lojix-inspect-store /safe-backup/lojix-schema-one.sema
lojix-reconstruct-schema-one /safe-backup/lojix-schema-one.sema /path/lojix-schema-two.sema
lojix-inspect-store /path/lojix-schema-two.sema
```

The reconstructor opens the source read-only, validates every decodable record
and every generation/GC-root and container/event pair, then performs one atomic
seed of the new schema-two store. It rejects a mismatched/corrupt source or an
already-existing destination without writing either path. Schema one never
persisted `DeploySubmission`; legacy in-flight deploy jobs are deliberately
omitted and reported as `MissingDeploySubmission` rather than inventing a Host
or UserEnvironment request. Reopen the destination with `lojix-daemon` only
after the final inspection succeeds.

## Non-production test-VM acceptance

Run only in a disposable test environment, never against daemon production
state. Start a daemon with a temporary state directory and a test-only rkyv
configuration, then execute the existing ignored witnesses:

```sh
LOJIX_UPDATE_SCHEMA_ARTIFACTS=1 cargo test --test test_op \
  daemon_socket_roundtrip_hermetic_check_mercury_passes --features nota-text -- --ignored --nocapture
LOJIX_UPDATE_SCHEMA_ARTIFACTS=1 cargo test --test build_smoke \
  daemon_binary_socket_roundtrip_eval --features nota-text -- --ignored --nocapture
```

Expected observations: one accepted test-run record for `mercury`, exactly one
terminal `Passed` record with a `/nix/store/` closure path, no additional live
generations or GC roots from the hermetic check, and no retained mirror-outbox
entries. Record `du -sh <temporary-state-directory>` before and after the run
and `free -h` / `df -h <temporary-state-directory>` at completion; these are
observations, not fixed production thresholds. Remove the temporary directory
only after inspection confirms the expected cardinalities.

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
