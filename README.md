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

## Maintained bootstrap app

`lojix-bootstrap` is the v0.17.2 flake-owned, daemon-free bootstrap surface. It
does not open a Lojix socket, read daemon configuration, reuse a Lojix store,
or derive a target route. The package wrapper carries Nix and systemd tooling
in its closure; the built system closure receives a caller-selected durable GC
root before any activation.

Run it through the maintained Lojix app, or the exact re-export in the
maintained CriomOS/Home flakes, with exactly one inline DOTOS object and no
flags or request files:

```text
nix run github:LiGoldragon/lojix/<rev>#lojix-bootstrap -- 'BootstrapRun.{<request-id> <mode>}'
```

The complete shape is positional and explicit:

```text
BootstrapRun.{
  <request-id>
  BuildOnly.{
    Direct.{<immutable-github-flake-ref> <nix-system> <output-selector>}
    | Horizon.{<proposal.dotos> <cluster> <node> <CompleteHost-or-BaseHost> <NoSecrets-or-SecretsDirectory.{<directory>}> <immutable-github-flake-ref> <nix-system> <output-selector>}
    <NoBuilder-or-NixBuilder.{<builder-spec>}>
    <journal-parent> <new-gc-root-path> <new-terminal-evidence-path>
  }
  | BootOnce.{
    <same-input> <same-builder>
    <NoTest-or-RunHermeticTest.{<immutable-github-flake-ref> <nix-system> <output-selector>}>
    <RemoteNixosSystemdBootV1.{<nix-store-uri> <ssh-destination> SshPolicy.{<private-identity-file> <private-known-hosts-file> RequireKnownHost} <system-profile-path> <boot-entries-directory>}
      | LocalBootstrapV1.{<system-profile-path> <boot-entries-directory>}>
    <journal-parent> <new-gc-root-path> <new-terminal-evidence-path>
  }
}
```

An immutable flake reference is exactly
`github:<owner>/<repo>/<40-lowercase-hex-revision>`; query, branch, tag, and
other mutable forms are rejected. The Remote backend requires a matching,
explicit pair: `ssh-ng://<user>@<lowercase-host>[:port]` and
`<user>@<lowercase-host>[:port]`. Both user and host are canonical, no SSH
options/config defaults or passwords can be embedded, and an explicit port is
passed as an SSH argument rather than folded into a host name. The required
`SshPolicy` supplies caller-owned-private `0600` identity and known-hosts
files plus `RequireKnownHost`; it forces identities-only, no agent, no proxy,
no multiplexing, and generates the same private OpenSSH config for direct SSH
and `nix copy`. The flake wrapper resolves OpenSSH from its own closure rather
than ambient `PATH`.

`BuildOnly` is the exact no-activation variant: it materializes, builds, and
creates the requested durable GC root, then writes terminal evidence. It has
no transport or backend field. `RemoteNixosSystemdBootV1` requires and uses the
exact paired identity, dispatches a request-hash-derived no-block transient
oneshot unit on the target, and reconciles that exact unit after interruption.
`LocalBootstrapV1` is a separate audited systemd-boot path with an explicit
backend PATH and never substitutes self-SSH or `nix copy`. Journal, GC-root,
and evidence parents must be caller-owned `0700` directories; evidence is
created `0600`. Roots and evidence use no-replace receipts, and the journal
records intent, receipt, and outcome before moving to each next stage. On a
crash, re-running the exact same inline request resumes from that receipt; it
does not repeat a rooted closure, copied closure, or dispatched unit. Terminal
evidence contains only a request hash, mode, status, and effect outcomes—never
proposal text, flake reference, transport identity, raw paths, or command
output—and is fsynced with its parent. Finalized private journals are retained
as the durable receipt/audit record: path-based recursive cleanup is forbidden
until every deletion can be made inode-handle-bound.

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
