# lojix — v4 operational contract

This document records the live operational boundary, not a standing authority
grant. Request ownership and machine access remain decisions for the caller.

## Explicit routing

Every host or user-environment deployment supplies a typed
`DeploymentTransport` pair:

- `nix_store_uri` is the exact URI used by `nix copy --to`.
- `ssh_destination` is the exact destination passed to SSH for target-side
  activation.

Both values are validated for their respective grammars and used verbatim.
Lojix never derives an address, domain, login, store URI, Nix output attribute,
or builder path from a cluster/node/user name. An explicit `root@…` SSH
destination is permitted, but is never assumed.

The same request also owns its input mode, exact flake output selector,
activation backend, and optional Nix builder specification. `Horizon` is an
explicit input mode; `Direct` does no Horizon materialization. A supplied
builder specification is passed to Nix through `--builders`; no machine-file
fallback exists. The daemon's evaluation stays local, and the route used for
copy and activation is private daemon state.

## Runtime configuration

The daemon has no default socket locations. Clients must receive both
`LOJIX_ORDINARY_SOCKET` and `LOJIX_OWNER_SOCKET` from their environment or
launch configuration. CriomOS configures those paths explicitly through
`services.lojix`; it also supplies the service account, socket modes, state
directory, exact store path, startup archive path, daemon host, and
effect timeout. Enabling the service without those values is rejected at Nix
evaluation time.

Production startup uses `NoTestDefaults`. Test fixtures may use explicit
`TestDefaults`, including the Nix system and exact test output selector. A
full test run carries its own `TestExecutionProfile`; Lojix does not construct
a test attribute from a host or node name.

## Resetting an old store

Schema v4 refuses older Lojix data. There is no migration or legacy-resume
path. With `lojix-daemon` stopped, a privileged operator may manually start
the dedicated `lojix-reset-store` unit. It invokes `lojix-reset-store` only
with inline `(ResetStore)` and supplies its generated startup archive as the
service-owned `LOJIX_CONFIGURATION` environment value; it conflicts with the
daemon.

The reset binary accepts exactly one pathless inline request object, never a
raw path, file, or flag. Its archive and configured store must be regular,
non-symlinked, absolute, and traversal-free. Only recognised pre-v4 Lojix
schemas are removed; v4 returns `AlreadyCurrent` without touching data. Only
then are the three protocol sidecars mechanically derived. It does not select,
follow, or modify a Spirit database. Do not run it as part of ordinary daemon
startup.
