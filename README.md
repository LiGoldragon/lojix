# lojix-next — schema-deep pilot

The schema-deep rewrite of the new lojix-horizon logics. One
authored `schema/lojix.schema` is the source of truth for every
typed noun the daemon touches; `nota-next` + `schema-next` +
`schema-rust-next` generate the Rust; hand-written code in
`src/runtime/` attaches Kameo 0.20 actor topology and methods to
the schema-emitted types.

## Binaries

- **`lojix-next-daemon`** — long-lived deploy daemon. Takes one NOTA
  argument: a `DaemonConfiguration` record.
- **`lojix-next`** — thin CLI client. Takes one NOTA argument: an
  `Input` record. Sends it as a signal-frame over a Unix socket.

## Run

```sh
nix flake check
```

Runs the full test family (10 tests) including the sandbox-OS
witnesses.

## Architecture

See `ARCHITECTURE.md`.
