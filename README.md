# lojix-daemon

Long-lived deploy orchestrator daemon. Owns the live generation set, GC
roots tree, deploy event log, and container lifecycle observation for
the cluster. Receives typed deploy requests over
`/run/lojix/daemon.sock`.

**Status: skeleton.** Documentation only — no code yet. See
`ARCHITECTURE.md` for the planned shape and
`~/primary/protocols/active-repositories.md` for the replacement-stack
context.

Replaces the implementation surface of today's `lojix-cli` once shipped;
`lojix-cli` becomes a thin client over this daemon.

## Related

- `signal-lojix` — the typed wire contract.
- `lojix-cli` — today's monolithic deploy orchestrator (parallel build
  until cutover).

## License

License of Non-Authority.
