# lojix

The new deploy stack — one crate, two binaries:

- **`lojix-daemon`** — long-lived deploy orchestrator. Owns the live
  generation set, GC roots tree, deploy event log, and container
  lifecycle observation. Binds `/run/lojix/daemon.sock`; receives
  `signal-core` frames carrying typed `signal-lojix` deploy requests;
  pushes `DeploymentObservation` + `CacheRetentionObservation` stream
  events to subscribers.
- **`lojix`** — thin CLI client. Reads one Nota request, sends it to
  the daemon as a `signal-lojix` frame, prints one Nota reply.

Storage lives in `sema-engine` (the typed database engine library);
wire framing is `signal-core`. The contract repo is `signal-lojix`.

**Status: in development.** The `horizon-re-engineering` branch has
the first socket/client/runtime slice plus typed `nota-config`
configuration for both binaries. The build-only deployment path is
active: it projects Horizon, builds through Nix, pins realized outputs
as GC roots before reporting success, and records deployment
observations in a sema-backed event log. Active deployment-observation
subscribers receive pushed stream-event frames for subsequent events.
Deploy-facing examples and
witness data on this branch target the matching
`horizon-re-engineering` branches of `CriomOS`, `goldragon`, and
`horizon-rs`. See `ARCHITECTURE.md` for the full constraint set and
`~/primary/protocols/active-repositories.md` for the broader context.

## Related

- `signal-lojix` — typed wire contract (DeploymentSubmission/Accepted/
  Rejected/Observation, CacheRetentionRequest/Accepted/Rejected/
  Observation, GenerationQuery/Listing).
- `signal-core` — wire kernel that signal-lojix builds on.
- `sema-engine` — typed database engine library used for durable state.
- `horizon-rs` — cluster-proposal projection (read-only per request).
- `lojix-cli` — legacy monolithic orchestrator; parallel build until
  CriomOS migrates over.
- `goldragon` — cluster proposal source.
- `clavifaber` — per-host key material (separate component).

## License

License of Non-Authority.
