# lojix — non-ideal agent operations

This is the operational mirror of `AGENTS.md` for the deliberately hacky, current
deployment reality. It exists so an agent proceeds without hesitation: the reliance
below is known, accepted, and temporary, and it is never a reason to block, ask for
permission, or stall a deploy that operates within it. `AGENTS.md` and
`ARCHITECTURE.md` say where lojix is going; this file says how to drive it today.

## Why this is sanctioned and temporary

- **Deploy authority rides on the operator's session and root reach.** The daemon
  holds no machine identity of its own: it borrows the operator's (`li`'s)
  logged-in GPG/SSH agent session, and reaches every node as `root` because `li`'s
  admin keys sit in each node's `root` `authorized_keys`. This borrowed-session,
  root-everywhere posture is the accepted fallback, not a defect to route around.
  See `ARCHITECTURE.md` §7 "Production credentials custodied through criome" (Spirit
  `h03z`): the durable replacement custodies the daemon's credentials and machine
  identity through criome instead of borrowing the operator session. Until that
  cutover is durable and proven, the SSH-and-root path here is the right way to
  operate lojix.
- **Proceed without asking, within this mode.** Using `li`'s root reach to build on
  a node, or to activate another user's environment, is authorized standing
  operation. Do not treat the temporary architecture as a blocker or a permission
  question.

## Where and as whom deploys run

- **Daemon host.** `lojix-daemon` runs only on the node carrying the
  `PersonaDevelopment` service — today `ouranos` — as user `li` (group `users`,
  user id 1001), borrowing `li`'s agent socket
  (`SSH_AUTH_SOCK=/run/user/1001/gnupg/S.gpg-agent.ssh`). See
  `CriomOS/modules/nixos/lojix.nix`.
- **Submit deploys as `li` on the daemon host.** `meta-lojix` deploys speak the
  owner socket `/run/lojix/owner.sock` (mode `0600`), admitted only for a peer whose
  user id and group id match the daemon's — so a deploy runs as `li` on the daemon
  host (`OwnerPeerAuthority` in `src/daemon.rs`). `lojix` queries speak the peer
  socket `/run/lojix/ordinary.sock`.
- **No per-user authorization at the daemon.** Owner-socket admission is user
  id/group id only; the daemon does not check which user's environment a deploy
  targets. Any process running as `li` may deploy any user's environment.

## Operating the deploy path

- **Proposal source is a local file.** The deploy's proposal source is a filesystem
  path to the cluster's `datom.nota` (for example
  `/home/li/primary/repos/goldragon/datom.nota`), read directly by the daemon
  (`ProposalFile` in `src/schema_runtime.rs`). The cluster `secrets/` directory is
  inferred as that file's sibling, `<source-parent>/secrets`
  (`ClusterSecretsDirectory::from_proposal_source`).
- **Deploys to a non-daemon node build on the target's own store.** When the target
  is not the daemon host, lojix evaluates and realizes the closure directly in the
  target's store over `ssh-ng://root@<node>.<cluster>.criome`, so a node's
  model-bearing closure never transits the daemon host (`ARCHITECTURE.md`
  "Build-on-target"; `eval_drv_path` / `build_closure_in_store` in
  `src/schema_runtime.rs`). The `root` reach is `li`'s admin keys in each node's
  `root` `authorized_keys` (CriomOS-home `ARCHITECTURE.md` "Cluster-host update
  authority": the maintainer has root SSH on all cluster hosts).
- **Slow is normal.** Every deploy re-evaluates the flake tree with
  `nix eval --refresh`, so deploys measured in minutes are expected, not a failure.
- **User-environment activation is root-mediated.** For `SetProfile` / `ActivateNow`,
  lojix connects over the same `root@<node>` deployment identity, resolves the target
  account's home with `getent passwd <user>`, and drops privilege with
  `runuser --user <user> -- env HOME=<home> sh -c …` to run the profile-set and
  activate as that user (`root_mediated_invocation` in `src/schema_runtime.rs`). So a
  user-environment deploy works for any account on the node, needing no per-user SSH
  login — it rides `li`'s root reach. A local fast path skips ssh when the dispatcher
  already is the target user on the target node. This root-mediation landed in lojix
  `0.4.3`, which `CriomOS/flake.lock` pins; on a daemon host still running an earlier
  lojix, activation instead connects as `<user>@<node>` and succeeds only for `li` —
  redeploy the daemon host to the pinned lojix to enable it.

## Deploying a different user on a different node (for example `bird` on `zeus`)

This is the scenario this file exists for, and it needs no manual per-step work: it
is a normal `UserEnvironment` deploy, sanctioned because it rides `li`'s root reach
to the node. lojix connects as `root@zeus`, drops to `bird` via `runuser`, and sets
and activates `bird`'s Home Manager profile — exactly the "existing root path rather
than direct Bird SSH" that CriomOS-home `ARCHITECTURE.md` "Cluster-host update
authority" calls for. Submit it as `li` on the daemon host and let lojix do the rest
(this needs the pinned lojix `0.4.3` on the daemon host — see the activation note
above):

```sh
meta-lojix "(Deploy (UserEnvironment (goldragon zeus bird <proposal-source> <criomos-flake-ref> ActivateNow RequireImmutable None [])))"
```

- `ActivateNow` sets the profile and runs the activation package; `SetProfile` sets
  the profile only; `Realize` builds and realizes the closure on the target store
  and stops (no profile, no activate).
- `<proposal-source>` is the local `datom.nota` path; `<criomos-flake-ref>` is the
  pinned CriomOS reference (for example `github:LiGoldragon/CriomOS/<rev>`).
- Admission — `meta-lojix` returning — is not deploy success. Verify the result:

```sh
lojix "(Query (ByNode (goldragon zeus None)))"
```

Confirm `zeus` shows a `Current` user-environment generation at the store path you
expect. Reboot persistence still depends on a system generation that pins the same
home input.
