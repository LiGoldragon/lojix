# Upgrades

## 0.21.0 — generated Datom and materialized Horizon definition

Lojix 0.21.0 replaces the retired text/DOTOS ingress and legacy schema roots
with generated current Datom contracts. Ordinary and owner requests now travel
only in bounded binary Signal frames; the private inspect, reset, bootstrap,
and configuration-writer commands accept their generated typed Datom roots.

For Horizon deployment, provide the externally composed public
`horizon-definition.datom` rather than a `ClusterProposal` document. The
definition must contain the selected cluster node and is projected by Lojix;
the public artifact contains no secret values or secret paths. Every deploy
also supplies `SecretsInput`: use `NoSecrets` for no secret authority, or an
existing absolute non-symlink `SecretsDirectory` owned by the caller. Lojix no
longer derives a sibling secrets directory from the public artifact.


### Durable-store cutover

The generated `SecretsInput` is persisted in each in-flight `DeployJob`, so
0.21.0 opens a v5 store and deliberately refuses a v4 store before any job is
decoded or resumed. It does not migrate historical deploy jobs, event history,
or secret authority. In particular, it never substitutes `NoSecrets` for a
v4 job and never resumes that job under changed meaning.

For a non-destructive cutover, stop `lojix-daemon`; retain its v4 primary store
at its existing configured absolute path; then generate the next daemon startup
archive with a distinct, new absolute `store_path` (for example, a `.v5`
sibling). Start the daemon only with that new archive. The fresh path is
initialized as v5 while the v4 store remains unchanged and can be examined
read-only with `lojix-inspect-store 'InspectStore.{ <absolute-v4-store-path> }'`.
Do not copy a v4 database into the new v5 path and do not point the v5 daemon
at the old path.

`lojix-reset-store` remains a separate, explicit discard option for a stopped
daemon: it may replace a recognized v2/v3/v4 Lojix store with an empty v5
store. That action discards the old store's jobs and history; archive or retain
the original v4 path first if those records are needed for inspection.

## 0.20.1 — query lookup wire shape

Lojix 0.20.1 preserves the public one-field product shape of
`Query.ByDeployment` and `Query.ByGeneration` while decoding both selectors
into the runtime model. Querying an unknown identifier now completes with the
ordinary typed empty `Queried` reply instead of ending the client exchange at
the wire boundary.

## 0.20.0 — canonical ClusterProposal artifact

Lojix 0.20.0 accepts a Horizon proposal only from a direct regular
`proposal.datom` artifact, whose content is embodied through
`Text<ClusterProposal>`. It no longer accepts a legacy `.dotos` proposal
source. Deploy the matching `goldragon/proposal.datom` and the Horizon 0.5
dependency together, then submit the normal immutable Lojix deployment request
with that exact canonical source path. Observe the returned deployment through
the ordinary Lojix client until its terminal record is `Succeeded`; admission
does not establish the upgrade.
