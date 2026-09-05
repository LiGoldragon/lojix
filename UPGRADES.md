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
