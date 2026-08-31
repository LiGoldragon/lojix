# Upgrades

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
