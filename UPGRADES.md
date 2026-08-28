# Upgrades

## Horizon 0.4.0 node-service removal

Lojix now pins Horizon 0.4.0, whose schema removes the retired Agent
Intercom node-service variants. Update the Horizon producer and proposal data
before starting this Lojix revision. Lojix materialization preserves the
declared service vectors and provides no compatibility translation.
