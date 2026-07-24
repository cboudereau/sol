---
status: accepted
---
# Loki schema version for demo

Addresses: [FR2](../designs/20260505_demo-grafana-update.md#fr2), [NFR1](../designs/20260505_demo-grafana-update.md#nfr1)

## Problem
Loki 3.x defaults to TSDB with v13 schema. The current config uses BoltDB-shipper with v11 schema, which is deprecated and will be removed.

Since the demo uses ephemeral storage (wiped on each `docker compose down -v`), we don't need to worry about migrating existing data.

## Options
| Option | Pros | Cons |
|---|---|---|
| Adopt v13 schema + TSDB | Aligns with Loki 3.x defaults, future-proof, simpler config | Requires config rewrite |
| Keep v11 + BoltDB-shipper | No config change | Deprecated, may break in future Loki releases |

## Decision
Adopt v13 schema with TSDB. The demo has no persistent data to migrate, so we can safely switch to the new defaults.

## Consequences
- The `schema_config` section in `loki/local-config.yaml` will be rewritten to use TSDB + v13.
- The `common.storage` section will be updated for TSDB paths.
