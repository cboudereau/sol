# demo-grafana-update — Design Doc

## Context

The `demo/otel-sol-grafana-dotnet` demo stack uses Grafana ecosystem components (Grafana, Loki, Mimir, Tempo) that are significantly outdated. The demo is used for local development, presentations, and showcasing Sol as an OpenTelemetry collector replacement.

Current versions vs latest stable:

| Component | Current | Latest Stable | Gap |
|---|---|---|---|
| `grafana/grafana-oss` | 10.3.3 | 13.0.1 | 3 major versions |
| `grafana/mimir` | 2.11.0 | 3.0.6 | 1 major version |
| `grafana/tempo` | 2.3.1 | 2.10.5 | 7 minor versions |
| `grafana/loki` | sha256 digest (main) | 3.7.1 | unstable -> stable |
| `postgres` | 16.2-alpine3.19 | 16.13-alpine3.23 | patch-level |
| `curlimages/curl` | 8.5.0 / 8.6.0 | 8.20.0 | minor |

The `demo/otel-drop-in` compose also uses `curlimages/curl:8.5.0`.

## Functional Requirements

### <a id="fr1"></a>FR1 — Update Grafana OSS image
Bump `grafana/grafana-oss` from `10.3.3` to latest stable. Update `grafana.ini` if feature toggles are no longer needed or have changed.

### <a id="fr2"></a>FR2 — Update Loki image
Replace the sha256 digest reference with a stable tagged version of `grafana/loki`. Update `loki/local-config.yaml` for any breaking schema/config changes (BoltDB -> TSDB migration, v11 -> v13 schema).

### <a id="fr3"></a>FR3 — Update Mimir image
Bump `grafana/mimir` from `2.11.0` to latest stable. Update `mimir/mimir.yaml` if config options have been removed or renamed.

### <a id="fr4"></a>FR4 — Update Tempo image
Bump `grafana/tempo` from `2.3.1` to latest stable. Update `tempo/tempo-local.yaml` if config has changed (vParquet storage format).

### <a id="fr5"></a>FR5 — Update Grafana datasource provisioning
Review and update datasource YAML files in `grafana/provisioning/datasources/` for any API changes in the new versions (e.g. Tempo datasource config changes).

### <a id="fr6"></a>FR6 — Update utility images
Bump `postgres` from `16.2-alpine3.19` to `16.13-alpine3.23` (same major, patch-only). Bump `curlimages/curl` to a consistent latest version across both demos.

## Non-Functional Requirements

### <a id="nfr1"></a>NFR1 — Demo must remain functional
After all updates, `./up.sh` must bring the stack up successfully. All services must start and Grafana must be accessible at `http://localhost:3000` with datasources connected.

### <a id="nfr2"></a>NFR2 — No Sol config changes
Sol configurations (`sol/*.yaml`) and `Dockerfile.sol` must remain unchanged — this is a Grafana ecosystem update only.

### <a id="nfr3"></a>NFR3 — Minimal config changes
Only change configs when required by breaking changes in new versions. Do not refactor or restructure configs beyond what is needed.

## Non-goals
- Upgrading PostgreSQL to a new major version (17.x) — would require `pg_upgrade` migration, unnecessary for a demo.
- Updating dotnet application code or dependencies.
- Updating Grafana dashboard JSON files — they will be checked for compatibility but not redesigned.
- Upgrading Sol itself or its Dockerfile.
- Production-readiness improvements (security, scaling, HA).

## Rabbit holes
- **Loki schema migration**: Loki 3.x defaults to TSDB/v13 schema. Since this is a demo with ephemeral data (`docker compose down -v` wipes everything), we can simply adopt the new defaults without worrying about migration of existing data.
- **Mimir query engine**: Mimir 3.x uses MQE by default. For a demo, the new default should work — do not spend time benchmarking or comparing engines.
- **Grafana feature toggles**: `tempoSearch` and `tempoBackendSearch` may be GA in Grafana 13.x. Check if `grafana.ini` needs cleanup, but don't audit every feature flag.
- **Loki OTLP empty attribute key**: Loki 3.7 strictly validates OTLP attribute keys via `otlptranslator.LabelNamer`. The dotnet OTel SDK sends a resource attribute with an empty key (`""`), causing `"symbolizer lookup: label name is empty"` rejection. Fixed by adding `limits_config.otlp_config` with an explicit allowlist and drop rules for empty keys. This also led to the discovery that the OTLP sink retries 4xx errors forever — tracked separately in [otlp-sink-error-classification](20260505_otlp-sink-error-classification.md).

## Design

This is a straightforward version bump with config adjustments. The approach:

1. Update image tags in `compose.yml` files
2. Update each component's config file for breaking changes
3. Validate by reviewing changelogs/docs for config compatibility

Decisions:
- [ADR-0016: Loki schema version](../adrs/0016-loki-schema-version.md)

## Cross-cutting Concerns
- **Rollback**: Git revert of the commit restores all previous versions.
- **Validation**: Manual `docker compose up` test after changes.
