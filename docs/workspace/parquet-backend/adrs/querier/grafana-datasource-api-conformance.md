---
status: draft
---
# Grafana datasource API conformance — what contract Sol targets

Addresses: [NFR2](../../DESIGN.md#nfr2), [FR1](../../DESIGN.md#fr1), [FR2](../../DESIGN.md#fr2), [FR3](../../DESIGN.md#fr3)

## Problem

Sol serves the Prometheus, Tempo, and Loki HTTP APIs so Grafana's stock
datasources render against it unchanged ([NFR2](../../DESIGN.md#nfr2)). To do that
correctly we need an authoritative definition of each response shape. Which
spec do we build (and test) against? The prose API docs are incomplete: real
Grafana failures (`Cannot read properties of undefined (reading '0')`,
`LogQL must start with a {...} stream selector`, broken trace waterfalls) come
from fields the *datasource frontend* reads that the docs never mention.

## Options

| Option | Pros | Cons |
|---|---|---|
| A. Prose HTTP-API docs (grafana.com) | Readable, official | Incomplete — omits exactly the fields that break the UI (`spanSets`, OTLP span JSON, volume matrix) |
| B. A single OpenAPI for all three | One machine-readable contract | **Does not exist.** Only Mimir ships OpenAPI; Tempo/Loki do not |
| C. The real backends' responses + the Grafana datasource source | Exact, executable, version-accurate | No single artifact; must be assembled; shifts with Grafana versions |

## Decision

**Adopt Option C — target the de-facto datasource contract, validated empirically.**
There is no single OpenAPI for the *datasource* contract. The authoritative
sources, per backend:

- **Mimir / Prometheus** — Mimir serves **OpenAPI 3.x at `/api/v1/openapi.yaml`**;
  its query API is 100% Prometheus-HTTP-API compatible under `/prometheus/*`.
- **Tempo** — the **`tempopb` protobuf** (`pkg/tempopb/tempo.proto`:
  `SearchResponse`, `TraceSearchMetadata`, …) is the source of truth; the JSON is
  "mostly-compatible OTLP JSON". (Tempo itself documents HTTP↔proto
  inconsistencies — issues #1910, #3802.)
- **Loki** — prose [HTTP API reference] only; shapes defined by the Loki source.
- **All three** — the operative contract is the **Grafana datasource code** in
  `grafana/grafana`: the Go backend `pkg/tsdb/<ds>/` (builds requests, parses
  responses into data frames) and the frontend `public/app/plugins/datasource/<ds>/`
  (query editors + transformers). This is what *actually* parses Sol's JSON.

**Validation is empirical, not static:** the demo runs the real Mimir/Tempo/Loki
beside Sol, so conformance is checked by **issuing the same request to both and
structurally diffing the JSON** (the real backend's shape is what Grafana
accepts). See [CONFORMANCE.md](../../CONFORMANCE.md) for the per-endpoint pass.

### Shape decisions the prose docs don't surface (the gotchas)

- **Tempo search** — emit **both** the deprecated singular `spanSet` *and* the
  current plural `spanSets` (Grafana 13 reads `trace.spanSets[0]`; its absence
  crashes the Search view).
- **Tempo trace-by-id** — serialize spans as **OTLP proto-JSON**: `traceId`/
  `spanId`/`parentSpanId` **base64**, `attributes` as a **KeyValue array**
  (`[{key,value:{stringValue|intValue…}}]`), `kind`/`status.code` as enum
  strings, `scope.name` set. Flat-object attributes / hex ids break the
  waterfall. Trace ids are **zero-padded to 32 hex** (Tempo strips leading
  zeros, so search hits can be shorter) rather than rejected.
- **Loki label matchers** — regex `=~`/`!~` are **anchored** (`^(?:RE)$`);
  DataFusion `regexp_like` is substring. (Loki `|~` *line* filters stay
  unanchored.)
- **Loki log volume** — the "Logs volume" panel issues a **metric** LogQL query
  (`sum by (level)(count_over_time({…}[range]))`) to `query_range`; respond with
  a Prometheus **`matrix`** (one series per `detected_level`), not `streams`.
- **Loki streams** — attach `detected_level` (from OTLP `severity_number`), the
  label Grafana uses to colour log lines.
- **Prometheus** — match Mimir's OTLP→Prometheus normalization: dotted→`_`
  names, unit/`_total`/`_bucket`/`_count`/`_sum` suffixes, absent label = empty,
  step-aligned range samples. (Some of these are still open — see CONFORMANCE.)

## Consequences

- Conformance is a **living** property: a Grafana upgrade can change which JSON
  fields the frontend reads, so the paired-diff pass must be **re-run on Grafana
  / backend version bumps**, not assumed from a frozen spec.
- Gaps are tracked in [CONFORMANCE.md](../../CONFORMANCE.md) and the
  [code-review backlog](../../CODE-REVIEW-BACKLOG.md) (`C-P*`/`C-T*`/`C-L*`).
- Sol need not implement the *entire* upstream API — only the endpoints + fields
  the Grafana datasources actually call/read. The demo's side-by-side
  `$datasource` toggle (Sol ↔ real backend) is the standing regression harness.
- Where Tempo's own HTTP/proto shapes are internally inconsistent, Sol follows
  what the **Grafana frontend parses** (e.g. `spanSets`), not the literal docs.

[HTTP API reference]: https://grafana.com/docs/loki/latest/reference/loki-http-api/
