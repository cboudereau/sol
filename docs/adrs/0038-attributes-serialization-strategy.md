---
status: accepted
---
# Attributes serialization strategy for Parquet

## Problem

OTLP attributes are `repeated KeyValue` where values are `AnyValue` — a recursive union type (string, bool, int, double, bytes, array of AnyValue, kvlist of AnyValue). Parquet requires a fixed schema at write time.

How should attributes be represented in Parquet columns?

## Options

| Option | Pros | Cons |
|---|---|---|
| A. JSON string column | Lossless, schema-stable, simple implementation, universally parseable | No predicate pushdown on individual attributes, larger column size |
| B. Flattened top-level columns (`attr_service_name`, `attr_http_method`, ...) | Predicate pushdown, smaller per-column size, natural SQL access | Schema instability (new attributes = schema change), unbounded column count, breaks for dynamic attributes |
| C. MAP column type (`MAP<UTF8, UTF8>`) | Semi-structured, some engines support map pushdown | Limited engine support (Athena doesn't pushdown MAPs well), still loses type info for non-string values |

## Decision

**Option A — JSON string column.**

Rationale:
- **Schema stability**: the Parquet schema is fixed regardless of which attributes are present. This is critical for a pipeline — attribute sets vary per service, per deployment, per version.
- **Lossless**: the recursive AnyValue structure (including nested arrays and kvlists) is preserved exactly. JSON roundtrips perfectly.
- **Queryable**: all major engines support JSON extraction functions (`json_extract`, `json_extract_scalar` in Athena/Presto, `json_extract_string` in DuckDB, `JSON_VALUE` in Spark). Example: `SELECT json_extract_scalar(attributes, '$.service.name') FROM logs`.
- **Consistent with ADR 0009**: Sol's non-OTLP codec strategy (ADR 0009) already uses OTLP/JSON serialization for structured data. Parquet attributes follow the same pattern.

Option B is rejected because observability attributes are inherently dynamic — a fixed column set per attribute would require schema migration on every deployment change, and the column count would grow unboundedly.

Option C is rejected because MAP support is inconsistent across engines, and nested AnyValue (array, kvlist) cannot be represented in a `MAP<UTF8, UTF8>` without JSON-serializing the values anyway.

## Consequences

- `attributes`, `resource_attributes`, and `scope_attributes` columns are `BYTE_ARRAY (UTF8)` containing JSON.
- `body` (which is also `AnyValue`) follows the same pattern: JSON-serialized UTF8 string.
- Predicate pushdown on individual attribute keys requires JSON functions in the query engine. This is a tradeoff: schema stability over pushdown performance.
- Future optimization: a follow-up workspace could add optional "promoted columns" — user-configured attributes promoted to top-level Parquet columns for pushdown. This is explicitly out of scope for this iteration.
