#!/bin/bash
set -eu

# Query Parquet files written by Sol's gateway.
# Usage: ./parquet-query.sh
#
# Requires the demo stack to be running (./up.sh) with the parquet profile:
#   docker compose --profile parquet up
#
# Wait ~30s for the first batch to flush, then run this script.

echo "=== Parquet files ==="
docker compose exec duckdb sh -c 'find /data/parquet -name "*.parquet" 2>/dev/null | sort'

echo ""
echo "=== Log schema ==="
docker compose exec duckdb duckdb -c \
  "DESCRIBE SELECT * FROM read_parquet('/data/parquet/logs/*.parquet');" 2>/dev/null || echo "(no log files yet)"

echo ""
echo "=== Trace schema ==="
docker compose exec duckdb duckdb -c \
  "DESCRIBE SELECT * FROM read_parquet('/data/parquet/traces/*.parquet');" 2>/dev/null || echo "(no trace files yet)"

echo ""
echo "=== Metric schema ==="
docker compose exec duckdb duckdb -c \
  "DESCRIBE SELECT * FROM read_parquet('/data/parquet/metrics/*.parquet', union_by_name=true);" 2>/dev/null || echo "(no metric files yet)"

echo ""
echo "=== Logs: last 10 entries ==="
docker compose exec duckdb duckdb -c "
  SELECT service_name, severity_text, body
  FROM read_parquet('/data/parquet/logs/*.parquet')
  ORDER BY time_unix_nano DESC
  LIMIT 10;
" 2>/dev/null || echo "(no log files yet)"

echo ""
echo "=== Traces: top 10 slowest spans ==="
docker compose exec duckdb duckdb -c "
  SELECT service_name, name, duration_nanos / 1e6 AS duration_ms, status_code
  FROM read_parquet('/data/parquet/traces/*.parquet')
  ORDER BY duration_nanos DESC
  LIMIT 10;
" 2>/dev/null || echo "(no trace files yet)"

echo ""
echo "=== Traces: span count by service ==="
docker compose exec duckdb duckdb -c "
  SELECT service_name, COUNT(*) AS span_count
  FROM read_parquet('/data/parquet/traces/*.parquet')
  GROUP BY service_name
  ORDER BY span_count DESC;
" 2>/dev/null || echo "(no trace files yet)"

echo ""
echo "=== Metrics: distinct metric names ==="
docker compose exec duckdb duckdb -c "
  SELECT DISTINCT name
  FROM read_parquet('/data/parquet/metrics/*.parquet', union_by_name=true)
  ORDER BY name
  LIMIT 20;
" 2>/dev/null || echo "(no metric files yet)"
