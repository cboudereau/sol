#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# ── Defaults ─────────────────────────────────────────────────────
DEFAULT_DURATION=60
WARMUP=10
DRAIN=5
STATS_INTERVAL=5
SOL_IMAGE="${SOL_IMAGE:-superbeeeeeee/sol:latest}"
SOL_MAIN_IMAGE="${SOL_MAIN_IMAGE:-superbeeeeeee/sol:v0.2.0}"
export SOL_IMAGE SOL_MAIN_IMAGE

# ── CLI parsing ──────────────────────────────────────────────────
SCENARIO_FILTER=""
DURATION_OVERRIDE=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --scenario) SCENARIO_FILTER="$2"; shift 2 ;;
    --duration) DURATION_OVERRIDE="$2"; shift 2 ;;
    --help|-h)
      echo "Usage: $0 [--scenario NAME] [--duration SECS]"
      echo "  --scenario  Run only this scenario (default: all)"
      echo "  --duration  Override duration in seconds (default: per-scenario)"
      exit 0
      ;;
    *) echo "Unknown option: $1"; exit 1 ;;
  esac
done

# ── Results directory ────────────────────────────────────────────
RESULTS_DIR="$SCRIPT_DIR/results"
RAW_DIR="$RESULTS_DIR/raw"
rm -rf "$RESULTS_DIR"
mkdir -p "$RAW_DIR"

# ── System info ──────────────────────────────────────────────────
{
  echo "=== System Info ==="
  echo "Date: $(date -u '+%Y-%m-%d %H:%M:%S UTC')"
  uname -a
  echo "CPUs: $(nproc)"
  free -h
  echo ""
  docker version --format '{{.Server.Version}}' 2>/dev/null | xargs -I{} echo "Docker: {}"
  echo ""
  echo "=== Images ==="
  echo "Sol image: ${SOL_IMAGE}"
  echo "Sol (main): ${SOL_MAIN_IMAGE}"
  echo "Sol: $(docker run --rm "${SOL_IMAGE}" --version 2>&1 || echo 'N/A')"
  echo "Sol (main): $(docker run --rm "${SOL_MAIN_IMAGE}" --version 2>&1 || echo 'N/A')"
  echo "Vector: $(docker run --rm timberio/vector:latest-alpine --version 2>&1 || echo 'N/A')"
  echo "otelcol: otel/opentelemetry-collector-contrib:0.122.0"
} > "$RAW_DIR/system-info.txt" 2>&1

echo "System info saved to results/raw/system-info.txt"

# ── Helpers ──────────────────────────────────────────────────────

compose() {
  docker compose "$@"
}

compose_vec() {
  docker compose --profile vector "$@"
}

compose_lb() {
  docker compose --profile lb "$@"
}

wait_for_port() {
  local host="$1" port="$2" timeout="${3:-30}"
  local end=$((SECONDS + timeout))
  while ! nc -z "$host" "$port" 2>/dev/null; do
    if (( SECONDS >= end )); then
      echo "  ERROR: $host:$port not ready after ${timeout}s"
      return 1
    fi
    sleep 1
  done
}

start_docker_stats() {
  local csv_file="$1"; shift
  local containers=("$@")
  echo "timestamp,container,cpu_pct,mem_usage,mem_limit,mem_pct" > "$csv_file"
  (
    while true; do
      local ts
      ts="$(date -u '+%Y-%m-%dT%H:%M:%SZ')"
      for cname in "${containers[@]}"; do
        docker stats --no-stream --format "{{.Name}},{{.CPUPerc}},{{.MemUsage}},{{.MemPerc}}" "$cname" 2>/dev/null \
          | while IFS=, read -r name cpu mem_usage mem_pct; do
              local mem_used mem_limit
              mem_used="$(echo "$mem_usage" | awk -F'/' '{gsub(/[[:space:]]/, "", $1); print $1}')"
              mem_limit="$(echo "$mem_usage" | awk -F'/' '{gsub(/[[:space:]]/, "", $2); print $2}')"
              echo "$ts,$name,$cpu,$mem_used,$mem_limit,$mem_pct"
            done
      done >> "$csv_file"
      sleep "$STATS_INTERVAL"
    done
  ) &
  STATS_PID=$!
}

stop_docker_stats() {
  if [[ -n "${STATS_PID:-}" ]]; then
    kill "$STATS_PID" 2>/dev/null || true
    wait "$STATS_PID" 2>/dev/null || true
    STATS_PID=""
  fi
}

# Scrape Sol/Vector metrics (same format, different prefix)
scrape_events_total() {
  local endpoint="$1" prefix="$2" component_id="$3" output_filter="${4:-}"
  local pattern="component_id=\"${component_id}\""
  if [[ -n "$output_filter" ]]; then
    pattern="${pattern}.*output=\"${output_filter}\""
  fi
  curl -s "$endpoint" 2>/dev/null \
    | grep "^${prefix}_component_sent_events_total{" \
    | grep "$pattern" \
    | awk '{print $2}' \
    | head -1 || echo "0"
}

scrape_otelcol_total() {
  local endpoint="$1" metric="$2"
  curl -s "$endpoint" 2>/dev/null \
    | grep "^${metric}{" \
    | awk '{sum += $2} END {print sum+0}' || echo "0"
}

run_tgen() {
  local name="$1" endpoint="$2" signal="$3" protocol="$4" rate="$5" workers="$6" duration="$7" extra_args="${8:-}" compression="${9:-}"
  local http_flag=""
  if [[ "$protocol" == "http" ]]; then
    http_flag="--otlp-http"
  fi
  local env_flags=()
  if [[ -n "$compression" ]]; then
    env_flags+=(-e "OTEL_EXPORTER_OTLP_COMPRESSION=$compression")
  fi
  local args="--duration=${duration}s --rate=${rate} --workers=${workers} --otlp-insecure"
  docker compose --profile tools run --rm -d "${env_flags[@]}" --name "$name" telemetrygen \
    -c "telemetrygen $signal $args $http_flag $extra_args --otlp-endpoint=$endpoint" >/dev/null 2>&1
}

wait_tgen() {
  local duration="$1"
  local deadline=$((SECONDS + duration + 30))
  while docker ps --format '{{.Names}}' | grep -q "tgen-"; do
    if (( SECONDS >= deadline )); then
      echo "  WARNING: telemetrygen still running after deadline, stopping"
      docker ps --format '{{.Names}}' | grep "tgen-" | xargs -r docker rm -f 2>/dev/null || true
      break
    fi
    sleep 2
  done
}

peak_cpu() {
  local csv_file="$1" pattern="$2"
  awk -F, "/${pattern}/ {gsub(/%/,\"\",\$3); if(\$3+0>max) max=\$3+0} END{printf \"%.1f\", max}" "$csv_file" 2>/dev/null || echo "0"
}

peak_mem() {
  local csv_file="$1" pattern="$2"
  awk -F, "/${pattern}/ {print \$4}" "$csv_file" 2>/dev/null | sort -h | tail -1 || echo "0"
}

to_rate() {
  python3 -c "print(f'{${1:-0} / ${2}:.0f}')" 2>/dev/null || echo "0"
}

json_field() {
  python3 -c "import json; d=json.load(open('$1')); print(d${2})" 2>/dev/null || echo "N/A"
}

ratio_table() {
  local raw_dir="$1"; shift
  local json_files=("$@")
  python3 -c "
import json, re, sys

def mem_to_mb(s):
    m = re.match(r'([\d.]+)\s*(MiB|GiB|KiB|B)', str(s))
    if not m: return 0.0
    v, u = float(m.group(1)), m.group(2)
    if u == 'GiB': return v * 1024
    if u == 'KiB': return v / 1024
    if u == 'B': return v / (1024 * 1024)
    return v

def ratio(a, b):
    if b == 0: return 'N/A'
    return f'{a / b:.2f}x'

files = sys.argv[1:]
rows = []
for f in files:
    try:
        d = json.load(open(f))
    except Exception:
        continue
    sol = d.get('sol', {})
    otel = d.get('otelcontribcol', {})
    sr, otr = sol.get('throughput_rate', 0), otel.get('throughput_rate', 0)
    sc, otc = float(sol.get('peak_cpu_pct', '0')), float(otel.get('peak_cpu_pct', '0'))
    sm, otm = mem_to_mb(sol.get('peak_mem', '0')), mem_to_mb(otel.get('peak_mem', '0'))
    rows.append(f\"| {d['scenario']} | {ratio(sr, otr)} | {ratio(sc, otc)} | {ratio(sm, otm)} |\")

print('| Scenario | Rate (Sol/otel) | CPU (Sol/otel) | Mem (Sol/otel) |')
print('|----------|----------------|---------------|---------------|')
for r in rows:
    print(r)
" "${json_files[@]}"
}

# ── Noop scenario runner (3 systems: Sol + Vector + otelcol) ─────

run_noop_scenario() {
  local scenario="$1" signal="$2" protocol="$3" rate="$4" workers="$5" duration="$6" extra_tgen="${7:-}" compression="${8:-}"

  echo ""
  echo "═══════════════════════════════════════════════════════════"
  echo "  Scenario: $scenario"
  echo "  Config: noop | Signal: $signal | Protocol: $protocol"
  echo "  Rate: $rate/s | Workers: $workers | Duration: ${duration}s"
  [[ -n "$extra_tgen" ]] && echo "  Extra args: $extra_tgen"
  [[ -n "$compression" ]] && echo "  Compression: $compression"
  echo "═══════════════════════════════════════════════════════════"

  compose down --remove-orphans --timeout 5 2>/dev/null || true
  compose_vec down --remove-orphans --timeout 5 2>/dev/null || true

  echo "  Starting services..."
  SOL_CONFIG=noop.yaml OTELCOL_CONFIG=noop.yml VECTOR_CONFIG=noop.yaml \
    compose_vec up -d sol sol-main vector otelcontribcol prometheus

  echo "  Waiting for services..."
  wait_for_port localhost 4327 30
  wait_for_port localhost 4367 30
  wait_for_port localhost 4357 30
  wait_for_port localhost 4317 30
  echo "  Services ready."

  echo "  Warming up (${WARMUP}s)..."
  sleep "$WARMUP"

  local sol_ctr sol_main_ctr vec_ctr otelcol_ctr
  sol_ctr="$(docker compose --profile vector ps --format '{{.Name}}' sol)"
  sol_main_ctr="$(docker compose --profile vector ps --format '{{.Name}}' sol-main)"
  vec_ctr="$(docker compose --profile vector ps --format '{{.Name}}' vector)"
  otelcol_ctr="$(docker compose --profile vector ps --format '{{.Name}}' otelcontribcol)"

  start_docker_stats "$RAW_DIR/docker-stats-${scenario}.csv" "$sol_ctr" "$sol_main_ctr" "$vec_ctr" "$otelcol_ctr"

  local sol_ep sol_main_ep vec_ep otelcol_ep
  if [[ "$protocol" == "http" ]]; then
    sol_ep="sol:4318"; sol_main_ep="sol-main:4318"; vec_ep="vector:4318"; otelcol_ep="otelcontribcol:4318"
  else
    sol_ep="sol:4317"; sol_main_ep="sol-main:4317"; vec_ep="vector:4317"; otelcol_ep="otelcontribcol:4317"
  fi

  echo "  Starting telemetrygen..."
  run_tgen "tgen-sol" "$sol_ep" "$signal" "$protocol" "$rate" "$workers" "$duration" "$extra_tgen" "$compression"
  run_tgen "tgen-sol-main" "$sol_main_ep" "$signal" "$protocol" "$rate" "$workers" "$duration" "$extra_tgen" "$compression"
  run_tgen "tgen-vec" "$vec_ep" "$signal" "$protocol" "$rate" "$workers" "$duration" "$extra_tgen" "$compression"
  run_tgen "tgen-otelcol" "$otelcol_ep" "$signal" "$protocol" "$rate" "$workers" "$duration" "$extra_tgen" "$compression"
  wait_tgen "$duration"

  echo "  Draining (${DRAIN}s)..."
  sleep "$DRAIN"

  echo "  Collecting metrics..."
  local otelcol_metric_name
  case "$signal" in
    logs)    otelcol_metric_name="otelcol_receiver_accepted_log_records" ;;
    traces)  otelcol_metric_name="otelcol_receiver_accepted_spans" ;;
    metrics) otelcol_metric_name="otelcol_receiver_accepted_metric_points" ;;
  esac

  local sol_total sol_main_total vec_total otelcol_total
  sol_total="$(scrape_events_total "http://localhost:9598/metrics" "sol_sol" "otlp" "$signal")"
  sol_main_total="$(scrape_events_total "http://localhost:9601/metrics" "sol_sol" "otlp" "$signal")"
  vec_total="$(scrape_events_total "http://localhost:9600/metrics" "vector" "otlp" "$signal")"
  otelcol_total="$(scrape_otelcol_total "http://localhost:8888/metrics" "$otelcol_metric_name")"

  local sol_rate sol_main_rate vec_rate otelcol_rate
  sol_rate="$(to_rate "$sol_total" "$duration")"
  sol_main_rate="$(to_rate "$sol_main_total" "$duration")"
  vec_rate="$(to_rate "$vec_total" "$duration")"
  otelcol_rate="$(to_rate "$otelcol_total" "$duration")"

  local csv="$RAW_DIR/docker-stats-${scenario}.csv"
  local sol_cpu sol_main_cpu vec_cpu otelcol_cpu sol_mem sol_main_mem vec_mem otelcol_mem
  sol_cpu="$(peak_cpu "$csv" "-sol-[0-9]")"
  sol_main_cpu="$(peak_cpu "$csv" "-sol-main-[0-9]")"
  vec_cpu="$(peak_cpu "$csv" "-vector-[0-9]")"
  otelcol_cpu="$(peak_cpu "$csv" "-otelcontribcol-")"
  sol_mem="$(peak_mem "$csv" "-sol-[0-9]")"
  sol_main_mem="$(peak_mem "$csv" "-sol-main-[0-9]")"
  vec_mem="$(peak_mem "$csv" "-vector-[0-9]")"
  otelcol_mem="$(peak_mem "$csv" "-otelcontribcol-")"

  cat > "$RAW_DIR/${scenario}.json" <<ENDJSON
{
  "scenario": "$scenario",
  "sol": { "total_events": $sol_total, "throughput_rate": $sol_rate, "peak_cpu_pct": "$sol_cpu", "peak_mem": "$sol_mem" },
  "sol_main": { "total_events": $sol_main_total, "throughput_rate": $sol_main_rate, "peak_cpu_pct": "$sol_main_cpu", "peak_mem": "$sol_main_mem" },
  "vector": { "total_events": $vec_total, "throughput_rate": $vec_rate, "peak_cpu_pct": "$vec_cpu", "peak_mem": "$vec_mem" },
  "otelcontribcol": { "total_events": $otelcol_total, "throughput_rate": $otelcol_rate, "peak_cpu_pct": "$otelcol_cpu", "peak_mem": "$otelcol_mem" }
}
ENDJSON

  echo "  Sol:      rate=${sol_rate}/s total=${sol_total} cpu=${sol_cpu}% mem=${sol_mem}"
  echo "  Sol main: rate=${sol_main_rate}/s total=${sol_main_total} cpu=${sol_main_cpu}% mem=${sol_main_mem}"
  echo "  Vector:   rate=${vec_rate}/s total=${vec_total} cpu=${vec_cpu}% mem=${vec_mem}"
  echo "  otelcol:  rate=${otelcol_rate}/s total=${otelcol_total} cpu=${otelcol_cpu}% mem=${otelcol_mem}"

  stop_docker_stats
  compose_vec down --timeout 5 2>/dev/null || true
  echo "  ✓ Scenario $scenario complete"
}

# ── Tail-sampling scenario runner (2 systems: Sol + otelcol) ─────

run_tail_sampling_scenario() {
  local scenario="$1" rate="$2" workers="$3" duration="$4" extra_tgen="${5:-}" compression="${6:-}"

  echo ""
  echo "═══════════════════════════════════════════════════════════"
  echo "  Scenario: $scenario"
  echo "  Config: tail-sampling | Rate: $rate/s | Duration: ${duration}s"
  [[ -n "$extra_tgen" ]] && echo "  Extra args: $extra_tgen"
  [[ -n "$compression" ]] && echo "  Compression: $compression"
  echo "═══════════════════════════════════════════════════════════"

  compose down --remove-orphans --timeout 5 2>/dev/null || true

  echo "  Starting services..."
  SOL_CONFIG=tail-sampling.yaml OTELCOL_CONFIG=tail-sampling.yml \
    compose up -d sol sol-main otelcontribcol prometheus

  echo "  Waiting for services..."
  wait_for_port localhost 4327 30
  wait_for_port localhost 4367 30
  wait_for_port localhost 4317 30
  echo "  Services ready."

  echo "  Warming up (${WARMUP}s)..."
  sleep "$WARMUP"

  local sol_ctr sol_main_ctr otelcol_ctr
  sol_ctr="$(docker compose ps --format '{{.Name}}' sol)"
  sol_main_ctr="$(docker compose ps --format '{{.Name}}' sol-main)"
  otelcol_ctr="$(docker compose ps --format '{{.Name}}' otelcontribcol)"
  start_docker_stats "$RAW_DIR/docker-stats-${scenario}.csv" "$sol_ctr" "$sol_main_ctr" "$otelcol_ctr"

  echo "  Starting telemetrygen..."
  run_tgen "tgen-sol" "sol:4317" "traces" "grpc" "$rate" "$workers" "$duration" "$extra_tgen" "$compression"
  run_tgen "tgen-sol-main" "sol-main:4317" "traces" "grpc" "$rate" "$workers" "$duration" "$extra_tgen" "$compression"
  run_tgen "tgen-otelcol" "otelcontribcol:4317" "traces" "grpc" "$rate" "$workers" "$duration" "$extra_tgen" "$compression"
  wait_tgen "$duration"

  echo "  Draining (${DRAIN}s)..."
  sleep "$DRAIN"

  echo "  Collecting metrics..."
  local sol_total sol_main_total otelcol_total
  sol_total="$(scrape_events_total "http://localhost:9598/metrics" "sol_sol" "otlp" "traces")"
  sol_main_total="$(scrape_events_total "http://localhost:9601/metrics" "sol_sol" "otlp" "traces")"
  otelcol_total="$(scrape_otelcol_total "http://localhost:8888/metrics" "otelcol_receiver_accepted_spans")"

  local sol_rate sol_main_rate otelcol_rate
  sol_rate="$(to_rate "$sol_total" "$duration")"
  sol_main_rate="$(to_rate "$sol_main_total" "$duration")"
  otelcol_rate="$(to_rate "$otelcol_total" "$duration")"

  local csv="$RAW_DIR/docker-stats-${scenario}.csv"
  local sol_cpu sol_main_cpu otelcol_cpu sol_mem sol_main_mem otelcol_mem
  sol_cpu="$(peak_cpu "$csv" "-sol-[0-9]")"
  sol_main_cpu="$(peak_cpu "$csv" "-sol-main-[0-9]")"
  otelcol_cpu="$(peak_cpu "$csv" "-otelcontribcol-")"
  sol_mem="$(peak_mem "$csv" "-sol-[0-9]")"
  sol_main_mem="$(peak_mem "$csv" "-sol-main-[0-9]")"
  otelcol_mem="$(peak_mem "$csv" "-otelcontribcol-")"

  cat > "$RAW_DIR/${scenario}.json" <<ENDJSON
{
  "scenario": "$scenario",
  "sol": { "total_events": $sol_total, "throughput_rate": $sol_rate, "peak_cpu_pct": "$sol_cpu", "peak_mem": "$sol_mem" },
  "sol_main": { "total_events": $sol_main_total, "throughput_rate": $sol_main_rate, "peak_cpu_pct": "$sol_main_cpu", "peak_mem": "$sol_main_mem" },
  "otelcontribcol": { "total_events": $otelcol_total, "throughput_rate": $otelcol_rate, "peak_cpu_pct": "$otelcol_cpu", "peak_mem": "$otelcol_mem" }
}
ENDJSON

  echo "  Sol:      rate=${sol_rate}/s total=${sol_total} cpu=${sol_cpu}% mem=${sol_mem}"
  echo "  Sol main: rate=${sol_main_rate}/s total=${sol_main_total} cpu=${sol_main_cpu}% mem=${sol_main_mem}"
  echo "  otelcol:  rate=${otelcol_rate}/s total=${otelcol_total} cpu=${otelcol_cpu}% mem=${otelcol_mem}"

  stop_docker_stats
  compose down --timeout 5 2>/dev/null || true
  echo "  ✓ Scenario $scenario complete"
}

# ── LB scenario runner (2 systems: Sol + otelcol) ────────────────

run_lb_scenario() {
  local scenario="$1" rate="$2" workers="$3" duration="$4" compression="${5:-}"

  echo ""
  echo "═══════════════════════════════════════════════════════════"
  echo "  Scenario: $scenario (load-balanced)"
  echo "  Topology: LB + 2× collector per system"
  echo "  Rate: $rate/s | Workers: $workers | Duration: ${duration}s"
  [[ -n "$compression" ]] && echo "  Compression: $compression"
  echo "═══════════════════════════════════════════════════════════"

  compose down --remove-orphans --timeout 5 2>/dev/null || true
  compose_lb down --remove-orphans --timeout 5 2>/dev/null || true

  echo "  Starting LB services..."
  compose_lb up -d sol-lb sol-collector sol-main-lb sol-main-collector otelcol-lb otelcol-collector prometheus

  echo "  Waiting for LB services..."
  wait_for_port localhost 4337 30
  wait_for_port localhost 4377 30
  wait_for_port localhost 4347 30
  echo "  LB services ready."

  echo "  Warming up (${WARMUP}s)..."
  sleep "$WARMUP"

  local containers=()
  while IFS= read -r name; do
    containers+=("$name")
  done < <(compose_lb ps --format '{{.Name}}' | grep -E '(sol|otelcol)' | grep -v prometheus)

  start_docker_stats "$RAW_DIR/docker-stats-${scenario}.csv" "${containers[@]}"

  echo "  Starting telemetrygen..."
  run_tgen "tgen-sol-lb" "sol-lb:4317" "traces" "grpc" "$rate" "$workers" "$duration" "" "$compression"
  run_tgen "tgen-sol-main-lb" "sol-main-lb:4317" "traces" "grpc" "$rate" "$workers" "$duration" "" "$compression"
  run_tgen "tgen-otelcol-lb" "otelcol-lb:4317" "traces" "grpc" "$rate" "$workers" "$duration" "" "$compression"
  wait_tgen "$duration"

  echo "  Draining (${DRAIN}s)..."
  sleep "$DRAIN"

  echo "  Collecting LB metrics..."
  local sol_total sol_main_total otelcol_total
  sol_total="$(scrape_events_total "http://localhost:9599/metrics" "sol_sol" "otlp" "traces")"
  sol_main_total="$(scrape_events_total "http://localhost:9602/metrics" "sol_sol" "otlp" "traces")"
  otelcol_total="$(scrape_otelcol_total "http://localhost:8889/metrics" "otelcol_receiver_accepted_spans")"

  local sol_rate sol_main_rate otelcol_rate
  sol_rate="$(to_rate "$sol_total" "$duration")"
  sol_main_rate="$(to_rate "$sol_main_total" "$duration")"
  otelcol_rate="$(to_rate "$otelcol_total" "$duration")"

  local csv="$RAW_DIR/docker-stats-${scenario}.csv"
  local sol_cpu sol_main_cpu otelcol_cpu sol_mem sol_main_mem otelcol_mem

  sol_cpu="$(awk -F, '
    /-(sol-lb|sol-collector)-/ && !/sol-main/ { gsub(/%/,"",$3); cpu[$1]+=$3+0 }
    END { max=0; for(ts in cpu) if(cpu[ts]>max) max=cpu[ts]; printf "%.1f",max }
  ' "$csv" 2>/dev/null || echo "0")"

  sol_main_cpu="$(awk -F, '
    /-(sol-main-lb|sol-main-collector)-/ { gsub(/%/,"",$3); cpu[$1]+=$3+0 }
    END { max=0; for(ts in cpu) if(cpu[ts]>max) max=cpu[ts]; printf "%.1f",max }
  ' "$csv" 2>/dev/null || echo "0")"

  otelcol_cpu="$(awk -F, '
    /-(otelcol-lb|otelcol-collector)-/ { gsub(/%/,"",$3); cpu[$1]+=$3+0 }
    END { max=0; for(ts in cpu) if(cpu[ts]>max) max=cpu[ts]; printf "%.1f",max }
  ' "$csv" 2>/dev/null || echo "0")"

  sol_mem="$(peak_mem "$csv" "-(sol-lb|sol-collector)-[0-9]")"
  sol_main_mem="$(peak_mem "$csv" "-(sol-main-lb|sol-main-collector)-")"
  otelcol_mem="$(peak_mem "$csv" "-(otelcol-lb|otelcol-collector)-")"

  cat > "$RAW_DIR/${scenario}.json" <<ENDJSON
{
  "scenario": "$scenario", "topology": "lb + 2x collector",
  "sol": { "total_events": $sol_total, "throughput_rate": $sol_rate, "peak_cpu_pct": "$sol_cpu", "peak_mem": "$sol_mem" },
  "sol_main": { "total_events": $sol_main_total, "throughput_rate": $sol_main_rate, "peak_cpu_pct": "$sol_main_cpu", "peak_mem": "$sol_main_mem" },
  "otelcontribcol": { "total_events": $otelcol_total, "throughput_rate": $otelcol_rate, "peak_cpu_pct": "$otelcol_cpu", "peak_mem": "$otelcol_mem" }
}
ENDJSON

  echo "  Sol:      rate=${sol_rate}/s total=${sol_total} cpu=${sol_cpu}% mem=${sol_mem}"
  echo "  Sol main: rate=${sol_main_rate}/s total=${sol_main_total} cpu=${sol_main_cpu}% mem=${sol_main_mem}"
  echo "  otelcol:  rate=${otelcol_rate}/s total=${otelcol_total} cpu=${otelcol_cpu}% mem=${otelcol_mem}"

  stop_docker_stats
  compose_lb down --timeout 5 2>/dev/null || true
  echo "  ✓ Scenario $scenario complete"
}

# ── Scenario definitions ─────────────────────────────────────────
# Noop format:          id|signal|protocol|rate|workers|duration|extra_tgen_args|compression
# Tail-sampling format: id|rate|workers|duration|extra_tgen_args|compression
# LB format:            id|rate|workers|duration|compression

declare -a NOOP_SCENARIOS=(
  "noop-traces-grpc-10k|traces|grpc|10000|4|$DEFAULT_DURATION||"
  "noop-traces-grpc-10k-gzip|traces|grpc|10000|4|$DEFAULT_DURATION||gzip"
  "noop-traces-grpc-50k|traces|grpc|50000|8|$DEFAULT_DURATION||"
  "noop-traces-http-10k|traces|http|10000|4|$DEFAULT_DURATION||"
  "noop-logs-grpc-10k|logs|grpc|10000|4|$DEFAULT_DURATION||"
  "noop-logs-grpc-10k-gzip|logs|grpc|10000|4|$DEFAULT_DURATION||gzip"
  "noop-logs-grpc-50k|logs|grpc|50000|8|$DEFAULT_DURATION||"
  "noop-logs-grpc-10k-batch|logs|grpc|10000|4|$DEFAULT_DURATION|--batch --batch-size=500|"
  "noop-logs-grpc-50k-batch|logs|grpc|50000|8|$DEFAULT_DURATION|--batch --batch-size=500|"
  "noop-logs-http-10k|logs|http|10000|4|$DEFAULT_DURATION||"
  "noop-metrics-grpc-10k|metrics|grpc|10000|4|$DEFAULT_DURATION||"
  "noop-metrics-grpc-10k-gzip|metrics|grpc|10000|4|$DEFAULT_DURATION||gzip"
  "noop-metrics-grpc-50k|metrics|grpc|50000|8|$DEFAULT_DURATION||"
  "noop-metrics-grpc-10k-batch|metrics|grpc|10000|4|$DEFAULT_DURATION|--batch --batch-size=500|"
  "noop-metrics-grpc-50k-batch|metrics|grpc|50000|8|$DEFAULT_DURATION|--batch --batch-size=500|"
  "noop-metrics-http-10k|metrics|http|10000|4|$DEFAULT_DURATION||"
)

declare -a TS_SCENARIOS=(
  "tail-sampling-traces-grpc-10k|10000|4|$DEFAULT_DURATION||"
  "tail-sampling-traces-grpc-10k-gzip|10000|4|$DEFAULT_DURATION||gzip"
  "tail-sampling-traces-grpc-50k|50000|8|$DEFAULT_DURATION||"
)

declare -a LB_SCENARIOS=(
  "lb-tail-sampling-traces-grpc-10k|10000|4|$DEFAULT_DURATION|"
  "lb-tail-sampling-traces-grpc-10k-gzip|10000|4|$DEFAULT_DURATION|gzip"
  "lb-tail-sampling-traces-grpc-50k|50000|8|$DEFAULT_DURATION|"
)

declare -a SUSTAINED_SCENARIOS=(
  "sustained-noop-logs-grpc-10k|logs|grpc|10000|4|300|"
  "sustained-tail-sampling-traces-grpc-10k|10000|4|300|"
)

# ── Build telemetrygen image ─────────────────────────────────────
echo "Building telemetrygen image..."
compose build telemetrygen 2>&1 | tail -3

# ── Run scenarios ────────────────────────────────────────────────

for entry in "${NOOP_SCENARIOS[@]}"; do
  IFS='|' read -r id signal protocol rate workers duration extra compression <<< "$entry"
  if [[ -n "$SCENARIO_FILTER" && "$id" != "$SCENARIO_FILTER" ]]; then continue; fi
  if [[ -n "$DURATION_OVERRIDE" ]]; then duration="$DURATION_OVERRIDE"; fi
  run_noop_scenario "$id" "$signal" "$protocol" "$rate" "$workers" "$duration" "$extra" "$compression"
done

for entry in "${TS_SCENARIOS[@]}"; do
  IFS='|' read -r id rate workers duration extra compression <<< "$entry"
  if [[ -n "$SCENARIO_FILTER" && "$id" != "$SCENARIO_FILTER" ]]; then continue; fi
  if [[ -n "$DURATION_OVERRIDE" ]]; then duration="$DURATION_OVERRIDE"; fi
  run_tail_sampling_scenario "$id" "$rate" "$workers" "$duration" "$extra" "$compression"
done

for entry in "${LB_SCENARIOS[@]}"; do
  IFS='|' read -r id rate workers duration compression <<< "$entry"
  if [[ -n "$SCENARIO_FILTER" && "$id" != "$SCENARIO_FILTER" ]]; then continue; fi
  if [[ -n "$DURATION_OVERRIDE" ]]; then duration="$DURATION_OVERRIDE"; fi
  run_lb_scenario "$id" "$rate" "$workers" "$duration" "$compression"
done

# Sustained: noop
for entry in "${SUSTAINED_SCENARIOS[@]}"; do
  IFS='|' read -r id rest <<< "$entry"
  if [[ -n "$SCENARIO_FILTER" && "$id" != "$SCENARIO_FILTER" ]]; then continue; fi
  if [[ "$id" == sustained-noop-* ]]; then
    IFS='|' read -r id signal protocol rate workers duration extra compression <<< "$entry"
    if [[ -n "$DURATION_OVERRIDE" ]]; then duration="$DURATION_OVERRIDE"; fi
    run_noop_scenario "$id" "$signal" "$protocol" "$rate" "$workers" "$duration" "$extra" "$compression"
  elif [[ "$id" == sustained-tail-* ]]; then
    IFS='|' read -r id rate workers duration extra compression <<< "$entry"
    if [[ -n "$DURATION_OVERRIDE" ]]; then duration="$DURATION_OVERRIDE"; fi
    run_tail_sampling_scenario "$id" "$rate" "$workers" "$duration" "$extra" "$compression"
  fi
done

# ── Generate report ──────────────────────────────────────────────

echo ""
echo "Generating report..."

generate_report() {
  local report="$RESULTS_DIR/RESULTS.md"

  cat > "$report" <<'HEADER'
# Benchmark Results: Sol vs Vector vs otelcontribcol

HEADER

  echo '## System Info' >> "$report"
  echo '```' >> "$report"
  cat "$RAW_DIR/system-info.txt" >> "$report"
  echo '```' >> "$report"
  echo '' >> "$report"

  # Noop table (3 systems)
  echo '## Noop Pipeline (OTLP → null sink)' >> "$report"
  echo '' >> "$report"
  echo '> Traces are batched (many spans per gRPC call). Unbatched log/metric scenarios send 1 item per gRPC call (per-request overhead). Batched scenarios (`*-batch`) use `--batch-size=500` (realistic production batching).' >> "$report"
  echo '' >> "$report"
  echo '| Scenario | Sol rate | Sol (main) rate | Vector rate | otelcol rate | Sol CPU | Sol (main) CPU | Vector CPU | otelcol CPU | Sol Mem | Sol (main) Mem | Vector Mem | otelcol Mem |' >> "$report"
  echo '|----------|---------|----------------|------------|-------------|---------|---------------|-----------|------------|---------|---------------|-----------|------------|' >> "$report"

  for entry in "${NOOP_SCENARIOS[@]}"; do
    IFS='|' read -r id signal protocol rate workers duration extra <<< "$entry"
    local f="$RAW_DIR/${id}.json"
    [[ -f "$f" ]] || continue
    echo "| $id | $(json_field "$f" "['sol']['throughput_rate']")/s | $(json_field "$f" "['sol_main']['throughput_rate']")/s | $(json_field "$f" "['vector']['throughput_rate']")/s | $(json_field "$f" "['otelcontribcol']['throughput_rate']")/s | $(json_field "$f" "['sol']['peak_cpu_pct']")% | $(json_field "$f" "['sol_main']['peak_cpu_pct']")% | $(json_field "$f" "['vector']['peak_cpu_pct']")% | $(json_field "$f" "['otelcontribcol']['peak_cpu_pct']")% | $(json_field "$f" "['sol']['peak_mem']") | $(json_field "$f" "['sol_main']['peak_mem']") | $(json_field "$f" "['vector']['peak_mem']") | $(json_field "$f" "['otelcontribcol']['peak_mem']") |" >> "$report"
  done
  echo '' >> "$report"

  # Tail sampling table (2 systems)
  echo '## Tail Sampling Pipeline (Sol vs otelcol only — Vector has no tail_sampling)' >> "$report"
  echo '' >> "$report"
  echo '> Both systems use two sequential tail_sampling stages (Sol: two transforms, otelcol: two processors).' >> "$report"
  echo '' >> "$report"
  echo '| Scenario | Sol rate | Sol (main) rate | otelcol rate | Sol CPU | Sol (main) CPU | otelcol CPU | Sol Mem | Sol (main) Mem | otelcol Mem |' >> "$report"
  echo '|----------|---------|----------------|-------------|---------|---------------|------------|---------|---------------|------------|' >> "$report"

  for entry in "${TS_SCENARIOS[@]}"; do
    IFS='|' read -r id rate workers duration extra <<< "$entry"
    local f="$RAW_DIR/${id}.json"
    [[ -f "$f" ]] || continue
    echo "| $id | $(json_field "$f" "['sol']['throughput_rate']")/s | $(json_field "$f" "['sol_main']['throughput_rate']")/s | $(json_field "$f" "['otelcontribcol']['throughput_rate']")/s | $(json_field "$f" "['sol']['peak_cpu_pct']")% | $(json_field "$f" "['sol_main']['peak_cpu_pct']")% | $(json_field "$f" "['otelcontribcol']['peak_cpu_pct']")% | $(json_field "$f" "['sol']['peak_mem']") | $(json_field "$f" "['sol_main']['peak_mem']") | $(json_field "$f" "['otelcontribcol']['peak_mem']") |" >> "$report"
  done
  echo '' >> "$report"

  # LB table
  echo '## Load-Balanced Tail Sampling (LB + 2× collector)' >> "$report"
  echo '' >> "$report"
  echo '> 1 LB + 2 collectors per system (1 CPU / 1 GB each). Aggregated metrics.' >> "$report"
  echo '' >> "$report"
  echo '| Scenario | Sol rate | Sol (main) rate | otelcol rate | Sol CPU | Sol (main) CPU | otelcol CPU | Sol Mem | Sol (main) Mem | otelcol Mem |' >> "$report"
  echo '|----------|---------|----------------|-------------|---------|---------------|------------|---------|---------------|------------|' >> "$report"

  for entry in "${LB_SCENARIOS[@]}"; do
    IFS='|' read -r id rate workers duration <<< "$entry"
    local f="$RAW_DIR/${id}.json"
    [[ -f "$f" ]] || continue
    echo "| $id | $(json_field "$f" "['sol']['throughput_rate']")/s | $(json_field "$f" "['sol_main']['throughput_rate']")/s | $(json_field "$f" "['otelcontribcol']['throughput_rate']")/s | $(json_field "$f" "['sol']['peak_cpu_pct']")% | $(json_field "$f" "['sol_main']['peak_cpu_pct']")% | $(json_field "$f" "['otelcontribcol']['peak_cpu_pct']")% | $(json_field "$f" "['sol']['peak_mem']") | $(json_field "$f" "['sol_main']['peak_mem']") | $(json_field "$f" "['otelcontribcol']['peak_mem']") |" >> "$report"
  done
  echo '' >> "$report"

  # Sustained memory
  echo '## Sustained Memory (5-minute runs)' >> "$report"
  echo '' >> "$report"

  for entry in "${SUSTAINED_SCENARIOS[@]}"; do
    IFS='|' read -r id rest <<< "$entry"
    local csv="$RAW_DIR/docker-stats-${id}.csv"
    [[ -f "$csv" ]] || continue
    echo "### $id" >> "$report"
    if [[ "$id" == sustained-noop-* ]]; then
      echo '| System | Mem (start) | Mem (end) |' >> "$report"
      echo '|--------|-----------|---------|' >> "$report"
      echo "| Sol | $(awk -F, '/-sol-[0-9]/{print $4;exit}' "$csv") | $(awk -F, '/-sol-[0-9]/{l=$4}END{print l}' "$csv") |" >> "$report"
      echo "| Sol (main) | $(awk -F, '/-sol-main-[0-9]/{print $4;exit}' "$csv") | $(awk -F, '/-sol-main-[0-9]/{l=$4}END{print l}' "$csv") |" >> "$report"
      echo "| Vector | $(awk -F, '/-vector-[0-9]/{print $4;exit}' "$csv") | $(awk -F, '/-vector-[0-9]/{l=$4}END{print l}' "$csv") |" >> "$report"
      echo "| otelcol | $(awk -F, '/-otelcontribcol-/{print $4;exit}' "$csv") | $(awk -F, '/-otelcontribcol-/{l=$4}END{print l}' "$csv") |" >> "$report"
    else
      echo '| System | Mem (start) | Mem (end) |' >> "$report"
      echo '|--------|-----------|---------|' >> "$report"
      echo "| Sol | $(awk -F, '/-sol-[0-9]/{print $4;exit}' "$csv") | $(awk -F, '/-sol-[0-9]/{l=$4}END{print l}' "$csv") |" >> "$report"
      echo "| Sol (main) | $(awk -F, '/-sol-main-[0-9]/{print $4;exit}' "$csv") | $(awk -F, '/-sol-main-[0-9]/{l=$4}END{print l}' "$csv") |" >> "$report"
      echo "| otelcol | $(awk -F, '/-otelcontribcol-/{print $4;exit}' "$csv") | $(awk -F, '/-otelcontribcol-/{l=$4}END{print l}' "$csv") |" >> "$report"
    fi
    echo '' >> "$report"
  done

  # Ratio tables
  echo '## Sol / otelcol Ratios' >> "$report"
  echo '' >> "$report"
  echo '> Rate: >1x = Sol faster. CPU & Mem: <1x = Sol more efficient.' >> "$report"
  echo '' >> "$report"

  echo '### Noop' >> "$report"
  echo '' >> "$report"
  local noop_jsons=()
  for entry in "${NOOP_SCENARIOS[@]}"; do
    IFS='|' read -r id rest <<< "$entry"
    [[ -f "$RAW_DIR/${id}.json" ]] && noop_jsons+=("$RAW_DIR/${id}.json")
  done
  ratio_table "$RAW_DIR" "${noop_jsons[@]}" >> "$report"
  echo '' >> "$report"

  echo '### Tail Sampling' >> "$report"
  echo '' >> "$report"
  local ts_jsons=()
  for entry in "${TS_SCENARIOS[@]}"; do
    IFS='|' read -r id rest <<< "$entry"
    [[ -f "$RAW_DIR/${id}.json" ]] && ts_jsons+=("$RAW_DIR/${id}.json")
  done
  ratio_table "$RAW_DIR" "${ts_jsons[@]}" >> "$report"
  echo '' >> "$report"

  echo '### Load-Balanced Tail Sampling' >> "$report"
  echo '' >> "$report"
  local lb_jsons=()
  for entry in "${LB_SCENARIOS[@]}"; do
    IFS='|' read -r id rest <<< "$entry"
    [[ -f "$RAW_DIR/${id}.json" ]] && lb_jsons+=("$RAW_DIR/${id}.json")
  done
  ratio_table "$RAW_DIR" "${lb_jsons[@]}" >> "$report"
  echo '' >> "$report"

  # Methodology
  cat >> "$report" <<'METHODOLOGY'
## Methodology

### Systems under test
- **Sol**: Vector fork with OTLP-native core, tail_sampling, trace-aware load balancing (`SOL_IMAGE`)
- **Sol (main)**: Sol at `superbeeeeeee/sol:v0.2.0` (last release) — baseline for branch-vs-main regression detection under identical system load
- **Vector**: Upstream Vector (same codebase minus Sol additions) — baseline for regression detection
- **otelcol**: OpenTelemetry Collector Contrib — the reference Go implementation

### Batching
- `telemetrygen traces` batches by default: many spans per gRPC request — realistic production scenario
- Unbatched log/metric scenarios send 1 item per gRPC request — exposes per-request overhead
- Batched log/metric scenarios (`*-batch`) use `--batch --batch-size=500` — realistic production batching

### Fairness measures
- Identical resource limits (2 CPU / 2 GB per system) via Docker Compose
- Null sinks: Sol/Vector `blackhole`, otelcol `nop`
- Separate telemetrygen per system, identical config — each system gets the load it can handle
- CPU/memory from `docker stats` (identical measurement)
- Sol and Sol (main) run simultaneously so system-level variance cancels out in branch-vs-main comparisons

### How to reproduce
```bash
cd demo/benchmark
bash run.sh                                              # all scenarios
bash run.sh --scenario noop-traces-grpc-10k --duration 15  # single scenario
SOL_IMAGE=my-branch:latest SOL_MAIN_IMAGE=superbeeeeeee/sol:latest bash run.sh  # branch vs main
```
METHODOLOGY

  echo "Report written to $report"
}

generate_report

echo ""
echo "═══════════════════════════════════════════════════════════"
echo "  Benchmark complete!"
echo "  Results: $RESULTS_DIR/RESULTS.md"
echo "═══════════════════════════════════════════════════════════"
