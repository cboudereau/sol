# node-exporter-dashboard-demo — Design Doc

## Context

The SOL demo (`demo/otel-sol-grafana-dotnet/`) currently monitors the pipeline itself (via the SOL Pipeline dashboard) and application metrics (via the dotnet webapi dashboard). The existing "OpenTelemetry Collector HostMetrics (Node Exporter)" dashboard in `OpenTelemetry Collector Contrib/` is a legacy reference from the OTel Collector setup — it uses `system_*` metric names from the OTel `hostmetrics` receiver and is not fed by any SOL source.

The [sol-telemetry-monitoring design](../../sol-telemetry-monitoring/designs/20260504_sol-telemetry-monitoring.md) listed host metrics as a **non-goal**, stating "Vector does not have a `hostmetrics` receiver equivalent." This was incorrect — SOL inherits Vector's native `host_metrics` source (`src/sources/host_metrics/`), which collects CPU, memory, disk, filesystem, network, load average, and uptime metrics using the `heim` crate.

The `host_metrics` source produces metrics with a configurable namespace (default: `"host"`), but this namespace is stored as an OTLP resource attribute (`metric.namespace`), not as a metric name prefix. To achieve the `node_*` prefix expected by Node Exporter dashboards, the namespace is set to `""` and a VRL remap transform prepends `node_` to metric names.

### Reference dashboard: Node Exporter Full (Grafana ID 1860)

The canonical [Node Exporter Full dashboard](https://grafana.com/grafana/dashboards/1860-node-exporter-full/) (latest: revision 45, 2026-04-11) is the industry-standard Grafana dashboard for Prometheus Node Exporter. It queries **217 unique `node_*` metrics** across **16 sections** (CPU, memory, disk, filesystem, network, systemd, hardware, etc.).

Sol's `host_metrics` source produces **~37 metrics** — covering the core observability signals (CPU, memory, swap, disk I/O, filesystem, network, load, uptime) but not the deep Linux kernel internals (vmstat, netstat, sockstat, hwmon, systemd, PSI pressure, timesync, etc.).

**Key naming difference**: Node Exporter uses PascalCase from `/proc/meminfo` (e.g., `node_memory_MemTotal_bytes`), while Sol uses snake_case (e.g., `node_memory_total_bytes`). This requires rewriting PromQL expressions in adapted sections.

The approach: start from the Node Exporter Full (rev 45) layout, keep sections where Sol has meaningful coverage, remove sections with 0% coverage, and adapt all metric names to Sol's convention. The result is a "Sol Node Exporter" dashboard that follows the latest community layout but only shows what Sol can show.

## Functional Requirements

### <a id="fr1"></a>FR1 — Add host_metrics source to sol-gateway

Add a `host_metrics` source to `sol-gateway.yaml` with `namespace: ""` and a VRL remap transform that prepends `node_` to metric names. The source must collect CPU, memory, disk, filesystem, network, and load metrics from the container.

### <a id="fr2"></a>FR2 — Route host_metrics to Mimir via OTLP

Add a transform and sink to route host_metrics through the existing OTLP pipeline to Mimir. The `service.name: sol` resource attribute maps to the Prometheus `job=sol` label via Mimir's standard OTLP translation. The `host` attribute is already added by the `host_metrics` source automatically.

### <a id="fr3"></a>FR3 — Provision the Node Exporter dashboard in Grafana

Build a Sol-adapted Node Exporter dashboard from the Node Exporter Full (ID 1860, rev 45) reference. The dashboard must:
- Use `mimir` as the datasource UID
- Adapt template variables: use `node_uptime` (Sol produces this) instead of `node_uname_info` (not produced); use `host` label instead of `instance`/`nodename`
- Keep sections with meaningful Sol coverage, remove sections with 0% coverage
- Rewrite PromQL expressions to use Sol's metric names

**Sections kept** (adapted):
| Section | Coverage | Action |
|---|---|---|
| Quick CPU / Mem / Disk | 50% | Keep, remove PSI pressure panels, adapt memory names |
| Basic CPU / Mem / Net / Disk | 83% | Keep, adapt memory names |
| CPU / Memory / Net / Disk (details) | 52% | Keep, remove pressure/speed panels, adapt names |
| Storage Disk | 24% | Keep I/O bytes and ops panels only |
| Storage Filesystem | 33% | Keep space usage panels, adapt inode names |
| Network Traffic | 23% | Keep bandwidth and error panels only |

**Sections removed** (0% or near-0% coverage — Sol does not produce these metrics):
Memory Meminfo, Memory Vmstat, System Timesync, System Processes, System Misc, Hardware Misc, Systemd, Network Sockstat, Network Netstat, Node Exporter.

### <a id="fr4"></a>FR4 — Remove legacy OTel Collector Contrib dashboards

Remove the entire `OpenTelemetry Collector Contrib/` dashboard directory from the demo's Grafana provisioning. Both dashboards are now superseded by Sol-native equivalents:
- "OpenTelemetry Collector" → replaced by `Sol/SOL Pipeline.json` (already exists)
- "OpenTelemetry Collector HostMetrics (Node Exporter)" → replaced by `Sol/Node Exporter (host_metrics).json` (this workspace)

No OTel Collector runs in the demo — these dashboards query `otelcol_*` and `system_*` metrics that are never produced. They add confusion.

### <a id="fr5"></a>FR5 — Docker volumes for host-level metrics

Mount `/proc` and `/sys` from the Docker host into the sol-gateway container and set `PROCFS_ROOT` / `SYSFS_ROOT` environment variables, so the `host_metrics` source reports actual host metrics rather than container-scoped metrics. This makes the dashboard meaningful in the demo.

## Non-Functional Requirements

### <a id="nfr1"></a>NFR1 — Start from Node Exporter Full, adapt freely

Use the Node Exporter Full (ID 1860, rev 45) as the structural reference. Freely adapt: remove entire sections for metrics Sol does not produce, rewrite metric names, change template variables. The goal is a clean, working dashboard that covers Sol's host_metrics capabilities, not fidelity to the 217-metric original.

### <a id="nfr2"></a>NFR2 — Consistent with existing demo conventions

Follow the same patterns used by the existing self-monitoring pipeline: OTLP HTTP to Mimir, attribute promotion via remap transforms, batch settings matching existing sinks.

## Non-goals

- **Full Node Exporter parity** — Sol's `host_metrics` covers ~37 of 217 metrics. Deep kernel internals (vmstat, netstat, sockstat, hwmon, systemd, timesync, PSI pressure) are not produced and their dashboard sections are removed. This is acceptable — the gap can narrow as Sol adds more collectors.
- **Host metrics on every SOL instance** — only sol-gateway runs `host_metrics`. In production/K8s, this would be a DaemonSet agent concern, not a gateway concern.
- **Alerting rules** — deferred, consistent with the telemetry monitoring design non-goals.

## Rabbit holes

- **OTLP → Prometheus metric naming with Mimir**: counters like `node_cpu_seconds_total` already have the `_total` suffix; gauges like `node_memory_total_bytes` already have `_bytes`. The OTLP → Prometheus spec says existing suffixes are not duplicated. The `infer_unit()` function in Sol sets OTLP unit to `"By"` for `*_bytes` and `"s"` for `*_seconds` — Mimir should recognize these and not double-suffix. **Cap**: verify with a single metric (`node_uptime`) after deployment; if naming is wrong, adjust the namespace or strip units.
  - **Discovery**: `infer_unit()` returned `"1"` for dimensionless metrics (uptime, load). Mimir with `otel-metric-suffixes-enabled` appended `_ratio` to these → `node_uptime_ratio`. Fixed by returning `""` instead of `"1"`.
- **OTLP namespace is a resource attribute, not a metric name prefix**: Sol's `host_metrics` `namespace` config stores the value as an OTLP resource attribute (`metric.namespace`), NOT as a prefix in the metric name string. Setting `namespace: "node"` did NOT produce `node_cpu_seconds_total` — the metric name stayed `cpu_seconds_total`. Fixed by using `namespace: ""` and prepending `node_` via a VRL remap transform.
- **Mimir `service.name` → `job` label mapping**: Mimir's OTLP translation maps the `service.name` resource attribute to the Prometheus `job` label. Setting `service.name: sol/host_metrics` produced `job=sol/host_metrics`, not `job=sol`. Setting `service.name: sol` produces the correct `job=sol`. VRL `.attributes."job" = "sol"` is a data point attribute and does NOT override the resource-attribute-based mapping.
- **Docker `/proc` and `/sys` access**: mounting host procfs/sysfs requires the container to have read access. On some Docker runtimes (rootless Docker, restrictive security profiles), this may not work. **Cap**: make the mounts optional — if they fail, the dashboard shows container-level metrics instead.
- **Dashboard JSON size**: the full Node Exporter Full dashboard is 15,535 lines. After removing 10 sections it will be significantly smaller but still large. **Cap**: use a script to extract/transform, don't hand-edit 15k lines of JSON.
- **Cargo feature flag**: The `sources-host_metrics` feature must be included in `demo/Dockerfile.sol` FEATURES arg. Without it, sol-gateway crashes on startup with `unknown variant 'host_metrics'`, killing all OTLP receivers and breaking all dashboards.

## Design

### Architecture

```
sol-gateway container
  ├── host_metrics source (namespace: "", scrape_interval: 15s, service.name: sol)
  │     ├── reads /host/proc (mounted from host /proc)
  │     └── reads /host/sys  (mounted from host /sys)
  │
  ├── remap transform: .name = "node_" + string!(.name)
  │
  └── otlp_self_metrics sink → mimir:9009/otlp (shared with self_metrics)
```

The `host_metrics` source runs alongside the existing `otlp` and `self_metrics` sources in sol-gateway. Metrics are prefixed with `node_` via a VRL remap transform, then exported to Mimir via the existing `otlp_self_metrics` sink. The `service.name: sol` resource attribute maps to `job=sol` in Mimir via standard OTLP-to-Prometheus translation.

### Metric name mapping (Node Exporter → Sol)

| Node Exporter metric | Sol metric | Notes |
|---|---|---|
| `node_memory_MemTotal_bytes` | `node_memory_total_bytes` | |
| `node_memory_MemFree_bytes` | `node_memory_free_bytes` | |
| `node_memory_MemAvailable_bytes` | `node_memory_available_bytes` | |
| `node_memory_Buffers_bytes` | `node_memory_buffers_bytes` | linux only |
| `node_memory_Cached_bytes` | `node_memory_cached_bytes` | linux only |
| `node_memory_Shmem_bytes` | `node_memory_shared_bytes` | |
| `node_memory_SwapTotal_bytes` | `node_memory_swap_total_bytes` | |
| `node_memory_SwapFree_bytes` | `node_memory_swap_free_bytes` | |
| `node_memory_SwapCached_bytes` | not produced | remove from queries |
| `node_memory_SReclaimable_bytes` | not produced | remove from queries |
| `node_filesystem_size_bytes` | `node_filesystem_total_bytes` | |
| `node_filesystem_avail_bytes` | `node_filesystem_free_bytes` | semantic: avail≈free for demo |
| `node_filesystem_files` | `node_filesystem_inodes_total` | |
| `node_filesystem_files_free` | `node_filesystem_inodes_free` | |
| `node_boot_time_seconds` | `node_boot_time` | |
| `node_time_seconds` | not produced | replace uptime calc with `node_uptime` |

Template variable mapping:
| Node Exporter Full | Sol dashboard | Notes |
|---|---|---|
| `node_uname_info` (for job/nodename/instance) | `node_uptime` (for job/host) | Sol doesn't produce uname_info |
| `instance` label | `host` label | Sol tags all metrics with `host` |
| `nodename` label | `host` label | Same physical meaning |
| `fstype` label | `filesystem` label | Sol uses `filesystem` tag name |

### Dashboard placement

```
grafana/provisioning/dashboards/
  Sol/
    SOL Pipeline.json              (existing)
    Node Exporter (host_metrics).json  (new)
```

Grafana's `foldersFromFilesStructure: true` setting creates the folder automatically.

Decisions:
- [Host metrics namespace and job label](../adrs/20260505_host-metrics-namespace-and-job-label.md)
