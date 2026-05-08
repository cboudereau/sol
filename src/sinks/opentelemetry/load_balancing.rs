//! Consistent-hash load balancing for the OTel gRPC sink.
//!
//! Routes events to multiple backends via consistent hashing on traceID or service name,
//! following the OTel Collector Contrib `loadbalancingexporter` pattern.

use sol_lib::configurable::configurable_component;

// ---------------------------------------------------------------------------
// Configuration types
// ---------------------------------------------------------------------------

/// Load-balancing configuration for the OTel gRPC sink.
#[configurable_component]
#[derive(Clone, Debug)]
pub struct LoadBalancingConfig {
    /// Key used to route events to backends.
    #[serde(default)]
    pub routing_key: RoutingKey,

    /// Backend resolver configuration.
    pub resolver: ResolverConfig,
}

/// Routing key strategy for load balancing.
#[configurable_component]
#[derive(Clone, Debug, Default)]
#[serde(rename_all = "camelCase")]
pub enum RoutingKey {
    /// Hash on span trace_id — all spans from the same trace go to the same backend.
    #[default]
    TraceID,
    /// Hash on resource service.name — all events from the same service go to the same backend.
    Service,
}

/// Resolver configuration — determines how backends are discovered.
#[configurable_component]
#[derive(Clone, Debug)]
#[serde(rename_all = "snake_case", tag = "type")]
pub enum ResolverConfig {
    /// Fixed list of backend hostnames.
    Static(StaticResolverConfig),
    /// Periodic DNS resolution of a hostname.
    Dns(DnsResolverConfig),
    /// Kubernetes EndpointSlice watcher.
    #[cfg(feature = "kubernetes")]
    K8s(K8sResolverConfig),
}

/// Static resolver: fixed list of hostnames.
#[configurable_component]
#[derive(Clone, Debug)]
pub struct StaticResolverConfig {
    /// List of backend host:port addresses.
    pub hostnames: Vec<String>,
}

/// DNS resolver: periodic A/AAAA lookup.
#[configurable_component]
#[derive(Clone, Debug)]
pub struct DnsResolverConfig {
    /// Hostname to resolve (e.g. headless Service DNS name).
    pub hostname: String,
    /// Port to append to resolved addresses.
    #[serde(default = "default_dns_port")]
    pub port: u16,
    /// Resolution interval (e.g. "5s", "30s", "1m").
    #[serde(default = "default_dns_interval")]
    pub interval: String,
}

const fn default_dns_port() -> u16 {
    4317
}

fn default_dns_interval() -> String {
    "5s".to_string()
}

/// Kubernetes EndpointSlice resolver.
#[cfg(feature = "kubernetes")]
#[configurable_component]
#[derive(Clone, Debug)]
pub struct K8sResolverConfig {
    /// Service name in format "name" or "name.namespace".
    pub service: String,
    /// Ports to use from the EndpointSlice.
    #[serde(default = "default_k8s_ports")]
    pub ports: Vec<u16>,
}

#[cfg(feature = "kubernetes")]
fn default_k8s_ports() -> Vec<u16> {
    vec![4317]
}

// ---------------------------------------------------------------------------
// Resolver trait + implementations
// (mirrors otel-col-contrib resolver_static.go / resolver_dns.go / resolver_k8s.go)
// ---------------------------------------------------------------------------

use std::collections::BTreeSet;
use std::time::Duration;
use tokio::sync::watch;

/// A resolver discovers backend endpoints.
#[async_trait::async_trait]
pub trait Resolver: Send + 'static {
    async fn resolve(&mut self) -> crate::Result<Vec<String>>;
}

/// Start a resolver background task. Returns a watch channel that updates on
/// backend changes, plus the task handle.
pub fn start_resolver(
    config: ResolverConfig,
) -> (watch::Receiver<Vec<String>>, tokio::task::JoinHandle<()>) {
    let (tx, rx) = watch::channel(Vec::new());

    let handle = tokio::spawn(async move {
        let (mut resolver, interval): (Box<dyn Resolver>, Duration) = match config {
            ResolverConfig::Static(cfg) => (
                Box::new(StaticResolver(cfg.hostnames)),
                Duration::from_secs(u64::MAX), // resolve once
            ),
            ResolverConfig::Dns(cfg) => {
                let interval = parse_duration(&cfg.interval).unwrap_or(Duration::from_secs(5));
                (
                    Box::new(DnsResolver {
                        hostname: cfg.hostname,
                        port: cfg.port,
                    }),
                    interval,
                )
            }
            #[cfg(feature = "kubernetes")]
            ResolverConfig::K8s(cfg) => {
                let (service, namespace) = match cfg.service.split_once('.') {
                    Some((name, ns)) => (name.to_string(), Some(ns.to_string())),
                    None => (cfg.service, None),
                };
                let port = cfg.ports.first().copied().unwrap_or(4317);
                (
                    Box::new(K8sResolver {
                        service,
                        namespace,
                        port,
                        client: None,
                    }),
                    Duration::from_secs(5),
                )
            }
        };

        let mut last: BTreeSet<String> = BTreeSet::new();
        loop {
            match resolver.resolve().await {
                Ok(mut endpoints) => {
                    endpoints.sort();
                    let current: BTreeSet<String> = endpoints.iter().cloned().collect();
                    if current != last {
                        debug!(
                            message = "Load balancer backends updated.",
                            count = endpoints.len()
                        );
                        last = current;
                        let _ = tx.send(endpoints);
                    }
                }
                Err(error) => {
                    metrics::counter!("lb_num_resolutions", "outcome" => "error").increment(1);
                    warn!(message = "Load balancer resolver failed.", %error);
                }
            }
            tokio::time::sleep(interval).await;
        }
    });

    (rx, handle)
}

fn parse_duration(s: &str) -> Option<Duration> {
    let s = s.trim();
    if let Some(secs) = s.strip_suffix('s') {
        secs.parse::<u64>().ok().map(Duration::from_secs)
    } else if let Some(mins) = s.strip_suffix('m') {
        mins.parse::<u64>()
            .ok()
            .map(|m| Duration::from_secs(m * 60))
    } else {
        s.parse::<u64>().ok().map(Duration::from_secs)
    }
}

/// Static resolver — fixed list, resolves once (mirrors resolver_static.go).
struct StaticResolver(Vec<String>);

#[async_trait::async_trait]
impl Resolver for StaticResolver {
    async fn resolve(&mut self) -> crate::Result<Vec<String>> {
        Ok(self.0.clone())
    }
}

/// DNS resolver — periodic A/AAAA lookup (mirrors resolver_dns.go).
struct DnsResolver {
    hostname: String,
    port: u16,
}

#[async_trait::async_trait]
impl Resolver for DnsResolver {
    async fn resolve(&mut self) -> crate::Result<Vec<String>> {
        let lookup = format!("{}:{}", self.hostname, self.port);
        let addrs = tokio::net::lookup_host(&lookup).await?;
        let endpoints: Vec<String> = addrs.map(|a| a.to_string()).collect();
        if endpoints.is_empty() {
            warn!(message = "DNS resolution returned no results.", hostname = %self.hostname);
        }
        Ok(endpoints)
    }
}

/// K8s EndpointSlice resolver (mirrors resolver_k8s.go).
/// Client is created once on first resolve and reused.
#[cfg(feature = "kubernetes")]
struct K8sResolver {
    service: String,
    namespace: Option<String>,
    port: u16,
    client: Option<kube::Client>,
}

#[cfg(feature = "kubernetes")]
#[async_trait::async_trait]
impl Resolver for K8sResolver {
    async fn resolve(&mut self) -> crate::Result<Vec<String>> {
        use k8s_openapi::api::discovery::v1::EndpointSlice;
        use kube::{Api, Client, api::ListParams};

        let client = match &self.client {
            Some(c) => c.clone(),
            None => {
                let c = Client::try_default().await?;
                self.client = Some(c.clone());
                c
            }
        };
        let api: Api<EndpointSlice> = match &self.namespace {
            Some(ns) => Api::namespaced(client, ns),
            None => Api::default_namespaced(client),
        };

        let label = format!("kubernetes.io/service-name={}", self.service);
        let slices = api.list(&ListParams::default().labels(&label)).await?;

        let mut endpoints = Vec::new();
        for slice in slices.items {
            for ep in slice.endpoints {
                for addr in ep.addresses {
                    let host = if addr.contains(':') {
                        format!("[{addr}]")
                    } else {
                        addr
                    };
                    endpoints.push(format!("{host}:{}", self.port));
                }
            }
        }
        Ok(endpoints)
    }
}

// ---------------------------------------------------------------------------
// Consistent Hash Ring
// ---------------------------------------------------------------------------

/// CRC32-based consistent hash ring following the OTel Collector Contrib pattern.
///
/// Ring space: 0–35999 (360 degrees × 100). Each endpoint gets `vnodes` virtual
/// nodes distributed across the ring via CRC32 hashing. Lookup is O(log n) via
/// binary search.
pub struct ConsistentHashRing {
    /// Sorted ring entries: (position, endpoint_index).
    ring: Vec<(u32, usize)>,
    /// Endpoint list (index corresponds to endpoint_index in ring).
    endpoints: Vec<String>,
}

const RING_SIZE: u32 = 36_000;
const DEFAULT_VNODES: u32 = 100;

impl ConsistentHashRing {
    /// Build a new hash ring from a list of endpoints.
    pub fn new(endpoints: &[String]) -> Self {
        Self::with_vnodes(endpoints, DEFAULT_VNODES)
    }

    /// Build a hash ring with a custom number of virtual nodes per endpoint.
    pub fn with_vnodes(endpoints: &[String], vnodes: u32) -> Self {
        let mut ring = Vec::with_capacity(endpoints.len() * vnodes as usize);
        for (idx, endpoint) in endpoints.iter().enumerate() {
            for vnode in 0..vnodes {
                let key = format!("{endpoint}-{vnode}");
                let hash = crc32_hash(key.as_bytes()) % RING_SIZE;
                ring.push((hash, idx));
            }
        }
        ring.sort_by_key(|&(pos, _)| pos);
        Self {
            ring,
            endpoints: endpoints.to_vec(),
        }
    }

    /// Look up the endpoint for a given key.
    ///
    /// Returns `None` if the ring is empty.
    pub fn get(&self, key: &[u8]) -> Option<&str> {
        if self.ring.is_empty() {
            return None;
        }
        let hash = crc32_hash(key) % RING_SIZE;
        // Binary search for the first ring entry >= hash.
        let idx = match self.ring.binary_search_by_key(&hash, |&(pos, _)| pos) {
            Ok(i) => i,
            Err(i) => {
                if i >= self.ring.len() {
                    0 // wrap around
                } else {
                    i
                }
            }
        };
        let endpoint_idx = self.ring[idx].1;
        Some(&self.endpoints[endpoint_idx])
    }

    /// Number of endpoints in the ring.
    pub const fn len(&self) -> usize {
        self.endpoints.len()
    }

    /// Whether the ring is empty.
    pub const fn is_empty(&self) -> bool {
        self.endpoints.is_empty()
    }

    /// The endpoint list.
    pub fn endpoints(&self) -> &[String] {
        &self.endpoints
    }
}

/// CRC32 IEEE hash using hardware-accelerated `crc32fast` crate.
pub fn crc32_hash(data: &[u8]) -> u32 {
    crc32fast::hash(data)
}

// ---------------------------------------------------------------------------
// Routing key extraction
// ---------------------------------------------------------------------------

use crate::event::Event;

/// Extract the routing key bytes from an event based on the routing strategy.
pub fn extract_routing_key(event: &Event, routing_key: &RoutingKey) -> Vec<u8> {
    match routing_key {
        RoutingKey::TraceID => match event {
            Event::Trace(span) => span.span().trace_id.clone(),
            Event::Log(log) => {
                service_name_from_resource(log.resource_proto().as_ref()).into_bytes()
            }
            Event::Metric(metric) => {
                service_name_from_resource(metric.resource_proto().as_ref()).into_bytes()
            }
        },
        RoutingKey::Service => {
            let resource = match event {
                Event::Trace(span) => span.resource_proto(),
                Event::Log(log) => log.resource_proto(),
                Event::Metric(metric) => metric.resource_proto(),
            };
            service_name_from_resource(resource.as_ref()).into_bytes()
        }
    }
}

/// Extract `service.name` from OTel resource attributes.
fn service_name_from_resource(
    resource: Option<&opentelemetry_proto::tonic::resource::v1::Resource>,
) -> String {
    resource
        .and_then(|r| {
            r.attributes
                .iter()
                .find(|kv| kv.key == "service.name")
                .and_then(|kv| {
                    kv.value.as_ref().and_then(|v| {
                        if let Some(
                            opentelemetry_proto::tonic::common::v1::any_value::Value::StringValue(
                                s,
                            ),
                        ) = &v.value
                        {
                            Some(s.clone())
                        } else {
                            None
                        }
                    })
                })
        })
        .unwrap_or_default()
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ring_deterministic() {
        let endpoints = vec![
            "backend-0:4317".to_string(),
            "backend-1:4317".to_string(),
            "backend-2:4317".to_string(),
        ];
        let ring = ConsistentHashRing::new(&endpoints);

        // Same key always maps to same endpoint.
        let key = b"trace-id-abc-123";
        let ep1 = ring.get(key).unwrap();
        let ep2 = ring.get(key).unwrap();
        assert_eq!(ep1, ep2);
    }

    #[test]
    fn ring_distribution() {
        let endpoints: Vec<String> = (0..3).map(|i| format!("backend-{i}:4317")).collect();
        let ring = ConsistentHashRing::new(&endpoints);

        let mut counts = [0u32; 3];
        for i in 0..3000 {
            let key = format!("trace-{i}");
            let ep = ring.get(key.as_bytes()).unwrap();
            let idx = endpoints.iter().position(|e| e == ep).unwrap();
            counts[idx] += 1;
        }

        // Each backend should get at least 15% of traffic (expect ~33%).
        for count in &counts {
            assert!(
                *count > 450,
                "Uneven distribution: {counts:?} — one backend got only {count}/3000"
            );
        }
    }

    #[test]
    fn ring_stability_on_add() {
        let endpoints_2: Vec<String> = (0..2).map(|i| format!("backend-{i}:4317")).collect();
        let endpoints_3: Vec<String> = (0..3).map(|i| format!("backend-{i}:4317")).collect();
        let ring_2 = ConsistentHashRing::new(&endpoints_2);
        let ring_3 = ConsistentHashRing::new(&endpoints_3);

        // Adding a third backend should only move ~1/3 of keys.
        let mut moved = 0;
        let total = 3000;
        for i in 0..total {
            let key = format!("trace-{i}");
            let ep_2 = ring_2.get(key.as_bytes()).unwrap();
            let ep_3 = ring_3.get(key.as_bytes()).unwrap();
            if ep_2 != ep_3 {
                moved += 1;
            }
        }

        // At most ~50% should move (ideal is ~33%).
        assert!(
            moved < total / 2,
            "Too many keys moved: {moved}/{total} — expected ~{}/{}",
            total / 3,
            total
        );
    }

    #[test]
    fn ring_empty() {
        let ring = ConsistentHashRing::new(&[]);
        assert!(ring.get(b"any-key").is_none());
        assert!(ring.is_empty());
    }

    #[test]
    fn ring_single_endpoint() {
        let ring = ConsistentHashRing::new(&["only-one:4317".to_string()]);
        assert_eq!(ring.get(b"any-key").unwrap(), "only-one:4317");
        assert_eq!(ring.get(b"another-key").unwrap(), "only-one:4317");
    }

    #[test]
    fn crc32_known_values() {
        // Verify our CRC32 implementation produces correct IEEE values.
        assert_eq!(crc32_hash(b""), 0);
        assert_eq!(crc32_hash(b"123456789"), 0xCBF4_3926);
    }

    #[tokio::test]
    async fn static_resolver_returns_hostnames() {
        let mut resolver = StaticResolver(vec!["backend-0:4317".into(), "backend-1:4317".into()]);
        let result = resolver.resolve().await.unwrap();
        assert_eq!(result, vec!["backend-0:4317", "backend-1:4317"]);
    }

    #[tokio::test]
    async fn dns_resolver_resolves_localhost() {
        // localhost should always resolve
        let mut resolver = DnsResolver {
            hostname: "localhost".into(),
            port: 4317,
        };
        let result = resolver.resolve().await.unwrap();
        assert!(
            !result.is_empty(),
            "localhost should resolve to at least one address"
        );
        for addr in &result {
            assert!(
                addr.contains("4317"),
                "resolved address should contain port: {addr}"
            );
        }
    }

    #[test]
    fn parse_duration_variants() {
        assert_eq!(parse_duration("5s"), Some(Duration::from_secs(5)));
        assert_eq!(parse_duration("30s"), Some(Duration::from_secs(30)));
        assert_eq!(parse_duration("1m"), Some(Duration::from_secs(60)));
        assert_eq!(parse_duration("10"), Some(Duration::from_secs(10)));
        assert_eq!(parse_duration("abc"), None);
    }

    #[tokio::test]
    async fn start_resolver_static_delivers_endpoints() {
        let config = ResolverConfig::Static(StaticResolverConfig {
            hostnames: vec!["a:4317".into(), "b:4317".into()],
        });
        let (mut rx, handle) = start_resolver(config);
        // Wait for first update
        rx.changed().await.unwrap();
        let endpoints = rx.borrow().clone();
        assert_eq!(endpoints, vec!["a:4317", "b:4317"]);
        handle.abort();
    }
}
