// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (c) 2025 Clément Boudereau
//! Optimized-logical-plan cache (promql-plan-cache task 2a, [FR2], [ADR A′]).
//!
//! Dashboards re-issue the same query *shapes* with only the time window
//! sliding, and the FR1 profile showed `state.optimize()` costs ~26 % of a
//! shape-warm `rate()` query. This module caches the **post-optimize**
//! `LogicalPlan` keyed by the query's shape and, on a hit, REBINDs the cached
//! plan to the current window — skipping the optimizer entirely:
//!
//! 1. **window-literal rewrite**: the window's time bounds are rewritten from
//!    the cached window's values to the current one's via a `TreeNode`
//!    rewrite;
//! 2. **provider swap**: every `TableScan`'s source is replaced with the
//!    current window's scoped provider (the cached plan embeds the *previous*
//!    window's file list — serving it unswapped would read stale/missing
//!    files).
//!
//! ## Window-literal identification (the chosen strategy)
//!
//! A literal is a *window bound* iff it appears as the literal side of a
//! comparison (`<`,`<=`,`>`,`>=`,`=`, `BETWEEN`) whose other side references a
//! `*time_unix_nano` column — exactly the shape produced by
//! `prometheus::prom_time_between` and the instant/log/trace time filters.
//! Post-optimizer these survive as the same comparisons with the cast
//! unwrapped onto the literal (`Int64` → `TimestampNanosecond`), so the same
//! structural rule identifies them in both the fresh (unoptimized) and the
//! cached (optimized) plan. No sentinel values are involved (they could be
//! constant-folded); identification is purely structural.
//!
//! This identification does **not** need to be complete to be correct — it is
//! *fail-safe by construction*:
//! - a window literal it misses stays in the shape text, so two windows of
//!   that shape get different keys and never share a plan (correct-but-slow);
//! - a shape constant it wrongly identifies has the same value in the cached
//!   and the fresh plan, so the rebind maps it to itself (no-op).
//!
//! ## Shape text (the chosen key source)
//!
//! The shape is the fresh **unoptimized** plan's `display_indent()` with every
//! identified window literal masked. The plan text — rather than the PromQL
//! expression string — is used because it uniquely identifies the exact
//! lowering: one PromQL query can lower several distinct `DataFrame`s (each
//! side of a binary expression, per-window scans), which must not collide
//! under one key.
//!
//! ## Rebind totality (bypass, never guess)
//!
//! A hit is served only when the rebind is provably total; otherwise the query
//! BYPASSES the cache and optimizes fresh:
//! - the cached↔fresh window-value correspondence must be a well-defined map
//!   (no old value mapped to two different new values);
//! - every identified window literal in the cached optimized plan must be in
//!   that map;
//! - every `TableScan` must find a same-schema fresh source that supports its
//!   pushed-down filters; a table name appearing with two *different* scoped
//!   sources in one fresh plan is ambiguous → bypass.
//!
//! Insertion runs the same rebind against the plan's own shape (identity
//! rebind), so a shape whose optimized plan cannot be rebound is never cached
//! in the first place.
//!
//! [FR2]: ../../../docs/workspace/promql-plan-cache/DESIGN.md#fr2
//! [ADR A′]: ../../../docs/workspace/promql-plan-cache/adrs/plan-cache-mechanism.md

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use datafusion::common::ScalarValue;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::logical_expr::{Expr, LogicalPlan, Operator, TableSource};

/// Entry-count bound (plans are small — a few KB of expression nodes — so a
/// count bound suffices; TinyLFU evicts cold shapes). Stale-generation entries
/// are additionally dropped eagerly at `QueryEngine::refresh`.
const PLAN_CACHE_MAX_ENTRIES: u64 = 256;

/// Complete plan-cache key ([ADR][adr]: every component has a
/// changing-it-misses test). `shape` carries the expression/lowering identity
/// (window literals masked out) and the resolved table names appear in it too;
/// `tables` repeats them explicitly, and the remaining components pin the
/// context the shape text cannot see.
///
/// [adr]: ../../../docs/workspace/promql-plan-cache/adrs/plan-cache-mechanism.md
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) struct PlanCacheKey {
    /// Masked plan display — the shape text (see module docs).
    pub(crate) shape: String,
    /// Query step (ns; `0` for stepless paths). Identity bucketing: dashboard
    /// refreshes keep their step, so each distinct step is its own bucket.
    pub(crate) step_ns: i64,
    /// Resolved table set (tier-routing outcome), sorted + deduped.
    pub(crate) tables: Vec<String>,
    /// [`super::inventory::FileInventory`] generation the plan was built
    /// against; a store change bumps it, making stale entries unreachable.
    pub(crate) inventory_generation: u64,
    /// Lookback-relevant engine config (`metadata_default_range_secs` today;
    /// FR3's staleness lookback joins it when it lands).
    pub(crate) lookback_cfg: u64,
}

/// A cached optimized plan plus the window-bound values (in deterministic
/// traversal order) of the *unoptimized* plan it was built from — the rebind
/// zips them with the fresh plan's values to form the old→new map.
pub(crate) struct CachedPlan {
    pub(crate) optimized: LogicalPlan,
    pub(crate) window_values: Vec<i64>,
}

/// Analysis of a fresh (unoptimized) plan: everything the cache needs to key,
/// insert, and rebind — produced by one traversal in [`analyze`].
pub(crate) struct PlanShape {
    /// Masked `display_indent()` (see module docs).
    pub(crate) shape: String,
    /// Sorted, deduped table names scanned by the plan.
    pub(crate) tables: Vec<String>,
    /// Identified window-bound values, in traversal order.
    pub(crate) window_values: Vec<i64>,
    /// Table name → the current window's scoped source, for the provider swap.
    sources: HashMap<String, Arc<dyn TableSource>>,
    /// One table name appeared with two different sources (e.g. two scoped
    /// scans of the same table over different sub-windows) — the swap target
    /// would be ambiguous, so rebind refuses ([`rebind`] → bypass).
    ambiguous_sources: bool,
}

/// Bounded in-memory plan cache with hit/miss/bypass accounting.
pub(crate) struct PlanCache {
    inner: moka::sync::Cache<PlanCacheKey, Arc<CachedPlan>>,
    hits: AtomicU64,
    misses: AtomicU64,
    bypasses: AtomicU64,
}

impl PlanCache {
    pub(crate) fn new() -> Self {
        Self {
            inner: moka::sync::Cache::builder()
                .max_capacity(PLAN_CACHE_MAX_ENTRIES)
                .build(),
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
            bypasses: AtomicU64::new(0),
        }
    }

    pub(crate) fn get(&self, key: &PlanCacheKey) -> Option<Arc<CachedPlan>> {
        self.inner.get(key)
    }

    pub(crate) fn insert(&self, key: PlanCacheKey, plan: Arc<CachedPlan>) {
        self.inner.insert(key, plan);
    }

    /// Drop every entry — called when the inventory generation bumps
    /// ([`super::QueryEngine::refresh`]): generation is a key component, so
    /// all existing entries just became unreachable; this frees them eagerly.
    pub(crate) fn invalidate_all(&self) {
        self.inner.invalidate_all();
    }

    /// Record a lookup outcome: internal counters (test observability) plus
    /// the `sol_querier_plan_cache_requests_total{result=…}` telemetry.
    pub(crate) fn note(&self, outcome: PlanCacheOutcome) {
        let (counter, label) = match outcome {
            PlanCacheOutcome::Hit => (&self.hits, "hit"),
            PlanCacheOutcome::Miss => (&self.misses, "miss"),
            PlanCacheOutcome::Bypass => (&self.bypasses, "bypass"),
        };
        counter.fetch_add(1, Ordering::Relaxed);
        super::telemetry::record_plan_cache(label);
    }

    /// `(hits, misses, bypasses)` since construction.
    #[cfg(test)]
    pub(crate) fn counts(&self) -> (u64, u64, u64) {
        (
            self.hits.load(Ordering::Relaxed),
            self.misses.load(Ordering::Relaxed),
            self.bypasses.load(Ordering::Relaxed),
        )
    }
}

/// A plan-cache lookup outcome, for [`PlanCache::note`].
#[derive(Clone, Copy, Debug)]
pub(crate) enum PlanCacheOutcome {
    /// Cached plan rebound and served — the optimize stage was skipped.
    Hit,
    /// No entry under the key — optimized fresh and (if rebindable) inserted.
    Miss,
    /// An entry existed but the rebind was not provably total, or the plan
    /// could not be analyzed — optimized fresh, never guessed.
    Bypass,
}

/// Analyze a fresh (unoptimized) plan in one traversal: masked shape text,
/// window-bound values in deterministic order, and the scanned tables with
/// their current scoped sources.
pub(crate) fn analyze(plan: &LogicalPlan) -> crate::Result<PlanShape> {
    let mut values: Vec<i64> = Vec::new();
    let mut tables: Vec<String> = Vec::new();
    let mut sources: HashMap<String, Arc<dyn TableSource>> = HashMap::new();
    let mut ambiguous_sources = false;
    let masked = plan
        .clone()
        .transform_up(|node| {
            if let LogicalPlan::TableScan(scan) = &node {
                let name = scan.table_name.to_string();
                match sources.entry(name.clone()) {
                    std::collections::hash_map::Entry::Occupied(e) => {
                        if !Arc::ptr_eq(e.get(), &scan.source) {
                            ambiguous_sources = true;
                        }
                    }
                    std::collections::hash_map::Entry::Vacant(e) => {
                        e.insert(Arc::clone(&scan.source));
                    }
                }
                tables.push(name);
            }
            node.map_expressions(|e| {
                e.transform_up(|e| {
                    Ok(rewrite_window_bounds(e, &mut |v, _lit| {
                        values.push(v);
                        // Mask for the shape text only (never executed), so the
                        // placeholder's type is irrelevant — a NULL literal can
                        // not collide with any real bound.
                        Some(Expr::Literal(ScalarValue::Int64(None), None))
                    }))
                })
            })
        })?
        .data;
    tables.sort();
    tables.dedup();
    Ok(PlanShape {
        shape: masked.display_indent().to_string(),
        tables,
        window_values: values,
        sources,
        ambiguous_sources,
    })
}

/// Rebind a cached optimized plan onto the `fresh` shape's window: rewrite
/// every identified window literal via the old→new value map and swap every
/// `TableScan` source to the fresh scoped one. `None` ⇔ the rebind is not
/// provably total (see module docs) — the caller must bypass, never guess.
pub(crate) fn rebind(cached: &CachedPlan, fresh: &PlanShape) -> Option<LogicalPlan> {
    if fresh.ambiguous_sources || cached.window_values.len() != fresh.window_values.len() {
        return None;
    }
    // Positional zip: both value sequences come from the same deterministic
    // traversal over equal-shape plans. A conflicting duplicate (one old value
    // needing two different new values — e.g. a cached lo==hi point window
    // rebound onto lo'<hi') is ambiguous.
    let mut map: HashMap<i64, i64> = HashMap::with_capacity(cached.window_values.len());
    for (old, new) in cached.window_values.iter().zip(&fresh.window_values) {
        match map.entry(*old) {
            std::collections::hash_map::Entry::Occupied(e) => {
                if *e.get() != *new {
                    return None;
                }
            }
            std::collections::hash_map::Entry::Vacant(e) => {
                e.insert(*new);
            }
        }
    }
    let mut total = true;
    let rebound = cached
        .optimized
        .clone()
        .transform_up(|node| {
            let (node, swapped) = match node {
                LogicalPlan::TableScan(mut scan) => {
                    match fresh.sources.get(scan.table_name.to_string().as_str()) {
                        Some(source) if source_swappable(&scan.source, source, &scan.filters) => {
                            scan.source = Arc::clone(source);
                            (LogicalPlan::TableScan(scan), true)
                        }
                        _ => {
                            total = false;
                            (LogicalPlan::TableScan(scan), false)
                        }
                    }
                }
                other => (other, false),
            };
            let mut t = node.map_expressions(|e| {
                e.transform_up(|e| {
                    Ok(rewrite_window_bounds(e, &mut |v, lit| match map.get(&v) {
                        Some(new) => Some(relit(lit, *new)),
                        None => {
                            // A window literal the map does not cover (e.g. one
                            // the optimizer derived by folding): not total.
                            total = false;
                            None
                        }
                    }))
                })
            })?;
            // `map_expressions` only flags expression changes; a source swap
            // must be flagged too or ancestors may skip adopting the new node.
            t.transformed |= swapped;
            Ok(t)
        })
        .ok()?
        .data;
    total.then_some(rebound)
}

/// Whether `fresh` can replace `old` inside a `TableScan`: identical schema
/// (both scoped providers of one table share the declared schema) and, when
/// the optimizer pushed filters into the scan, the fresh source must accept
/// them (an all-pruned window's empty `MemTable` does not — bypass rather than
/// hand a provider filters it never negotiated).
fn source_swappable(
    old: &Arc<dyn TableSource>,
    fresh: &Arc<dyn TableSource>,
    filters: &[Expr],
) -> bool {
    if old.schema() != fresh.schema() {
        return false;
    }
    if filters.is_empty() {
        return true;
    }
    let refs: Vec<&Expr> = filters.iter().collect();
    fresh.supports_filters_pushdown(&refs).is_ok_and(|support| {
        support.iter().all(|s| {
            !matches!(
                s,
                datafusion::logical_expr::TableProviderFilterPushDown::Unsupported
            )
        })
    })
}

/// The single structural window-bound matcher (see module docs), shared by
/// masking ([`analyze`]) and rewriting ([`rebind`]): a comparison or `BETWEEN`
/// whose non-literal side references a `*time_unix_nano` column. `on_bound`
/// receives each identified bound's ns value and literal; returning `Some`
/// replaces that literal.
fn rewrite_window_bounds(
    expr: Expr,
    on_bound: &mut impl FnMut(i64, &Expr) -> Option<Expr>,
) -> Transformed<Expr> {
    match expr {
        Expr::BinaryExpr(mut be) if is_comparison(be.op) => {
            let mut changed = false;
            if refs_time(&be.left) {
                changed |= replace_bound(&mut be.right, on_bound);
            } else if refs_time(&be.right) {
                changed |= replace_bound(&mut be.left, on_bound);
            }
            let e = Expr::BinaryExpr(be);
            if changed {
                Transformed::yes(e)
            } else {
                Transformed::no(e)
            }
        }
        Expr::Between(mut bt) if refs_time(&bt.expr) => {
            let mut changed = replace_bound(&mut bt.low, on_bound);
            changed |= replace_bound(&mut bt.high, on_bound);
            let e = Expr::Between(bt);
            if changed {
                Transformed::yes(e)
            } else {
                Transformed::no(e)
            }
        }
        other => Transformed::no(other),
    }
}

/// If `side` is a window-bound literal, offer it to `on_bound` and apply the
/// replacement. Returns whether a replacement happened.
fn replace_bound(side: &mut Expr, on_bound: &mut impl FnMut(i64, &Expr) -> Option<Expr>) -> bool {
    let Some(v) = bound_ns(side) else {
        return false;
    };
    if let Some(replacement) = on_bound(v, side) {
        *side = replacement;
        true
    } else {
        false
    }
}

/// The ns value of a bound literal: a plain `Int64` (as `prom_time_between`
/// writes) or a `TimestampNanosecond` (what the optimizer's cast-unwrapping
/// turns it into). Anything else is not treated as a window bound.
fn bound_ns(e: &Expr) -> Option<i64> {
    match e {
        Expr::Literal(ScalarValue::Int64(Some(v)), _)
        | Expr::Literal(ScalarValue::TimestampNanosecond(Some(v), _), _) => Some(*v),
        _ => None,
    }
}

/// Rebuild `lit` with a new ns value, preserving its scalar variant (and
/// timestamp timezone) so the rebound plan's schemas are untouched.
fn relit(lit: &Expr, new_ns: i64) -> Expr {
    match lit {
        Expr::Literal(ScalarValue::TimestampNanosecond(Some(_), tz), meta) => Expr::Literal(
            ScalarValue::TimestampNanosecond(Some(new_ns), tz.clone()),
            meta.clone(),
        ),
        Expr::Literal(_, meta) => Expr::Literal(ScalarValue::Int64(Some(new_ns)), meta.clone()),
        // `bound_ns` only matches literals; other exprs are never passed here.
        other => other.clone(),
    }
}

/// Comparison operators a window bound participates in.
fn is_comparison(op: Operator) -> bool {
    matches!(
        op,
        Operator::Eq | Operator::Lt | Operator::LtEq | Operator::Gt | Operator::GtEq
    )
}

/// Whether an expression references an event-time column (`time_unix_nano`,
/// `start_time_unix_nano`, `observed_time_unix_nano`).
fn refs_time(e: &Expr) -> bool {
    e.column_refs()
        .iter()
        .any(|c| c.name.ends_with("time_unix_nano"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::logical_expr::{LogicalPlanBuilder, col, lit};

    fn empty_plan() -> LogicalPlan {
        LogicalPlanBuilder::empty(false).build().unwrap()
    }

    fn dummy_entry() -> Arc<CachedPlan> {
        Arc::new(CachedPlan {
            optimized: empty_plan(),
            window_values: Vec::new(),
        })
    }

    /// FR2 key completeness ([ADR]): changing any single key component —
    /// shape/expr, step, resolved table set, inventory generation, lookback
    /// config — must miss.
    ///
    /// [ADR]: ../../../docs/workspace/promql-plan-cache/adrs/plan-cache-mechanism.md
    #[test]
    fn test_plan_cache_key_components_miss() {
        let cache = PlanCache::new();
        let base = PlanCacheKey {
            shape: "Filter: t BETWEEN $w AND $w\n  TableScan: metrics".to_string(),
            step_ns: 30_000_000_000,
            tables: vec!["metrics".to_string()],
            inventory_generation: 1,
            lookback_cfg: 3_600,
        };
        cache.insert(base.clone(), dummy_entry());
        assert!(cache.get(&base).is_some(), "sanity: exact key hits");

        let variants: Vec<(&str, PlanCacheKey)> = vec![
            (
                "shape/expr",
                PlanCacheKey {
                    shape: "TableScan: metrics".to_string(),
                    ..base.clone()
                },
            ),
            (
                "step bucket",
                PlanCacheKey {
                    step_ns: 60_000_000_000,
                    ..base.clone()
                },
            ),
            (
                "resolved table set",
                PlanCacheKey {
                    tables: vec!["metrics_1h".to_string()],
                    ..base.clone()
                },
            ),
            (
                "inventory generation",
                PlanCacheKey {
                    inventory_generation: 2,
                    ..base.clone()
                },
            ),
            (
                "lookback config",
                PlanCacheKey {
                    lookback_cfg: 300,
                    ..base.clone()
                },
            ),
        ];
        for (component, key) in variants {
            assert!(
                cache.get(&key).is_none(),
                "changing key component `{component}` must miss"
            );
        }
    }

    /// A scan+filter fixture plan over a one-column time schema, with the
    /// window `[lo, hi]` as `prom_time_between`-style literals.
    fn windowed_plan(lo: i64, hi: i64) -> LogicalPlan {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "time_unix_nano",
            DataType::Int64,
            true,
        )]));
        let source = datafusion::datasource::provider_as_source(Arc::new(
            datafusion::datasource::MemTable::try_new(schema, vec![vec![]]).unwrap(),
        ));
        LogicalPlanBuilder::scan("metrics", source, None)
            .unwrap()
            .filter(col("time_unix_nano").between(lit(lo), lit(hi)))
            .unwrap()
            .build()
            .unwrap()
    }

    #[test]
    fn test_analyze_masks_window_literals_into_shape() {
        let a = analyze(&windowed_plan(1_000, 2_000)).unwrap();
        let b = analyze(&windowed_plan(5_000, 9_000)).unwrap();
        // Same shape despite different windows; values collected in order.
        assert_eq!(a.shape, b.shape);
        assert_eq!(a.window_values, vec![1_000, 2_000]);
        assert_eq!(b.window_values, vec![5_000, 9_000]);
        assert_eq!(a.tables, vec!["metrics".to_string()]);
        // The masked shape must not leak the concrete bounds.
        assert!(!a.shape.contains("1000"), "shape: {}", a.shape);
    }

    #[test]
    fn test_rebind_conflicting_value_map_bypasses() {
        // Cached point window lo==hi; new window lo'<hi': the single old value
        // would need two different new values → not total → None.
        let point = windowed_plan(1_000, 1_000);
        let cached = CachedPlan {
            window_values: analyze(&point).unwrap().window_values,
            optimized: point,
        };
        let fresh = analyze(&windowed_plan(2_000, 3_000)).unwrap();
        assert!(rebind(&cached, &fresh).is_none(), "conflict must bypass");
        // Sanity: a consistent slide of the same point window rebinds fine.
        let fresh_point = analyze(&windowed_plan(4_000, 4_000)).unwrap();
        assert!(rebind(&cached, &fresh_point).is_some());
    }
}
