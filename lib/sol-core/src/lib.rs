//! The Vector Core Library
//!
//! The Vector Core Library are the foundational pieces needed to make a vector
//! and is not vector with pieces missing. While this library is obviously
//! tailored to the needs of vector it is written in such a way to make
//! experimentation and testing _in the library_ cheap and demonstrative.
//!
//! This library was extracted from the top-level project package, discussed in
//! RFC 7027.

#![deny(warnings)]
#![deny(clippy::all)]
#![deny(unreachable_pub)]
#![deny(unused_allocation)]
#![deny(unused_extern_crates)]
#![deny(unused_assignments)]
#![deny(unused_comparisons)]
#![allow(clippy::default_trait_access)] // triggers on generated prost code
#![allow(clippy::float_cmp)]
#![allow(clippy::type_complexity)]
// long-types happen, especially in async code
// --- pedantic lints allowed in bulk (pre-existing, not worth fixing one-by-one) ---
#![allow(clippy::doc_markdown)]
#![allow(clippy::redundant_closure_for_method_calls)]
#![allow(clippy::cast_lossless)]
#![allow(clippy::cast_possible_truncation)]
#![allow(clippy::cast_precision_loss)]
#![allow(clippy::map_unwrap_or)]
#![allow(clippy::single_match_else)]
#![allow(clippy::needless_pass_by_value)]
#![allow(clippy::return_self_not_must_use)]
#![allow(clippy::missing_errors_doc)]
#![allow(clippy::missing_panics_doc)]
#![allow(clippy::match_same_arms)]
#![allow(clippy::too_many_lines)]
#![allow(clippy::manual_let_else)]
#![allow(clippy::wildcard_imports)]
#![allow(clippy::uninlined_format_args)]
#![allow(clippy::explicit_iter_loop)]
#![allow(clippy::items_after_statements)]
#![allow(clippy::unreadable_literal)]
#![allow(clippy::semicolon_if_nothing_returned)]
#![allow(clippy::implicit_clone)]
// --- clippy::all lints allowed in bulk (pre-existing style issues) ---
#![allow(clippy::useless_vec)]
#![allow(clippy::collapsible_if)]
#![allow(clippy::redundant_closure)]
#![allow(clippy::needless_borrows_for_generic_args)]
#![allow(clippy::needless_borrow)]
#![allow(clippy::map_entry)]
#![allow(clippy::manual_map)]
#![allow(clippy::manual_is_multiple_of)]
#![allow(clippy::unnecessary_map_or)]
#![allow(clippy::too_many_arguments)]
#![allow(clippy::large_enum_variant)]
#![allow(clippy::implied_bounds_in_impls)]
#![allow(clippy::manual_midpoint)]

pub mod config;
pub mod event;
pub mod fanout;
pub mod ipallowlist;
pub mod latency;
pub mod metrics;
pub mod partition;
pub mod schema;
pub mod serde;
pub mod sink;
pub mod source;
pub mod source_sender;
pub mod tcp;
#[cfg(test)]
mod test_util;
pub mod time;
pub mod tls;
pub mod transform;
#[cfg(feature = "vrl")]
pub mod vrl;

use std::path::PathBuf;

pub use event::EstimatedJsonEncodedSizeOf;
use float_eq::FloatEq;

#[cfg(feature = "vrl")]
pub use crate::vrl::compile_vrl;

#[macro_use]
extern crate tracing;

pub fn default_data_dir() -> Option<PathBuf> {
    Some(PathBuf::from("/var/lib/sol/"))
}

pub(crate) use sol_common::Result;

pub(crate) fn float_eq(l_value: f64, r_value: f64) -> bool {
    (l_value.is_nan() && r_value.is_nan()) || l_value.eq_ulps(&r_value, &1)
}

// These macros aren't actually usable in lib crates without some `vector_lib` shenanigans.
#[macro_export]
macro_rules! emit {
    ($event:expr) => {
        sol_lib::internal_event::emit($event)
    };
}

#[macro_export]
macro_rules! register {
    ($event:expr) => {
        sol_lib::internal_event::register($event)
    };
}
