use std::time::Instant;

use metrics::gauge;
use sol_lib::NamedInternalEvent;
use sol_lib::internal_event::InternalEvent;

use crate::built_info;

#[derive(Debug, NamedInternalEvent)]
pub struct Heartbeat {
    pub since: Instant,
}

impl InternalEvent for Heartbeat {
    fn emit(self) {
        trace!(target: "vector", message = "Beep.");
        #[expect(
            clippy::cast_precision_loss,
            reason = "uptime seconds gauge; precise for |v| <= 2^53"
        )]
        let uptime = self.since.elapsed().as_secs() as f64;
        gauge!("uptime_seconds").set(uptime);
        gauge!(
            "build_info",
            "debug" => built_info::DEBUG,
            "version" => built_info::PKG_VERSION,
            "rust_version" => built_info::RUST_VERSION,
            "arch" => built_info::TARGET_ARCH,
            "revision" => built_info::SOL_BUILD_DESC.unwrap_or("")
        )
        .set(1.0);
    }
}
