use metrics::counter;
use sol_lib::{NamedInternalEvent, internal_event::InternalEvent};

/// Emitted when rows are successfully loaded into Doris.
#[derive(Debug, NamedInternalEvent)]
pub struct DorisRowsLoaded {
    pub loaded_rows: i64,
    pub load_bytes: i64,
}

impl InternalEvent for DorisRowsLoaded {
    fn emit(self) {
        trace!(
            message = "Doris rows loaded successfully.",
            loaded_rows = %self.loaded_rows,
            load_bytes = %self.load_bytes
        );

        // Record the number of rows loaded
        #[expect(
            clippy::cast_sign_loss,
            reason = "Doris loaded_rows expected non-negative"
        )]
        let loaded_rows = self.loaded_rows as u64;
        counter!("doris_rows_loaded_total").increment(loaded_rows);

        // Record the number of bytes loaded
        #[expect(
            clippy::cast_sign_loss,
            reason = "Doris load_bytes expected non-negative"
        )]
        let load_bytes = self.load_bytes as u64;
        counter!("doris_bytes_loaded_total").increment(load_bytes);
    }
}

/// Emitted when rows are filtered by Doris during loading.
#[derive(Debug, NamedInternalEvent)]
pub struct DorisRowsFiltered {
    pub filtered_rows: i64,
}

impl InternalEvent for DorisRowsFiltered {
    fn emit(self) {
        warn!(
            message = "Doris rows filtered during loading.",
            filtered_rows = %self.filtered_rows
        );

        #[expect(
            clippy::cast_sign_loss,
            reason = "Doris filtered_rows expected non-negative"
        )]
        let filtered_rows = self.filtered_rows as u64;
        counter!("doris_rows_filtered_total").increment(filtered_rows);
    }
}
