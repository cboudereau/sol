use std::sync::OnceLock;

use bytes::{BufMut, Bytes};

use super::EventArray;

/// Codec vtable registered by `opentelemetry-proto` at startup, enabling
/// `vector-core`'s disk-buffer layer to encode/decode `EventArray` as OTLP
/// without creating a circular crate dependency.
///
/// `vector-core` owns the trait and the global registry.
/// `opentelemetry-proto` (or the binary entry-point) registers the impl once,
/// before any disk buffer is opened.
pub trait OtlpCodec: Send + Sync + 'static {
    /// Encode `array` as an `OtlpBufferBatch` protobuf message, appending bytes to `buf`.
    ///
    /// # Errors
    /// Returns an error if encoding fails.
    fn encode(&self, array: &EventArray, buf: &mut Vec<u8>) -> Result<(), String>;

    /// Decode `buf` (a serialised `OtlpBufferBatch`) into an `EventArray`.
    ///
    /// # Errors
    /// Returns an error if decoding fails.
    fn decode(&self, buf: Bytes) -> Result<EventArray, String>;
}

static OTLP_CODEC: OnceLock<Box<dyn OtlpCodec>> = OnceLock::new();

/// Register the OTLP codec implementation.
///
/// Must be called once at process startup, before any disk buffer is opened.
/// Subsequent calls are silently ignored (idempotency for tests).
pub fn register_otlp_codec(codec: Box<dyn OtlpCodec>) {
    let _ = OTLP_CODEC.set(codec);
}

/// Encode `array` using the registered OTLP codec, writing into `buf`.
///
/// # Panics
/// Panics if `register_otlp_codec` was not called before the first buffer open.
pub(super) fn encode_as_otlp<B: BufMut>(array: &EventArray, buf: &mut B) -> Result<(), String> {
    let mut tmp = Vec::new();
    OTLP_CODEC
        .get()
        .expect("OTLP codec not registered — call register_otlp_codec() at startup")
        .encode(array, &mut tmp)?;
    if buf.remaining_mut() < tmp.len() {
        return Err("buffer too small".to_string());
    }
    buf.put_slice(&tmp);
    Ok(())
}

/// Decode `buf` using the registered OTLP codec.
///
/// # Panics
/// Panics if `register_otlp_codec` was not called before the first buffer open.
pub(super) fn decode_from_otlp(buf: Bytes) -> Result<EventArray, String> {
    OTLP_CODEC
        .get()
        .expect("OTLP codec not registered — call register_otlp_codec() at startup")
        .decode(buf)
}
