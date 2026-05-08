use serde::{Deserialize, Serialize};

use crate::decoding::{Decoder, DeserializerConfig, FramingConfig};

/// Config used to build a `Decoder`.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct DecodingConfig {
    /// The framing config.
    framing: FramingConfig,
    /// The decoding config.
    decoding: DeserializerConfig,
}

impl DecodingConfig {
    /// Creates a new `DecodingConfig` with the provided `FramingConfig` and
    /// `DeserializerConfig`.
    pub const fn new(framing: FramingConfig, decoding: DeserializerConfig) -> Self {
        Self { framing, decoding }
    }

    /// Get the decoding configuration.
    pub const fn config(&self) -> &DeserializerConfig {
        &self.decoding
    }

    /// Get the framing configuration.
    pub const fn framing(&self) -> &FramingConfig {
        &self.framing
    }

    /// Builds a `Decoder` from the provided configuration.
    pub fn build(&self) -> sol_common::Result<Decoder> {
        let framer = self.framing.build();
        let deserializer = self.decoding.build()?;
        Ok(Decoder::new(framer, deserializer))
    }
}
