use bytes::{Bytes, BytesMut};
use smallvec::SmallVec;
use sol_common::internal_event::emit;
use sol_core::event::Event;

use crate::{
    decoding::format::Deserializer as _,
    decoding::{
        BoxedFramingError, BytesDeserializer, Deserializer, Error, Framer, NewlineDelimitedDecoder,
    },
    internal_events::{DecoderDeserializeError, DecoderFramingError},
};

type DecodedFrame = (SmallVec<[Event; 1]>, usize);

/// A decoder that can decode structured events from a byte stream / byte
/// messages.
#[derive(Clone)]
pub struct Decoder {
    /// The framer being used.
    pub framer: Framer,
    /// The deserializer being used.
    pub deserializer: Deserializer,
}

impl Default for Decoder {
    fn default() -> Self {
        Self {
            framer: Framer::NewlineDelimited(NewlineDelimitedDecoder::new()),
            deserializer: Deserializer::Bytes(BytesDeserializer),
        }
    }
}

impl Decoder {
    /// Creates a new `Decoder` with the specified `Framer` to produce byte
    /// frames from the byte stream / byte messages and `Deserializer` to parse
    /// structured events from a byte frame.
    pub const fn new(framer: Framer, deserializer: Deserializer) -> Self {
        Self {
            framer,
            deserializer,
        }
    }

    /// Handles the framing result and parses it into a structured event, if
    /// possible.
    ///
    /// Emits logs if either framing or parsing failed.
    fn handle_framing_result(
        &mut self,
        frame: Result<Option<Bytes>, BoxedFramingError>,
    ) -> Result<Option<DecodedFrame>, Error> {
        let frame = frame.map_err(|error| {
            emit(DecoderFramingError { error: &error });
            Error::FramingError(error)
        })?;

        frame
            .map(|frame| self.deserializer_parse(frame))
            .transpose()
    }

    /// Parses a frame using the included deserializer, and handles any errors by logging.
    pub fn deserializer_parse(&self, frame: Bytes) -> Result<DecodedFrame, Error> {
        let byte_size = frame.len();

        self.deserializer
            .parse(frame)
            .map(|events| (events, byte_size))
            .map_err(|error| {
                emit(DecoderDeserializeError { error: &error });
                Error::ParsingError(error)
            })
    }
}

impl tokio_util::codec::Decoder for Decoder {
    type Item = DecodedFrame;
    type Error = Error;

    fn decode(&mut self, buf: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        loop {
            let frame = self.framer.decode(buf);
            match self.handle_framing_result(frame) {
                Ok(result) => return Ok(result),
                Err(Error::ParsingError(_)) => continue,
                Err(e) => return Err(e),
            }
        }
    }

    fn decode_eof(&mut self, buf: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        let frame = self.framer.decode_eof(buf);
        self.handle_framing_result(frame)
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use futures::{StreamExt, stream};
    use tokio_util::{codec::FramedRead, io::StreamReader};
    use vrl::value::Value;

    use super::Decoder;
    use crate::{
        JsonDeserializer, NewlineDelimitedDecoder,
        decoding::{Deserializer, Framer},
    };

    #[tokio::test]
    async fn framed_read_skips_all_invalid_frames() {
        let iter = stream::iter(
            ["invalid1\n", "invalid2\n", "{ \"ok\": true }\n"]
                .into_iter()
                .map(Bytes::from),
        );
        let stream = iter.map(Ok::<_, std::io::Error>);
        let reader = StreamReader::new(stream);
        let decoder = Decoder::new(
            Framer::NewlineDelimited(NewlineDelimitedDecoder::new()),
            Deserializer::Json(JsonDeserializer::default()),
        );
        let mut stream = FramedRead::new(reader, decoder);

        let next = stream.next().await.unwrap();
        let log = next.unwrap().0.pop().unwrap().into_log();
        assert_eq!(
            log.parse_path_and_get_value("ok").ok().flatten().unwrap(),
            Value::from(true)
        );
    }

    #[tokio::test]
    async fn framed_read_eof_returns_trailing_valid_frame() {
        // "invalid\n" is consumed by decode(); decode_eof handles the trailing valid frame
        let iter = stream::iter(["invalid\n{ \"eof\": 1 }"].into_iter().map(Bytes::from));
        let stream = iter.map(Ok::<_, std::io::Error>);
        let reader = StreamReader::new(stream);
        let decoder = Decoder::new(
            Framer::NewlineDelimited(NewlineDelimitedDecoder::new()),
            Deserializer::Json(JsonDeserializer::default()),
        );
        let mut stream = FramedRead::new(reader, decoder);

        let next = stream.next().await.unwrap();
        let log = next.unwrap().0.pop().unwrap().into_log();
        assert_eq!(
            log.parse_path_and_get_value("eof").ok().flatten().unwrap(),
            Value::from(1)
        );
    }

    #[tokio::test]
    async fn framed_read_recover_from_error() {
        let iter = stream::iter(
            ["{ \"foo\": 1 }\n", "invalid\n", "{ \"bar\": 2 }\n"]
                .into_iter()
                .map(Bytes::from),
        );
        let stream = iter.map(Ok::<_, std::io::Error>);
        let reader = StreamReader::new(stream);
        let decoder = Decoder::new(
            Framer::NewlineDelimited(NewlineDelimitedDecoder::new()),
            Deserializer::Json(JsonDeserializer::default()),
        );
        let mut stream = FramedRead::new(reader, decoder);

        let next = stream.next().await.unwrap();
        let log = next.unwrap().0.pop().unwrap().into_log();
        assert_eq!(
            log.parse_path_and_get_value("foo").ok().flatten().unwrap(),
            Value::from(1)
        );

        // "invalid\n" is skipped — parsing error is logged internally
        // and the decoder continues to the next frame

        let next = stream.next().await.unwrap();
        let log = next.unwrap().0.pop().unwrap().into_log();
        assert_eq!(
            log.parse_path_and_get_value("bar").ok().flatten().unwrap(),
            Value::from(2)
        );
    }
}
