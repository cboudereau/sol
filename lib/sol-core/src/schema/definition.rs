use std::collections::{BTreeMap, BTreeSet};

use lookup::{
    OwnedTargetPath, OwnedValuePath, PathPrefix, lookup_v2::TargetPath, owned_value_path,
};
use vrl::value::{Kind, kind::Collection};

/// The definition of a schema.
///
/// This struct contains all the information needed to inspect the schema of an event emitted by
/// a source/transform.
#[derive(Clone, Debug, PartialEq, PartialOrd)]
pub struct Definition {
    /// The type of the event
    event_kind: Kind,

    /// The type of the metadata.
    metadata_kind: Kind,

    /// Semantic meaning assigned to fields within the collection.
    ///
    /// The value within this map points to a path inside the `event_kind`.
    /// Meanings currently can't point to metadata.
    meaning: BTreeMap<String, MeaningPointer>,
}

/// In regular use, a semantic meaning points to exactly _one_ location in the collection. However,
/// when merging two [`Definition`]s, we need to be able to allow for two definitions with the same
/// semantic meaning identifier to be merged together.
///
/// We cannot error when this happens, because a follow-up component (such as the `remap`
/// transform) might rectify the issue of having a semantic meaning with multiple pointers.
///
/// Because of this, we encapsulate this state in an enum. The schema validation step done by the
/// sink builder, will return an error if the definition stores an "invalid" meaning pointer.
#[derive(Clone, Debug, PartialEq, PartialOrd)]
enum MeaningPointer {
    Valid(OwnedTargetPath),
    Invalid(BTreeSet<OwnedTargetPath>),
}

impl MeaningPointer {
    fn merge(self, other: Self) -> Self {
        let set = match (self, other) {
            (Self::Valid(lhs), Self::Valid(rhs)) if lhs == rhs => return Self::Valid(lhs),
            (Self::Valid(lhs), Self::Valid(rhs)) => BTreeSet::from([lhs, rhs]),
            (Self::Valid(lhs), Self::Invalid(mut rhs)) => {
                rhs.insert(lhs);
                rhs
            }
            (Self::Invalid(mut lhs), Self::Valid(rhs)) => {
                lhs.insert(rhs);
                lhs
            }
            (Self::Invalid(mut lhs), Self::Invalid(rhs)) => {
                lhs.extend(rhs);
                lhs
            }
        };

        Self::Invalid(set)
    }
}

impl Definition {
    /// The most general possible definition. The `Kind` is `any`.
    pub fn any() -> Self {
        Self {
            event_kind: Kind::any(),
            metadata_kind: Kind::any(),
            meaning: BTreeMap::default(),
        }
    }

    /// Creates a new definition that is of the event kind specified, and an empty object for metadata.
    /// There are no meanings.
    pub fn new_with_default_metadata(event_kind: Kind) -> Self {
        Self {
            event_kind,
            metadata_kind: Kind::object(Collection::any()),
            meaning: BTreeMap::default(),
        }
    }

    /// Creates a new definition, specifying both the event and metadata kind.
    /// There are no meanings.
    pub fn new(event_kind: Kind, metadata_kind: Kind) -> Self {
        Self {
            event_kind,
            metadata_kind,
            meaning: BTreeMap::default(),
        }
    }

    pub fn default_definition() -> Self {
        Self::new_with_default_metadata(Kind::any())
    }

    pub fn empty_definition() -> Self {
        Self::new_with_default_metadata(Kind::object(Collection::empty()))
    }

    /// Adds the `source_type` and `ingest_timestamp` metadata fields, which are added to every Vector source.
    /// This function should be called in the same order as the values are actually inserted into the event.
    #[must_use]
    pub fn with_standard_vector_source_metadata(self) -> Self {
        let def = self.with_vector_metadata(&owned_value_path!("source_type"), Kind::bytes(), None);

        def.with_metadata_field(
            &owned_value_path!("vector", "ingest_timestamp"),
            Kind::timestamp(),
            None,
        )
    }

    /// Register metadata type information for a source-namespaced field.
    /// Corresponds to `insert_source_metadata`.
    #[must_use]
    pub fn with_source_metadata(
        self,
        source_name: &str,
        vector_path: &OwnedValuePath,
        kind: Kind,
        meaning: Option<&str>,
    ) -> Self {
        self.with_metadata_field(&vector_path.with_field_prefix(source_name), kind, meaning)
    }

    /// Register metadata type information for a vector-namespaced field.
    /// Corresponds to `insert_vector_metadata`.
    #[must_use]
    pub fn with_vector_metadata(
        self,
        vector_path: &OwnedValuePath,
        kind: Kind,
        meaning: Option<&str>,
    ) -> Self {
        self.with_metadata_field(&vector_path.with_field_prefix("vector"), kind, meaning)
    }

    /// Add type information for an event or metadata field.
    /// A non-root required field means the root type must be an object, so the type will be automatically
    /// restricted to an object.
    ///
    /// # Panics
    /// - If the path is not root, and the definition does not allow the type to be an object.
    #[must_use]
    pub fn with_field(
        self,
        target_path: &OwnedTargetPath,
        kind: Kind,
        meaning: Option<&str>,
    ) -> Self {
        match target_path.prefix {
            PathPrefix::Event => self.with_event_field(&target_path.path, kind, meaning),
            PathPrefix::Metadata => self.with_metadata_field(&target_path.path, kind, meaning),
        }
    }

    /// Add type information for an event field.
    /// A non-root required field means the root type must be an object, so the type will be automatically
    /// restricted to an object.
    ///
    /// # Panics
    /// - If the path is not root, and the definition does not allow the type to be an object.
    /// - Provided path has one or more coalesced segments (e.g. `.(foo | bar)`).
    #[must_use]
    pub fn with_event_field(
        mut self,
        path: &OwnedValuePath,
        kind: Kind,
        meaning: Option<&str>,
    ) -> Self {
        if !path.is_root() {
            assert!(
                self.event_kind.as_object().is_some(),
                "Setting a field on a value that cannot be an object"
            );
        }

        self.event_kind.set_at_path(path, kind);

        if let Some(meaning) = meaning {
            self.meaning.insert(
                meaning.to_owned(),
                MeaningPointer::Valid(OwnedTargetPath::event(path.clone())),
            );
        }

        self
    }

    /// Add type information for an event field.
    /// This inserts type information similar to `LogEvent::try_insert`.
    #[must_use]
    pub fn try_with_field(
        mut self,
        path: &OwnedValuePath,
        kind: Kind,
        meaning: Option<&str>,
    ) -> Self {
        let existing_type = self.event_kind.at_path(path);

        if existing_type.is_undefined() {
            // Guaranteed to never be set, so the insertion will always succeed.
            self.with_event_field(path, kind, meaning)
        } else if !existing_type.contains_undefined() {
            // Guaranteed to always be set (or is never), so the insertion will always fail.
            self
        } else {
            // Not sure if the insertion will be successful. The type definition should contain both
            // possibilities. The meaning is not set, since it can't be relied on.

            let success_definition = self.clone().with_event_field(path, kind, None);
            // If the existing type contains `undefined`, the new type will always be used, so remove it.
            self.event_kind
                .set_at_path(path, existing_type.without_undefined());
            self.merge(success_definition)
        }
    }

    /// Add type information for an event field.
    /// A non-root required field means the root type must be an object, so the type will be automatically
    /// restricted to an object.
    ///
    /// # Panics
    /// - If the path is not root, and the definition does not allow the type to be an object
    /// - Provided path has one or more coalesced segments (e.g. `.(foo | bar)`).
    #[must_use]
    pub fn with_metadata_field(
        mut self,
        path: &OwnedValuePath,
        kind: Kind,
        meaning: Option<&str>,
    ) -> Self {
        if !path.is_root() {
            assert!(
                self.metadata_kind.as_object().is_some(),
                "Setting a field on a value that cannot be an object"
            );
        }

        self.metadata_kind.set_at_path(path, kind);

        if let Some(meaning) = meaning {
            self.meaning.insert(
                meaning.to_owned(),
                MeaningPointer::Valid(OwnedTargetPath::metadata(path.clone())),
            );
        }

        self
    }

    /// Add type information for an optional event field.
    ///
    /// # Panics
    ///
    /// See `Definition::require_field`.
    #[must_use]
    pub fn optional_field(self, path: &OwnedValuePath, kind: Kind, meaning: Option<&str>) -> Self {
        self.with_event_field(path, kind.or_undefined(), meaning)
    }

    /// Register a semantic meaning for the definition.
    ///
    /// # Panics
    ///
    /// This method panics if the provided path points to an unknown location in the collection.
    #[must_use]
    pub fn with_meaning(mut self, target_path: OwnedTargetPath, meaning: &str) -> Self {
        self.add_meaning(target_path, meaning);
        self
    }

    /// Adds the meaning pointing to the given path to our list of meanings.
    ///
    /// # Panics
    ///
    /// This method panics if the provided path points to an unknown location in the collection.
    pub fn add_meaning(&mut self, target_path: OwnedTargetPath, meaning: &str) {
        self.try_with_meaning(target_path, meaning)
            .unwrap_or_else(|err| panic!("{}", err));
    }

    /// Register a semantic meaning for the definition.
    ///
    /// # Errors
    ///
    /// Returns an error if the provided path points to an unknown location in the collection.
    pub fn try_with_meaning(
        &mut self,
        target_path: OwnedTargetPath,
        meaning: &str,
    ) -> Result<(), &'static str> {
        match target_path.prefix {
            PathPrefix::Event
                if !self
                    .event_kind
                    .at_path(&target_path.path)
                    .contains_any_defined() =>
            {
                Err("meaning must point to a valid path")
            }

            PathPrefix::Metadata
                if !self
                    .metadata_kind
                    .at_path(&target_path.path)
                    .contains_any_defined() =>
            {
                Err("meaning must point to a valid path")
            }

            _ => {
                self.meaning
                    .insert(meaning.to_owned(), MeaningPointer::Valid(target_path));
                Ok(())
            }
        }
    }

    /// Set the kind for all unknown fields.
    #[must_use]
    pub fn unknown_fields(mut self, unknown: impl Into<Kind>) -> Self {
        let unknown = unknown.into();
        if let Some(object) = self.event_kind.as_object_mut() {
            object.set_unknown(unknown.clone());
        }
        if let Some(array) = self.event_kind.as_array_mut() {
            array.set_unknown(unknown);
        }
        self
    }

    /// Merge `other` definition into `self`.
    ///
    /// This just takes the union of both definitions.
    #[must_use]
    pub fn merge(mut self, other: Self) -> Self {
        for (other_id, other_meaning) in other.meaning {
            let meaning = match self.meaning.remove(&other_id) {
                Some(this_meaning) => this_meaning.merge(other_meaning),
                None => other_meaning,
            };

            self.meaning.insert(other_id, meaning);
        }

        self.event_kind = self.event_kind.union(other.event_kind);
        self.metadata_kind = self.metadata_kind.union(other.metadata_kind);
        self
    }

    /// Returns an `OwnedTargetPath` into an event, based on the provided `meaning`, if the meaning exists.
    pub fn meaning_path(&self, meaning: &str) -> Option<&OwnedTargetPath> {
        match self.meaning.get(meaning) {
            Some(MeaningPointer::Valid(path)) => Some(path),
            None | Some(MeaningPointer::Invalid(_)) => None,
        }
    }

    pub fn invalid_meaning(&self, meaning: &str) -> Option<&BTreeSet<OwnedTargetPath>> {
        match &self.meaning.get(meaning) {
            Some(MeaningPointer::Invalid(paths)) => Some(paths),
            None | Some(MeaningPointer::Valid(_)) => None,
        }
    }

    pub fn meanings(&self) -> impl Iterator<Item = (&String, &OwnedTargetPath)> {
        self.meaning
            .iter()
            .filter_map(|(id, pointer)| match pointer {
                MeaningPointer::Valid(path) => Some((id, path)),
                MeaningPointer::Invalid(_) => None,
            })
    }

    /// Adds the meanings provided by an iterator over the given meanings.
    ///
    /// # Panics
    ///
    /// This method panics if the provided path from any of the incoming meanings point to
    /// an unknown location in the collection.
    pub fn add_meanings<'a>(
        &'a mut self,
        meanings: impl Iterator<Item = (&'a String, &'a OwnedTargetPath)>,
    ) {
        for (meaning, path) in meanings {
            self.add_meaning(path.clone(), meaning);
        }
    }

    pub fn event_kind(&self) -> &Kind {
        &self.event_kind
    }

    pub fn event_kind_mut(&mut self) -> &mut Kind {
        &mut self.event_kind
    }

    pub fn metadata_kind(&self) -> &Kind {
        &self.metadata_kind
    }

    #[allow(clippy::needless_pass_by_value)]
    pub fn kind_at<'a>(&self, target_path: impl TargetPath<'a>) -> Kind {
        match target_path.prefix() {
            PathPrefix::Event => self.event_kind.at_path(target_path.value_path()),
            PathPrefix::Metadata => self.metadata_kind.at_path(target_path.value_path()),
        }
    }
}

#[cfg(any(test, feature = "test"))]
mod test_utils {
    use super::{Definition, Kind};
    use crate::event::Event;
    use vrl::value::Value;

    impl Definition {
        /// Checks that the schema definition is _valid_ for the given event.
        ///
        /// # Errors
        ///
        /// If the definition is not valid, debug info will be returned.
        pub fn is_valid_for_event(&self, event: &Event) -> Result<(), String> {
            if let Some(otel_log) = event.maybe_as_log() {
                let actual_kind = Kind::from(Value::Object(otel_log.as_map().unwrap_or_default()));
                if let Err(path) = self.event_kind.is_superset(&actual_kind) {
                    return Result::Err(format!(
                        "Event value doesn't match at path: {}\n\nEvent type at path = {:?}\n\nDefinition at path = {:?}",
                        path,
                        actual_kind.at_path(&path).debug_info(),
                        self.event_kind.at_path(&path).debug_info()
                    ));
                }

                let actual_metadata_kind = Kind::from(otel_log.metadata().value());
                if let Err(path) = self.metadata_kind.is_superset(&actual_metadata_kind) {
                    return Result::Err(format!(
                        "Event METADATA value doesn't match at path: {}\n\nMetadata type at path = {:?}\n\nDefinition at path = {:?}",
                        path,
                        actual_metadata_kind.at_path(&path).debug_info(),
                        self.metadata_kind.at_path(&path).debug_info()
                    ));
                }
                Ok(())
            } else {
                // schema definitions currently only apply to logs
                Ok(())
            }
        }

        /// Asserts that the schema definition is _valid_ for the given event.
        ///
        /// # Panics
        ///
        /// If the definition is not valid for the event.
        pub fn assert_valid_for_event(&self, event: &Event) {
            if let Err(err) = self.is_valid_for_event(event) {
                panic!("Schema definition assertion failed: {err}");
            }
        }

        /// Asserts that the schema definition is _invalid_ for the given event.
        ///
        /// # Panics
        ///
        /// If the definition is valid for the event.
        pub fn assert_invalid_for_event(&self, event: &Event) {
            assert!(
                self.is_valid_for_event(event).is_err(),
                "Schema definition assertion should not be valid"
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, HashMap};

    use lookup::{lookup_v2::parse_target_path, owned_value_path};
    use vrl::value::Value;

    use super::*;
    use crate::event::{Event, EventMetadata, OtelLog};

    #[test]
    fn test_definition_validity() {
        struct TestCase {
            title: &'static str,
            definition: Definition,
            event: Event,
            valid: bool,
        }

        for TestCase {
            title,
            definition,
            event,
            valid,
        } in [
            TestCase {
                title: "match",
                definition: Definition::new(Kind::any(), Kind::any()),
                event: Event::Log(OtelLog::from(Value::Object(BTreeMap::new()))),
                valid: true,
            },
            TestCase {
                title: "event mismatch",
                definition: Definition::new(Kind::object(Collection::empty()), Kind::any()),
                event: Event::Log(OtelLog::from(Value::Object(BTreeMap::from([(
                    "foo".into(),
                    4.into(),
                )])))),
                valid: false,
            },
            TestCase {
                title: "metadata mismatch",
                definition: Definition::new(Kind::any(), Kind::object(Collection::empty())),
                event: Event::Log(OtelLog::from_value_map(
                    Value::Object(BTreeMap::new()),
                    EventMetadata::default_with_value(
                        BTreeMap::from([("foo".into(), 4.into())]).into(),
                    ),
                )),
                valid: false,
            },
            TestCase {
                title: "event mismatch - null vs undefined",
                definition: Definition::new(Kind::object(Collection::empty()), Kind::any()),
                event: Event::Log(OtelLog::from(Value::Object(BTreeMap::from([(
                    "foo".into(),
                    Value::Null,
                )])))),
                valid: false,
            },
        ] {
            let result = definition.is_valid_for_event(&event);
            assert_eq!(result.is_ok(), valid, "{title}");
        }
    }

    #[test]
    fn test_empty_legacy_field() {
        let definition = Definition::default_definition().with_vector_metadata(
            &owned_value_path!(),
            Kind::integer(),
            None,
        );

        // with_vector_metadata prefixes the path with "vector", so an empty path
        // results in a "vector" metadata field being added
        assert_ne!(definition, Definition::default_definition());
    }

    #[test]
    fn test_required_field() {
        struct TestCase {
            path: OwnedValuePath,
            kind: Kind,
            meaning: Option<&'static str>,
            want: Definition,
        }

        for (
            title,
            TestCase {
                path,
                kind,
                meaning,
                want,
            },
        ) in HashMap::from([
            (
                "simple",
                TestCase {
                    path: owned_value_path!("foo"),
                    kind: Kind::boolean(),
                    meaning: Some("foo_meaning"),
                    want: Definition {
                        event_kind: Kind::object(BTreeMap::from([("foo".into(), Kind::boolean())])),
                        metadata_kind: Kind::object(Collection::empty()),
                        meaning: [(
                            "foo_meaning".to_owned(),
                            MeaningPointer::Valid(parse_target_path("foo").unwrap()),
                        )]
                        .into(),
                    },
                },
            ),
            (
                "nested fields",
                TestCase {
                    path: owned_value_path!("foo", "bar"),
                    kind: Kind::regex().or_null(),
                    meaning: Some("foobar"),
                    want: Definition {
                        event_kind: Kind::object(BTreeMap::from([(
                            "foo".into(),
                            Kind::object(BTreeMap::from([("bar".into(), Kind::regex().or_null())])),
                        )])),
                        metadata_kind: Kind::object(Collection::empty()),
                        meaning: [(
                            "foobar".to_owned(),
                            MeaningPointer::Valid(parse_target_path(".foo.bar").unwrap()),
                        )]
                        .into(),
                    },
                },
            ),
            (
                "no meaning",
                TestCase {
                    path: owned_value_path!("foo"),
                    kind: Kind::boolean(),
                    meaning: None,
                    want: Definition {
                        event_kind: Kind::object(BTreeMap::from([("foo".into(), Kind::boolean())])),
                        metadata_kind: Kind::object(Collection::empty()),
                        meaning: BTreeMap::default(),
                    },
                },
            ),
        ]) {
            let got = Definition::empty_definition().with_event_field(&path, kind, meaning);
            assert_eq!(got.event_kind(), want.event_kind(), "{title}");
        }
    }

    #[test]
    fn test_optional_field() {
        struct TestCase {
            path: OwnedValuePath,
            kind: Kind,
            meaning: Option<&'static str>,
            want: Definition,
        }

        for (
            title,
            TestCase {
                path,
                kind,
                meaning,
                want,
            },
        ) in [
            (
                "simple",
                TestCase {
                    path: owned_value_path!("foo"),
                    kind: Kind::boolean(),
                    meaning: Some("foo_meaning"),
                    want: Definition {
                        event_kind: Kind::object(BTreeMap::from([(
                            "foo".into(),
                            Kind::boolean().or_undefined(),
                        )])),
                        metadata_kind: Kind::object(Collection::any()),
                        meaning: [(
                            "foo_meaning".to_owned(),
                            MeaningPointer::Valid(parse_target_path("foo").unwrap()),
                        )]
                        .into(),
                    },
                },
            ),
            (
                "nested fields",
                TestCase {
                    path: owned_value_path!("foo", "bar"),
                    kind: Kind::regex().or_null(),
                    meaning: Some("foobar"),
                    want: Definition {
                        event_kind: Kind::object(BTreeMap::from([(
                            "foo".into(),
                            Kind::object(BTreeMap::from([(
                                "bar".into(),
                                Kind::regex().or_null().or_undefined(),
                            )])),
                        )])),
                        metadata_kind: Kind::object(Collection::any()),
                        meaning: [(
                            "foobar".to_owned(),
                            MeaningPointer::Valid(parse_target_path(".foo.bar").unwrap()),
                        )]
                        .into(),
                    },
                },
            ),
            (
                "no meaning",
                TestCase {
                    path: owned_value_path!("foo"),
                    kind: Kind::boolean(),
                    meaning: None,
                    want: Definition {
                        event_kind: Kind::object(BTreeMap::from([(
                            "foo".into(),
                            Kind::boolean().or_undefined(),
                        )])),
                        metadata_kind: Kind::object(Collection::any()),
                        meaning: BTreeMap::default(),
                    },
                },
            ),
        ] {
            let mut got = Definition::new_with_default_metadata(Kind::object(BTreeMap::new()));
            got = got.optional_field(&path, kind, meaning);

            assert_eq!(got, want, "{title}");
        }
    }

    #[test]
    fn test_unknown_fields() {
        let want = Definition {
            event_kind: Kind::object(Collection::from_unknown(Kind::bytes().or_integer())),
            metadata_kind: Kind::object(Collection::any()),
            meaning: BTreeMap::default(),
        };

        let mut got = Definition::new_with_default_metadata(Kind::object(Collection::empty()));
        got = got.unknown_fields(Kind::boolean());
        got = got.unknown_fields(Kind::bytes().or_integer());

        assert_eq!(got, want);
    }

    #[test]
    fn test_meaning_path() {
        let def = Definition::new(
            Kind::object(Collection::empty()),
            Kind::object(Collection::empty()),
        )
        .with_event_field(
            &owned_value_path!("foo"),
            Kind::boolean(),
            Some("foo_meaning"),
        )
        .with_metadata_field(
            &owned_value_path!("bar"),
            Kind::boolean(),
            Some("bar_meaning"),
        );

        assert_eq!(
            def.meaning_path("foo_meaning").unwrap(),
            &OwnedTargetPath::event(owned_value_path!("foo"))
        );
        assert_eq!(
            def.meaning_path("bar_meaning").unwrap(),
            &OwnedTargetPath::metadata(owned_value_path!("bar"))
        );
    }

    #[test]
    #[allow(clippy::too_many_lines)]
    fn test_merge() {
        struct TestCase {
            this: Definition,
            other: Definition,
            want: Definition,
        }

        for (title, TestCase { this, other, want }) in HashMap::from([
            (
                "equal definitions",
                TestCase {
                    this: Definition {
                        event_kind: Kind::object(Collection::from(BTreeMap::from([(
                            "foo".into(),
                            Kind::boolean().or_null(),
                        )]))),
                        metadata_kind: Kind::object(Collection::empty()),
                        meaning: BTreeMap::from([(
                            "foo_meaning".to_owned(),
                            MeaningPointer::Valid(parse_target_path("foo").unwrap()),
                        )]),
                    },
                    other: Definition {
                        event_kind: Kind::object(Collection::from(BTreeMap::from([(
                            "foo".into(),
                            Kind::boolean().or_null(),
                        )]))),
                        metadata_kind: Kind::object(Collection::empty()),
                        meaning: BTreeMap::from([(
                            "foo_meaning".to_owned(),
                            MeaningPointer::Valid(parse_target_path("foo").unwrap()),
                        )]),
                    },
                    want: Definition {
                        event_kind: Kind::object(Collection::from(BTreeMap::from([(
                            "foo".into(),
                            Kind::boolean().or_null(),
                        )]))),
                        metadata_kind: Kind::object(Collection::empty()),
                        meaning: BTreeMap::from([(
                            "foo_meaning".to_owned(),
                            MeaningPointer::Valid(parse_target_path("foo").unwrap()),
                        )]),
                    },
                },
            ),
            (
                "this optional, other required",
                TestCase {
                    this: Definition {
                        event_kind: Kind::object(Collection::from(BTreeMap::from([(
                            "foo".into(),
                            Kind::boolean().or_null(),
                        )]))),
                        metadata_kind: Kind::object(Collection::empty()),
                        meaning: BTreeMap::default(),
                    },
                    other: Definition {
                        event_kind: Kind::object(Collection::from(BTreeMap::from([(
                            "foo".into(),
                            Kind::boolean(),
                        )]))),
                        metadata_kind: Kind::object(Collection::empty()),
                        meaning: BTreeMap::default(),
                    },
                    want: Definition {
                        event_kind: Kind::object(Collection::from(BTreeMap::from([(
                            "foo".into(),
                            Kind::boolean().or_null(),
                        )]))),
                        metadata_kind: Kind::object(Collection::empty()),
                        meaning: BTreeMap::default(),
                    },
                },
            ),
            (
                "this required, other optional",
                TestCase {
                    this: Definition {
                        event_kind: Kind::object(Collection::from(BTreeMap::from([(
                            "foo".into(),
                            Kind::boolean(),
                        )]))),
                        metadata_kind: Kind::object(Collection::empty()),
                        meaning: BTreeMap::default(),
                    },
                    other: Definition {
                        event_kind: Kind::object(Collection::from(BTreeMap::from([(
                            "foo".into(),
                            Kind::boolean().or_null(),
                        )]))),
                        metadata_kind: Kind::object(Collection::empty()),
                        meaning: BTreeMap::default(),
                    },
                    want: Definition {
                        event_kind: Kind::object(Collection::from(BTreeMap::from([(
                            "foo".into(),
                            Kind::boolean().or_null(),
                        )]))),
                        metadata_kind: Kind::object(Collection::empty()),
                        meaning: BTreeMap::default(),
                    },
                },
            ),
            (
                "this required, other required",
                TestCase {
                    this: Definition {
                        event_kind: Kind::object(Collection::from(BTreeMap::from([(
                            "foo".into(),
                            Kind::boolean(),
                        )]))),
                        metadata_kind: Kind::object(Collection::empty()),
                        meaning: BTreeMap::default(),
                    },
                    other: Definition {
                        event_kind: Kind::object(Collection::from(BTreeMap::from([(
                            "foo".into(),
                            Kind::boolean(),
                        )]))),
                        metadata_kind: Kind::object(Collection::empty()),
                        meaning: BTreeMap::default(),
                    },
                    want: Definition {
                        event_kind: Kind::object(Collection::from(BTreeMap::from([(
                            "foo".into(),
                            Kind::boolean(),
                        )]))),
                        metadata_kind: Kind::object(Collection::empty()),
                        meaning: BTreeMap::default(),
                    },
                },
            ),
            (
                "same meaning, pointing to different paths",
                TestCase {
                    this: Definition {
                        event_kind: Kind::object(Collection::from(BTreeMap::from([(
                            "foo".into(),
                            Kind::boolean(),
                        )]))),
                        metadata_kind: Kind::object(Collection::empty()),
                        meaning: BTreeMap::from([(
                            "foo".into(),
                            MeaningPointer::Valid(parse_target_path("foo").unwrap()),
                        )]),
                    },
                    other: Definition {
                        event_kind: Kind::object(Collection::from(BTreeMap::from([(
                            "foo".into(),
                            Kind::boolean(),
                        )]))),
                        metadata_kind: Kind::object(Collection::empty()),
                        meaning: BTreeMap::from([(
                            "foo".into(),
                            MeaningPointer::Valid(parse_target_path("bar").unwrap()),
                        )]),
                    },
                    want: Definition {
                        event_kind: Kind::object(Collection::from(BTreeMap::from([(
                            "foo".into(),
                            Kind::boolean(),
                        )]))),
                        metadata_kind: Kind::object(Collection::empty()),
                        meaning: BTreeMap::from([(
                            "foo".into(),
                            MeaningPointer::Invalid(BTreeSet::from([
                                parse_target_path("foo").unwrap(),
                                parse_target_path("bar").unwrap(),
                            ])),
                        )]),
                    },
                },
            ),
            (
                "same meaning, pointing to same path",
                TestCase {
                    this: Definition {
                        event_kind: Kind::object(Collection::from(BTreeMap::from([(
                            "foo".into(),
                            Kind::boolean(),
                        )]))),
                        metadata_kind: Kind::object(Collection::empty()),
                        meaning: BTreeMap::from([(
                            "foo".into(),
                            MeaningPointer::Valid(parse_target_path("foo").unwrap()),
                        )]),
                    },
                    other: Definition {
                        event_kind: Kind::object(Collection::from(BTreeMap::from([(
                            "foo".into(),
                            Kind::boolean(),
                        )]))),
                        metadata_kind: Kind::object(Collection::empty()),
                        meaning: BTreeMap::from([(
                            "foo".into(),
                            MeaningPointer::Valid(parse_target_path("foo").unwrap()),
                        )]),
                    },
                    want: Definition {
                        event_kind: Kind::object(Collection::from(BTreeMap::from([(
                            "foo".into(),
                            Kind::boolean(),
                        )]))),
                        metadata_kind: Kind::object(Collection::empty()),
                        meaning: BTreeMap::from([(
                            "foo".into(),
                            MeaningPointer::Valid(parse_target_path("foo").unwrap()),
                        )]),
                    },
                },
            ),
        ]) {
            let got = this.merge(other);

            assert_eq!(got, want, "{title}");
        }
    }
}
