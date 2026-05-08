use sol_lib::configurable::configurable_component;

pub(crate) use crate::schema::Definition;

/// Schema options.
#[configurable_component]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[serde(default, deny_unknown_fields)]
pub struct Options {
    /// Whether or not schema is enabled.
    #[serde(default = "default_enabled")]
    pub enabled: bool,

    /// Whether or not schema validation is enabled.
    #[serde(default = "default_validation")]
    pub validation: bool,
}

impl Options {
    /// Merges two schema options together.
    pub const fn append(&mut self, with: Self, _errors: &mut Vec<String>) {
        // If either config enables these flags, it is enabled.
        self.enabled |= with.enabled;
        self.validation |= with.validation;
    }
}

impl Default for Options {
    fn default() -> Self {
        Self {
            enabled: default_enabled(),
            validation: default_validation(),
        }
    }
}

const fn default_enabled() -> bool {
    false
}

const fn default_validation() -> bool {
    false
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_append() {
        for (test, mut a, b, expected) in [
            (
                "enable schemas",
                Options {
                    enabled: false,
                    validation: false,
                },
                Options {
                    enabled: true,
                    validation: false,
                },
                Options {
                    enabled: true,
                    validation: false,
                },
            ),
            (
                "enable sink requirements",
                Options {
                    enabled: false,
                    validation: false,
                },
                Options {
                    enabled: false,
                    validation: true,
                },
                Options {
                    enabled: false,
                    validation: true,
                },
            ),
        ] {
            let mut errors = vec![];
            a.append(b, &mut errors);
            assert!(errors.is_empty(), "unexpected error: {test}");
            assert_eq!(a, expected, "result mismatch: {test}");
        }
    }
}
