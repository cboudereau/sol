use mlua::prelude::*;
use vrl::value::Value;

use super::{
    super::{Event, EventMetadata, OtelLog, OtelMetric},
    metric::LuaMetric,
};

pub struct LuaEvent {
    pub event: Event,
    pub metric_multi_value_tags: bool,
}

impl IntoLua for LuaEvent {
    #![allow(clippy::wrong_self_convention)] // this trait is defined by mlua
    fn into_lua(self, lua: &Lua) -> LuaResult<LuaValue> {
        let table = lua.create_table()?;
        match self.event {
            Event::Log(otel_log) => {
                // Always use the canonical proto layout so Lua sees a table,
                // regardless of the internal namespace setting.  `value()`
                // would return only the body scalar in Vector-namespace mode,
                // which is not a Lua table and breaks all field-access code.
                let value = Value::Object(otel_log.as_map().unwrap_or_default());
                table.raw_set("log", value.into_lua(lua)?)?;
            }
            Event::Metric(otel_metric) => {
                table.raw_set(
                    "metric",
                    LuaMetric {
                        otel: otel_metric,
                        multi_value_tags: self.metric_multi_value_tags,
                    }
                    .into_lua(lua)?,
                )?;
            }
            Event::Trace(_) => {
                return Err(LuaError::ToLuaConversionError {
                    from: String::from("Event"),
                    to: "table",
                    message: Some("Trace events are not supported in Lua".to_string()),
                });
            }
        }
        Ok(LuaValue::Table(table))
    }
}

impl FromLua for Event {
    fn from_lua(value: LuaValue, lua: &Lua) -> LuaResult<Self> {
        let LuaValue::Table(table) = &value else {
            return Err(LuaError::FromLuaConversionError {
                from: value.type_name(),
                to: String::from("Event"),
                message: Some("Event should be a Lua table".to_string()),
            });
        };
        match (table.raw_get("log")?, table.raw_get("metric")?) {
            (LuaValue::Table(log), LuaValue::Nil) => {
                // Construct OtelLog directly from the Lua-decoded Value tree,
                // bypassing the LogEvent intermediate.
                let value = Value::from_lua(LuaValue::Table(log), lua)?;
                Ok(Event::Log(OtelLog::from_value_map(
                    value,
                    EventMetadata::default(),
                )))
            }
            (LuaValue::Nil, LuaValue::Table(metric)) => {
                let otel = OtelMetric::from_lua(LuaValue::Table(metric), lua)?;
                Ok(Event::Metric(otel))
            }
            _ => Err(LuaError::FromLuaConversionError {
                from: value.type_name(),
                to: String::from("Event"),
                message: Some(
                    "Event should contain either \"log\" or \"metric\" key at the top level"
                        .to_string(),
                ),
            }),
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::event::{OtelMetric, Value, metric::MetricKind};

    fn assert_event(event: Event, assertions: Vec<&'static str>) {
        let lua = Lua::new();
        lua.globals()
            .set(
                "event",
                LuaEvent {
                    event,
                    metric_multi_value_tags: false,
                },
            )
            .unwrap();
        for assertion in assertions {
            assert!(
                lua.load(assertion).eval::<bool>().expect(assertion),
                "{}",
                assertion
            );
        }
    }

    #[test]
    fn into_lua_log() {
        // Build an OtelLog by round-tripping through Lua FromLua — this
        // exercises the production path (Value tree → OtelLog) without
        // requiring a LogEvent intermediate.
        let lua_event = r#"{ log = { field = "value" } }"#;
        let event = Lua::new().load(lua_event).eval::<Event>().unwrap();

        let assertions = vec![
            "type(event) == 'table'",
            "event.metric == nil",
            "type(event.log) == 'table'",
            "event.log.field == 'value'",
        ];

        assert_event(event, assertions);
    }

    #[test]
    fn into_lua_metric() {
        let event = Event::Metric(OtelMetric::new_counter(
            "example counter",
            MetricKind::Absolute,
            0.577_215_66,
        ));

        let assertions = vec![
            "type(event) == 'table'",
            "event.log == nil",
            "type(event.metric) == 'table'",
            "event.metric.name == 'example counter'",
            "event.metric.counter.value == 0.57721566",
        ];

        assert_event(event, assertions);
    }

    #[test]
    fn from_lua_log() {
        let lua_event = r#"
        {
            log = {
                field = "example",
                nested = {
                    field = "another example"
                }
            }
        }"#;

        let event = Lua::new().load(lua_event).eval::<Event>().unwrap();
        let log = event.as_log();
        assert_eq!(
            log.parse_path_and_get_value("field")
                .ok()
                .flatten()
                .unwrap(),
            Value::Bytes("example".into())
        );
        assert_eq!(
            log.parse_path_and_get_value("nested.field")
                .ok()
                .flatten()
                .unwrap(),
            Value::Bytes("another example".into())
        );
    }

    #[test]
    fn from_lua_metric() {
        let lua_event = r#"
        {
            metric = {
                name = "example counter",
                counter = {
                    value = 0.57721566
                }
            }
        }"#;
        let expected = Event::Metric(OtelMetric::new_counter(
            "example counter",
            MetricKind::Absolute,
            0.577_215_66,
        ));

        let event = Lua::new().load(lua_event).eval::<Event>().unwrap();
        sol_common::assert_event_data_eq!(event, expected);
    }

    #[test]
    // the panic message a) is platform dependent and b) can change if any code is added before this function.
    #[allow(clippy::should_panic_without_expect)]
    #[should_panic]
    fn from_lua_missing_log_and_metric() {
        let lua_event = r"{
            some_field: {}
        }";
        Lua::new().load(lua_event).eval::<Event>().unwrap();
    }
}
