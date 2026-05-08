mod common;
mod serialization;

use std::collections::HashSet;

use super::*;

#[test]
fn event_iteration() {
    let mut log = OtelLog::default();

    log.insert(
        vrl::event_path!("Kesha"),
        "It's going down, I'm yelling timber",
    );
    log.insert(
        vrl::event_path!("Pitbull"),
        "The bigger they are, the harder they fall",
    );

    let all: HashSet<(String, String)> = log
        .all_event_fields()
        .unwrap()
        .into_iter()
        .map(|(k, v)| (k.to_string(), v.to_string_lossy().into_owned()))
        .collect();
    assert_eq!(
        all,
        vec![
            (
                "Pitbull".to_string(),
                "The bigger they are, the harder they fall".to_string()
            ),
            (
                "Kesha".to_string(),
                "It's going down, I'm yelling timber".to_string()
            ),
        ]
        .into_iter()
        .collect::<HashSet<_>>()
    );
}

#[test]
fn event_iteration_order() {
    let mut log = OtelLog::default();
    log.insert(vrl::event_path!("lZDfzKIL"), Value::from("tOVrjveM"));
    log.insert(vrl::event_path!("o9amkaRY"), Value::from("pGsfG7Nr"));
    log.insert(vrl::event_path!("YRjhxXcg"), Value::from("nw8iM5Jr"));

    let collected: Vec<_> = log.all_event_fields().unwrap();
    assert_eq!(
        collected,
        vec![
            ("YRjhxXcg".into(), Value::from("nw8iM5Jr")),
            ("lZDfzKIL".into(), Value::from("tOVrjveM")),
            ("o9amkaRY".into(), Value::from("pGsfG7Nr")),
        ]
    );
}
