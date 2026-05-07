use std::convert::TryFrom;

use chrono::Utc;
use criterion::{BatchSize, Criterion, criterion_group};
use sol_lib::lookup::{OwnedTargetPath, owned_value_path};
use sol::event::{Event, OtelLog};

fn bench_elasticsearch_index(c: &mut Criterion) {
    use sol::template::Template;

    let mut group = c.benchmark_group("template");

    group.bench_function("dynamic", |b| {
        let index = Template::try_from("index-%Y.%m.%d").unwrap();
        let mut event = Event::Log(OtelLog::from("hello world"));
        event.as_mut_log().insert(
            &OwnedTargetPath::event(owned_value_path!("time_unix_nano")),
            Utc::now(),
        );

        b.iter_batched(
            || event.clone(),
            |event| index.render(&event),
            BatchSize::SmallInput,
        )
    });

    group.bench_function("static", |b| {
        let index = Template::try_from("index").unwrap();
        let mut event = Event::Log(OtelLog::from("hello world"));
        event.as_mut_log().insert(
            &OwnedTargetPath::event(owned_value_path!("time_unix_nano")),
            Utc::now(),
        );

        b.iter_batched(
            || event.clone(),
            |event| index.render(&event),
            BatchSize::SmallInput,
        )
    });

    group.finish();
}

criterion_group!(
    name = benches;
    // encapsulates CI noise we saw in
    // https://github.com/vectordotdev/vector/issues/5394
    config = Criterion::default().noise_threshold(0.20);
    targets = bench_elasticsearch_index
);
