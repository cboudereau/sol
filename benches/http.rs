use std::net::SocketAddr;

use criterion::{BatchSize, BenchmarkId, Criterion, SamplingMode, Throughput, criterion_group};
use http_body_util::Empty;
use hyper::Response;
use hyper::body::Bytes;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper_util::rt::TokioIo;
use sol::{
    config,
    sinks::{
        self,
        util::{BatchConfig, Compression},
    },
    sources,
    template::Template,
    test_util::{addr::next_addr, random_lines, runtime, send_lines, start_topology, wait_for_tcp},
};
use sol_lib::codecs::{TextSerializerConfig, encoding::FramingConfig};
use tokio::runtime::Runtime;

fn benchmark_http(c: &mut Criterion) {
    let num_lines: usize = 1_000;
    let line_size: usize = 100;

    let (_guard_0, in_addr) = next_addr();
    let (_guard_1, out_addr) = next_addr();

    let _srv = serve(out_addr);

    let mut group = c.benchmark_group("http");
    group.throughput(Throughput::Bytes((num_lines * line_size) as u64));
    group.sampling_mode(SamplingMode::Flat);

    for compression in [Compression::None, Compression::gzip_default()].iter() {
        group.bench_with_input(
            BenchmarkId::new("compression", compression),
            compression,
            |b, compression| {
                b.iter_batched(
                    || {
                        let mut config = config::Config::builder();
                        config.add_source(
                            "in",
                            sources::socket::SocketConfig::make_basic_tcp_config(in_addr),
                        );
                        let mut batch = BatchConfig::default();
                        batch.max_bytes = Some(num_lines * line_size);

                        config.add_sink(
                            "out",
                            &["in"],
                            sinks::http::config::HttpSinkConfig {
                                uri: Template::try_from(out_addr.to_string()).unwrap(),
                                compression: *compression,
                                method: Default::default(),
                                auth: Default::default(),
                                headers: Default::default(),
                                payload_prefix: Default::default(),
                                payload_suffix: Default::default(),
                                batch,
                                encoding: (None::<FramingConfig>, TextSerializerConfig::default())
                                    .into(),
                                request: Default::default(),
                                tls: Default::default(),
                                acknowledgements: Default::default(),
                            },
                        );

                        let rt = runtime();
                        let topology = rt.block_on(async move {
                            let (topology, _crash) =
                                start_topology(config.build().unwrap(), false).await;
                            wait_for_tcp(in_addr).await;
                            topology
                        });
                        (rt, topology)
                    },
                    |(rt, topology)| {
                        rt.block_on(async move {
                            let lines = random_lines(line_size).take(num_lines);
                            send_lines(in_addr, lines).await.unwrap();
                            topology.stop().await;
                        })
                    },
                    BatchSize::PerIteration,
                )
            },
        );
    }

    group.finish();
}

fn serve(addr: SocketAddr) -> Runtime {
    let rt = runtime();
    rt.spawn(async move {
        let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
        loop {
            let (stream, _) = listener.accept().await.unwrap();
            let io = TokioIo::new(stream);
            tokio::spawn(async move {
                http1::Builder::new()
                    .serve_connection(
                        io,
                        service_fn(|_req| async {
                            Ok::<_, std::convert::Infallible>(Response::new(Empty::<Bytes>::new()))
                        }),
                    )
                    .await
                    .ok();
            });
        }
    });
    rt
}

criterion_group!(benches, benchmark_http);
