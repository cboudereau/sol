//! HTTP routing for the query backend (warp filters).
//!
//! Shared across the Grafana APIs — task 3 mounts Loki `query_range`; tasks
//! 4/5/7/13 add the Prometheus / Tempo / SQL filters here.

use std::convert::Infallible;
use std::sync::Arc;

use serde::Deserialize;
use warp::{Filter, Reply, filters::BoxedFilter};

use super::{QueryEngine, loki};

/// Inject the shared `QueryEngine` into a filter chain.
fn with_engine(
    engine: Arc<QueryEngine>,
) -> impl Filter<Extract = (Arc<QueryEngine>,), Error = Infallible> + Clone {
    warp::any().map(move || engine.clone())
}

#[derive(Debug, Deserialize)]
struct LokiQueryParams {
    query: String,
    start: Option<String>,
    end: Option<String>,
    limit: Option<u32>,
    direction: Option<String>,
}

fn parse_ns(s: &Option<String>, default: i64) -> i64 {
    s.as_ref().and_then(|v| v.parse::<i64>().ok()).unwrap_or(default)
}

async fn loki_query_range(
    params: LokiQueryParams,
    engine: Arc<QueryEngine>,
) -> Result<warp::reply::Response, Infallible> {
    let start = parse_ns(&params.start, 0);
    let end = parse_ns(&params.end, i64::MAX);
    let limit = params.limit.unwrap_or(100);
    let forward = params.direction.as_deref() == Some("forward");
    match loki::handle_query_range(&engine, &params.query, start, end, limit, forward).await {
        Ok(resp) => Ok(warp::reply::json(&resp).into_response()),
        Err(error) => {
            let body = serde_json::json!({"status": "error", "error": error.to_string()});
            Ok(warp::reply::with_status(
                warp::reply::json(&body),
                warp::http::StatusCode::BAD_REQUEST,
            )
            .into_response())
        }
    }
}

/// Build the query backend's warp routes against a shared engine.
pub fn make_routes(engine: Arc<QueryEngine>) -> BoxedFilter<(impl Reply,)> {
    let loki = warp::path!("loki" / "api" / "v1" / "query_range")
        .and(warp::get())
        .and(warp::query::<LokiQueryParams>())
        .and(with_engine(engine))
        .and_then(loki_query_range);

    // Discovery probe so Grafana data sources "Save & Test" passes.
    let ready = warp::path!("ready")
        .and(warp::get())
        .map(|| warp::reply::with_status("ready", warp::http::StatusCode::OK));

    loki.or(ready).boxed()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::query::{Options, StorageConfig};
    use datafusion::arrow::array::{StringArray, TimestampNanosecondArray};
    use datafusion::arrow::datatypes::{DataType, Field, Schema, TimeUnit};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::parquet::arrow::ArrowWriter;

    async fn fixture_engine() -> Arc<QueryEngine> {
        let tmp = Box::leak(Box::new(tempfile::tempdir().unwrap()));
        let dir = tmp.path().join("logs").join("dt=2026-06-01");
        std::fs::create_dir_all(&dir).unwrap();
        let schema = Arc::new(Schema::new(vec![
            Field::new("service_name", DataType::Utf8, false),
            Field::new(
                "time_unix_nano",
                DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
                true,
            ),
            Field::new("body", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(vec!["client"])),
                Arc::new(TimestampNanosecondArray::from(vec![10i64]).with_timezone("UTC")),
                Arc::new(StringArray::from(vec!["hello world"])),
            ],
        )
        .unwrap();
        let f = std::fs::File::create(dir.join("f.parquet")).unwrap();
        let mut w = ArrowWriter::try_new(f, schema, None).unwrap();
        w.write(&batch).unwrap();
        w.close().unwrap();

        let opts = Options {
            storage: StorageConfig { path: tmp.path().into(), url: None },
            ..Options::default()
        };
        Arc::new(QueryEngine::new(&opts).await.unwrap())
    }

    #[tokio::test]
    async fn test_loki_route_serves_streams_json() {
        let routes = make_routes(fixture_engine().await);
        let resp = warp::test::request()
            .method("GET")
            .path("/loki/api/v1/query_range?query=%7Bservice_name%3D%22client%22%7D&start=0&end=1000&limit=10")
            .reply(&routes)
            .await;
        assert_eq!(resp.status(), 200);
        let body = String::from_utf8(resp.body().to_vec()).unwrap();
        assert!(body.contains(r#""resultType":"streams""#), "body: {body}");
        assert!(body.contains("hello world"), "body: {body}");
    }

    #[tokio::test]
    async fn test_ready_probe() {
        let routes = make_routes(fixture_engine().await);
        let resp = warp::test::request().method("GET").path("/ready").reply(&routes).await;
        assert_eq!(resp.status(), 200);
    }
}
