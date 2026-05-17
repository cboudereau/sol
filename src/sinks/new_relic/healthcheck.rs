use std::sync::Arc;

use http::Request;

use super::NewRelicCredentials;
use crate::{http::HttpClient, sinks::HealthcheckError};

pub(crate) async fn healthcheck(
    client: HttpClient,
    credentials: Arc<NewRelicCredentials>,
) -> crate::Result<()> {
    let request = Request::post(credentials.get_uri())
        .header("Api-Key", credentials.license_key.clone())
        .body(http_body_util::Full::new(bytes::Bytes::new()))
        .unwrap();

    let response = client.send(request).await?;

    match response.status() {
        status if status.is_success() => Ok(()),
        other => Err(HealthcheckError::UnexpectedStatus { status: other }.into()),
    }
}
