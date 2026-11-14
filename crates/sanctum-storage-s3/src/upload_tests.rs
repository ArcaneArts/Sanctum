use std::sync::{Arc, Mutex};

use axum::{
    Router,
    body::{Body, to_bytes},
    extract::{Query, State},
    routing::post,
};
use http::{Method, Response, StatusCode, header};
use tokio::{
    io::{AsyncReadExt, repeat},
    net::TcpListener,
    sync::Notify,
};

use super::*;

const UPLOAD_ID: &str = "fixture-upload-id";

#[derive(Default)]
struct MultipartFixture {
    requests: Mutex<Vec<(Method, Option<String>)>>,
    aborted: Notify,
}

async fn upload_request(
    State(fixture): State<Arc<MultipartFixture>>,
    Query(query): Query<std::collections::HashMap<String, String>>,
    method: Method,
    body: Body,
) -> Response<Body> {
    let id = query.get("uploadId").cloned();
    fixture
        .requests
        .lock()
        .unwrap()
        .push((method.clone(), id.clone()));
    if method == Method::POST && query.contains_key("uploads") {
        return Response::new(Body::from(concat!(
            "<InitiateMultipartUploadResult><Bucket>fixture</Bucket><Key>object</Key>",
            "<UploadId>fixture-upload-id</UploadId></InitiateMultipartUploadResult>",
        )));
    }
    assert_eq!(id.as_deref(), Some(UPLOAD_ID));
    match method {
        Method::PUT => {
            assert_eq!(query["partNumber"], "1");
            let bytes = to_bytes(body, PART_BYTES).await.unwrap();
            assert_eq!(bytes.len(), PART_BYTES);
            assert!(bytes.iter().all(|byte| *byte == b'x'));
            Response::builder()
                .header(header::ETAG, "\"first-part\"")
                .body(Body::empty())
                .unwrap()
        }
        Method::DELETE => {
            fixture.aborted.notify_one();
            Response::builder()
                .status(StatusCode::NO_CONTENT)
                .body(Body::empty())
                .unwrap()
        }
        _ => panic!("unexpected {method} for an incomplete upload"),
    }
}

#[tokio::test]
async fn dropped_multipart_upload_aborts_the_original_upload_id() {
    let fixture = Arc::new(MultipartFixture::default());
    let router = Router::new()
        .route(
            "/fixture/object",
            post(upload_request)
                .put(upload_request)
                .delete(upload_request),
        )
        .with_state(fixture.clone());
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let storage = S3Storage(
        AmazonS3Builder::new()
            .with_bucket_name("fixture")
            .with_region("us-east-1")
            .with_endpoint(format!("http://{}", listener.local_addr().unwrap()))
            .with_skip_signature(true)
            .with_client_options(ClientOptions::new().with_allow_http(true))
            .with_http_connector(download::CheckedConnector)
            .build()
            .unwrap(),
    );
    let server = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
    let reading_next_part = Arc::new(Notify::new());
    let next_part = reading_next_part.clone();
    let stalled = stream::once(async move {
        next_part.notify_one();
        std::future::pending::<std::io::Result<Bytes>>().await
    });
    let input = repeat(b'x')
        .take(PART_BYTES as u64)
        .chain(StreamReader::new(Box::pin(stalled)));
    let key = Key::parse("object").unwrap();
    let mut upload = Box::pin(storage.put_stream(&key, Box::pin(input), PART_BYTES as u64 + 1));
    tokio::select! {
        result = &mut upload => panic!("upload ended before cancellation: {result:?}"),
        reached = timeout(Duration::from_secs(5), reading_next_part.notified()) => reached.unwrap(),
    }
    drop(upload);
    timeout(Duration::from_secs(5), fixture.aborted.notified())
        .await
        .expect("dropping the upload aborted the open multipart session");
    assert_eq!(
        *fixture.requests.lock().unwrap(),
        [
            (Method::POST, None),
            (Method::PUT, Some(UPLOAD_ID.into())),
            (Method::DELETE, Some(UPLOAD_ID.into())),
        ],
    );
    server.abort();
}
