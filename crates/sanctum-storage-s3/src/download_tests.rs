use std::{
    io,
    sync::{Arc, Mutex},
};

use axum::{Router, body::Body, extract::State, routing::get};
use futures::StreamExt;
use http::{HeaderMap, Response, StatusCode, header};
use object_store::RetryConfig;
use tokio::{net::TcpListener, sync::Notify, time::timeout};

use super::*;

const TEXT: &[u8] = b"0123456789";
const SPLIT: usize = 4;

#[derive(Clone, Copy)]
enum Reply {
    Valid,
    Full,
    WrongRange,
    WrongTotal,
    WrongVersion,
    Compressed,
}

struct Fixture {
    request_range: Option<ByteRange>,
    reply: Reply,
    break_body: Notify,
    requests: Mutex<Vec<Option<String>>>,
}

async fn download(State(state): State<Arc<Fixture>>, headers: HeaderMap) -> Response<Body> {
    let range = headers
        .get(header::RANGE)
        .map(|range| range.to_str().unwrap().to_owned());
    let first = {
        let mut requests = state.requests.lock().unwrap();
        let first = requests.is_empty();
        requests.push(range);
        first
    };
    let original = state.request_range.unwrap_or(ByteRange {
        start: 0,
        end: TEXT.len() as u64,
    });
    let mut response = Response::builder()
        .header(header::ETAG, "\"same-etag\"")
        .header(
            "x-amz-version-id",
            if !first && matches!(state.reply, Reply::WrongVersion) {
                "2"
            } else {
                "1"
            },
        );
    if first {
        response = response.header(header::CONTENT_LENGTH, original.end - original.start);
        if state.request_range.is_some() {
            response = response.status(StatusCode::PARTIAL_CONTENT).header(
                header::CONTENT_RANGE,
                format!(
                    "bytes {}-{}/{}",
                    original.start,
                    original.end - 1,
                    TEXT.len()
                ),
            );
        }
        let partial =
            stream::once(
                async move { Ok(Bytes::from_static(&TEXT[original.start as usize..SPLIT])) },
            )
            .chain(stream::once(async move {
                state.break_body.notified().await;
                Err(io::Error::other("fixture stopped the transfer"))
            }));
        return response.body(Body::from_stream(partial)).unwrap();
    }

    let end = original.end as usize;
    let (status, start, total, bytes) = match state.reply {
        Reply::Full => (StatusCode::OK, 0, TEXT.len(), &TEXT[..end - SPLIT]),
        Reply::WrongRange => (
            StatusCode::PARTIAL_CONTENT,
            SPLIT - 1,
            TEXT.len(),
            &TEXT[SPLIT - 1..end - 1],
        ),
        Reply::WrongTotal => (
            StatusCode::PARTIAL_CONTENT,
            SPLIT,
            TEXT.len() + 1,
            &TEXT[SPLIT..end],
        ),
        _ => (
            StatusCode::PARTIAL_CONTENT,
            SPLIT,
            TEXT.len(),
            &TEXT[SPLIT..end],
        ),
    };
    if matches!(state.reply, Reply::Compressed) {
        response = response.header(header::CONTENT_ENCODING, "br");
    }
    response = response
        .status(status)
        .header(header::CONTENT_LENGTH, bytes.len());
    if status == StatusCode::PARTIAL_CONTENT {
        response = response.header(
            header::CONTENT_RANGE,
            format!("bytes {start}-{}/{total}", start + bytes.len() - 1),
        );
    }
    response.body(Body::from(bytes)).unwrap()
}

async fn start(
    request_range: Option<ByteRange>,
    reply: Reply,
) -> (S3Storage, Arc<Fixture>, tokio::task::JoinHandle<()>) {
    let fixture = Arc::new(Fixture {
        request_range,
        reply,
        break_body: Notify::new(),
        requests: Mutex::new(Vec::new()),
    });
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let router = Router::new()
        .route("/fixture/object", get(download))
        .with_state(fixture.clone());
    let server = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
    let storage = S3Storage(
        AmazonS3Builder::new()
            .with_bucket_name("fixture")
            .with_region("us-east-1")
            .with_endpoint(endpoint)
            .with_skip_signature(true)
            .with_client_options(ClientOptions::new().with_allow_http(true))
            .with_retry(RetryConfig {
                max_retries: 2,
                backoff: object_store::BackoffConfig {
                    init_backoff: Duration::from_millis(10),
                    ..Default::default()
                },
                ..Default::default()
            })
            .with_http_connector(download::CheckedConnector)
            .build()
            .unwrap(),
    );
    (storage, fixture, server)
}

#[tokio::test]
async fn streamed_get_verifies_the_resumed_range_encoding_and_object_version() {
    for response in [
        Reply::Valid,
        Reply::Full,
        Reply::WrongRange,
        Reply::WrongTotal,
        Reply::WrongVersion,
        Reply::Compressed,
    ] {
        let (storage, fixture, server) = start(None, response).await;
        let mut file = storage.get(&Key::parse("object").unwrap()).await.unwrap();
        let mut prefix = [0; SPLIT];
        file.reader.read_exact(&mut prefix).await.unwrap();
        assert_eq!(&prefix, &TEXT[..SPLIT]);
        fixture.break_body.notify_one();
        let mut suffix = Vec::new();
        let result = timeout(Duration::from_secs(5), file.reader.read_to_end(&mut suffix))
            .await
            .unwrap();
        if matches!(response, Reply::Valid) {
            assert_eq!(result.unwrap(), TEXT.len() - SPLIT);
            assert_eq!(suffix, TEXT[SPLIT..]);
        } else {
            assert!(result.is_err());
            assert!(suffix.is_empty());
        }
        assert_eq!(
            *fixture.requests.lock().unwrap(),
            [None, Some("bytes=4-9".into())]
        );
        server.abort();
    }
}

#[tokio::test]
async fn interrupted_bounded_read_keeps_its_original_end() {
    let range = ByteRange { start: 1, end: 8 };
    let (storage, fixture, server) = start(Some(range), Reply::Valid).await;
    let response = storage
        .0
        .get_opts(&Path::from("object"), download::read_options(Some(1..8)))
        .await
        .unwrap();
    let mut stream = response.into_stream();
    let mut prefix = Vec::new();
    while prefix.len() < SPLIT - 1 {
        let part = timeout(Duration::from_secs(5), stream.try_next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        prefix.extend_from_slice(&part);
    }
    assert_eq!(prefix, TEXT[1..SPLIT]);
    fixture.break_body.notify_one();
    let suffix = timeout(Duration::from_secs(5), stream.try_collect::<Vec<_>>())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(suffix.concat(), TEXT[SPLIT..8]);
    assert_eq!(
        *fixture.requests.lock().unwrap(),
        [Some("bytes=1-7".into()), Some("bytes=4-7".into())]
    );
    server.abort();
}
