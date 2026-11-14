use std::sync::Mutex;

use axum::{
    Json, Router,
    body::Body,
    extract::{Path, Query, State},
    response::IntoResponse,
    routing::{get, post, put},
};
use futures::StreamExt;
use serde_json::json;
use tokio::{io::repeat, net::TcpListener, sync::Notify};

use super::*;

struct TestCredential;

#[async_trait::async_trait]
impl Credential for TestCredential {
    async fn token(&self) -> Result<String> {
        Ok("local-fixture-token".into())
    }
}

#[derive(Default)]
struct Fixture {
    writes: Mutex<Vec<String>>,
    parts: Mutex<Vec<String>>,
    reads: Mutex<Vec<String>>,
    lists: Mutex<Vec<Option<String>>>,
    interrupt_download: Arc<Notify>,
}

async fn start() -> (GcsStorage, Arc<Fixture>, tokio::task::JoinHandle<()>) {
    let fixture = Arc::new(Fixture::default());
    let router = Router::new()
        .route("/upload/storage/v1/b/sanctum-test/o", post(upload))
        .route("/storage/v1/b/sanctum-test/o", get(list))
        .route("/storage/v1/b/sanctum-test/o/{key}", get(object))
        .route("/session", put(part))
        .with_state(fixture.clone());
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = Url::parse(&format!("http://{}/", listener.local_addr().unwrap())).unwrap();
    let server = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
    let storage =
        GcsStorage::with_endpoint("sanctum-test".into(), url, Arc::new(TestCredential)).unwrap();
    (storage, fixture, server)
}

fn assert_credentials(headers: &HeaderMap) {
    assert_eq!(headers[header::AUTHORIZATION], "Bearer local-fixture-token");
    assert_eq!(headers[header::ACCEPT_ENCODING], "identity");
}

async fn upload(
    State(fixture): State<Arc<Fixture>>,
    Query(query): Query<std::collections::HashMap<String, String>>,
    headers: HeaderMap,
    body: Bytes,
) -> axum::response::Response {
    assert_credentials(&headers);
    assert_eq!(query["fields"], "name,size,etag,generation,updated");
    let generation = &query["ifGenerationMatch"];
    fixture.writes.lock().unwrap().push(generation.clone());
    let key = &query["name"];
    if query["uploadType"] == "resumable" {
        assert_eq!(generation, "0");
        assert_eq!(key, "default/notes/bulk");
        assert_eq!(headers["x-upload-content-length"], "8388615");
        assert!(body.is_empty());
        return (StatusCode::OK, [(header::LOCATION, "/session")]).into_response();
    }
    assert_eq!(key, "default/notes/HEAD.json");
    assert_eq!(&body[..], b"control");
    match generation.as_str() {
        "0" => Json(metadata(key, 7, 11)).into_response(),
        "11" => Json(metadata(key, 7, 12)).into_response(),
        _ => StatusCode::PRECONDITION_FAILED.into_response(),
    }
}

async fn part(
    State(fixture): State<Arc<Fixture>>,
    headers: HeaderMap,
    body: Body,
) -> axum::response::Response {
    assert!(headers.get(header::AUTHORIZATION).is_none());
    let range = headers[header::CONTENT_RANGE].to_str().unwrap();
    fixture.parts.lock().unwrap().push(range.into());
    let bytes = axum::body::to_bytes(body, PART_BYTES as usize)
        .await
        .unwrap();
    assert!(bytes.iter().all(|byte| *byte == b'z'));
    match range {
        "bytes 0-8388607/8388615" => {
            assert_eq!(bytes.len(), PART_BYTES as usize);
            (
                StatusCode::PERMANENT_REDIRECT,
                [(header::RANGE, "bytes=0-8388607")],
            )
                .into_response()
        }
        "bytes 8388608-8388614/8388615" => {
            assert_eq!(bytes.len(), 7);
            Json(metadata("default/notes/bulk", 8_388_615, 13)).into_response()
        }
        _ => StatusCode::BAD_REQUEST.into_response(),
    }
}

async fn list(
    State(fixture): State<Arc<Fixture>>,
    Query(query): Query<std::collections::HashMap<String, String>>,
    headers: HeaderMap,
) -> axum::response::Response {
    assert_credentials(&headers);
    assert_eq!(
        query["fields"],
        "nextPageToken,items(name,size,etag,generation,updated)"
    );
    let token = query.get("pageToken");
    let attempt = {
        let mut requests = fixture.lists.lock().unwrap();
        requests.push(token.cloned());
        requests
            .iter()
            .filter(|previous| previous.as_ref() == token)
            .count()
    };
    if query["prefix"] == "usage/" {
        assert_eq!(query["maxResults"], "1000");
        return match (token.map(String::as_str), attempt) {
            (None, 1) => Json(json!({
                "items": [metadata("usage/a", 4, 9)], "nextPageToken": "opaque/+ token="
            }))
            .into_response(),
            (Some("opaque/+ token="), 1) => StatusCode::TOO_MANY_REQUESTS.into_response(),
            (Some("opaque/+ token="), 2) => {
                let body = stream::iter([Ok(Bytes::from_static(b"{\"items\":["))]).chain(
                    stream::once(async {
                        tokio::time::sleep(Duration::from_millis(10)).await;
                        Err(io::Error::new(
                            io::ErrorKind::ConnectionReset,
                            "listing body interrupted",
                        ))
                    }),
                );
                Body::from_stream(body).into_response()
            }
            (Some("opaque/+ token="), 3) => Json(json!({
                "items": [metadata("usage/b", 13, 10)]
            }))
            .into_response(),
            _ => panic!("unexpected usage list attempt {attempt}"),
        };
    }
    if query["prefix"] == "maximum/" {
        assert_eq!(query["maxResults"], "1000");
        let segment = "a".repeat(250);
        let last = "z".repeat(8);
        let items: Vec<_> = (0..1000)
            .map(|id| {
                let key = format!("maximum/{segment}/{segment}/{segment}/{segment}/{id:04}{last}");
                assert_eq!(key.len(), 1024);
                metadata(&key, 1, id + 1)
            })
            .collect();
        return Json(json!({ "items": items, "nextPageToken": "more/+ =token" })).into_response();
    }
    assert_eq!(query["prefix"], "default/notes/");
    assert_eq!(query["maxResults"], "1");
    match token.map(String::as_str) {
        None => Json(
            json!({"items": [metadata("default/notes/a", 4, 9)], "nextPageToken": "opaque-token"}),
        )
        .into_response(),
        Some("opaque-token") => Json(json!({})).into_response(),
        Some("unavailable") => StatusCode::SERVICE_UNAVAILABLE.into_response(),
        Some("forbidden") => StatusCode::FORBIDDEN.into_response(),
        Some("oversized") => oversized_metadata(LIST_PAGE_OVERHEAD_BYTES + LIST_ITEM_BYTES),
        Some("too-many") => Json(json!({
            "items": [metadata("default/notes/a", 4, 9), metadata("default/notes/b", 6, 10)]
        }))
        .into_response(),
        Some("invalid") => "{\"items\":[".into_response(),
        Some("stalled") if attempt == 1 => std::future::pending().await,
        Some("stalled") => Json(json!({})).into_response(),
        _ => StatusCode::BAD_REQUEST.into_response(),
    }
}

fn oversized_metadata(limit: usize) -> axum::response::Response {
    let chunks = stream::iter(
        (0..=limit / 1024).map(|_| Ok::<_, io::Error>(Bytes::from_static(&[b' '; 1024]))),
    )
    .chain(stream::pending());
    Body::from_stream(chunks).into_response()
}

async fn object(
    State(fixture): State<Arc<Fixture>>,
    Path(key): Path<String>,
    Query(query): Query<std::collections::HashMap<String, String>>,
    headers: HeaderMap,
) -> axum::response::Response {
    assert_credentials(&headers);
    if !query.contains_key("alt") {
        assert_eq!(query["fields"], "name,size,etag,generation,updated");
        if key.ends_with("/oversized") {
            return oversized_metadata(MAX_CONTROL_BYTES);
        }
    } else {
        assert!(!query.contains_key("fields"));
    }
    if let Some(scenario) = key.strip_prefix("default/notes/resume-") {
        return download_object(&fixture, &key, scenario, &query, &headers);
    }
    if let Some(scenario) = key.strip_prefix("default/notes/range-") {
        assert_eq!(query["alt"], "media");
        assert_eq!(headers[header::RANGE], "bytes=1-4");
        let attempt = {
            let mut calls = fixture.reads.lock().unwrap();
            calls.push("range:1-4".into());
            calls.len()
        };
        return match (scenario, attempt) {
            ("recover", 1) => StatusCode::REQUEST_TIMEOUT.into_response(),
            ("recover", 2) => {
                let body =
                    stream::iter([Ok(Bytes::from_static(b"bc"))]).chain(stream::once(async {
                        tokio::time::sleep(Duration::from_millis(20)).await;
                        Err(io::Error::new(
                            io::ErrorKind::ConnectionReset,
                            "interrupted range",
                        ))
                    }));
                (
                    StatusCode::PARTIAL_CONTENT,
                    [(header::CONTENT_RANGE, "bytes 1-4/6")],
                    Body::from_stream(body),
                )
                    .into_response()
            }
            ("recover", 3) => (
                StatusCode::PARTIAL_CONTENT,
                [(header::CONTENT_RANGE, "bytes 1-4/6")],
                "bcde",
            )
                .into_response(),
            ("stall", 1) => std::future::pending().await,
            ("stall", 2) => (
                StatusCode::PARTIAL_CONTENT,
                [(header::CONTENT_RANGE, "bytes 1-4/6")],
                "bcde",
            )
                .into_response(),
            ("unavailable", _) => StatusCode::SERVICE_UNAVAILABLE.into_response(),
            ("missing", _) => StatusCode::NOT_FOUND.into_response(),
            ("wrong", _) => (
                StatusCode::PARTIAL_CONTENT,
                [(header::CONTENT_RANGE, "bytes 0-3/6")],
                "abcd",
            )
                .into_response(),
            _ => panic!("unexpected range attempt {attempt} for {scenario}"),
        };
    }
    if let Some(scenario) = key.strip_prefix("default/notes/open-") {
        return opening_object(
            &fixture,
            &key,
            scenario,
            query.get("ifGenerationMatch").map(String::as_str),
        )
        .await;
    }
    if key.ends_with("/replaced") || key.ends_with("/removed") || key.ends_with("/changing") {
        return changing_object(
            &fixture,
            &key,
            query.get("ifGenerationMatch").map(String::as_str),
        );
    }
    if !query.contains_key("alt") {
        return Json(metadata(&key, 6, 11)).into_response();
    }
    assert_eq!(query["alt"], "media");
    if let Some(range) = headers.get(header::RANGE) {
        assert_eq!(range, "bytes=1-4");
        return match key.as_str() {
            "default/notes/full" => (StatusCode::OK, "abcdef").into_response(),
            "default/notes/encoding" => (
                StatusCode::PARTIAL_CONTENT,
                [
                    (header::CONTENT_RANGE, "bytes 1-4/6"),
                    (header::CONTENT_ENCODING, "gzip"),
                ],
                "bcde",
            )
                .into_response(),
            "default/notes/truncated" => (
                StatusCode::PARTIAL_CONTENT,
                [(header::CONTENT_RANGE, "bytes 1-4/6")],
                "bcd",
            )
                .into_response(),
            "default/notes/wrong" => (
                StatusCode::PARTIAL_CONTENT,
                [(header::CONTENT_RANGE, "bytes 0-3/6")],
                "abcd",
            )
                .into_response(),
            _ => (
                StatusCode::PARTIAL_CONTENT,
                [(header::CONTENT_RANGE, "bytes 1-4/6")],
                "bcde",
            )
                .into_response(),
        };
    }
    assert_eq!(query["ifGenerationMatch"], "11");
    "abcdef".into_response()
}

async fn opening_object(
    fixture: &Fixture,
    key: &str,
    scenario: &str,
    condition: Option<&str>,
) -> axum::response::Response {
    let attempt = {
        let mut calls = fixture.reads.lock().unwrap();
        calls.push(condition.unwrap_or("metadata").into());
        calls.len()
    };
    match (scenario, attempt) {
        ("retry", 1) => StatusCode::TOO_MANY_REQUESTS.into_response(),
        ("retry", 2) => Json(metadata(key, 6, 11)).into_response(),
        ("retry", 3) => {
            assert_eq!(condition, Some("11"));
            StatusCode::SERVICE_UNAVAILABLE.into_response()
        }
        ("retry", 4) | ("stalled", 2) => Json(metadata(key, 6, 12)).into_response(),
        ("retry", 5) | ("stalled", 3) => {
            assert_eq!(condition, Some("12"));
            "xyzabc".into_response()
        }
        ("stalled", 1) => std::future::pending().await,
        ("unavailable", _) => StatusCode::REQUEST_TIMEOUT.into_response(),
        ("forbidden", 1) => StatusCode::FORBIDDEN.into_response(),
        ("wrong-key", 1) => Json(metadata("default/notes/unrelated", 6, 11)).into_response(),
        _ => panic!("unexpected download attempt {attempt} for {scenario}"),
    }
}

fn changing_object(
    fixture: &Fixture,
    key: &str,
    condition: Option<&str>,
) -> axum::response::Response {
    let mut reads = fixture.reads.lock().unwrap();
    let attempt = reads.len() / 2;
    let generation = 11 + attempt as u64;
    let body = "new manifest";
    match condition {
        None => {
            reads.push("metadata".into());
            if key.ends_with("/removed") && attempt > 0 {
                return StatusCode::NOT_FOUND.into_response();
            }
            let length = if attempt == 0 { 6 } else { body.len() as u64 };
            Json(metadata(key, length, generation)).into_response()
        }
        Some(condition) => {
            reads.push(condition.into());
            assert_eq!(condition, generation.to_string());
            if key.ends_with("/removed") {
                StatusCode::NOT_FOUND.into_response()
            } else if attempt == 0 || key.ends_with("/changing") {
                StatusCode::PRECONDITION_FAILED.into_response()
            } else {
                body.into_response()
            }
        }
    }
}

fn metadata(name: &str, size: u64, generation: u64) -> serde_json::Value {
    json!({
        "name": name, "size": size.to_string(), "generation": generation.to_string(),
        "etag": "same-etag", "updated": "2026-11-13T09:00:00.123Z",
    })
}

fn download_object(
    fixture: &Fixture,
    key: &str,
    scenario: &str,
    query: &std::collections::HashMap<String, String>,
    headers: &HeaderMap,
) -> axum::response::Response {
    let mut calls = fixture.reads.lock().unwrap();
    if !query.contains_key("alt") {
        calls.push("metadata".into());
        return Json(metadata(key, 10, 11)).into_response();
    }
    assert_eq!(query["alt"], "media");
    assert_eq!(query["ifGenerationMatch"], "11");
    let range = header_value(headers, header::RANGE);
    calls.push(range.unwrap_or("full").into());
    if range.is_none() {
        let ready = fixture.interrupt_download.clone();
        let body = futures::stream::iter([Ok(Bytes::from_static(b"abc"))]).chain(
            futures::stream::once(async move {
                ready.notified().await;
                Err(io::Error::new(
                    io::ErrorKind::ConnectionReset,
                    "fixture interrupted the body",
                ))
            }),
        );
        return (StatusCode::OK, Body::from_stream(body)).into_response();
    }
    match (scenario, calls.len()) {
        ("success", 3) => StatusCode::SERVICE_UNAVAILABLE.into_response(),
        ("success", 4) => (
            StatusCode::PARTIAL_CONTENT,
            [(header::CONTENT_RANGE, "bytes 3-9/10")],
            Body::from_stream(futures::stream::iter([Ok::<_, io::Error>(
                Bytes::from_static(b"def"),
            )])),
        )
            .into_response(),
        ("success", 5) => (
            StatusCode::PARTIAL_CONTENT,
            [(header::CONTENT_RANGE, "bytes 6-9/10")],
            "ghij",
        )
            .into_response(),
        ("replaced", 3) => StatusCode::PRECONDITION_FAILED.into_response(),
        ("wrong-total", 3) => (
            StatusCode::PARTIAL_CONTENT,
            [(header::CONTENT_RANGE, "bytes 3-9/11")],
            "defghij",
        )
            .into_response(),
        ("stalled", _) => (
            StatusCode::PARTIAL_CONTENT,
            [(header::CONTENT_RANGE, "bytes 3-9/10")],
            Body::from_stream(futures::stream::empty::<io::Result<Bytes>>()),
        )
            .into_response(),
        _ => panic!("unexpected resumed download for {scenario}"),
    }
}

#[tokio::test]
async fn native_generation_conditions_and_resumable_offsets() {
    let (storage, fixture, server) = start().await;
    let control = Key::parse("default/notes/HEAD.json").unwrap();
    let bytes = Bytes::from_static(b"control");
    let created = storage
        .put_control(&control, bytes.clone(), Condition::Create)
        .await
        .unwrap();
    assert_eq!(created.generation.as_deref(), Some("11"));
    let changed = storage
        .put_control(&control, bytes.clone(), Condition::Match(created))
        .await
        .unwrap();
    assert_eq!(changed.generation.as_deref(), Some("12"));
    assert!(matches!(
        storage
            .put_control(
                &control,
                bytes,
                Condition::Match(Version {
                    etag: changed.etag,
                    generation: Some("10".into())
                })
            )
            .await,
        Err(Error::Conflict(_))
    ));
    assert_eq!(&*fixture.writes.lock().unwrap(), &["0", "11", "10"]);
    let bulk = Key::parse("default/notes/bulk").unwrap();
    let length = PART_BYTES + 7;
    let version = storage
        .put_stream(&bulk, Box::pin(repeat(b'z').take(length)), length)
        .await
        .unwrap();
    assert_eq!(version.generation.as_deref(), Some("13"));
    assert_eq!(fixture.parts.lock().unwrap().len(), 2);
    server.abort();
}

#[tokio::test]
async fn native_downloads_are_generation_fenced_and_ranges_are_exact() {
    let (storage, _, server) = start().await;
    let key = Key::parse("default/notes/text").unwrap();
    let mut download = storage.get(&key).await.unwrap();
    assert_eq!(download.object.version.generation.as_deref(), Some("11"));
    let mut result = Vec::new();
    download.reader.read_to_end(&mut result).await.unwrap();
    assert_eq!(result, b"abcdef");
    let range = ByteRange { start: 1, end: 5 };
    assert_eq!(storage.read_range(&key, range).await.unwrap(), "bcde");
    for name in ["full", "encoding", "truncated", "wrong"] {
        let key = Key::parse(format!("default/notes/{name}")).unwrap();
        assert!(storage.read_range(&key, range).await.is_err(), "{name}");
    }
    let prefix = Key::parse("default/notes").unwrap();
    let page = storage.list(Some(&prefix), None, 1).await.unwrap();
    assert_eq!(page.objects[0].length, 4);
    assert!(
        storage
            .list(Some(&prefix), page.next_cursor.as_deref(), 1)
            .await
            .unwrap()
            .next_cursor
            .is_none()
    );
    server.abort();
}

#[tokio::test]
async fn usage_retries_the_same_gcs_page_without_counting_it_twice() {
    let (storage, fixture, server) = start().await;
    let prefix = Key::parse("usage").unwrap();
    let mut progress = Vec::new();
    let measured = sanctum_storage::usage_with_progress(&storage, Some(&prefix), |bytes, files| {
        progress.push((bytes, files));
        Ok(())
    })
    .await
    .unwrap();
    assert_eq!(progress, [(4, 1)]);
    assert_eq!((measured.bytes, measured.files), (17, 2));
    assert_eq!(
        *fixture.lists.lock().unwrap(),
        [
            None,
            Some("opaque/+ token=".into()),
            Some("opaque/+ token=".into()),
            Some("opaque/+ token=".into())
        ]
    );
    server.abort();
}

#[tokio::test]
async fn list_page_bounds_retries_and_preserves_permanent_errors() {
    let (storage, fixture, server) = start().await;
    let prefix = Key::parse("default/notes").unwrap();
    assert!(matches!(
        storage.list(Some(&prefix), Some("unavailable"), 1).await,
        Err(Error::Busy(_))
    ));
    assert_eq!(fixture.lists.lock().unwrap().len(), 3);
    assert!(matches!(
        storage.list(Some(&prefix), Some("forbidden"), 1).await,
        Err(Error::Invalid(_))
    ));
    assert_eq!(fixture.lists.lock().unwrap().len(), 4);
    assert!(
        matches!(storage.list(Some(&prefix), Some("invalid"), 1).await,
        Err(Error::Io(error)) if error.kind() == io::ErrorKind::InvalidData)
    );
    assert_eq!(fixture.lists.lock().unwrap().len(), 5);
    server.abort();
}

#[tokio::test]
async fn gcs_metadata_is_bounded_and_preserves_valid_maximum_pages() {
    let (storage, fixture, server) = start().await;
    let prefix = Key::parse("maximum").unwrap();
    let page = storage.list(Some(&prefix), None, 1000).await.unwrap();
    assert_eq!(page.objects.len(), 1000);
    assert_eq!(
        page.objects[999].version.generation.as_deref(),
        Some("1000")
    );
    assert_eq!(page.next_cursor.as_deref(), Some("more/+ =token"));

    let prefix = Key::parse("default/notes").unwrap();
    for token in ["oversized", "too-many"] {
        let result = tokio::time::timeout(
            Duration::from_secs(3),
            storage.list(Some(&prefix), Some(token), 1),
        )
        .await
        .expect("respond without waiting for the oversized stream to close");
        assert!(matches!(result,
            Err(Error::Io(error)) if error.kind() == io::ErrorKind::InvalidData
        ));
    }
    assert_eq!(
        *fixture.lists.lock().unwrap(),
        [None, Some("oversized".into()), Some("too-many".into())],
    );
    let key = Key::parse("default/notes/oversized").unwrap();
    let result = tokio::time::timeout(Duration::from_secs(3), storage.head(&key))
        .await
        .expect("stop reading oversized object metadata");
    assert!(matches!(result,
        Err(Error::Io(error)) if error.kind() == io::ErrorKind::InvalidData
    ));
    server.abort();
}

#[tokio::test]
async fn list_page_retries_a_stalled_response() {
    let (storage, fixture, server) = start().await;
    let mut url = storage.objects_url(false, None);
    url.query_pairs_mut()
        .append_pair("prefix", "default/notes/")
        .append_pair("maxResults", "1")
        .append_pair("fields", LIST_FIELDS)
        .append_pair("pageToken", "stalled");
    let page = storage
        .list_page(url, 1, Duration::from_millis(250))
        .await
        .unwrap();
    assert!(page.objects.is_empty() && page.next_cursor.is_none());
    assert_eq!(
        *fixture.lists.lock().unwrap(),
        [Some("stalled".into()), Some("stalled".into())]
    );
    server.abort();
}

#[tokio::test]
async fn range_read_recovers_after_timeout_and_an_interrupted_partial_body() {
    let (storage, fixture, server) = start().await;
    let key = Key::parse("default/notes/range-recover").unwrap();
    let range = ByteRange { start: 1, end: 5 };
    assert_eq!(storage.read_range(&key, range).await.unwrap(), "bcde");
    assert_eq!(*fixture.reads.lock().unwrap(), ["range:1-4"; 3]);
    server.abort();
}

#[tokio::test]
async fn range_read_bounds_retries_and_preserves_permanent_errors() {
    let range = ByteRange { start: 1, end: 5 };
    for (scenario, expected_attempts) in [("unavailable", 3), ("missing", 1), ("wrong", 1)] {
        let (storage, fixture, server) = start().await;
        let key = Key::parse(format!("default/notes/range-{scenario}")).unwrap();
        let error = storage.read_range(&key, range).await.unwrap_err();
        match scenario {
            "unavailable" => assert!(matches!(error, Error::Busy(_))),
            "missing" => assert!(matches!(error, Error::NotFound(_))),
            "wrong" => assert!(matches!(error, Error::Io(_))),
            _ => unreachable!(),
        }
        assert_eq!(
            *fixture.reads.lock().unwrap(),
            vec!["range:1-4"; expected_attempts]
        );
        server.abort();
    }
}

#[tokio::test]
async fn range_read_can_retry_a_stalled_request() {
    let (storage, fixture, server) = start().await;
    let key = Key::parse("default/notes/range-stall").unwrap();
    let range = ByteRange { start: 1, end: 5 };
    assert_eq!(storage.read_range(&key, range).await.unwrap(), "bcde");
    assert_eq!(*fixture.reads.lock().unwrap(), ["range:1-4"; 2]);
    server.abort();
}

#[tokio::test]
async fn download_retries_a_generation_replaced_between_metadata_and_media() {
    let (storage, fixture, server) = start().await;
    let key = Key::parse("default/notes/replaced").unwrap();
    let mut download = storage.get(&key).await.unwrap();
    assert_eq!(download.object.version.generation.as_deref(), Some("12"));
    assert_eq!(download.object.length, 12);
    let mut body = String::new();
    download.reader.read_to_string(&mut body).await.unwrap();
    assert_eq!(body, "new manifest");
    assert_eq!(
        *fixture.reads.lock().unwrap(),
        ["metadata", "11", "metadata", "12"]
    );
    server.abort();
}

#[tokio::test]
async fn download_rechecks_metadata_after_the_object_disappears() {
    let (storage, fixture, server) = start().await;
    let key = Key::parse("default/notes/removed").unwrap();
    assert!(matches!(storage.get(&key).await, Err(Error::NotFound(_))));
    assert_eq!(
        *fixture.reads.lock().unwrap(),
        ["metadata", "11", "metadata"]
    );
    server.abort();
}

#[tokio::test]
async fn download_returns_retryable_error_when_every_generation_is_replaced() {
    let (storage, fixture, server) = start().await;
    let key = Key::parse("default/notes/changing").unwrap();
    assert!(matches!(storage.get(&key).await, Err(Error::Busy(_))));
    assert_eq!(
        *fixture.reads.lock().unwrap(),
        [
            "metadata", "11", "metadata", "12", "metadata", "13", "metadata", "14"
        ],
    );
    server.abort();
}

#[tokio::test]
async fn download_retries_transient_metadata_and_media_errors_before_returning_a_reader() {
    let (storage, fixture, server) = start().await;
    let key = Key::parse("default/notes/open-retry").unwrap();
    let mut download = storage.get(&key).await.unwrap();
    assert_eq!(download.object.version.generation.as_deref(), Some("12"));
    let mut body = String::new();
    download.reader.read_to_string(&mut body).await.unwrap();
    assert_eq!(body, "xyzabc");
    assert_eq!(
        *fixture.reads.lock().unwrap(),
        ["metadata", "metadata", "11", "metadata", "12"]
    );
    server.abort();
}

#[tokio::test]
async fn download_retries_a_stalled_metadata_request() {
    let (storage, fixture, server) = start().await;
    let key = Key::parse("default/notes/open-stalled").unwrap();
    let download = storage
        .open_download(&key, Duration::from_millis(250))
        .await
        .unwrap();
    assert_eq!(download.object.version.generation.as_deref(), Some("12"));
    assert_eq!(
        *fixture.reads.lock().unwrap(),
        ["metadata", "metadata", "12"]
    );
    server.abort();
}

#[tokio::test]
async fn download_open_bounds_retries_and_preserves_permanent_errors() {
    let (storage, fixture, server) = start().await;
    let key = Key::parse("default/notes/open-unavailable").unwrap();
    assert!(matches!(storage.get(&key).await, Err(Error::Busy(_))));
    assert_eq!(*fixture.reads.lock().unwrap(), ["metadata"; 4]);

    fixture.reads.lock().unwrap().clear();
    let key = Key::parse("default/notes/open-forbidden").unwrap();
    assert!(matches!(storage.get(&key).await, Err(Error::Invalid(_))));
    assert_eq!(*fixture.reads.lock().unwrap(), ["metadata"]);

    fixture.reads.lock().unwrap().clear();
    let key = Key::parse("default/notes/open-wrong-key").unwrap();
    assert!(matches!(storage.get(&key).await, Err(Error::Io(error))
        if error.kind() == io::ErrorKind::InvalidData));
    assert_eq!(*fixture.reads.lock().unwrap(), ["metadata"]);
    server.abort();
}

#[tokio::test]
async fn full_download_resumes_the_same_generation_at_delivered_byte_offsets() {
    let (storage, fixture, server) = start().await;
    let key = Key::parse("default/notes/resume-success").unwrap();
    let mut download = storage.get(&key).await.unwrap();
    assert_eq!(download.object.version.generation.as_deref(), Some("11"));
    let mut result = interrupt_body(&mut download, &fixture).await;
    download.reader.read_to_end(&mut result).await.unwrap();
    assert_eq!(result, b"abcdefghij");
    assert_eq!(
        *fixture.reads.lock().unwrap(),
        ["metadata", "full", "bytes=3-9", "bytes=3-9", "bytes=6-9"],
    );
    server.abort();
}

#[tokio::test]
async fn full_download_rejects_a_replaced_generation_or_different_length() {
    for name in ["replaced", "wrong-total"] {
        let (storage, fixture, server) = start().await;
        let key = Key::parse(format!("default/notes/resume-{name}")).unwrap();
        let mut download = storage.get(&key).await.unwrap();
        let mut result = interrupt_body(&mut download, &fixture).await;
        assert!(download.reader.read_to_end(&mut result).await.is_err());
        assert_eq!(result, b"abc");
        assert_eq!(
            *fixture.reads.lock().unwrap(),
            ["metadata", "full", "bytes=3-9"]
        );
        server.abort();
    }
}

#[tokio::test]
async fn full_download_stops_retrying_when_no_more_bytes_arrive() {
    let (storage, fixture, server) = start().await;
    let key = Key::parse("default/notes/resume-stalled").unwrap();
    let mut download = storage.get(&key).await.unwrap();
    let mut result = interrupt_body(&mut download, &fixture).await;
    let error = download.reader.read_to_end(&mut result).await.unwrap_err();
    assert!(matches!(
        error.get_ref().and_then(|error| error.downcast_ref()),
        Some(Error::Busy(_))
    ));
    assert_eq!(result, b"abc");
    assert_eq!(
        *fixture.reads.lock().unwrap(),
        [
            "metadata",
            "full",
            "bytes=3-9",
            "bytes=3-9",
            "bytes=3-9",
            "bytes=3-9"
        ],
    );
    server.abort();
}

async fn interrupt_body(download: &mut Download, fixture: &Fixture) -> Vec<u8> {
    let mut first = vec![0; 3];
    tokio::time::timeout(
        Duration::from_secs(5),
        download.reader.read_exact(&mut first),
    )
    .await
    .expect("fixture sent part of the response body")
    .unwrap();
    assert_eq!(first, b"abc");
    fixture.interrupt_download.notify_one();
    first
}

const RECOVERY_LENGTH: u64 = 2 * PART_BYTES + 7;
const FIRST_ACK: u64 = PART_BYTES / 2 + 17;

struct RecoveryFixture {
    calls: Mutex<Vec<String>>,
    input: Bytes,
    scenario: RecoveryScenario,
}

#[derive(PartialEq)]
enum RecoveryScenario {
    Interrupted,
    InvalidOffset,
    NoProgress,
}

async fn start_recovery(
    scenario: RecoveryScenario,
) -> (
    GcsStorage,
    Arc<RecoveryFixture>,
    tokio::task::JoinHandle<()>,
) {
    let fixture = Arc::new(RecoveryFixture {
        calls: Mutex::new(Vec::new()),
        input: (0..RECOVERY_LENGTH)
            .map(|offset| (offset % 251) as u8)
            .collect(),
        scenario,
    });
    let router = Router::new()
        .route("/upload/storage/v1/b/sanctum-test/o", post(start_session))
        .route("/session", put(interrupted_part).delete(cancel_session))
        .with_state(fixture.clone());
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = Url::parse(&format!("http://{}/", listener.local_addr().unwrap())).unwrap();
    let server = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
    let storage =
        GcsStorage::with_endpoint("sanctum-test".into(), url, Arc::new(TestCredential)).unwrap();
    (storage, fixture, server)
}

async fn start_session(
    Query(query): Query<std::collections::HashMap<String, String>>,
    headers: HeaderMap,
) -> impl IntoResponse {
    assert_credentials(&headers);
    assert_eq!(query["fields"], "name,size,etag,generation,updated");
    assert_eq!(query["ifGenerationMatch"], "0");
    assert_eq!(query["name"], "default/notes/bulk");
    assert_eq!(query["uploadType"], "resumable");
    assert_eq!(
        headers["x-upload-content-length"],
        RECOVERY_LENGTH.to_string()
    );
    (
        StatusCode::OK,
        [(header::LOCATION, "/session?upload_id=local-fixture")],
    )
}

async fn interrupted_part(
    State(fixture): State<Arc<RecoveryFixture>>,
    headers: HeaderMap,
    body: Body,
) -> axum::response::Response {
    assert!(headers.get(header::AUTHORIZATION).is_none());
    let range = headers[header::CONTENT_RANGE].to_str().unwrap();
    let bytes = axum::body::to_bytes(body, PART_BYTES as usize)
        .await
        .unwrap();
    assert_eq!(headers[header::CONTENT_LENGTH], bytes.len().to_string());
    let mut calls = fixture.calls.lock().unwrap();
    calls.push(range.into());
    if range == format!("bytes */{RECOVERY_LENGTH}") {
        assert!(bytes.is_empty());
    } else {
        let (interval, length) = range
            .strip_prefix("bytes ")
            .unwrap()
            .split_once('/')
            .unwrap();
        let (start, end) = interval.split_once('-').unwrap();
        let start: usize = start.parse().unwrap();
        let end: usize = end.parse().unwrap();
        assert_eq!(length, RECOVERY_LENGTH.to_string());
        assert_eq!(&bytes[..], &fixture.input[start..=end]);
        if end + 1 < RECOVERY_LENGTH as usize {
            assert_eq!(bytes.len(), PART_BYTES as usize);
        }
    }
    let incomplete = |offset: u64| {
        (
            StatusCode::PERMANENT_REDIRECT,
            [(header::RANGE, format!("bytes=0-{}", offset - 1))],
        )
            .into_response()
    };
    if fixture.scenario == RecoveryScenario::NoProgress {
        return StatusCode::PERMANENT_REDIRECT.into_response();
    }
    match calls.len() {
        // The upload request fails before the caller receives any indication
        // of how much of the first part reached the storage service.
        1 | 2 => StatusCode::SERVICE_UNAVAILABLE.into_response(),
        3 if fixture.scenario == RecoveryScenario::InvalidOffset => incomplete(PART_BYTES + 1),
        3 => incomplete(FIRST_ACK),
        4 => incomplete(FIRST_ACK + PART_BYTES),
        5 => {
            // The object is now committed, but the metadata response is lost.
            let lost = futures::stream::iter([Err::<Bytes, _>(std::io::Error::new(
                std::io::ErrorKind::ConnectionReset,
                "fixture dropped the response",
            ))]);
            (StatusCode::OK, Body::from_stream(lost)).into_response()
        }
        6 => Json(metadata("default/notes/bulk", RECOVERY_LENGTH, 14)).into_response(),
        _ => panic!("unexpected resumable request"),
    }
}

async fn cancel_session(
    State(fixture): State<Arc<RecoveryFixture>>,
    headers: HeaderMap,
) -> StatusCode {
    assert!(headers.get(header::AUTHORIZATION).is_none());
    assert_eq!(headers[header::CONTENT_LENGTH], "0");
    fixture.calls.lock().unwrap().push("cancel".into());
    StatusCode::from_u16(499).unwrap()
}

#[tokio::test]
async fn resumable_upload_recovers_partial_part_and_lost_completion() {
    let (storage, fixture, server) = start_recovery(RecoveryScenario::Interrupted).await;
    let key = Key::parse("default/notes/bulk").unwrap();
    let version = storage
        .put_stream(
            &key,
            Box::pin(std::io::Cursor::new(fixture.input.clone())),
            RECOVERY_LENGTH,
        )
        .await
        .unwrap();
    assert_eq!(version.generation.as_deref(), Some("14"));
    let probe = format!("bytes */{RECOVERY_LENGTH}");
    assert_eq!(
        *fixture.calls.lock().unwrap(),
        [
            format!("bytes 0-{}/{RECOVERY_LENGTH}", PART_BYTES - 1),
            probe.clone(),
            probe.clone(),
            format!(
                "bytes {FIRST_ACK}-{}/{RECOVERY_LENGTH}",
                FIRST_ACK + PART_BYTES - 1
            ),
            format!(
                "bytes {}-{}/{RECOVERY_LENGTH}",
                FIRST_ACK + PART_BYTES,
                RECOVERY_LENGTH - 1
            ),
            probe,
        ]
    );
    server.abort();
}

#[tokio::test]
async fn resumable_upload_rejects_an_offset_beyond_the_sent_part() {
    let (storage, fixture, server) = start_recovery(RecoveryScenario::InvalidOffset).await;
    let key = Key::parse("default/notes/bulk").unwrap();
    assert!(matches!(
        storage.put_stream(&key, Box::pin(std::io::Cursor::new(fixture.input.clone())), RECOVERY_LENGTH).await,
        Err(Error::Io(error)) if error.kind() == std::io::ErrorKind::InvalidData
    ));
    assert_eq!(fixture.calls.lock().unwrap().len(), 4);
    assert_eq!(fixture.calls.lock().unwrap().last().unwrap(), "cancel");
    server.abort();
}

#[tokio::test]
async fn resumable_upload_stops_after_bounded_attempts_without_progress() {
    let (storage, fixture, server) = start_recovery(RecoveryScenario::NoProgress).await;
    let key = Key::parse("default/notes/bulk").unwrap();
    assert!(matches!(
        storage.put_stream(
            &key,
            Box::pin(std::io::Cursor::new(fixture.input.clone())),
            RECOVERY_LENGTH,
        ).await,
        Err(Error::Busy(message)) if message.contains("no progress")
    ));
    let calls = fixture.calls.lock().unwrap();
    let requests = calls.split_last().unwrap();
    assert_eq!(requests.0, "cancel");
    assert_eq!(requests.1.len(), MAX_UPLOAD_RETRIES as usize + 1);
    assert!(requests.1.iter().all(|range| range == &calls[0]));
    server.abort();
}
