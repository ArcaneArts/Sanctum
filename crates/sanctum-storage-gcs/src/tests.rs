use std::sync::Mutex;

use axum::{
    Json, Router,
    body::Body,
    extract::{Path, Query, State},
    response::IntoResponse,
    routing::{get, post, put},
};
use serde_json::json;
use tokio::{io::repeat, net::TcpListener};

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
    Query(query): Query<std::collections::HashMap<String, String>>,
    headers: HeaderMap,
) -> axum::response::Response {
    assert_credentials(&headers);
    assert_eq!(query["prefix"], "default/notes/");
    assert_eq!(query["maxResults"], "1");
    match query.get("pageToken").map(String::as_str) {
        None => Json(
            json!({"items": [metadata("default/notes/a", 4, 9)], "nextPageToken": "opaque-token"}),
        )
        .into_response(),
        Some("opaque-token") => Json(json!({})).into_response(),
        _ => StatusCode::BAD_REQUEST.into_response(),
    }
}

async fn object(
    State(fixture): State<Arc<Fixture>>,
    Path(key): Path<String>,
    Query(query): Query<std::collections::HashMap<String, String>>,
    headers: HeaderMap,
) -> axum::response::Response {
    assert_credentials(&headers);
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
