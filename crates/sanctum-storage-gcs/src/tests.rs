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
    Path(key): Path<String>,
    Query(query): Query<std::collections::HashMap<String, String>>,
    headers: HeaderMap,
) -> axum::response::Response {
    assert_credentials(&headers);
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
    if key.ends_with("/changed") {
        return StatusCode::PRECONDITION_FAILED.into_response();
    }
    "abcdef".into_response()
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
    assert!(matches!(
        storage
            .get(&Key::parse("default/notes/changed").unwrap())
            .await,
        Err(Error::Conflict(_))
    ));

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
