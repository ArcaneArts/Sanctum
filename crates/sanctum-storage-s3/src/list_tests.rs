use std::collections::HashMap;

use axum::{Router, extract::Query, routing::get};
use tokio::net::TcpListener;

use super::*;

async fn list(Query(query): Query<HashMap<String, String>>) -> String {
    assert_eq!(query["list-type"], "2");
    assert_eq!(query["prefix"], "default/notes/");
    let keys: &[&str] = match query.get("continuation-token").map(String::as_str) {
        None => &["default/notes/a"],
        Some("sibling") => &["default/notes-other/a"],
        Some("prefix-object") => &["default/notes"],
        Some("too-many") => &["default/notes/a", "default/notes/b"],
        _ => panic!("unexpected list cursor"),
    };
    assert_eq!(query["max-keys"], "1");
    let objects = keys
        .iter()
        .map(|key| {
            format!(
                "<Contents><Key>{key}</Key><LastModified>2026-11-14T00:00:00Z</LastModified>\
                 <ETag>fixture</ETag><Size>4</Size><StorageClass>STANDARD</StorageClass></Contents>"
            )
        })
        .collect::<String>();
    format!(
        "<ListBucketResult><Name>fixture</Name><IsTruncated>false</IsTruncated>{objects}</ListBucketResult>"
    )
}

#[tokio::test]
async fn list_rejects_out_of_scope_or_oversized_pages() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let routes = Router::new().route("/fixture", get(list));
    let server = tokio::spawn(async move { axum::serve(listener, routes).await.unwrap() });
    let storage = S3Storage(
        AmazonS3Builder::new()
            .with_bucket_name("fixture")
            .with_region("us-east-1")
            .with_endpoint(endpoint)
            .with_skip_signature(true)
            .with_client_options(ClientOptions::new().with_allow_http(true))
            .build()
            .unwrap(),
    );
    let prefix = Key::parse("default/notes").unwrap();
    let page = storage.list(Some(&prefix), None, 1).await.unwrap();
    assert_eq!(page.objects.len(), 1);
    assert_eq!(page.objects[0].key.as_str(), "default/notes/a");
    for token in ["sibling", "prefix-object", "too-many"] {
        assert!(
            matches!(
                storage.list(Some(&prefix), Some(token), 1).await,
                Err(Error::Io(error)) if error.kind() == std::io::ErrorKind::InvalidData
            ),
            "provider response for {token} should be rejected"
        );
    }
    server.abort();
}
