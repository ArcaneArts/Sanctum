use std::sync::Arc;

use sanctum_proto::v1::{self, sanctum_client::SanctumClient};
use sanctum_service::HostedCatalog;
use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;
use tokio_util::sync::CancellationToken;
use tonic::{Code, transport::Server};

#[tokio::test]
async fn hosted_catalog_recovers_across_server_restart_and_stays_in_scope() {
    let directory = tempfile::tempdir().unwrap();
    let store = Arc::new(sanctum_storage_fs::FileStorage::open(directory.path()).unwrap());
    let mut state = start(HostedCatalog::new(store.clone(), None).unwrap()).await;
    let request_id = uuid::Uuid::new_v4().to_string();
    let create =
        |tenant: &str, collection: &str, request_id: Option<String>| v1::CreateCollectionRequest {
            scope: Some(v1::Scope {
                tenant: tenant.into(),
                collection: collection.into(),
            }),
            request_id,
        };
    // Simulate a crash after initial collection publication and before activation.
    let interrupted = sanctum_core::Scope {
        tenant: "default".parse().unwrap(),
        collection: "catalog".parse().unwrap(),
    };
    let executor =
        sanctum_db::Publication::new(store.clone(), std::time::Duration::from_secs(15)).unwrap();
    let unit = sanctum_db::Unit::collection(None, &interrupted).unwrap();
    let mut writer = executor.acquire(&unit).await.unwrap();
    let (initial, _) = writer
        .apply(
            vec![sanctum_db::Command {
                operation: uuid::Uuid::nil(),
                payload: (),
            }],
            |tx, ()| {
                sanctum_db_sqlite::initialize_collection(tx)?;
                Ok("initialized".into())
            },
        )
        .await
        .unwrap();
    writer.release().await.unwrap();
    let empty = state
        .client
        .list_collections(v1::ListCollectionsRequest {
            tenant: "default".into(),
            ..Default::default()
        })
        .await
        .unwrap()
        .into_inner();
    assert!(empty.collections.is_empty());

    let first = create("default", "catalog", Some(request_id.clone()));
    state.client.create_collection(first.clone()).await.unwrap();
    assert_eq!(executor.generation(&unit).await.unwrap(), Some(initial));
    state
        .client
        .create_collection(create("default", "notes", None))
        .await
        .unwrap();
    state
        .client
        .create_collection(create("other", "private", None))
        .await
        .unwrap();
    state.stop().await;

    let mut state = start(HostedCatalog::new(store.clone(), None).unwrap()).await;
    state.client.create_collection(first).await.unwrap();
    assert_eq!(
        state
            .client
            .create_collection(create("default", "catalog", None))
            .await
            .unwrap_err()
            .code(),
        Code::AlreadyExists
    );
    assert_eq!(
        state
            .client
            .create_collection(create("default", "conflict", Some(request_id)))
            .await
            .unwrap_err()
            .code(),
        Code::AlreadyExists
    );
    let mut request = v1::ListCollectionsRequest {
        tenant: "default".into(),
        limit: Some(1),
        ..Default::default()
    };
    let page = state
        .client
        .list_collections(request.clone())
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        page.collections[0].scope.as_ref().unwrap().collection,
        "catalog"
    );
    request.after = page.next_after;
    let page = state
        .client
        .list_collections(request)
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        page.collections[0].scope.as_ref().unwrap().collection,
        "notes"
    );
    assert!(page.next_after.is_none());

    use v1::storage_usage_request::Scope;
    let tenant = state
        .client
        .get_storage_usage(v1::StorageUsageRequest {
            scope: Some(Scope::Tenant("default".into())),
        })
        .await
        .unwrap()
        .into_inner();
    let collection = state
        .client
        .get_storage_usage(v1::StorageUsageRequest {
            scope: Some(Scope::Collection(v1::Scope {
                tenant: "default".into(),
                collection: "catalog".into(),
            })),
        })
        .await
        .unwrap()
        .into_inner();
    let expected = sanctum_storage::usage(
        store.as_ref(),
        Some(&sanctum_storage::Key::parse("default").unwrap()),
    )
    .await
    .unwrap();
    assert_eq!(
        (tenant.bytes, tenant.files),
        (expected.bytes, expected.files)
    );
    assert_eq!(collection.files, 2);
    assert!(collection.bytes > 0 && collection.bytes < tenant.bytes);
    assert_eq!(
        state
            .client
            .get_local_queue(v1::Empty {})
            .await
            .unwrap_err()
            .code(),
        Code::Unimplemented
    );
    state.stop().await;
}

struct Running {
    client: SanctumClient<tonic::transport::Channel>,
    stop: CancellationToken,
    server: tokio::task::JoinHandle<Result<(), tonic::transport::Error>>,
}

impl Running {
    async fn stop(self) {
        drop(self.client);
        self.stop.cancel();
        self.server.await.unwrap().unwrap();
    }
}

async fn start(catalog: HostedCatalog) -> Running {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let stop = CancellationToken::new();
    let cancellation = stop.clone();
    let server = tokio::spawn(async move {
        Server::builder()
            .add_service(sanctum_api::hosted_grpc(
                catalog,
                cancellation.child_token(),
            ))
            .serve_with_incoming_shutdown(
                TcpListenerStream::new(listener),
                cancellation.cancelled_owned(),
            )
            .await
    });
    let client = SanctumClient::connect(format!("http://{address}"))
        .await
        .unwrap();
    Running {
        client,
        stop,
        server,
    }
}
