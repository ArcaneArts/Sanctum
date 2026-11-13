use sanctum_storage_s3::{S3Config, S3Storage};

#[tokio::test]
#[ignore = "requires a live MinIO bucket and S3 credentials"]
async fn minio_storage_contract() {
    let storage = S3Storage::new(S3Config {
        bucket: std::env::var("SANCTUM_MINIO_BUCKET").expect("SANCTUM_MINIO_BUCKET"),
        region: "us-east-1".into(),
        endpoint: Some(std::env::var("SANCTUM_MINIO_ENDPOINT").expect("SANCTUM_MINIO_ENDPOINT")),
        allow_http: true,
        virtual_hosted_style: false,
    })
    .unwrap();
    let prefix = format!("sanctum-contract-{}", uuid::Uuid::new_v4());
    sanctum_storage::contract::verify(&storage, &prefix).await;
}
