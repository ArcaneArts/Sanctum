use sanctum_storage_gcs::GcsStorage;

#[tokio::test]
#[ignore = "requires an authorized GCS test bucket and Application Default Credentials"]
async fn gcs_storage_contract() {
    let bucket = std::env::var("SANCTUM_GCS_BUCKET").expect("SANCTUM_GCS_BUCKET");
    let storage = GcsStorage::new(bucket).unwrap();
    let prefix = format!("sanctum-contract-{}", uuid::Uuid::new_v4());
    sanctum_storage::contract::verify(&storage, &prefix).await;
}
