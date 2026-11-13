#[tokio::test]
async fn filesystem_storage_contract() {
    let directory = tempfile::tempdir().unwrap();
    let storage = sanctum_storage_fs::FileStorage::open(directory.path()).unwrap();
    sanctum_storage::contract::verify(&storage, "default").await;
}
