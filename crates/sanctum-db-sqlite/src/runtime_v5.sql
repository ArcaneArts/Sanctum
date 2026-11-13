CREATE INDEX ingestion_tenant_activity ON ingestion_jobs (tenant, created_at DESC, operation_id DESC);
CREATE INDEX ingestion_collection_activity ON ingestion_jobs (tenant, collection, created_at DESC, operation_id DESC);
