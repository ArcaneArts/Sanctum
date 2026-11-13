ALTER TABLE ingestion_jobs ADD COLUMN import_hash TEXT;
CREATE INDEX ingestion_external ON ingestion_jobs (tenant, collection, external_namespace, external_id);
