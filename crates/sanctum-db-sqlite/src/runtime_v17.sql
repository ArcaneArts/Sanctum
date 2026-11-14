ALTER TABLE ingestion_jobs ADD COLUMN cleanup_after INTEGER;

UPDATE ingestion_jobs SET cleanup_after = unixepoch()
WHERE status = 'canceled'
   OR (status = 'failed' AND error_code IS NOT 'retryable');

CREATE INDEX ingestion_cleanup ON ingestion_jobs (cleanup_after, operation_id)
WHERE cleanup_after IS NOT NULL;
