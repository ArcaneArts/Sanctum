ALTER TABLE ingestion_jobs ADD COLUMN reconcile_after INTEGER NOT NULL DEFAULT 0;

CREATE INDEX ingestion_publication_recovery
ON ingestion_jobs (reconcile_after, operation_id)
WHERE publication_started = 1
  AND status = 'failed' AND error_code = 'retryable';
