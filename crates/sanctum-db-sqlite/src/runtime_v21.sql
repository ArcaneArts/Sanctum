DROP INDEX IF EXISTS ingestion_publication_recovery;

CREATE INDEX ingestion_publication_recovery
ON ingestion_jobs (reconcile_after, operation_id)
WHERE (replacement = 1 OR publication_started = 1)
  AND status = 'failed' AND error_code = 'retryable';
