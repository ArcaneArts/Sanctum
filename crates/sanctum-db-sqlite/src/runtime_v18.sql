CREATE INDEX ingestion_superseded_failures ON ingestion_jobs (tenant, collection, source_id)
WHERE status = 'failed' AND error_code = 'retryable'
  AND publication_started = 0 AND update_sequence > 0;
