ALTER TABLE ingestion_jobs ADD COLUMN finished_at INTEGER;

-- Give legacy terminal jobs the same grace period from their migration.
UPDATE ingestion_jobs SET finished_at = unixepoch()
WHERE status IN ('text_ready', 'failed', 'canceled');

CREATE INDEX ingestion_finished ON ingestion_jobs (finished_at, operation_id)
WHERE finished_at IS NOT NULL AND (
    status IN ('text_ready', 'canceled')
    OR (status = 'failed' AND error_code IS NOT 'retryable')
);
