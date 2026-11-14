-- Older workers reported a retryable failure after committing text if only
-- original-file removal failed. The recovered revision supplies the receipt.
UPDATE ingestion_jobs
SET status = 'accepted', error_code = NULL, error_message = NULL, finished_at = NULL
WHERE status = 'failed' AND error_code = 'retryable' AND publication_started = 1
  AND extract_rtf = 1 AND retain_original = 0
  AND error_message = 'could not remove the staged original';
