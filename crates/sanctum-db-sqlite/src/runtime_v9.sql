ALTER TABLE ingestion_jobs ADD COLUMN chunk_window_bytes INTEGER NOT NULL DEFAULT 4096;
ALTER TABLE ingestion_jobs ADD COLUMN chunk_overlap_bytes INTEGER NOT NULL DEFAULT 400;
