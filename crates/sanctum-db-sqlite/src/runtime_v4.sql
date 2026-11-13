ALTER TABLE ingestion_jobs ADD COLUMN extract_rtf INTEGER NOT NULL DEFAULT 0;
ALTER TABLE ingestion_jobs ADD COLUMN retain_original INTEGER NOT NULL DEFAULT 0;
ALTER TABLE ingestion_jobs ADD COLUMN extracting INTEGER NOT NULL DEFAULT 0;
ALTER TABLE ingestion_jobs ADD COLUMN canonical_bytes INTEGER CHECK (canonical_bytes >= 0);
ALTER TABLE ingestion_jobs ADD COLUMN original_hash TEXT;
ALTER TABLE ingestion_jobs ADD COLUMN source_encoding TEXT;
ALTER TABLE ingestion_jobs ADD COLUMN extractor TEXT;
