ALTER TABLE revisions ADD COLUMN original_hash TEXT;
ALTER TABLE revisions ADD COLUMN source_encoding TEXT NOT NULL DEFAULT 'UTF-8';
ALTER TABLE revisions ADD COLUMN extractor TEXT;
ALTER TABLE revisions ADD COLUMN retain_original INTEGER NOT NULL DEFAULT 0;
