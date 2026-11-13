ALTER TABLE ingestion_jobs ADD COLUMN replacement INTEGER NOT NULL DEFAULT 0;
ALTER TABLE ingestion_jobs ADD COLUMN update_sequence INTEGER NOT NULL DEFAULT 0;
CREATE INDEX ingestion_source ON ingestion_jobs (tenant, collection, source_id, revision_id);

CREATE TABLE replacement_sequences (
    tenant TEXT NOT NULL,
    collection TEXT NOT NULL,
    source_id TEXT NOT NULL,
    sequence INTEGER NOT NULL,
    PRIMARY KEY (tenant, collection, source_id)
);
