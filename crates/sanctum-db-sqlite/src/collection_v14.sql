CREATE TABLE local_source_deletion_receipts (
    request_id TEXT PRIMARY KEY NOT NULL,
    source_id TEXT NOT NULL,
    expected_revision_id TEXT,
    revision_id TEXT NOT NULL,
    expires_at INTEGER NOT NULL
);
CREATE INDEX local_source_deletion_receipts_expiry
    ON local_source_deletion_receipts (expires_at, request_id);
