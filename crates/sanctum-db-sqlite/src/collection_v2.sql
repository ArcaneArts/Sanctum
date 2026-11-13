ALTER TABLE sources ADD COLUMN deleted INTEGER NOT NULL DEFAULT 0;
ALTER TABLE revisions ADD COLUMN retire_at INTEGER;
CREATE INDEX revisions_retirement ON revisions (retire_at) WHERE retire_at IS NOT NULL;

CREATE TABLE deleted_sources (
    id TEXT PRIMARY KEY NOT NULL,
    expires_at INTEGER NOT NULL
);
CREATE INDEX deleted_sources_expiration ON deleted_sources (expires_at);

CREATE TABLE revision_garbage (
    source_id TEXT NOT NULL,
    revision_id TEXT NOT NULL,
    PRIMARY KEY (source_id, revision_id)
);
