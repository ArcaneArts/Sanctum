CREATE TABLE local_collection_deletions (
    request_id TEXT PRIMARY KEY NOT NULL,
    collection TEXT NOT NULL REFERENCES deleted_collections(id),
    recorded_at INTEGER NOT NULL DEFAULT (unixepoch())
);
CREATE INDEX local_collection_deletions_recorded_at
    ON local_collection_deletions(recorded_at);
