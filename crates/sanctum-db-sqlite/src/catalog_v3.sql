CREATE TABLE deleted_collections (
    id TEXT PRIMARY KEY NOT NULL,
    deleted_at INTEGER NOT NULL DEFAULT (unixepoch())
);
