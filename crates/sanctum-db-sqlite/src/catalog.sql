CREATE TABLE collections (
    id TEXT PRIMARY KEY NOT NULL,
    created_at INTEGER NOT NULL DEFAULT (unixepoch())
);
