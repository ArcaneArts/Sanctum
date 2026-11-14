CREATE TABLE local_collection_creations (
    collection TEXT PRIMARY KEY NOT NULL,
    request_id TEXT UNIQUE,
    finished_at INTEGER
);
