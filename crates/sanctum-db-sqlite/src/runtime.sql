CREATE TABLE ingestion_jobs (
    operation_id TEXT PRIMARY KEY NOT NULL,
    tenant TEXT NOT NULL,
    collection TEXT NOT NULL,
    source_id TEXT NOT NULL,
    revision_id TEXT NOT NULL,
    name TEXT NOT NULL,
    external_namespace TEXT,
    external_id TEXT,
    total_bytes INTEGER NOT NULL CHECK (total_bytes >= 0),
    processed_bytes INTEGER NOT NULL DEFAULT 0 CHECK (processed_bytes >= 0),
    attempts INTEGER NOT NULL DEFAULT 0,
    status TEXT NOT NULL DEFAULT 'accepted'
        CHECK (status IN ('accepted', 'indexing', 'publishing', 'text_ready', 'failed', 'canceled')),
    cancel_requested INTEGER NOT NULL DEFAULT 0,
    publication_started INTEGER NOT NULL DEFAULT 0,
    error_code TEXT,
    error_message TEXT,
    content_hash TEXT,
    chunks INTEGER,
    created_at INTEGER NOT NULL DEFAULT (unixepoch())
);

CREATE INDEX ingestion_queue ON ingestion_jobs (status, created_at, operation_id);

CREATE TABLE executor_settings (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    paused INTEGER NOT NULL CHECK (paused IN (0, 1))
);

INSERT INTO executor_settings (id, paused) VALUES (1, 0);
