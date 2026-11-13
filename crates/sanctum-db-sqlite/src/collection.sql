CREATE TABLE sources (
    id TEXT PRIMARY KEY NOT NULL,
    name TEXT NOT NULL,
    external_namespace TEXT,
    external_id TEXT,
    current_revision TEXT NOT NULL,
    created_at INTEGER NOT NULL DEFAULT (unixepoch()),
    UNIQUE (external_namespace, external_id),
    FOREIGN KEY (id, current_revision) REFERENCES revisions (source_id, id)
        DEFERRABLE INITIALLY DEFERRED
);

CREATE TABLE revisions (
    source_id TEXT NOT NULL REFERENCES sources (id),
    id TEXT NOT NULL,
    content_hash TEXT NOT NULL,
    bytes INTEGER NOT NULL CHECK (bytes >= 0),
    chunk_count INTEGER NOT NULL CHECK (chunk_count >= 0),
    chunker TEXT NOT NULL,
    PRIMARY KEY (source_id, id)
);

CREATE TABLE chunks (
    id INTEGER PRIMARY KEY,
    source_id TEXT NOT NULL,
    revision_id TEXT NOT NULL,
    ordinal INTEGER NOT NULL CHECK (ordinal >= 0),
    start_byte INTEGER NOT NULL CHECK (start_byte >= 0),
    end_byte INTEGER NOT NULL CHECK (end_byte > start_byte),
    content_hash TEXT NOT NULL,
    UNIQUE (source_id, revision_id, ordinal),
    FOREIGN KEY (source_id, revision_id) REFERENCES revisions (source_id, id)
        DEFERRABLE INITIALLY DEFERRED
);

CREATE VIRTUAL TABLE chunks_fts USING fts5(body, content='', contentless_delete=1);
