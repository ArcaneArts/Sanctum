CREATE TABLE embedding_cache (
    profile TEXT NOT NULL,
    input_hash TEXT NOT NULL,
    vector BLOB,
    attempts INTEGER NOT NULL DEFAULT 0,
    retry_at INTEGER,
    error TEXT,
    PRIMARY KEY (profile, input_hash)
) WITHOUT ROWID;

CREATE INDEX chunks_content_hash ON chunks (content_hash);
