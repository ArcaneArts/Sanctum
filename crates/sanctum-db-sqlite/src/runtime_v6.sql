CREATE TABLE watched_folders (
    id TEXT PRIMARY KEY NOT NULL,
    tenant TEXT NOT NULL,
    collection TEXT NOT NULL,
    path TEXT NOT NULL,
    options_json TEXT NOT NULL,
    interval_seconds INTEGER NOT NULL CHECK (interval_seconds BETWEEN 1 AND 86400),
    next_scan_at INTEGER NOT NULL DEFAULT 0,
    last_scan_at INTEGER,
    last_error TEXT,
    queued INTEGER NOT NULL DEFAULT 0,
    missing INTEGER NOT NULL DEFAULT 0,
    deleted INTEGER NOT NULL DEFAULT 0,
    failed INTEGER NOT NULL DEFAULT 0,
    UNIQUE (tenant, collection, path)
);
CREATE INDEX watched_folders_scope ON watched_folders (tenant, collection, id);
CREATE INDEX watched_folders_due ON watched_folders (next_scan_at, id);
