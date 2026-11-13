CREATE TABLE revision_metadata (
    source TEXT NOT NULL,
    revision TEXT NOT NULL,
    ordinal INTEGER NOT NULL CHECK (ordinal >= -1),
    namespace TEXT NOT NULL,
    key TEXT NOT NULL,
    value TEXT NOT NULL CHECK (json_valid(value)),
    PRIMARY KEY (source, revision, ordinal, namespace, key),
    FOREIGN KEY (source, revision) REFERENCES revisions(source_id, id) ON DELETE CASCADE
) WITHOUT ROWID;

CREATE TABLE source_changes (
    cursor INTEGER PRIMARY KEY AUTOINCREMENT,
    kind TEXT NOT NULL,
    source TEXT NOT NULL,
    revision TEXT NOT NULL,
    previous_revision TEXT,
    ordinal INTEGER
);
