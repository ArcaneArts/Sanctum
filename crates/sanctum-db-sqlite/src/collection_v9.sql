CREATE TABLE hosted_revision_objects (
    source_id TEXT NOT NULL,
    revision_id TEXT NOT NULL,
    text_key TEXT NOT NULL,
    original_key TEXT,
    PRIMARY KEY (source_id, revision_id),
    FOREIGN KEY (source_id, revision_id) REFERENCES revisions (source_id, id) ON DELETE CASCADE
);
