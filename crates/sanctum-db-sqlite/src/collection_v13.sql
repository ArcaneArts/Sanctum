ALTER TABLE revision_garbage ADD COLUMN cleanup_after INTEGER NOT NULL DEFAULT 0;
CREATE INDEX revision_garbage_cleanup ON revision_garbage (cleanup_after, source_id, revision_id);
