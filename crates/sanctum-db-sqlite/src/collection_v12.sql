ALTER TABLE hosted_object_garbage ADD COLUMN source_id TEXT;
CREATE INDEX hosted_object_garbage_source ON hosted_object_garbage (source_id);
