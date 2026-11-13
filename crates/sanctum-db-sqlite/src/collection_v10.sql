CREATE TABLE hosted_object_garbage (
    object_key TEXT PRIMARY KEY NOT NULL,
    delete_after INTEGER NOT NULL
);

CREATE INDEX hosted_object_garbage_due ON hosted_object_garbage (delete_after);
