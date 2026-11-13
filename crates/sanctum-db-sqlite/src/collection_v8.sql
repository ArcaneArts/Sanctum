CREATE TABLE hosted_revision_intents (
    source_id TEXT PRIMARY KEY NOT NULL,
    operation_id TEXT NOT NULL,
    revision_id TEXT NOT NULL,
    update_sequence INTEGER NOT NULL CHECK (update_sequence > 0),
    creating INTEGER NOT NULL CHECK (creating IN (0, 1)),
    external_namespace TEXT,
    external_id TEXT
);

CREATE UNIQUE INDEX hosted_pending_external_source
    ON hosted_revision_intents (external_namespace, external_id)
    WHERE creating = 1;
