CREATE TABLE metadata_indexes (
    name TEXT PRIMARY KEY NOT NULL,
    level TEXT NOT NULL CHECK (level IN ('collection', 'source', 'chunk')),
    namespace TEXT NOT NULL,
    key TEXT NOT NULL,
    UNIQUE (level, namespace, key)
) WITHOUT ROWID;
