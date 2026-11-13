CREATE TABLE collection_metadata (
    collection TEXT NOT NULL REFERENCES collections(id) ON DELETE CASCADE,
    namespace TEXT NOT NULL,
    key TEXT NOT NULL,
    value TEXT NOT NULL CHECK (json_valid(value)),
    PRIMARY KEY (collection, namespace, key)
) WITHOUT ROWID;
