ALTER TABLE deleted_collections
    ADD COLUMN reclaimed INTEGER NOT NULL DEFAULT 0 CHECK (reclaimed IN (0, 1));
