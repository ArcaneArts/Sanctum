CREATE TABLE local_upload_requests (
    tenant TEXT NOT NULL,
    collection TEXT NOT NULL,
    request_id TEXT NOT NULL,
    operation_id TEXT,
    fingerprint TEXT,
    PRIMARY KEY (tenant, collection, request_id)
) WITHOUT ROWID;

CREATE INDEX local_upload_request_operation ON local_upload_requests (operation_id)
WHERE operation_id IS NOT NULL;
