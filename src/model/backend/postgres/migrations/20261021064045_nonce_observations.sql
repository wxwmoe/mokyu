CREATE TABLE nonce_observations (
    block_key_id uuid PRIMARY KEY REFERENCES managed_keys(id),
    data jsonb NOT NULL,
    revision bigint NOT NULL CHECK (revision > 0)
);

CREATE INDEX chunks_pending_key ON chunks(block_key_id) WHERE state IN ('preparing','uploading');

UPDATE mokyu_meta SET schema_version=52;
