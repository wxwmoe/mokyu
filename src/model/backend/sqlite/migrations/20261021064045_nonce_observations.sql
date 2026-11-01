CREATE TABLE nonce_observations (
    block_key_id BLOB NOT NULL PRIMARY KEY REFERENCES managed_keys(id),
    data TEXT NOT NULL CHECK(json_valid(data)),
    revision INTEGER NOT NULL CHECK(revision > 0)
) WITHOUT ROWID;

CREATE INDEX chunks_pending_key ON chunks(block_key_id) WHERE state IN ('preparing','uploading');

UPDATE mokyu_meta SET schema_version=27 WHERE id=1;
