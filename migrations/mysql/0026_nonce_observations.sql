CREATE TABLE IF NOT EXISTS nonce_observations (
    block_key_id BINARY(16) NOT NULL PRIMARY KEY,
    data JSON NOT NULL,
    revision BIGINT NOT NULL CHECK (revision > 0),
    CONSTRAINT nonce_observations_key FOREIGN KEY(block_key_id) REFERENCES managed_keys(id)
) ENGINE=InnoDB;

UPDATE mokyu_meta SET schema_version=26 WHERE id=1;
