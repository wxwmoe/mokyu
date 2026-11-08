ALTER TABLE pack_members
    ADD COLUMN encoding_uuid BINARY(16),
    ADD COLUMN encoded_size INT,
    ADD COLUMN compressed BOOLEAN,
    ADD COLUMN encryption_algorithm VARCHAR(32) CHARACTER SET ascii COLLATE ascii_bin,
    ADD COLUMN block_key_id BINARY(16),
    ADD COLUMN nonce VARBINARY(12),
    ADD CONSTRAINT members_encoded_size CHECK (encoded_size BETWEEN 1 AND 4194320),
    ADD CONSTRAINT members_nonce_size CHECK (nonce IS NULL OR OCTET_LENGTH(nonce) = 12),
    ADD INDEX pack_members_key (block_key_id),
    ADD CONSTRAINT members_key FOREIGN KEY (block_key_id) REFERENCES managed_keys(id);
