ALTER TABLE pack_members
    ADD COLUMN encoding_uuid uuid,
    ADD COLUMN encoded_size integer CHECK (encoded_size BETWEEN 1 AND 4194320),
    ADD COLUMN compressed boolean,
    ADD COLUMN encryption_algorithm text,
    ADD COLUMN block_key_id uuid REFERENCES managed_keys(id),
    ADD COLUMN nonce bytea CHECK (nonce IS NULL OR octet_length(nonce) = 12);

CREATE INDEX pack_members_key ON pack_members(block_key_id) WHERE block_key_id IS NOT NULL;
