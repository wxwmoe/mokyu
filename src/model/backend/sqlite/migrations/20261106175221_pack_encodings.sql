ALTER TABLE pack_members ADD COLUMN encoding_uuid BLOB CHECK (encoding_uuid IS NULL OR length(encoding_uuid) = 16);
ALTER TABLE pack_members ADD COLUMN encoded_size INTEGER CHECK (encoded_size BETWEEN 1 AND 4194320);
ALTER TABLE pack_members ADD COLUMN compressed INTEGER CHECK (compressed IN (0, 1));
ALTER TABLE pack_members ADD COLUMN encryption_algorithm TEXT;
ALTER TABLE pack_members ADD COLUMN block_key_id BLOB REFERENCES managed_keys(id);
ALTER TABLE pack_members ADD COLUMN nonce BLOB CHECK (nonce IS NULL OR length(nonce) = 12);

CREATE INDEX pack_members_key ON pack_members(block_key_id) WHERE block_key_id IS NOT NULL;
