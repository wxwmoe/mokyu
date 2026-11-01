CREATE TABLE nonce_counters (
    block_key_id BLOB NOT NULL REFERENCES managed_keys(id),
    type_tag INTEGER NOT NULL CHECK (type_tag BETWEEN 0 AND 31),
    reserved_id INTEGER NOT NULL DEFAULT 0 CHECK (reserved_id>=0),
    PRIMARY KEY (block_key_id,type_tag)
) STRICT;

CREATE UNIQUE INDEX managed_keys_fingerprint ON managed_keys(fingerprint);

ALTER TABLE chunk_encodings ADD COLUMN nonce_id INTEGER;
UPDATE chunk_encodings SET nonce_id=encoding_id WHERE nonce IS NOT NULL;
CREATE TRIGGER chunk_encodings_nonce_insert BEFORE INSERT ON chunk_encodings WHEN NOT ((NEW.encryption_algorithm='none' AND NEW.nonce_id IS NULL AND NEW.nonce IS NULL AND NEW.block_key_id IS NULL) OR (NEW.encryption_algorithm<>'none' AND NEW.block_key_id IS NOT NULL AND ((NEW.nonce IS NULL AND NEW.nonce_id IS NULL AND NEW.state<>'ready') OR (NEW.nonce IS NOT NULL AND NEW.nonce_id IS NOT NULL AND NEW.nonce_id>0)))) BEGIN SELECT RAISE(ABORT,'invalid nonce metadata'); END;
CREATE TRIGGER chunk_encodings_nonce_update BEFORE UPDATE ON chunk_encodings WHEN NOT ((NEW.encryption_algorithm='none' AND NEW.nonce_id IS NULL AND NEW.nonce IS NULL AND NEW.block_key_id IS NULL) OR (NEW.encryption_algorithm<>'none' AND NEW.block_key_id IS NOT NULL AND ((NEW.nonce IS NULL AND NEW.nonce_id IS NULL AND NEW.state<>'ready') OR (NEW.nonce IS NOT NULL AND NEW.nonce_id IS NOT NULL AND NEW.nonce_id>0)))) BEGIN SELECT RAISE(ABORT,'invalid nonce metadata'); END;

ALTER TABLE packs ADD COLUMN nonce_id INTEGER;
UPDATE packs SET nonce_id=id WHERE nonce IS NOT NULL;
CREATE TRIGGER packs_nonce_insert BEFORE INSERT ON packs WHEN NOT ((NEW.encryption_algorithm='none' AND NEW.nonce_id IS NULL AND NEW.nonce IS NULL AND NEW.block_key_id IS NULL) OR (NEW.encryption_algorithm<>'none' AND NEW.block_key_id IS NOT NULL AND ((NEW.nonce IS NULL AND NEW.nonce_id IS NULL AND NEW.state<>'ready') OR (NEW.nonce IS NOT NULL AND NEW.nonce_id IS NOT NULL AND NEW.nonce_id>0)))) BEGIN SELECT RAISE(ABORT,'invalid nonce metadata'); END;
CREATE TRIGGER packs_nonce_update BEFORE UPDATE ON packs WHEN NOT ((NEW.encryption_algorithm='none' AND NEW.nonce_id IS NULL AND NEW.nonce IS NULL AND NEW.block_key_id IS NULL) OR (NEW.encryption_algorithm<>'none' AND NEW.block_key_id IS NOT NULL AND ((NEW.nonce IS NULL AND NEW.nonce_id IS NULL AND NEW.state<>'ready') OR (NEW.nonce IS NOT NULL AND NEW.nonce_id IS NOT NULL AND NEW.nonce_id>0)))) BEGIN SELECT RAISE(ABORT,'invalid nonce metadata'); END;

INSERT INTO nonce_counters(block_key_id,type_tag,reserved_id) SELECT id,0,COALESCE((SELECT seq FROM sqlite_sequence WHERE name='chunk_encodings'),0) FROM managed_keys WHERE purpose='block';
INSERT INTO nonce_counters(block_key_id,type_tag,reserved_id) SELECT id,1,COALESCE((SELECT seq FROM sqlite_sequence WHERE name='packs'),0) FROM managed_keys WHERE purpose='block';

UPDATE mokyu_meta SET schema_version=25 WHERE id=1;
