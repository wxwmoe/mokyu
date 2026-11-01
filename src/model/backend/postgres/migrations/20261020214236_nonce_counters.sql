CREATE TABLE nonce_counters (
    block_key_id uuid NOT NULL REFERENCES managed_keys(id),
    type_tag bigint NOT NULL CHECK (type_tag BETWEEN 0 AND 31),
    reserved_id bigint NOT NULL DEFAULT 0 CHECK (reserved_id>=0),
    PRIMARY KEY (block_key_id,type_tag)
);

CREATE UNIQUE INDEX managed_keys_fingerprint ON managed_keys(fingerprint);

ALTER TABLE chunk_encodings ADD COLUMN nonce_id bigint;
UPDATE chunk_encodings SET nonce_id=encoding_id WHERE nonce IS NOT NULL;
ALTER TABLE chunk_encodings ADD CONSTRAINT chunk_encodings_nonce_id CHECK ((encryption_algorithm='none' AND nonce_id IS NULL AND nonce IS NULL AND block_key_id IS NULL) OR (encryption_algorithm<>'none' AND block_key_id IS NOT NULL AND ((nonce IS NULL AND nonce_id IS NULL AND state<>'ready') OR (nonce IS NOT NULL AND nonce_id IS NOT NULL AND nonce_id>0))));

ALTER TABLE packs ADD COLUMN nonce_id bigint;
UPDATE packs SET nonce_id=id WHERE nonce IS NOT NULL;
ALTER TABLE packs ADD CONSTRAINT packs_nonce_id CHECK ((encryption_algorithm='none' AND nonce_id IS NULL AND nonce IS NULL AND block_key_id IS NULL) OR (encryption_algorithm<>'none' AND block_key_id IS NOT NULL AND ((nonce IS NULL AND nonce_id IS NULL AND state<>'ready') OR (nonce IS NOT NULL AND nonce_id IS NOT NULL AND nonce_id>0))));

INSERT INTO nonce_counters(block_key_id,type_tag,reserved_id) SELECT k.id,0,s.last_value FROM managed_keys k CROSS JOIN chunk_locations_id_seq s WHERE k.purpose='block';
INSERT INTO nonce_counters(block_key_id,type_tag,reserved_id) SELECT k.id,1,s.last_value FROM managed_keys k CROSS JOIN packs_id_seq s WHERE k.purpose='block';

UPDATE mokyu_meta SET schema_version=50 WHERE singleton;
