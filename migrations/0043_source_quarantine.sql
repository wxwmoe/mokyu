ALTER TABLE chunk_encodings
    ADD COLUMN quarantined_at timestamptz,
    ADD COLUMN quarantine_code text,
    ADD COLUMN quarantine_revision bigint NOT NULL DEFAULT 0,
    ADD CONSTRAINT encodings_quarantine CHECK ((quarantined_at IS NULL) = (quarantine_code IS NULL)),
    ADD CONSTRAINT encodings_quarantine_revision CHECK (quarantine_revision >= 0);
ALTER TABLE packs
    ADD COLUMN quarantined_at timestamptz,
    ADD COLUMN quarantine_code text,
    ADD COLUMN quarantine_revision bigint NOT NULL DEFAULT 0,
    ADD CONSTRAINT packs_quarantine CHECK ((quarantined_at IS NULL) = (quarantine_code IS NULL)),
    ADD CONSTRAINT packs_quarantine_revision CHECK (quarantine_revision >= 0);
CREATE INDEX encodings_quarantined ON chunk_encodings (storage_id, encoding_id) WHERE quarantined_at IS NOT NULL;
CREATE INDEX packs_quarantined ON packs (storage_id, id) WHERE quarantined_at IS NOT NULL;
ALTER TABLE maintenance_previews DROP CONSTRAINT maintenance_previews_action_check,
    ADD CONSTRAINT maintenance_previews_action_check CHECK (action IN ('unpack','sweep','quarantine','recheck'));
UPDATE mokyu_meta SET schema_version=43;
