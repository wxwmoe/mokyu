ALTER TABLE tasks DROP COLUMN batch_cursor;
ALTER TABLE nodes DROP COLUMN credential_version;
ALTER TABLE nodes DROP COLUMN load_score;
ALTER TABLE nodes DROP COLUMN load_sampled_at;
UPDATE mokyu_meta SET schema_version=38;
