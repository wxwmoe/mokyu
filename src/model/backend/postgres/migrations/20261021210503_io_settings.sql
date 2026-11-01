ALTER TABLE runtime_settings DROP CONSTRAINT runtime_settings_section_check;
ALTER TABLE runtime_settings ADD CONSTRAINT runtime_settings_section_check CHECK (section IN ('web','requests','uploads','metrics','maintenance','io'));
UPDATE mokyu_meta SET schema_version=61 WHERE singleton;
