ALTER TABLE mokyu_meta
    ADD COLUMN deployment_mode VARCHAR(16) NOT NULL DEFAULT 'single',
    ADD CONSTRAINT mokyu_meta_mode CHECK (deployment_mode IN ('single', 'single_control')),
    ADD COLUMN deployment_mode_revision BIGINT NOT NULL DEFAULT 1 CHECK (deployment_mode_revision > 0),
    ADD COLUMN control_epoch BIGINT NOT NULL DEFAULT 0 CHECK (control_epoch >= 0);

UPDATE mokyu_meta SET schema_version = 20261102175500;
