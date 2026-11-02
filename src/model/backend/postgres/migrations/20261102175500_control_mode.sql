ALTER TABLE mokyu_meta
    DROP CONSTRAINT mokyu_meta_deployment_mode_check,
    ADD CHECK (deployment_mode IN ('single', 'single_control', 'cluster')),
    ADD COLUMN control_epoch BIGINT NOT NULL DEFAULT 0 CHECK (control_epoch >= 0);

UPDATE mokyu_meta SET schema_version = 20261102175500;
