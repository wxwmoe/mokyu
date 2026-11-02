ALTER TABLE mokyu_meta ADD COLUMN deployment_mode TEXT NOT NULL DEFAULT 'single'
    CHECK (deployment_mode IN ('single', 'single_control'));
ALTER TABLE mokyu_meta ADD COLUMN deployment_mode_revision INTEGER NOT NULL DEFAULT 1
    CHECK (deployment_mode_revision > 0);
ALTER TABLE mokyu_meta ADD COLUMN control_epoch INTEGER NOT NULL DEFAULT 0 CHECK (control_epoch >= 0);

UPDATE mokyu_meta SET schema_version = 20261102175500;
