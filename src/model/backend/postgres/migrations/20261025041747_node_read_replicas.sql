ALTER TABLE node_reads ADD COLUMN replica_allowed boolean NOT NULL DEFAULT false;

UPDATE mokyu_meta SET schema_version=74 WHERE singleton;
