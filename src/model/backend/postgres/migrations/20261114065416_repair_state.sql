ALTER TABLE storage_repairs DROP CONSTRAINT storage_repair_state;
ALTER TABLE storage_repairs ADD CONSTRAINT storage_repair_state CHECK(state::text IN ('queued','healthy','writing','installed'));

UPDATE mokyu_meta SET schema_version=20261114065416 WHERE singleton;
