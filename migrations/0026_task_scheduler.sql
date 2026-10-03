ALTER TABLE tasks DROP CONSTRAINT tasks_kind_check;
ALTER TABLE tasks ADD CONSTRAINT tasks_kind_check CHECK(kind IN ('purge','sweep','integrity','pack','unpack','upload','cache_flush','gc','cleanup','storage_migration'));

UPDATE mokyu_meta SET schema_version=26;
