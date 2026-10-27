ALTER TABLE nodes DROP CONSTRAINT nodes_role_check;
ALTER TABLE nodes ADD CONSTRAINT nodes_role_check CHECK (role IN ('control','worker','data','node'));

UPDATE mokyu_meta SET schema_version=87 WHERE singleton;
