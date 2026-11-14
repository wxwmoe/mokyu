ALTER TABLE raw_sources DROP CONSTRAINT raw_sources_medium_check;
UPDATE raw_sources SET medium='shared' WHERE medium='shared_s3';
ALTER TABLE raw_sources ADD CONSTRAINT raw_sources_medium_check CHECK(medium IN ('local','memory','shared'));
UPDATE mokyu_meta SET schema_version=20261113183012 WHERE singleton;
