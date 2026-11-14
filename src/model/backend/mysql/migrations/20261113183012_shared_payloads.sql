SET @ddl = IF(LOCATE('MariaDB',VERSION())>0,
    'SET @medium_column=(SELECT COUNT(*) FROM information_schema.CHECK_CONSTRAINTS WHERE CONSTRAINT_SCHEMA=DATABASE() AND TABLE_NAME=''raw_sources'' AND LEVEL=''Column'' AND CHECK_CLAUSE LIKE ''%shared_s3%'')',
    'SET @medium_column=0');
PREPARE storage_ddl FROM @ddl;
EXECUTE storage_ddl;
DEALLOCATE PREPARE storage_ddl;
SET @ddl = IF(@medium_column>0,
    'ALTER TABLE raw_sources MODIFY COLUMN medium varchar(10) CHARACTER SET ascii COLLATE ascii_bin NOT NULL DEFAULT ''local''', 'SELECT 1');
PREPARE storage_ddl FROM @ddl;
EXECUTE storage_ddl;
DEALLOCATE PREPARE storage_ddl;
SET @medium_check = (SELECT tc.CONSTRAINT_NAME FROM information_schema.TABLE_CONSTRAINTS tc JOIN information_schema.CHECK_CONSTRAINTS cc ON cc.CONSTRAINT_SCHEMA=tc.CONSTRAINT_SCHEMA AND cc.CONSTRAINT_NAME=tc.CONSTRAINT_NAME WHERE tc.TABLE_SCHEMA=DATABASE() AND tc.TABLE_NAME='raw_sources' AND tc.CONSTRAINT_TYPE='CHECK' AND cc.CHECK_CLAUSE LIKE '%shared_s3%' LIMIT 1);
SET @ddl = IF(@medium_check IS NULL,'SELECT 1',CONCAT('ALTER TABLE raw_sources DROP ',
    IF(LOCATE('MariaDB',VERSION())>0,'CONSTRAINT','CHECK'),' `',REPLACE(@medium_check,'`','``'),'`'));
PREPARE storage_ddl FROM @ddl;
EXECUTE storage_ddl;
DEALLOCATE PREPARE storage_ddl;
UPDATE raw_sources SET medium='shared' WHERE medium='shared_s3';
SET @ddl = IF(EXISTS(SELECT 1 FROM information_schema.TABLE_CONSTRAINTS WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='raw_sources' AND CONSTRAINT_NAME='raw_sources_medium'),'SELECT 1',
    'ALTER TABLE raw_sources ADD CONSTRAINT raw_sources_medium CHECK(medium IN (''local'',''memory'',''shared''))');
PREPARE storage_ddl FROM @ddl;
EXECUTE storage_ddl;
DEALLOCATE PREPARE storage_ddl;
UPDATE mokyu_meta SET schema_version=20261113183012 WHERE id=1;
