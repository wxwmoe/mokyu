-- MySQL DDL commits independently; retrying after an interrupted ALTER is safe.
SET @mokyu_http_compression_ddl = IF(
    EXISTS(SELECT 1 FROM information_schema.columns
           WHERE table_schema=DATABASE() AND table_name='streams' AND column_name='http_compression'),
    'DO 0',
    'ALTER TABLE streams ADD COLUMN http_compression JSON NOT NULL DEFAULT (JSON_OBJECT())'
);
PREPARE mokyu_http_compression FROM @mokyu_http_compression_ddl;
EXECUTE mokyu_http_compression;
DEALLOCATE PREPARE mokyu_http_compression;
UPDATE mokyu_meta SET schema_version=17 WHERE id=1;
