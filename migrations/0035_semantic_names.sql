ALTER TABLE objects RENAME COLUMN catalog_kind TO catalog_category;
ALTER FUNCTION catalog_kind(text, text) RENAME TO catalog_category;
ALTER INDEX IF EXISTS objects_catalog_kind RENAME TO objects_catalog_category;
UPDATE catalog_build SET current_index='objects_catalog_category'
WHERE current_index='objects_catalog_kind';

-- PL/pgSQL bodies retain column and function names as text.
CREATE OR REPLACE FUNCTION catalog_object() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE item streams;
BEGIN
    IF NEW.stream_id IS NULL THEN
        NEW.catalog_size:=NULL; NEW.catalog_modified:=NULL; NEW.catalog_type:=NULL;
        NEW.catalog_category:=NULL; NEW.catalog_public:=NULL;
    ELSE
        SELECT * INTO STRICT item FROM streams WHERE id=NEW.stream_id;
        NEW.catalog_size:=item.size;
        NEW.catalog_modified:=item.created_at;
        NEW.catalog_type:=coalesce(item.metadata->>'content_type','application/octet-stream');
        NEW.catalog_category:=catalog_category(NEW.object_key,NEW.catalog_type);
        NEW.catalog_public:=item.public_read;
    END IF;
    RETURN NEW;
END $$;

CREATE OR REPLACE FUNCTION catalog_stream() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    UPDATE objects SET catalog_size=NEW.size,catalog_modified=NEW.created_at,
        catalog_type=coalesce(NEW.metadata->>'content_type','application/octet-stream'),
        catalog_category=catalog_category(object_key,coalesce(NEW.metadata->>'content_type','application/octet-stream')),
        catalog_public=NEW.public_read WHERE stream_id=NEW.id;
    RETURN NULL;
END $$;

ALTER TABLE maintenance_controls RENAME COLUMN kind TO policy;
ALTER TABLE maintenance_controls RENAME CONSTRAINT maintenance_controls_kind_check TO maintenance_controls_policy_check;

DROP INDEX tasks_kind_recent;
UPDATE tasks SET detail=jsonb_build_object('policy',detail->'kind') || (detail-'kind')
WHERE detail ? 'kind';
CREATE INDEX tasks_policy_recent ON tasks((COALESCE(detail->>'policy',task_type)),created_at DESC,id DESC);

UPDATE mokyu_meta SET schema_version=35;
