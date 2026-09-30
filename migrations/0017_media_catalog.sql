CREATE EXTENSION IF NOT EXISTS pg_trgm;

ALTER TABLE objects ADD COLUMN catalog_size bigint,
    ADD COLUMN catalog_modified timestamptz,
    ADD COLUMN catalog_type text,
    ADD COLUMN catalog_kind text,
    ADD COLUMN catalog_public boolean;

CREATE TABLE catalog_build (
    singleton boolean PRIMARY KEY DEFAULT true CHECK (singleton),
    phase text NOT NULL DEFAULT 'indexes' CHECK (phase IN ('indexes','backfill','ready')),
    cursor_bucket uuid,
    cursor_key text COLLATE "C",
    scanned bigint NOT NULL DEFAULT 0,
    current_index text,
    last_error text,
    updated_at timestamptz NOT NULL DEFAULT now()
);
INSERT INTO catalog_build DEFAULT VALUES;

CREATE FUNCTION catalog_kind(key text, mime text) RETURNS text LANGUAGE sql IMMUTABLE AS $$
    SELECT CASE
        WHEN mime LIKE 'image/%' OR key ~* '\.(png|jpe?g|webp|gif|avif|svg|bmp|heic)$' THEN 'image'
        WHEN mime LIKE 'video/%' OR key ~* '\.(mp4|webm|mkv|mov|avi|m4v)$' THEN 'video'
        WHEN mime LIKE 'audio/%' OR key ~* '\.(mp3|ogg|opus|wav|flac|aac|m4a)$' THEN 'audio'
        WHEN mime IN ('application/zip','application/gzip','application/x-7z-compressed','application/x-tar') OR key ~* '\.(zip|gz|7z|rar|tar|zst)$' THEN 'archive'
        WHEN mime LIKE 'text/%' OR mime='application/pdf' OR key ~* '\.(pdf|txt|md|json|csv|html?|xml|docx?|xlsx?|pptx?)$' THEN 'document'
        ELSE 'other' END
$$;

CREATE FUNCTION catalog_object() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE item streams;
BEGIN
    IF NEW.stream_id IS NULL THEN
        NEW.catalog_size:=NULL; NEW.catalog_modified:=NULL; NEW.catalog_type:=NULL;
        NEW.catalog_kind:=NULL; NEW.catalog_public:=NULL;
    ELSE
        SELECT * INTO STRICT item FROM streams WHERE id=NEW.stream_id;
        NEW.catalog_size:=item.size;
        NEW.catalog_modified:=item.created_at;
        NEW.catalog_type:=coalesce(item.metadata->>'content_type','application/octet-stream');
        NEW.catalog_kind:=catalog_kind(NEW.key,NEW.catalog_type);
        NEW.catalog_public:=item.public_read;
    END IF;
    RETURN NEW;
END $$;
CREATE TRIGGER catalog_object BEFORE INSERT OR UPDATE OF stream_id ON objects FOR EACH ROW EXECUTE FUNCTION catalog_object();

CREATE FUNCTION catalog_stream() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    UPDATE objects SET catalog_size=NEW.size,catalog_modified=NEW.created_at,
        catalog_type=coalesce(NEW.metadata->>'content_type','application/octet-stream'),
        catalog_kind=catalog_kind(key,coalesce(NEW.metadata->>'content_type','application/octet-stream')),
        catalog_public=NEW.public_read WHERE stream_id=NEW.id;
    RETURN NULL;
END $$;
CREATE TRIGGER catalog_stream AFTER UPDATE OF size,metadata,public_read,created_at ON streams
    FOR EACH ROW WHEN (NEW.size IS DISTINCT FROM OLD.size OR NEW.metadata IS DISTINCT FROM OLD.metadata OR NEW.public_read IS DISTINCT FROM OLD.public_read OR NEW.created_at IS DISTINCT FROM OLD.created_at)
    EXECUTE FUNCTION catalog_stream();

-- Derived metadata updates do not change quota ownership or reservations.
DROP TRIGGER quota_object ON objects;
CREATE TRIGGER quota_object AFTER INSERT OR UPDATE OF stream_id,write_epoch OR DELETE ON objects FOR EACH ROW EXECUTE FUNCTION quota_object();
UPDATE mokyu_meta SET schema_version=17;
