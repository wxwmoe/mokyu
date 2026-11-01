ALTER TABLE buckets
    ADD COLUMN settings_revision bigint NOT NULL DEFAULT 0,
    ADD COLUMN uploads_paused boolean NOT NULL DEFAULT false,
    ADD COLUMN public_base_url text NOT NULL DEFAULT '';
CREATE FUNCTION bucket_settings_revision() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    NEW.settings_revision:=OLD.settings_revision+1;
    RETURN NEW;
END $$;
CREATE TRIGGER bucket_settings_revision BEFORE UPDATE OF cors,website_enabled,index_document,error_document,project_id,state,uploads_paused,public_base_url
ON buckets FOR EACH ROW EXECUTE FUNCTION bucket_settings_revision();
CREATE FUNCTION domain_settings_revision() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    UPDATE buckets SET settings_revision=settings_revision+1 WHERE id IN (NEW.bucket_id,OLD.bucket_id);
    RETURN NULL;
END $$;
CREATE TRIGGER domain_settings_revision AFTER INSERT OR UPDATE OR DELETE ON domains FOR EACH ROW EXECUTE FUNCTION domain_settings_revision();

CREATE OR REPLACE FUNCTION quota_bucket() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE q quota_accounts; p uuid; source quota_accounts;
BEGIN
    IF TG_OP='UPDATE' THEN
        IF OLD.project_id<>NEW.project_id THEN
            PERFORM 1 FROM quota_accounts WHERE kind='project' AND id IN (OLD.project_id,NEW.project_id) ORDER BY id FOR UPDATE;
            SELECT * INTO STRICT source FROM quota_accounts WHERE kind='bucket' AND id=NEW.id FOR UPDATE;
            IF source.reserved_bytes<>0 OR source.inflight_bytes<>0 THEN RAISE EXCEPTION 'wait for bucket uploads to drain' USING ERRCODE='MKQ02'; END IF;
            SELECT * INTO STRICT q FROM quota_accounts WHERE kind='project' AND id=NEW.project_id;
            IF (q.bucket_limit IS NOT NULL AND q.bucket_count>=q.bucket_limit) OR
               (source.used_bytes>0 AND q.byte_limit IS NOT NULL AND source.used_bytes>greatest(q.byte_limit-q.used_bytes-q.reserved_bytes,0)) THEN
                RAISE EXCEPTION 'storage quota exceeded' USING ERRCODE='MKQ01';
            END IF;
            UPDATE quota_accounts SET bucket_count=bucket_count-1,used_bytes=used_bytes-source.used_bytes,object_count=object_count-source.object_count
            WHERE kind='project' AND id=OLD.project_id;
            UPDATE quota_accounts SET bucket_count=bucket_count+1,used_bytes=used_bytes+source.used_bytes,object_count=object_count+source.object_count
            WHERE kind='project' AND id=NEW.project_id;
        END IF;
        RETURN NULL;
    END IF;
    p:=coalesce(NEW.project_id,OLD.project_id);
    SELECT * INTO STRICT q FROM quota_accounts WHERE kind='project' AND id=p FOR UPDATE;
    IF TG_OP='INSERT' THEN
        IF q.bucket_limit IS NOT NULL AND q.bucket_count>=q.bucket_limit THEN RAISE EXCEPTION 'storage quota exceeded' USING ERRCODE='MKQ01'; END IF;
        UPDATE quota_accounts SET bucket_count=bucket_count+1 WHERE kind='project' AND id=p;
        INSERT INTO quota_accounts(kind,id) VALUES('bucket',NEW.id);
    ELSE
        UPDATE quota_accounts SET bucket_count=bucket_count-1 WHERE kind='project' AND id=p;
        DELETE FROM quota_accounts WHERE kind='bucket' AND id=OLD.id;
    END IF;
    RETURN NULL;
END $$;
UPDATE mokyu_meta SET schema_version=19;
