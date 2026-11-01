ALTER TABLE bucket_change_gates ADD COLUMN required_targets INTEGER NOT NULL DEFAULT 0 CHECK (required_targets>=0);
ALTER TABLE redundancy_links ADD COLUMN synchronous BOOLEAN NOT NULL DEFAULT FALSE CHECK (NOT synchronous OR target_bucket IS NOT NULL);
CREATE INDEX redundancy_links_admission ON redundancy_links(source_bucket,synchronous,state,id);

CREATE FUNCTION count_required_targets() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
    delta INTEGER := 0;
BEGIN
    IF TG_OP<>'INSERT' AND OLD.synchronous AND OLD.state NOT IN ('pausing','paused','removing') THEN delta := delta-1; END IF;
    IF TG_OP<>'DELETE' AND NEW.synchronous AND NEW.state NOT IN ('pausing','paused','removing') THEN delta := delta+1; END IF;
    IF delta<>0 THEN
        UPDATE bucket_change_gates SET required_targets=required_targets+delta WHERE bucket_id=COALESCE(NEW.source_bucket,OLD.source_bucket);
    END IF;
    RETURN NULL;
END;
$$;
CREATE TRIGGER redundancy_links_required AFTER INSERT OR DELETE OR UPDATE OF synchronous,state ON redundancy_links FOR EACH ROW EXECUTE FUNCTION count_required_targets();

CREATE TABLE replica_publications (
    id uuid NOT NULL PRIMARY KEY,
    source_bucket uuid NOT NULL,
    object_key text COLLATE "C" NOT NULL,
    source_pin uuid,
    phase text NOT NULL DEFAULT 'admitted' CHECK (phase IN ('admitted','preparing','published','aborted')),
    early_ack BOOLEAN NOT NULL DEFAULT FALSE,
    intent jsonb,
    intent_hash bytea,
    result jsonb,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    FOREIGN KEY (source_bucket) REFERENCES buckets(id),
    FOREIGN KEY (source_pin) REFERENCES streams(id),
    CHECK (OCTET_LENGTH(object_key) BETWEEN 1 AND 1024),
    CHECK (intent_hash IS NULL OR OCTET_LENGTH(intent_hash)=32)
);
CREATE INDEX replica_publications_pin ON replica_publications(source_pin);
CREATE INDEX replica_publications_recovery ON replica_publications(phase,updated_at,id);

CREATE TABLE replica_targets (
    publication_id uuid NOT NULL,
    link_id uuid NOT NULL,
    link_revision BIGINT NOT NULL CHECK (link_revision>0),
    target_bucket uuid NOT NULL,
    target_key text COLLATE "C" NOT NULL,
    storage_id uuid NOT NULL,
    profile_revision BIGINT NOT NULL CHECK (profile_revision>0),
    authority jsonb NOT NULL,
    permissions text NOT NULL,
    expected_version uuid,
    candidate uuid,
    task_id uuid,
    phase text NOT NULL DEFAULT 'admitted' CHECK (phase IN ('admitted','preparing','prepared','published','aborted')),
    PRIMARY KEY (publication_id,link_id),
    FOREIGN KEY (publication_id) REFERENCES replica_publications(id) ON DELETE CASCADE,
    FOREIGN KEY (target_bucket) REFERENCES buckets(id),
    FOREIGN KEY (storage_id) REFERENCES storages(id),
    FOREIGN KEY (candidate) REFERENCES streams(id),
    FOREIGN KEY (task_id) REFERENCES tasks(id),
    CHECK (OCTET_LENGTH(target_key) BETWEEN 1 AND 1024)
);
CREATE INDEX replica_targets_link ON replica_targets(link_id,phase,publication_id);
CREATE UNIQUE INDEX replica_targets_task ON replica_targets(task_id);
CREATE INDEX replica_targets_candidate ON replica_targets(candidate);

ALTER TABLE streams ADD COLUMN publication_id uuid REFERENCES replica_publications(id);
CREATE INDEX streams_publication ON streams(publication_id) WHERE publication_id IS NOT NULL;

ALTER TABLE node_writes DROP CONSTRAINT node_writes_check1;
ALTER TABLE node_writes ADD CONSTRAINT node_writes_publish_receipt CHECK (result IS NULL OR publish_hash IS NOT NULL);

UPDATE mokyu_meta SET schema_version=99;
