CREATE TABLE storage_payloads (
    operation BLOB NOT NULL PRIMARY KEY,
    namespace_id BLOB NOT NULL,
    payload_id BLOB NOT NULL,
    revision INTEGER NOT NULL,
    cluster_id BLOB NOT NULL,
    intent TEXT NOT NULL CHECK(json_valid(intent)),
    state TEXT NOT NULL DEFAULT 'prepared',
    write_until INTEGER NOT NULL DEFAULT 0,
    protected_until INTEGER NOT NULL DEFAULT 0,
    CONSTRAINT storage_payload_revision CHECK (revision>0),
    CONSTRAINT storage_payload_state CHECK (state IN ('prepared','published','retired')),
    CONSTRAINT storage_payload_protection CHECK (protected_until>=0),
    CONSTRAINT storage_payload_cluster FOREIGN KEY (cluster_id) REFERENCES storage_clusters(id),
    CONSTRAINT storage_payload_identity UNIQUE(namespace_id,payload_id,revision)
) STRICT, WITHOUT ROWID;

CREATE TABLE storage_fragments (
    id BLOB NOT NULL PRIMARY KEY,
    operation BLOB NOT NULL,
    media_id BLOB NOT NULL,
    sequence INTEGER NOT NULL,
    copy_index INTEGER NOT NULL,
    shard_index INTEGER NOT NULL,
    header BLOB NOT NULL,
    file_bytes INTEGER NOT NULL,
    required INTEGER NOT NULL CHECK(required IN (0,1)),
    root BLOB,
    commitment BLOB,
    deleted INTEGER NOT NULL DEFAULT FALSE CHECK(deleted IN (0,1)),
    CONSTRAINT storage_fragment_operation FOREIGN KEY (operation) REFERENCES storage_payloads(operation),
    CONSTRAINT storage_fragment_slot UNIQUE(operation,sequence,copy_index,shard_index),
    CONSTRAINT storage_fragment_sequence CHECK (sequence BETWEEN 0 AND 4294967295),
    CONSTRAINT storage_fragment_copy CHECK (copy_index BETWEEN 0 AND 255),
    CONSTRAINT storage_fragment_shard CHECK (shard_index BETWEEN 0 AND 255),
    CONSTRAINT storage_fragment_header CHECK (length(header)=320),
    CONSTRAINT storage_fragment_size CHECK (file_bytes>=432),
    CONSTRAINT storage_fragment_receipt CHECK ((root IS NULL AND commitment IS NULL) OR (root IS NOT NULL AND commitment IS NOT NULL AND length(root)=32 AND length(commitment)=32))
) STRICT, WITHOUT ROWID;

CREATE INDEX storage_fragment_media ON storage_fragments(media_id,operation);
CREATE INDEX storage_fragment_reclaim ON storage_fragments(operation,deleted,id);
UPDATE mokyu_meta SET schema_version=20261113055500 WHERE id=1;
