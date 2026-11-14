CREATE TABLE storage_journal_publications (
    id UUID NOT NULL PRIMARY KEY,
    namespace_id UUID NOT NULL,
    cluster_id UUID NOT NULL,
    revision BIGINT NOT NULL CHECK(revision>0),
    manifest_revision BIGINT NOT NULL CHECK(manifest_revision>0),
    proof JSONB NOT NULL,
    settled BOOLEAN NOT NULL DEFAULT FALSE,
    CONSTRAINT storage_journal_publication_revision UNIQUE(namespace_id,revision)
);

CREATE TABLE storage_journal_receipts (
    namespace_id UUID NOT NULL,
    media_id UUID NOT NULL,
    revision BIGINT NOT NULL CHECK(revision>=0),
    PRIMARY KEY(namespace_id,media_id)
);

UPDATE mokyu_meta SET schema_version=20261114003300 WHERE singleton;
