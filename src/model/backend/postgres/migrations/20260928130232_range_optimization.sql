ALTER TABLE pack_access_windows ADD COLUMN partial_bytes bigint NOT NULL DEFAULT 0;
ALTER TABLE pack_access_windows ADD COLUMN updated_at timestamptz NOT NULL DEFAULT now();
ALTER TABLE packs ADD COLUMN range_checked_at timestamptz;
CREATE TABLE pack_member_access_windows (
    window_start timestamptz NOT NULL,
    pack_id bigint NOT NULL REFERENCES packs(id) ON DELETE CASCADE,
    chunk_id bigint NOT NULL REFERENCES chunks(id) ON DELETE CASCADE,
    downloads bigint NOT NULL DEFAULT 0,
    PRIMARY KEY(window_start,pack_id,chunk_id)
);
CREATE INDEX pack_member_access_lookup ON pack_member_access_windows(pack_id,window_start);
CREATE INDEX chunks_range_repack ON chunks(repack_after,id) WHERE range_split_at IS NOT NULL;
UPDATE gateway_meta SET schema_version=7;
