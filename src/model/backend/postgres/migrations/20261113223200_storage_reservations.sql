ALTER TABLE storage_clusters ADD COLUMN allocation_group UUID;

CREATE TABLE storage_reservations (
    operation UUID NOT NULL,
    media_id UUID NOT NULL,
    cluster_id UUID NOT NULL,
    bytes BIGINT NOT NULL CHECK(bytes>0),
    accounting_boot UUID,
    accounting_revision BIGINT NOT NULL DEFAULT 0 CHECK(accounting_revision>=0),
    complete BOOLEAN NOT NULL DEFAULT FALSE,
    PRIMARY KEY(operation,media_id),
    CONSTRAINT storage_reservation_payload FOREIGN KEY(operation) REFERENCES storage_payloads(operation),
    CONSTRAINT storage_reservation_cluster FOREIGN KEY(cluster_id) REFERENCES storage_clusters(id)
);

CREATE INDEX storage_reservation_totals ON storage_reservations(cluster_id,media_id);
CREATE INDEX storage_reservation_settlement ON storage_reservations(media_id,complete,accounting_boot,accounting_revision);
UPDATE mokyu_meta SET schema_version=20261113223200 WHERE singleton;
