CREATE TABLE transfer_imports (
    item_id BIGINT PRIMARY KEY,
    part_stream BIGINT,
    node_id BINARY(16),
    operation_id BINARY(16),
    FOREIGN KEY (item_id) REFERENCES transfer_items(id) ON DELETE CASCADE,
    FOREIGN KEY (part_stream) REFERENCES streams(id),
    CHECK ((node_id IS NULL)=(operation_id IS NULL))
);
CREATE INDEX transfer_imports_part ON transfer_imports(part_stream);

UPDATE mokyu_meta SET schema_version=49 WHERE id=1;
