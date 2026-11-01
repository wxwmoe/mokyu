CREATE TABLE transfer_imports (
    item_id uuid PRIMARY KEY REFERENCES transfer_items(id) ON DELETE CASCADE,
    part_stream uuid REFERENCES streams(id),
    node_id uuid,
    operation_id uuid,
    CHECK ((node_id IS NULL)=(operation_id IS NULL))
);
CREATE INDEX transfer_imports_part ON transfer_imports(part_stream);

UPDATE mokyu_meta SET schema_version=96 WHERE singleton;
