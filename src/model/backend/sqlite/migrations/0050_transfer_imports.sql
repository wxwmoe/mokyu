CREATE TABLE transfer_imports (
    item_id INTEGER PRIMARY KEY REFERENCES transfer_items(id) ON DELETE CASCADE,
    part_stream INTEGER REFERENCES streams(id),
    node_id BLOB CHECK (node_id IS NULL OR length(node_id)=16),
    operation_id BLOB CHECK (operation_id IS NULL OR length(operation_id)=16),
    CHECK ((node_id IS NULL)=(operation_id IS NULL))
);
CREATE INDEX transfer_imports_part ON transfer_imports(part_stream);

UPDATE mokyu_meta SET schema_version=50 WHERE id=1;
