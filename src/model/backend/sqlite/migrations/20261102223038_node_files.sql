ALTER TABLE raw_uploads ADD COLUMN node_id BLOB;
CREATE INDEX raw_uploads_node ON raw_uploads(node_id,next_attempt_at,source_id);
ALTER TABLE multipart_files ADD COLUMN node_id BLOB;
ALTER TABLE multipart_files ADD COLUMN part_stream INTEGER;
CREATE UNIQUE INDEX multipart_files_part ON multipart_files(part_stream);
CREATE INDEX multipart_files_node ON multipart_files(node_id,state,id);
ALTER TABLE pending_uploads ADD COLUMN node_id BLOB;
CREATE INDEX pending_uploads_node ON pending_uploads(node_id,chunk_id);
ALTER TABLE tasks ADD COLUMN source_node BLOB;
CREATE INDEX tasks_source_node ON tasks(source_node,state,task_type);

UPDATE mokyu_meta SET schema_version = 20261102223038;
