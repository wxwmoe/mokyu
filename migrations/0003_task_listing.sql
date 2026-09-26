CREATE INDEX tasks_list ON tasks(created_at DESC,id DESC);
CREATE INDEX tasks_state_list ON tasks(state,created_at DESC,id DESC);
DROP INDEX tasks_active;

UPDATE gateway_meta SET schema_version=3;
