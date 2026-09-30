ALTER TABLE web_users ADD COLUMN must_change_password boolean NOT NULL DEFAULT false;
CREATE INDEX web_users_directory ON web_users(username COLLATE "C");
UPDATE mokyu_meta SET schema_version=12;
