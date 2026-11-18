PRAGMA application_id=1296778320;
PRAGMA user_version=1;
CREATE TABLE identity(value BLOB NOT NULL);
CREATE TABLE buckets(id BLOB, generation INTEGER, name TEXT NOT NULL, seq INTEGER NOT NULL, paused INTEGER DEFAULT 0, visible INTEGER DEFAULT 0, PRIMARY KEY(id,generation));
CREATE UNIQUE INDEX visible_names ON buckets(name) WHERE visible=1;
CREATE TABLE objects(bucket BLOB, generation INTEGER, key TEXT, seq INTEGER, size INTEGER, attributes BLOB, PRIMARY KEY(bucket,generation,key));
CREATE TABLE spans(bucket BLOB, seq INTEGER, offset INTEGER, source_offset INTEGER, length INTEGER, material BLOB, PRIMARY KEY(bucket,seq,offset));
CREATE TABLE materials(ref BLOB PRIMARY KEY, size INTEGER, pack BLOB, ordinal INTEGER, block_key BLOB, value BLOB NOT NULL);
CREATE TABLE keys(id BLOB PRIMARY KEY, value BLOB NOT NULL);
CREATE TEMP VIEW invalid_spans AS
  SELECT s.bucket,s.seq FROM spans s
  LEFT JOIN materials m ON m.ref=s.material
  LEFT JOIN materials p ON p.ref=m.pack
  LEFT JOIN keys k ON k.id=m.block_key
  WHERE m.ref IS NULL OR m.size-s.length<s.source_offset
     OR (m.pack IS NOT NULL AND p.ref IS NULL)
     OR (m.block_key IS NOT NULL AND k.id IS NULL);
CREATE TEMP TABLE paths(first BLOB PRIMARY KEY,path TEXT NOT NULL);
CREATE TEMP TABLE marks(bucket BLOB PRIMARY KEY,seq INTEGER,excluded INTEGER);
CREATE TEMP TABLE pages(bucket BLOB,generation INTEGER,seq INTEGER,n INTEGER,end INTEGER,digest BLOB,PRIMARY KEY(bucket,generation,seq));
CREATE TEMP TABLE ranges(bucket BLOB,generation INTEGER,seq INTEGER,offset INTEGER,source_offset INTEGER,length INTEGER,material BLOB,PRIMARY KEY(bucket,generation,seq,offset));
CREATE TEMP TABLE events(bucket BLOB,seq INTEGER,value BLOB,digest BLOB,applied INTEGER DEFAULT 0,PRIMARY KEY(bucket,seq));
CREATE INDEX pending_events ON events(bucket,seq) WHERE applied=0;
