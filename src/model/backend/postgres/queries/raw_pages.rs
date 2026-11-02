pub(in super::super) type Connection = sqlx::PgConnection;
pub(in super::super) const LOCK: &str =
    "SELECT state,size,digest,page_count FROM raw_sources WHERE id=$1 FOR UPDATE";
pub(in super::super) const INSERT: &str = "INSERT INTO raw_page_groups(source_id,group_no,hashes) VALUES($1,$2,$3) ON CONFLICT DO NOTHING";
pub(in super::super) const GROUP_HASHES: &str =
    "SELECT hashes FROM raw_page_groups WHERE source_id=$1 AND group_no=$2";
pub(in super::super) const COUNTS: &str = "SELECT COUNT(*),COUNT(CASE WHEN (group_no<$1 AND length(hashes)=8192) OR (group_no=$2 AND length(hashes)=$3) THEN 1 END) FROM raw_page_groups WHERE source_id=$4";
pub(in super::super) const SEAL: &str =
    "UPDATE raw_sources SET page_count=$1 WHERE id=$2 AND page_count IS NULL";
pub(in super::super) const READ: &str = "SELECT r.page_count,g.group_no,g.hashes FROM raw_sources r LEFT JOIN raw_page_groups g ON g.source_id=r.id AND g.group_no BETWEEN $1 AND $2 WHERE r.id=$3 AND r.size=$4 AND r.state='ready' ORDER BY g.group_no";
pub(in super::super) const CLEAR: &str = "DELETE FROM raw_page_groups WHERE source_id=$1";
pub(in super::super) const COPY_GROUPS: &str = "INSERT INTO raw_page_groups(source_id,group_no,hashes) SELECT $1,group_no,hashes FROM raw_page_groups WHERE source_id=$2";
