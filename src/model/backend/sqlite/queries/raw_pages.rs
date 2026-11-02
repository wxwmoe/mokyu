pub(in super::super) type Connection = sqlx::SqliteConnection;
pub(in super::super) const LOCK: &str =
    "SELECT state,size,digest,page_count FROM raw_sources WHERE id=?";
pub(in super::super) const INSERT: &str =
    "INSERT INTO raw_page_groups(source_id,group_no,hashes) VALUES(?,?,?) ON CONFLICT DO NOTHING";
pub(in super::super) const GROUP_HASHES: &str =
    "SELECT hashes FROM raw_page_groups WHERE source_id=? AND group_no=?";
pub(in super::super) const COUNTS: &str = "SELECT COUNT(*),COUNT(CASE WHEN (group_no<? AND length(hashes)=8192) OR (group_no=? AND length(hashes)=?) THEN 1 END) FROM raw_page_groups WHERE source_id=?";
pub(in super::super) const SEAL: &str =
    "UPDATE raw_sources SET page_count=? WHERE id=? AND page_count IS NULL";
pub(in super::super) const READ: &str = "SELECT r.page_count,g.group_no,g.hashes FROM raw_sources r LEFT JOIN raw_page_groups g ON g.source_id=r.id AND g.group_no BETWEEN ? AND ? WHERE r.id=? AND r.size=? AND r.state='ready' ORDER BY g.group_no";
pub(in super::super) const CLEAR: &str = "DELETE FROM raw_page_groups WHERE source_id=?";
pub(in super::super) const COPY_GROUPS: &str = "INSERT INTO raw_page_groups(source_id,group_no,hashes) SELECT ?,group_no,hashes FROM raw_page_groups WHERE source_id=?";
