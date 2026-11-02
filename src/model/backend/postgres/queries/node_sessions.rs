use super::*;

pub(in super::super) fn quarantine_memory_raw(boot: Uuid) -> Statement<'static> {
    sqlx::query("UPDATE raw_sources SET pending_upload=FALSE,quarantine_code='remote_missing',quarantined_at=clock_timestamp(),revision=revision+1 WHERE durability IN ('buffered','volatile') AND state='ready' AND quarantine_code IS NULL AND medium='memory' AND boot_id<>$1 AND quarantine_code IS NULL").bind(boot)
}

#[derive(sqlx::FromRow)]
pub(in super::super) struct RegisteredNode {
    pub role: String,
    pub enabled: bool,
    pub credential_encrypted: Option<Vec<u8>>,
    pub credential_generation: i64,
    pub deployment_id: Uuid,
    pub deployment_mode: String,
}

pub(in super::super) fn registered_node(
    id: Uuid,
    exclusive: bool,
) -> Rows<'static, RegisteredNode> {
    let sql = if exclusive {
        "SELECT n.role,n.enabled,n.credential_encrypted,n.credential_generation,m.deployment_id,m.deployment_mode FROM nodes n CROSS JOIN mokyu_meta m WHERE n.id=$1 AND m.singleton FOR UPDATE OF n"
    } else {
        "SELECT n.role,n.enabled,n.credential_encrypted,n.credential_generation,m.deployment_id,m.deployment_mode FROM nodes n CROSS JOIN mokyu_meta m WHERE n.id=$1 AND m.singleton FOR SHARE OF n"
    };
    sqlx::query_as(sql).bind(id)
}

pub(in super::super) fn node_registration_lock(name: &str) -> Statement<'_> {
    sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1,724))").bind(name)
}

pub(in super::super) fn node_registration(name: &str) -> Rows<'_, (Uuid, String, bool, i64)> {
    sqlx::query_as(
        "SELECT id,role,enabled,credential_generation FROM nodes WHERE name=$1 FOR UPDATE",
    )
    .bind(name)
}

pub(in super::super) fn node_deployment() -> Rows<'static, (Uuid, String)> {
    sqlx::query_as("SELECT deployment_id,deployment_mode FROM mokyu_meta WHERE singleton")
}

pub(in super::super) fn other_control(name: &str) -> Scalar<'_, bool> {
    sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM nodes WHERE enabled AND role='control' AND name<>$1)",
    )
    .bind(name)
}

pub(in super::super) fn new_node_id() -> Scalar<'static, Uuid> {
    sqlx::query_scalar("SELECT uuidv7()")
}

pub(in super::super) fn save_node_identity<'q>(
    state: &'q crate::cluster::NodeState,
    name: &'q str,
    encrypted: &'q [u8],
) -> Statement<'q> {
    sqlx::query("INSERT INTO nodes(id,name,role,credential_encrypted,credential_generation) VALUES($1,$2,$3,$4,$5) ON CONFLICT(id) DO UPDATE SET credential_encrypted=EXCLUDED.credential_encrypted,credential_generation=EXCLUDED.credential_generation,enabled=true,status='registered',recovery_required=false,last_seen_at=NULL,updated_at=now()")
        .bind(state.node_id).bind(name).bind(&state.role).bind(encrypted).bind(state.credential_generation)
}

pub(in super::super) fn node_registration_conflict(node: Uuid) -> Statement<'static> {
    sqlx::query("UPDATE nodes SET status='recovery_required',recovery_required=true,updated_at=now() WHERE id=$1").bind(node)
}

pub(in super::super) fn clear_node_session(node: Uuid) -> Statement<'static> {
    sqlx::query("DELETE FROM node_sessions WHERE node_id=$1").bind(node)
}

pub(in super::super) fn clear_node_boots(node: Uuid) -> Statement<'static> {
    sqlx::query("DELETE FROM node_boots WHERE node_id=$1").bind(node)
}

pub(in super::super) fn node_session(
    node: Uuid,
    exclusive: bool,
) -> Rows<'static, crate::model::session::Session> {
    let sql = if exclusive {
        "SELECT credential_generation,boot_id,fence,epoch,connection_id,expires_at,sequence,draining FROM node_sessions WHERE node_id=$1 FOR UPDATE"
    } else {
        "SELECT credential_generation,boot_id,fence,epoch,connection_id,expires_at,sequence,draining FROM node_sessions WHERE node_id=$1 FOR SHARE"
    };
    sqlx::query_as(sql).bind(node)
}

pub(in super::super) fn demand_clock() -> Rows<'static, (DateTime<Utc>, bool)> {
    sqlx::query_as("SELECT clock_timestamp(),EXISTS(SELECT 1 FROM bucket_tiering)")
}

pub(in super::super) fn node_clock() -> Scalar<'static, DateTime<Utc>> {
    sqlx::query_scalar("SELECT clock_timestamp()")
}

pub(in super::super) fn node_read_admission() -> Scalar<'static, bool> {
    sqlx::query_scalar(
        "SELECT NOT maintenance AND deployment_mode='cluster' FROM mokyu_meta WHERE singleton FOR SHARE",
    )
}

pub(in super::super) fn node_read_credential(access: &str) -> Rows<'_, (i64, Vec<u8>)> {
    sqlx::query_as("SELECT authorization_revision,secret_encrypted FROM s3_credentials WHERE access_key_id=$1 AND enabled AND (expires_at IS NULL OR expires_at>clock_timestamp()) FOR SHARE").bind(access)
}

pub(in super::super) fn node_read_bucket(route: &crate::model::ReadRoute) -> Rows<'_, Bucket> {
    match route.base() {
        crate::model::ReadRoute::Image { .. } => unreachable!(),
        crate::model::ReadRoute::Bucket { name, alias, .. } => sqlx::query_as("SELECT * FROM buckets WHERE id=(SELECT id FROM buckets WHERE name IN ($1,$2) ORDER BY CASE WHEN name=$2 THEN 0 ELSE 1 END LIMIT 1) FOR SHARE")
            .bind(name).bind(alias.as_ref().map(|(name, _)| name)),
        crate::model::ReadRoute::Host { host, .. } => sqlx::query_as("SELECT b.* FROM buckets b JOIN bucket_domains d ON d.bucket_id=b.id WHERE d.host=$1 FOR SHARE OF b,d").bind(host),
    }
}

pub(in super::super) fn node_read_allowed(
    access: &str,
    project: Uuid,
    bucket: Uuid,
) -> Scalar<'_, bool> {
    node_request_allowed(access, project, bucket, "object.read")
}

pub(in super::super) fn node_request_allowed<'q>(
    access: &'q str,
    project: Uuid,
    bucket: Uuid,
    action: &'q str,
) -> Scalar<'q, bool> {
    sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM s3_credential_grants g JOIN s3_credentials c ON c.access_key_id=g.access_key WHERE g.access_key=$1 AND c.project_id=$2 AND g.bucket_id=$3 AND $4=ANY(g.actions))")
        .bind(access).bind(project).bind(bucket).bind(action)
}

pub(in super::super) fn node_read_release<'q>(
    state: &'q crate::cluster::NodeState,
    run: crate::model::session::Run,
    ids: &'q [Uuid],
) -> Statement<'q> {
    sqlx::query("DELETE FROM stream_leases l USING node_reads r WHERE l.id=r.read_id AND r.node_id=$1 AND r.credential_generation=$2 AND r.boot_id=$3 AND r.fence=$4 AND l.id=ANY($5)")
        .bind(state.node_id).bind(state.credential_generation).bind(run.boot).bind(run.fence).bind(ids)
}

pub(in super::super) fn node_boot_retired(node: Uuid, boot: Uuid) -> Scalar<'static, bool> {
    sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM node_boots WHERE node_id=$1 AND boot_id=$2 AND retired_at IS NOT NULL)").bind(node).bind(boot)
}

pub(in super::super) fn close_node_session(node: Uuid) -> Statement<'static> {
    sqlx::query(
        "UPDATE node_sessions SET expires_at=clock_timestamp(),draining=true WHERE node_id=$1",
    )
    .bind(node)
}

pub(in super::super) fn node_boot_known(node: Uuid, boot: Uuid) -> Scalar<'static, bool> {
    sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM node_boots WHERE node_id=$1 AND boot_id=$2)")
        .bind(node)
        .bind(boot)
}

pub(in super::super) fn node_boot_count(node: Uuid) -> Scalar<'static, i64> {
    sqlx::query_scalar("SELECT count(*) FROM node_boots WHERE node_id=$1").bind(node)
}

pub(in super::super) fn retire_node_boots(node: Uuid) -> Statement<'static> {
    sqlx::query("UPDATE node_boots SET retired_at=clock_timestamp() WHERE node_id=$1 AND retired_at IS NULL").bind(node)
}

pub(in super::super) fn insert_node_boot(
    node: Uuid,
    generation: i64,
    boot: Uuid,
) -> Statement<'static> {
    sqlx::query("INSERT INTO node_boots(node_id,credential_generation,boot_id) VALUES($1,$2,$3)")
        .bind(node)
        .bind(generation)
        .bind(boot)
}

pub(in super::super) fn save_node_session(
    node: Uuid,
    generation: i64,
    run: crate::model::session::Run,
    epoch: i64,
    connection: Uuid,
    draining: bool,
) -> Statement<'static> {
    sqlx::query("INSERT INTO node_sessions(node_id,credential_generation,boot_id,fence,epoch,connection_id,expires_at,draining) VALUES($1,$2,$3,$4,$5,$6,clock_timestamp()+$7*interval '1 second',$8) ON CONFLICT(node_id) DO UPDATE SET credential_generation=EXCLUDED.credential_generation,boot_id=EXCLUDED.boot_id,fence=EXCLUDED.fence,epoch=EXCLUDED.epoch,connection_id=EXCLUDED.connection_id,expires_at=EXCLUDED.expires_at,sequence=0,draining=EXCLUDED.draining")
        .bind(node).bind(generation).bind(run.boot).bind(run.fence).bind(epoch).bind(connection).bind(crate::model::session::LEASE_SECONDS).bind(draining)
}

pub(in super::super) fn pulse_node_session(
    node: Uuid,
    sequence: i64,
    draining: bool,
) -> Statement<'static> {
    sqlx::query("UPDATE node_sessions SET sequence=$2,draining=draining OR $3,expires_at=clock_timestamp()+$4*interval '1 second' WHERE node_id=$1")
        .bind(node).bind(sequence).bind(draining).bind(crate::model::session::LEASE_SECONDS)
}

pub(in super::super) fn maintain_node_session(node: Uuid, sequence: i64) -> Scalar<'static, i64> {
    sqlx::query_scalar("UPDATE node_sessions SET sequence=$2 WHERE node_id=$1 RETURNING greatest(0,floor(extract(epoch FROM (expires_at-clock_timestamp()))*1000))::bigint").bind(node).bind(sequence)
}

pub(in super::super) fn node_session_status(
    node: Uuid,
) -> Rows<'static, (Uuid, Uuid, i64, i64, bool, bool, String, i64)> {
    sqlx::query_as("SELECT s.boot_id,s.fence,s.epoch,GREATEST(0,(extract(epoch FROM (s.expires_at-clock_timestamp()))*1000)::bigint),m.maintenance,s.draining,m.deployment_mode,m.deployment_mode_revision FROM node_sessions s CROSS JOIN mokyu_meta m WHERE s.node_id=$1 AND m.singleton AND m.deployment_mode IN ('cluster','single_control')").bind(node)
}

pub(in super::super) fn node_settings() -> Rows<'static, (String, Value, i64)> {
    sqlx::query_as("SELECT section,value,revision FROM runtime_settings WHERE section=ANY($1) ORDER BY section").bind(crate::model::session::SECTIONS)
}
