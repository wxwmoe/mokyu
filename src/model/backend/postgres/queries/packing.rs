use super::*;

#[derive(sqlx::FromRow)]
pub(super) struct PackCandidateRow {
    #[sqlx(flatten)]
    pub(super) row: Mapping,
    pub(super) eligible: bool,
    pub(super) unit_count: i64,
}

pub(in super::super) fn pack_ready_inputs<'q>(ids: &'q [Id]) -> Scalar<'q, Id> {
    sqlx::query_scalar(
        "SELECT id FROM chunks WHERE id=ANY($1) AND state='ready' ORDER BY id FOR UPDATE",
    )
    .bind(ids)
}

pub(in super::super) fn insert_pack_inputs<'q>(task: Uuid, ids: &'q [Id]) -> Statement<'q> {
    sqlx::query("INSERT INTO pack_inputs(task_id,chunk_id) SELECT $1,unnest($2::uuid[]) ON CONFLICT(task_id,chunk_id) DO NOTHING").bind(task).bind(ids)
}

pub(in super::super) fn lock_pack_policy(key: String) -> Statement<'static> {
    sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1, 0))").bind(key)
}

pub(in super::super) fn active_pack_task<'q>(policy: &'q str) -> Scalar<'q, Uuid> {
    sqlx::query_scalar("SELECT id FROM tasks WHERE task_type='pack' AND detail->>'policy'=$1 AND state IN ('queued','running') LIMIT 1").bind(policy)
}

pub(in super::super) fn pack_storages(policy: Option<&str>) -> Rows<'_, (Uuid, String, i64)> {
    sqlx::query_as(
        "SELECT s.id,s.type,sp.revision FROM storages s JOIN storage_profiles sp ON sp.storage_id=s.id WHERE s.enabled AND CASE WHEN $1 IN ('pack','repack') THEN sp.pack_enabled WHEN $1='range' THEN sp.pack_range_enabled ELSE true END ORDER BY s.created_at,s.id",
    ).bind(policy)
}

pub(in super::super) fn scope_pack_task<'q>(
    child: Uuid,
    parent: Uuid,
    scope: &'q str,
    node: Option<Uuid>,
) -> Statement<'q> {
    sqlx::query("UPDATE tasks SET parent_task_id=$2,batch_total=1,execution_scope=$3,target_node_id=$4 WHERE id=$1").bind(child).bind(parent).bind(scope).bind(node)
}

pub(in super::super) fn pack_parent_total(task: Uuid, count: i64) -> Statement<'static> {
    sqlx::query("UPDATE tasks SET batch_total=$2,state=CASE WHEN $2=0 THEN 'completed' ELSE state END WHERE id=$1").bind(task).bind(count)
}

pub(in super::super) fn active_unpack_task(
    all: String,
    pack: Option<String>,
) -> Scalar<'static, Uuid> {
    sqlx::query_scalar("SELECT id FROM tasks WHERE task_type='unpack' AND state IN ('queued','running','paused','failed') AND detail->>'all'=$1 AND detail->>'pack_id' IS NOT DISTINCT FROM $2 LIMIT 1").bind(all).bind(pack)
}

pub(in super::super) fn unpack_storage(pack: Id) -> Rows<'static, (Uuid, String, i64)> {
    sqlx::query_as("SELECT s.id,s.type,sp.revision FROM storages s JOIN storage_profiles sp ON sp.storage_id=s.id JOIN packs p ON p.storage_id=s.id WHERE p.id=$1").bind(pack)
}

pub(in super::super) fn insert_pack_member(
    pack: Id,
    ordinal: i32,
    chunk: Id,
    offset: i64,
) -> Statement<'static> {
    sqlx::query(
        "INSERT INTO pack_members(pack_id,ordinal,chunk_id,offset_bytes) VALUES($1,$2,$3,$4)",
    )
    .bind(pack)
    .bind(ordinal)
    .bind(chunk)
    .bind(offset)
}

pub(in super::super) fn lock_pack_layout<'q>(ids: &'q [Id]) -> Rows<'q, (Id, Option<Id>)> {
    sqlx::query_as(
        "SELECT id,pack_id FROM chunks WHERE id=ANY($1) AND state='ready' ORDER BY id FOR UPDATE",
    )
    .bind(ids)
}

pub(in super::super) fn pack_layout_bytes<'q>(ids: &'q [Id]) -> Scalar<'q, i64> {
    sqlx::query_scalar("SELECT COALESCE(sum(bytes),0)::bigint FROM (SELECT standalone_size bytes FROM chunk_encodings WHERE chunk_id=ANY($1) AND state='ready' UNION ALL SELECT stored_size FROM packs WHERE id IN (SELECT pack_id FROM chunks WHERE id=ANY($1))) sources").bind(ids)
}

pub(in super::super) fn pack_added_bytes(task: Uuid) -> Scalar<'static, i64> {
    sqlx::query_scalar("SELECT COALESCE(sum(bytes),0)::bigint FROM (SELECT stored_size bytes FROM packs WHERE owner_task=$1 AND state='preparing' UNION ALL SELECT standalone_size FROM chunk_encodings WHERE owner_task=$1 AND state='uploading') sources").bind(task)
}

pub(in super::super) fn pack_is_upload(task: Uuid) -> Scalar<'static, bool> {
    sqlx::query_scalar("SELECT task_type IN ('upload','cache_flush') FROM tasks WHERE id=$1")
        .bind(task)
}

pub(in super::super) fn pack_is_range(task: Uuid) -> Scalar<'static, bool> {
    sqlx::query_scalar(
        "SELECT task_type='pack' AND detail->>'policy'='range' FROM tasks WHERE id=$1",
    )
    .bind(task)
}

pub(in super::super) fn pack_exclusive<'q>(
    ids: &'q [Id],
    cooldown: f64,
    window: f64,
    flush: f64,
) -> Scalar<'q, bool> {
    sqlx::query_scalar("SELECT NOT EXISTS(SELECT 1 FROM chunks c CROSS JOIN mokyu_meta g WHERE c.id=ANY($1) AND ((SELECT count(*) FROM extents WHERE chunk_id=c.id)<>1 OR NOT COALESCE(c.repack_after<=now(),true) OR (c.range_split_at IS NOT NULL AND (g.access_coverage_since IS NULL OR g.access_coverage_since>date_trunc('hour',now()-$3*interval '1 second') OR g.access_flushed_at IS NULL OR g.access_flushed_at<now()-$4*interval '1 second' OR EXISTS(SELECT 1 FROM chunk_access_windows w WHERE w.chunk_id=c.id AND w.range_reads>0 AND w.window_start>=date_trunc('hour',now()-$3*interval '1 second')))) OR (c.split_at IS NOT NULL AND c.reference_changed_at>now()-$2*interval '1 second')))").bind(ids).bind(cooldown).bind(window).bind(flush)
}

pub(in super::super) fn publish_prepared_pack(pack: Id) -> Statement<'static> {
    sqlx::query("UPDATE packs SET state='ready',unreferenced_at=NULL,stored_at=clock_timestamp() WHERE id=$1 AND state='preparing'").bind(pack)
}

pub(in super::super) fn retire_pack_encodings<'q>(ids: &'q [Id]) -> Statement<'q> {
    sqlx::query("UPDATE chunk_encodings SET state='retired',unreferenced_at=now() WHERE chunk_id=ANY($1) AND state='ready'").bind(ids)
}

pub(in super::super) fn publish_chunk_encoding<'q>(chunk: &'q Chunk) -> Statement<'q> {
    sqlx::query("UPDATE chunks SET current_encoding_id=$2,standalone_size=$3,standalone_compressed=$4,nonce=$5,encryption_algorithm=$6,block_key_id=$7 WHERE id=$1 AND state='ready'").bind(chunk.id).bind(chunk.encoding_id).bind(chunk.stored_size).bind(chunk.compressed).bind(&chunk.nonce).bind(&chunk.algorithm).bind(chunk.block_key_id)
}

pub(in super::super) fn pack_has_references<'q>(ids: &'q [Id]) -> Scalar<'q, bool> {
    sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM extents WHERE chunk_id=ANY($1))").bind(ids)
}

pub(in super::super) fn fail_dead_pack_chunks<'q>(ids: &'q [Id]) -> Statement<'q> {
    sqlx::query("UPDATE chunks SET state='failed',pack_id=NULL,unreferenced_at=COALESCE(unreferenced_at,now()) WHERE id=ANY($1)").bind(ids)
}

pub(in super::super) fn schedule_repacked_streams<'q>(
    ids: &'q [Id],
    cooldown: f64,
) -> Statement<'q> {
    sqlx::query("INSERT INTO pack_maintenance(stream_id,reason,next_check_at) SELECT DISTINCT e.stream_id,'repack',now()+$2*interval '1 second' FROM extents e JOIN streams s ON s.id=e.stream_id WHERE e.chunk_id=ANY($1) AND s.state='ready' ON CONFLICT(stream_id) DO UPDATE SET generation=pack_maintenance.generation+1,cursor=0,reason='repack',updated_at=now(),next_check_at=EXCLUDED.next_check_at").bind(ids).bind(cooldown)
}

pub(in super::super) fn retire_replaced_packs(ids: Vec<Id>) -> Statement<'static> {
    sqlx::query("UPDATE packs p SET state='retired',unreferenced_at=now() WHERE id=ANY($1) AND state='ready' AND NOT EXISTS(SELECT 1 FROM chunks WHERE pack_id=p.id AND state='ready')").bind(ids)
}

pub(in super::super) fn record_pack_rewrite(task: Uuid, detail: Value) -> Statement<'static> {
    sqlx::query(
        "UPDATE tasks SET detail=detail||jsonb_build_object('last_rewrite',$2::jsonb) WHERE id=$1",
    )
    .bind(task)
    .bind(detail)
}

pub(in super::super) fn mark_range_checked(pack: Id) -> Statement<'static> {
    sqlx::query("UPDATE packs SET range_checked_at=now() WHERE id=$1").bind(pack)
}

pub(in super::super) fn record_range_result(
    task: Uuid,
    cursor: String,
    processed: i64,
    decision: Value,
) -> Statement<'static> {
    sqlx::query("UPDATE tasks SET cursor=$2,processed=processed+$3,detail=detail||jsonb_build_object('last_range',$4::jsonb,'evaluated',COALESCE((detail->>'evaluated')::bigint,0)+1),updated_at=now() WHERE id=$1 AND state='running'").bind(task).bind(cursor).bind(processed).bind(decision)
}

#[async_trait::async_trait]
impl PackingStore for Backend {
    async fn in_storage(&self, pack: Id, storage: Uuid) -> Result<bool> {
        Ok(sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM packs WHERE id=$1 AND storage_id=$2 AND state='ready')",
        )
        .bind(pack)
        .bind(storage)
        .fetch_one(&self.pool)
        .await?)
    }

    async fn scoped_page(
        &self,
        principal: &Principal,
        scope: &ScopeQuery,
        after: Id,
        limit: i64,
        state: Option<&str>,
    ) -> Result<Vec<PackSummary>> {
        transactions::scoped_pack_page(&self.pool, principal, scope, after, limit, state).await
    }
    async fn scoped_detail(
        &self,
        principal: &Principal,
        scope: &ScopeQuery,
        id: Id,
        after: Id,
        limit: i64,
    ) -> Result<(PackSummary, Vec<PackMember>, bool)> {
        transactions::scoped_pack_detail(&self.pool, principal, scope, id, after, limit).await
    }
    async fn scoped_objects(
        &self,
        principal: &Principal,
        scope: &ScopeQuery,
        id: Id,
        cursor: Option<(Uuid, String)>,
    ) -> Result<Vec<PackObject>> {
        transactions::scoped_pack_objects(&self.pool, principal, scope, id, cursor).await
    }

    async fn page(&self, after: Id, limit: i64) -> Result<Vec<Value>> {
        let rows:Vec<Value>=sqlx::query_scalar("SELECT to_jsonb(p)||jsonb_build_object('id',p.id::text) FROM packs p WHERE id>$1 ORDER BY id LIMIT $2")
        .bind(after).bind(limit).fetch_all(&self.pool).await?;
        Ok(rows)
    }
    async fn detail(&self, id: Id) -> Result<(Value, Vec<Value>)> {
        let pack: Value = sqlx::query_scalar(
            "SELECT to_jsonb(p)||jsonb_build_object('id',p.id::text) FROM packs p WHERE id=$1",
        )
        .bind(id)
        .fetch_optional(&self.pool)
        .await?
        .ok_or_else(|| s3s::s3_error!(NoSuchKey))?;
        let members:Vec<Value>=sqlx::query_scalar("SELECT jsonb_build_object('ordinal',m.ordinal,'chunk_id',m.chunk_id::text,'offset',m.offset_bytes,'raw_size',c.raw_size,'current',c.pack_id=m.pack_id) FROM pack_members m JOIN chunks c ON c.id=m.chunk_id WHERE m.pack_id=$1 ORDER BY ordinal LIMIT 4096").bind(id).fetch_all(&self.pool).await?;
        Ok((pack, members))
    }

    async fn rewrite_counts(
        &self,
        storage: Uuid,
        algorithm: &str,
        key: Option<Uuid>,
    ) -> Result<(i64, i64)> {
        Ok(rewrite_counts(storage, algorithm, key)
            .fetch_one(&self.pool)
            .await?)
    }
    async fn start_rewrite(&self, job: &RewriteTask, node: Option<Uuid>) -> Result<(Uuid, bool)> {
        transactions::start_rewrite(&self.pool, job, node, self.single).await
    }
    async fn rewrite_candidates(
        &self,
        after: Id,
        job: &RewriteTask,
    ) -> Result<(Option<Chunk>, Option<(Id, Id)>)> {
        let loose = rewrite_loose_candidate(&self.pool, after, job).await?;
        let pack = rewrite_pack_candidate(&self.pool, after, job).await?;
        Ok((loose, pack))
    }
    async fn rewrite_pack_inputs(&self, pack: Id) -> Result<(Pack, Vec<Chunk>, Vec<Id>)> {
        let old = rewrite_pack(pack).fetch_one(&self.pool).await?;
        let members = rewrite_members(pack).fetch_all(&self.pool).await?;
        anyhow::ensure!(
            members.len() == old.member_count as usize,
            "pack member count changed during rewrite"
        );
        let current = rewrite_current_members(pack).fetch_all(&self.pool).await?;
        Ok((old, members, current))
    }
    async fn commit_rewrite_chunk(
        &self,
        task: Uuid,
        token: Uuid,
        source: &Chunk,
        physical: &Chunk,
    ) -> Result<()> {
        transactions::commit_rewrite_chunk(&self.pool, task, token, source, physical).await
    }
    async fn commit_rewrite_pack(
        &self,
        task: Uuid,
        token: Uuid,
        old: &Pack,
        new: &Pack,
        current: &[Id],
        members: &[Chunk],
        first: Id,
    ) -> Result<()> {
        transactions::commit_rewrite_pack(
            &self.pool, task, token, old, new, current, members, first,
        )
        .await
    }
    async fn complete_rewrite(&self, task: Uuid, token: Uuid, after: Id) -> Result<()> {
        complete_rewrite(task, token, after)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    async fn pack_totals(&self) -> Result<Vec<Value>> {
        Ok(sqlx::query_scalar("SELECT jsonb_build_object('state',state,'count',count(*),'raw_bytes',COALESCE(sum(raw_size),0),'stored_bytes',COALESCE(sum(stored_size),0)) FROM packs GROUP BY state").fetch_all(&self.pool).await?)
    }
    async fn unpack_totals(&self, pack: Option<Id>) -> Result<(i64, i64)> {
        Ok(sqlx::query_as("SELECT count(*),COALESCE(sum(raw_size),0)::bigint FROM packs WHERE state='ready' AND ($1::uuid IS NULL OR id=$1)").bind(pack).fetch_one(&self.pool).await?)
    }
    async fn unpack_identity(&self, pack: Option<Id>) -> Result<(String, String)> {
        Ok(sqlx::query_as("SELECT coalesce(max(id::text COLLATE \"C\"),''),coalesce(bit_xor(hashtextextended(id::text,0)),0)::text FROM packs WHERE state='ready' AND ($1::uuid IS NULL OR id=$1)").bind(pack).fetch_one(&self.pool).await?)
    }
    async fn preparing_packs(&self) -> Result<bool> {
        Ok(
            sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM packs WHERE state='preparing')")
                .fetch_one(&self.pool)
                .await?,
        )
    }
    async fn ready_encoding(&self, chunk: Id) -> Result<Option<(Id, Uuid, i32)>> {
        pack_work::ready_encoding(&self.pool, chunk).await
    }

    async fn prepare_encoding(&self, physical: &Chunk, task: Execution) -> Result<Id> {
        sqlx::query_scalar("INSERT INTO chunk_encodings(chunk_id,storage_id,standalone_compressed,state,created_at,unreferenced_at,owner_task,encryption_algorithm,block_key_id) SELECT $1,$2,false,'uploading',$3,now(),$4,$5,$6 FROM (SELECT id FROM tasks WHERE id=$4 AND state='running' AND lease_token=$7 AND ((lease_node_id IS NULL AND lease_expires_at IS NULL) OR lease_expires_at>clock_timestamp()) FOR UPDATE) owner RETURNING encoding_id").bind(physical.id).bind(physical.storage_id).bind(physical.created_at).bind(task.id).bind(&physical.algorithm).bind(physical.block_key_id).bind(task.token).fetch_optional(&self.pool).await?.context("task execution expired")
    }
    async fn encoding_metadata(&self, physical: &Chunk) -> Result<()> {
        sqlx::query("UPDATE chunk_encodings SET standalone_size=$2,standalone_compressed=$3,nonce=$4,nonce_id=$5 WHERE encoding_id=$1").bind(physical.encoding_id).bind(physical.stored_size).bind(physical.compressed).bind(&physical.nonce).bind(crate::content::nonce::id(physical.nonce.as_deref())).execute(&self.pool).await?;
        Ok(())
    }
    async fn prepare_pack(
        &self,
        storage: Uuid,
        algorithm: &str,
        key: Option<Uuid>,
        bytes: i64,
        count: i32,
        task: Execution,
    ) -> Result<Pack> {
        Ok(sqlx::query_as("INSERT INTO packs(storage_id,encryption_algorithm,block_key_id,raw_size,member_count,state,unreferenced_at,owner_task) SELECT $1,$2,$3,$4,$5,'preparing',now(),$6 FROM (SELECT id FROM tasks WHERE id=$6 AND state='running' AND lease_token=$7 AND ((lease_node_id IS NULL AND lease_expires_at IS NULL) OR lease_expires_at>clock_timestamp()) FOR UPDATE) owner RETURNING *").bind(storage).bind(algorithm).bind(key).bind(bytes).bind(count).bind(task.id).bind(task.token).fetch_one(&self.pool).await?)
    }
    async fn discard_pack(&self, pack: Id) -> Result<()> {
        sqlx::query("UPDATE packs SET state='retired' WHERE id=$1")
            .bind(pack)
            .execute(&self.pool)
            .await?;
        Ok(())
    }
    async fn pack_metadata(&self, pack: &Pack) -> Result<()> {
        sqlx::query(
            "UPDATE packs SET stored_size=$2,pack_compressed=$3,nonce=$4,digest=$5,nonce_id=$6 WHERE id=$1",
        )
        .bind(pack.id)
        .bind(pack.stored_size)
        .bind(pack.compressed)
        .bind(&pack.nonce)
        .bind(&pack.digest)
        .bind(crate::content::nonce::id(pack.nonce.as_deref()))
        .execute(&self.pool)
        .await?;
        Ok(())
    }
    async fn pack_has_pending(&self, pack: Id) -> Result<bool> {
        pack_work::pack_has_pending(&self.pool, pack).await
    }

    async fn pack_layout(&self, pack: Id) -> Result<Vec<Mapping>> {
        pack_work::pack_layout(&self.pool, pack).await
    }

    async fn pack_has_writers(&self, pack: Id) -> Result<bool> {
        pack_work::pack_has_writers(&self.pool, pack).await
    }

    async fn pack_raw_size(&self, pack: Id) -> Result<i64> {
        pack_work::pack_raw_size(&self.pool, pack).await
    }

    async fn chunk_has_references(&self, chunk: Id) -> Result<bool> {
        pack_work::chunk_has_references(&self.pool, chunk).await
    }

    async fn pack_stored_size(&self, pack: Id) -> Result<i64> {
        pack_work::pack_stored_size(&self.pool, pack).await
    }

    async fn pack_candidates(
        &self,
        stream: Id,
        offset: i64,
        cooldown: f64,
    ) -> Result<Vec<PackCandidate>> {
        pack_work::pack_candidates(&self.pool, stream, offset, cooldown).await
    }

    async fn pack_member_count(&self, pack: Id) -> Result<i64> {
        pack_work::pack_member_count(&self.pool, pack).await
    }

    async fn pack_stream_metadata(&self, stream: Id) -> Result<(Value, String)> {
        Ok(
            sqlx::query_as("SELECT metadata,object_key FROM streams WHERE id=$1")
                .bind(stream)
                .fetch_one(&self.pool)
                .await?,
        )
    }
    async fn physical_bytes(&self, ids: &[Id]) -> Result<i64> {
        pack_work::physical_bytes(&self.pool, ids).await
    }

    async fn pause_pack_task(&self, task: Execution) -> Result<()> {
        sqlx::query(
            "UPDATE tasks SET state='paused',updated_at=now() WHERE id=$1 AND state='running' AND lease_token=$2 AND ((lease_node_id IS NULL AND lease_expires_at IS NULL) OR lease_expires_at>clock_timestamp())",
        )
        .bind(task.id)
        .bind(task.token).execute(&self.pool)
        .await?;
        Ok(())
    }
    async fn next_pack(
        &self,
        after: Id,
        pack: Option<Id>,
        upper: Option<DateTime<Utc>>,
        storage: Option<Uuid>,
    ) -> Result<Option<Id>> {
        pack_work::next_pack(&self.pool, after, pack, upper, storage).await
    }

    async fn next_pack_stream(
        &self,
        after: Id,
        offset: i64,
        upper: DateTime<Utc>,
        storage: Option<Uuid>,
    ) -> Result<Option<Id>> {
        pack_work::next_pack_stream(&self.pool, after, offset, upper, storage).await
    }

    async fn advance_pack_task(
        &self,
        task: Execution,
        next: &Option<String>,
        processed: i64,
    ) -> Result<()> {
        if self.single && next.is_none() {
            return transactions::advance_pack_storage(&self.pool, task, processed).await;
        }
        sqlx::query("UPDATE tasks SET cursor=$2,processed=processed+$3,state=CASE WHEN $2::text IS NULL THEN 'completed' ELSE state END,updated_at=now() WHERE id=$1 AND state='running' AND lease_token=$4 AND ((lease_node_id IS NULL AND lease_expires_at IS NULL) OR lease_expires_at>clock_timestamp())").bind(task.id).bind(next).bind(processed).bind(task.token).execute(&self.pool).await?;
        Ok(())
    }
    async fn next_pack_change(
        &self,
        cutoff: DateTime<Utc>,
        policy: &str,
        storage: Option<Uuid>,
    ) -> Result<Option<(Id, i64)>> {
        pack_work::next_pack_change(&self.pool, cutoff, policy, storage).await
    }

    async fn finish_pack_change(&self, task: Execution, pack: Id, generation: i64) -> Result<()> {
        pack_work::finish_pack_change(&self.pool, task, pack, generation).await
    }

    async fn next_pack_queue(
        &self,
        cutoff: DateTime<Utc>,
        policy: &str,
        storage: Option<Uuid>,
    ) -> Result<Option<(Id, i64, i64)>> {
        pack_work::next_pack_queue(&self.pool, cutoff, policy, storage).await
    }

    async fn stream_repack_after(
        &self,
        stream: Id,
        cooldown: f64,
    ) -> Result<Option<DateTime<Utc>>> {
        pack_work::stream_repack_after(&self.pool, stream, cooldown).await
    }

    async fn defer_pack_queue(
        &self,
        task: Execution,
        stream: Id,
        generation: i64,
        later: DateTime<Utc>,
    ) -> Result<()> {
        pack_work::defer_pack_queue(&self.pool, task, stream, generation, later).await
    }

    async fn finish_pack_queue(&self, task: Execution, stream: Id, generation: i64) -> Result<()> {
        pack_work::finish_pack_queue(&self.pool, task, stream, generation).await
    }

    async fn advance_pack_queue(
        &self,
        task: Execution,
        stream: Id,
        generation: i64,
        cursor: i64,
    ) -> Result<()> {
        pack_work::advance_pack_queue(&self.pool, task, stream, generation, cursor).await
    }

    async fn pin_pack_inputs(&self, task: Execution, rows: &[Mapping]) -> Result<()> {
        transactions::pin_pack_inputs(&self.pool, task, rows).await
    }

    async fn start_pack(&self, policy: &str, automatic: bool, node: Option<Uuid>) -> Result<Value> {
        transactions::start_pack(&self.pool, self.single, policy, automatic, node).await
    }
    async fn start_unpack(&self, pack: Option<Id>, all: bool, node: Option<Uuid>) -> Result<Value> {
        transactions::start_unpack(&self.pool, self.single, pack, all, node).await
    }
    async fn insert_pack_members(&self, pack: Id, rows: &[Mapping]) -> Result<()> {
        transactions::insert_pack_members(&self.pool, pack, rows).await
    }
    async fn commit_pack_outputs(
        &self,
        task: Execution,
        rows: &[Mapping],
        outputs: &[PackOutput],
        split: bool,
        settings: &crate::content::pack::Config,
        flush_seconds: f64,
    ) -> Result<()> {
        transactions::commit_pack_outputs(
            &self.pool,
            task,
            rows,
            outputs,
            split,
            settings,
            flush_seconds,
        )
        .await
    }
    async fn same_references(&self, a: Id, b: Id, distance: i64) -> Result<bool> {
        Ok(same_references(a, b, distance)
            .fetch_one(&self.pool)
            .await?)
    }
    async fn next_range_pack(
        &self,
        storage: Uuid,
        cutoff: DateTime<Utc>,
        window: f64,
        minimum: i64,
    ) -> Result<Option<Id>> {
        pack_work::next_range_pack(&self.pool, storage, cutoff, window, minimum).await
    }

    async fn complete_range_task(&self, task: Execution) -> Result<()> {
        if self.single {
            return transactions::advance_pack_storage(&self.pool, task, 0).await;
        }
        sqlx::query(
            "UPDATE tasks SET state='completed',updated_at=now() WHERE id=$1 AND state='running' AND lease_token=$2 AND ((lease_node_id IS NULL AND lease_expires_at IS NULL) OR lease_expires_at>clock_timestamp())",
        )
        .bind(task.id)
        .bind(task.token).execute(&self.pool)
        .await?;
        Ok(())
    }
    async fn range_pack_blocked(&self, pack: Id) -> Result<bool> {
        pack_work::range_pack_blocked(&self.pool, pack).await
    }

    async fn pack_range_totals(&self, pack: Id, window: f64) -> Result<(i64, i64, i64)> {
        pack_work::pack_range_totals(&self.pool, pack, window).await
    }

    async fn pack_member_hits(&self, pack: Id, window: f64) -> Result<Vec<(Id, i64)>> {
        pack_work::pack_member_hits(&self.pool, pack, window).await
    }

    async fn pack_dimensions(&self, pack: Id) -> Result<(i64, i64, i32)> {
        pack_work::pack_dimensions(&self.pool, pack).await
    }

    async fn range_pack_layout(&self, pack: Id) -> Result<Vec<Mapping>> {
        pack_work::range_pack_layout(&self.pool, pack).await
    }

    async fn defer_range_repack(
        &self,
        stream: Id,
        window: f64,
        flush: f64,
        retry: f64,
        offset: i64,
    ) -> Result<()> {
        pack_work::defer_range_repack(&self.pool, stream, window, flush, retry, offset).await
    }

    async fn record_range(
        &self,
        task: Execution,
        id: Id,
        processed: usize,
        decision: Value,
    ) -> Result<()> {
        transactions::record_range(&self.pool, task, id, processed, decision).await
    }
}

pub(in super::super) fn resolve_block_key<'q>(name: &'q str) -> Rows<'q, (Uuid, String, String)> {
    sqlx::query_as("SELECT id,name,key_algorithm FROM managed_keys WHERE (id=$1::uuid OR name=$2) AND purpose='block' AND revoked_at IS NULL").bind(name.parse::<Uuid>().ok()).bind(name)
}

pub(in super::super) fn rewrite_counts<'q>(
    storage: Uuid,
    algorithm: &'q str,
    key: Option<Uuid>,
) -> Rows<'q, (i64, i64)> {
    sqlx::query_as("SELECT count(*) FILTER (WHERE c.pack_id IS NULL AND (l.encoding_id IS NULL OR l.encoding_fingerprint IS DISTINCT FROM digest(concat_ws(':',$3::uuid::text,$2::text,l.standalone_compressed::text,coalesce(sp.compression_enabled::text,''),coalesce(sp.compression_strategy,''),coalesce(sp.compression_level::text,''),coalesce(sp.compression_sample_level::text,''),coalesce(sp.compression_min_savings_percent::text,''),coalesce(sp.compression_min_savings_bytes::text,''),'standalone'),'sha256') OR ((NOT sp.compression_enabled OR sp.compression_strategy='never') AND l.standalone_compressed))), count(*) FILTER (WHERE c.pack_id IS NOT NULL AND (p.pack_fingerprint IS DISTINCT FROM digest(concat_ws(':',$3::uuid::text,$2::text,p.pack_compressed::text,p.member_count::text,coalesce(sp.compression_enabled::text,''),coalesce(sp.compression_strategy,''),coalesce(sp.compression_level::text,''),coalesce(sp.compression_sample_level::text,''),coalesce(sp.compression_min_savings_percent::text,''),coalesce(sp.compression_min_savings_bytes::text,''),coalesce(sp.pack_max_size,''),coalesce(sp.pack_range_enabled::text,''),coalesce(sp.pack_range_min_downloads::text,''),coalesce(sp.pack_range_min_savings_percent::text,''),coalesce(sp.pack_range_min_savings_bytes,''),'pack'),'sha256') OR ((NOT sp.compression_enabled OR sp.compression_strategy='never') AND p.pack_compressed))) FROM chunks c JOIN storage_profiles sp ON sp.storage_id=c.storage_id LEFT JOIN chunk_encodings l ON l.chunk_id=c.id AND l.state='ready' LEFT JOIN packs p ON p.id=c.pack_id AND p.state='ready' WHERE c.storage_id=$1 AND c.state='ready'").bind(storage).bind(algorithm).bind(key)
}

pub(in super::super) fn lock_rewrite_storage(storage: Uuid) -> Statement<'static> {
    sqlx::query("SELECT id FROM storages WHERE id=$1 AND enabled FOR UPDATE").bind(storage)
}

pub(in super::super) fn active_rewrite(storage: Uuid) -> Scalar<'static, Uuid> {
    sqlx::query_scalar("SELECT id FROM tasks WHERE task_type='rewrite' AND parent_task_id IS NULL AND detail->>'storage_id'=$1 AND state IN ('queued','running','paused') LIMIT 1").bind(storage.to_string())
}

pub(in super::super) fn rewrite_upper(storage: Uuid) -> Scalar<'static, Id> {
    sqlx::query_scalar("SELECT COALESCE((SELECT id FROM chunks WHERE storage_id=$1 ORDER BY id DESC LIMIT 1),'00000000-0000-0000-0000-000000000000'::uuid)").bind(storage)
}

pub(in super::super) fn rewrite_is_local(storage: Uuid) -> Scalar<'static, bool> {
    sqlx::query_scalar("SELECT type='local' FROM storages WHERE id=$1").bind(storage)
}

pub(in super::super) fn scope_rewrite<'q>(
    id: Uuid,
    scope: &'q str,
    node: Option<Uuid>,
    shards: i64,
) -> Statement<'q> {
    sqlx::query("UPDATE tasks SET execution_scope=$2,target_node_id=$3,batch_total=$4 WHERE id=$1")
        .bind(id)
        .bind(scope)
        .bind(node)
        .bind(shards)
}

pub(in super::super) fn scope_rewrite_child<'q>(
    id: Uuid,
    parent: Uuid,
    scope: &'q str,
    node: Option<Uuid>,
    shards: i64,
) -> Statement<'q> {
    sqlx::query("UPDATE tasks SET parent_task_id=$2,execution_scope=$3,target_node_id=$4,batch_total=$5 WHERE id=$1").bind(id).bind(parent).bind(scope).bind(node).bind(shards)
}

pub(in super::super) fn complete_empty_rewrite(id: Uuid) -> Statement<'static> {
    sqlx::query("UPDATE tasks SET state='completed',batch_completed=0,updated_at=now() WHERE id=$1")
        .bind(id)
}

pub(in super::super) fn active_block_key(id: Uuid) -> Rows<'static, (String, String)> {
    sqlx::query_as("SELECT name,key_algorithm FROM managed_keys WHERE id=$1 AND purpose='block' AND revoked_at IS NULL").bind(id)
}

pub(in super::super) async fn rewrite_loose_candidate(
    db: impl sqlx::PgExecutor<'_>,
    after: Id,
    job: &RewriteTask,
) -> Result<Option<Chunk>> {
    let mut query = sqlx::QueryBuilder::<Postgres>::new(
        "SELECT (SELECT h.material FROM tier_locations h WHERE h.unit=0 AND h.physical_id=l.encoding_id) AS tier_material,c.id,l.encoding_id AS current_encoding_id,l.encoding_id AS location_id,c.storage_id,c.hash,c.raw_size,l.standalone_size AS standalone_size,l.encryption_algorithm AS encryption_algorithm,l.block_key_id,l.standalone_compressed AS standalone_compressed,l.nonce,c.format,c.state,c.created_at FROM chunks c JOIN chunk_encodings l ON l.chunk_id=c.id AND l.state='ready' JOIN storage_profiles sp ON sp.storage_id=c.storage_id WHERE c.id>",
    );
    query.push_bind(after).push(" AND c.id<=");
    query
        .push_bind(job.upper_chunk_id)
        .push(" AND c.storage_id=");
    query.push_bind(job.storage_id).push(" AND c.state='ready' AND c.pack_id IS NULL AND (l.encoding_fingerprint IS DISTINCT FROM digest(concat_ws(':',");
    query.push_bind(job.target_key_id).push("::uuid::text,");
    query.push_bind(&job.target_algorithm).push("::text,l.standalone_compressed::text,coalesce(sp.compression_enabled::text,''),coalesce(sp.compression_strategy,''),coalesce(sp.compression_level::text,''),coalesce(sp.compression_sample_level::text,''),coalesce(sp.compression_min_savings_percent::text,''),coalesce(sp.compression_min_savings_bytes::text,''),'standalone'),'sha256') OR ((NOT sp.compression_enabled OR sp.compression_strategy='never') AND l.standalone_compressed))");
    if job.shards > 1 {
        query
            .push(" AND ((hashtextextended(c.id::text,0) & 9223372036854775807) % ")
            .push_bind(job.shards)
            .push(")=")
            .push_bind(job.shard);
    }
    query.push(" ORDER BY c.id LIMIT 1");
    Ok(query.build_query_as().fetch_optional(db).await?)
}

pub(in super::super) async fn rewrite_pack_candidate(
    db: impl sqlx::PgExecutor<'_>,
    after: Id,
    job: &RewriteTask,
) -> Result<Option<(Id, Id)>> {
    let mut query = sqlx::QueryBuilder::<Postgres>::new(
        "SELECT c.id,p.id FROM packs p JOIN storage_profiles sp ON sp.storage_id=p.storage_id JOIN LATERAL (SELECT id,state,storage_id FROM chunks WHERE pack_id=p.id AND storage_id=p.storage_id AND state='ready' ORDER BY id LIMIT 1) c ON true WHERE p.state='ready' AND c.storage_id=",
    );
    query.push_bind(job.storage_id).push(
        " AND c.state='ready' AND (p.pack_fingerprint IS DISTINCT FROM digest(concat_ws(':',",
    );
    query.push_bind(job.target_key_id).push("::uuid::text,");
    query.push_bind(&job.target_algorithm).push("::text,p.pack_compressed::text,p.member_count::text,coalesce(sp.compression_enabled::text,''),coalesce(sp.compression_strategy,''),coalesce(sp.compression_level::text,''),coalesce(sp.compression_sample_level::text,''),coalesce(sp.compression_min_savings_percent::text,''),coalesce(sp.compression_min_savings_bytes::text,''),coalesce(sp.pack_max_size,''),coalesce(sp.pack_range_enabled::text,''),coalesce(sp.pack_range_min_downloads::text,''),coalesce(sp.pack_range_min_savings_percent::text,''),coalesce(sp.pack_range_min_savings_bytes,''),'pack'),'sha256') OR ((NOT sp.compression_enabled OR sp.compression_strategy='never') AND p.pack_compressed)) AND c.id>");
    query.push_bind(after).push(" AND c.id<=");
    query.push_bind(job.upper_chunk_id).push("");
    if job.shards > 1 {
        query
            .push(" AND ((hashtextextended(c.id::text,0) & 9223372036854775807) % ")
            .push_bind(job.shards)
            .push(")=")
            .push_bind(job.shard);
    }
    query.push(" ORDER BY c.id LIMIT 1");
    Ok(query.build_query_as().fetch_optional(db).await?)
}

pub(in super::super) fn lock_rewrite_chunk(id: Id) -> Scalar<'static, Option<Id>> {
    sqlx::query_scalar("SELECT pack_id FROM chunks WHERE id=$1 AND state='ready' FOR UPDATE")
        .bind(id)
}

pub(in super::super) fn retire_rewrite_encoding(id: Id) -> Statement<'static> {
    sqlx::query("UPDATE chunk_encodings SET state='retired',unreferenced_at=now() WHERE encoding_id=$1 AND state='ready'").bind(id)
}

pub(in super::super) fn publish_rewrite_encoding<'q>(physical: &'q Chunk) -> Statement<'q> {
    sqlx::query("UPDATE chunk_encodings SET standalone_size=$2,standalone_compressed=$3,nonce=$4,nonce_id=$5,state='ready',unreferenced_at=NULL,stored_at=clock_timestamp(),owner_task=NULL WHERE encoding_id=$1 AND state='uploading'").bind(physical.encoding_id).bind(physical.stored_size).bind(physical.compressed).bind(&physical.nonce).bind(crate::content::nonce::id(physical.nonce.as_deref()))
}

pub(in super::super) fn publish_rewrite_chunk<'q>(physical: &'q Chunk) -> Statement<'q> {
    sqlx::query("UPDATE chunks SET current_encoding_id=$2,standalone_size=$3,standalone_compressed=$4,nonce=$5,encryption_algorithm=$6,block_key_id=$7 WHERE id=$1 AND state='ready'").bind(physical.id).bind(physical.encoding_id).bind(physical.stored_size).bind(physical.compressed).bind(&physical.nonce).bind(&physical.algorithm).bind(physical.block_key_id)
}

pub(in super::super) fn advance_rewrite(id: Uuid, cursor: String) -> Statement<'static> {
    sqlx::query("UPDATE tasks SET cursor=$2,processed=processed+1,updated_at=now() WHERE id=$1")
        .bind(id)
        .bind(cursor)
}

pub(in super::super) fn rewrite_pack(id: Id) -> Rows<'static, Pack> {
    sqlx::query_as("SELECT (SELECT h.material FROM tier_locations h WHERE h.unit=1 AND h.physical_id=p.id) AS tier_material,p.* FROM packs p WHERE id=$1 AND state='ready'").bind(id)
}

pub(in super::super) fn rewrite_members(id: Id) -> Rows<'static, Chunk> {
    sqlx::query_as("SELECT c.id,c.current_encoding_id AS current_encoding_id,COALESCE(l.encoding_id,'00000000-0000-0000-0000-000000000000'::uuid) AS location_id,c.storage_id,c.hash,c.raw_size,COALESCE(l.standalone_size,c.standalone_size) AS standalone_size,COALESCE(l.encryption_algorithm,c.encryption_algorithm) AS encryption_algorithm,COALESCE(l.block_key_id,c.block_key_id) AS block_key_id,COALESCE(l.standalone_compressed,c.standalone_compressed) AS standalone_compressed,COALESCE(l.nonce,c.nonce) AS nonce,c.format,c.state,c.created_at FROM pack_members m JOIN chunks c ON c.id=m.chunk_id LEFT JOIN chunk_encodings l ON l.encoding_id=c.current_encoding_id AND l.state='ready' WHERE m.pack_id=$1 ORDER BY m.ordinal").bind(id)
}

pub(in super::super) fn rewrite_current_members(id: Id) -> Scalar<'static, Id> {
    sqlx::query_scalar("SELECT id FROM chunks WHERE pack_id=$1 AND state='ready' ORDER BY id")
        .bind(id)
}

pub(in super::super) fn lock_rewrite_members(id: Id) -> Scalar<'static, Id> {
    sqlx::query_scalar(
        "SELECT id FROM chunks WHERE pack_id=$1 AND state='ready' ORDER BY id FOR UPDATE",
    )
    .bind(id)
}

pub(in super::super) fn publish_rewrite_pack<'q>(pack: &'q Pack) -> Statement<'q> {
    sqlx::query("UPDATE packs SET stored_size=$2,pack_compressed=$3,nonce=$4,nonce_id=$6,digest=$5,state='ready',unreferenced_at=NULL,stored_at=clock_timestamp(),owner_task=NULL WHERE id=$1 AND state='preparing'").bind(pack.id).bind(pack.stored_size).bind(pack.compressed).bind(&pack.nonce).bind(&pack.digest).bind(crate::content::nonce::id(pack.nonce.as_deref()))
}

pub(in super::super) fn replace_rewrite_pack(old: Id, new: Id) -> Statement<'static> {
    sqlx::query("UPDATE chunks SET pack_id=$2 WHERE pack_id=$1 AND state='ready'")
        .bind(old)
        .bind(new)
}

pub(in super::super) fn retire_rewrite_pack(id: Id) -> Statement<'static> {
    sqlx::query("UPDATE packs SET state='retired',unreferenced_at=now(),owner_task=NULL WHERE id=$1 AND state='ready'").bind(id)
}

pub(in super::super) fn complete_rewrite(id: Uuid, token: Uuid, after: Id) -> Statement<'static> {
    sqlx::query("UPDATE tasks SET cursor=$2,state='completed',updated_at=now() WHERE id=$1 AND state='running' AND lease_token=$3 AND ((lease_node_id IS NULL AND lease_expires_at IS NULL) OR lease_expires_at>clock_timestamp())").bind(id).bind(after.to_string()).bind(token)
}

pub(in super::super) fn lock_task_execution(id: Uuid, token: Uuid) -> Scalar<'static, Uuid> {
    sqlx::query_scalar("SELECT id FROM tasks WHERE id=$1 AND state='running' AND lease_token=$2 AND ((lease_node_id IS NULL AND lease_expires_at IS NULL) OR lease_expires_at>clock_timestamp()) FOR UPDATE").bind(id).bind(token)
}

pub(in super::super) fn purge_counts(bucket: Uuid) -> Rows<'static, (i64, i64)> {
    sqlx::query_as("SELECT (SELECT count(*) FROM objects WHERE bucket_id=$1 AND stream_id IS NOT NULL),(SELECT count(*) FROM uploads WHERE bucket_id=$1 AND state IN ('active','completing'))").bind(bucket)
}

pub(in super::super) fn lock_purge_bucket<'q>(name: &'q str, id: Uuid) -> Scalar<'q, Uuid> {
    sqlx::query_scalar("SELECT id FROM buckets WHERE name=$1 AND id=$2 FOR UPDATE")
        .bind(name)
        .bind(id)
}

pub(in super::super) fn existing_purge(id: Uuid) -> Scalar<'static, Uuid> {
    sqlx::query_scalar(
        "SELECT id FROM tasks WHERE task_type='purge' AND bucket_id=$1 AND state<>'completed'",
    )
    .bind(id)
}

pub(in super::super) fn seal_purge_bucket(id: Uuid) -> Statement<'static> {
    sqlx::query("UPDATE buckets SET state='purging' WHERE id=$1").bind(id)
}

pub(in super::super) fn chunks_indexed() -> Scalar<'static, bool> {
    sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM chunks)")
}

pub(in super::super) fn sweep_preview(id: Uuid) -> Scalar<'static, Value> {
    sqlx::query_scalar(
        "SELECT detail FROM tasks WHERE id=$1 AND task_type='sweep' AND state='completed'",
    )
    .bind(id)
}

pub(in super::super) fn pause_active_task(id: Execution) -> Statement<'static> {
    sqlx::query("UPDATE tasks SET state='paused',updated_at=now() WHERE id=$1 AND state IN ('queued','running') AND lease_token=$2 AND ((lease_node_id IS NULL AND lease_expires_at IS NULL) OR lease_expires_at>clock_timestamp())").bind(id.id)
        .bind(id.token)
}

pub(in super::super) fn task_running(task: Execution) -> Scalar<'static, bool> {
    sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM tasks WHERE id=$1 AND state='running' AND lease_token=$2 AND ((lease_node_id IS NULL AND lease_expires_at IS NULL) OR lease_expires_at>clock_timestamp()))").bind(task.id).bind(task.token)
}

pub(in super::super) fn purge_batch_limit(bucket: Uuid) -> Scalar<'static, Option<i32>> {
    sqlx::query_scalar("SELECT maintenance_concurrency FROM storage_profiles WHERE storage_id=(SELECT storage_id FROM buckets WHERE id=$1)").bind(bucket)
}

pub(in super::super) fn purge_uploads(bucket: Uuid, batch: i64) -> Scalar<'static, Uuid> {
    sqlx::query_scalar("SELECT id FROM uploads WHERE bucket_id=$1 AND state IN ('active','completing') ORDER BY id LIMIT $2").bind(bucket).bind(batch)
}

pub(in super::super) fn lock_purge_objects<'q>(
    bucket: Uuid,
    cursor: Option<&'q str>,
    batch: i64,
) -> Rows<'q, (String, Option<Id>)> {
    sqlx::query_as("SELECT object_key,stream_id FROM objects WHERE bucket_id=$1 AND ($2::text IS NULL OR object_key>$2) ORDER BY object_key LIMIT $3 FOR UPDATE").bind(bucket).bind(cursor).bind(batch)
}

pub(in super::super) fn purge_object<'q>(bucket: Uuid, key: &'q str, token: Id) -> Statement<'q> {
    sqlx::query(
        "UPDATE objects SET stream_id=NULL,write_token=$3 WHERE bucket_id=$1 AND object_key=$2",
    )
    .bind(bucket)
    .bind(key)
    .bind(token)
}

pub(in super::super) fn unpacking_all() -> Scalar<'static, bool> {
    sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM tasks WHERE task_type='unpack' AND detail->>'all'='true' AND state<>'completed')",
    )
}

pub(in super::super) fn pause_pack_creation(paused: bool) -> Statement<'static> {
    sqlx::query("UPDATE mokyu_meta SET pack_creation_paused=$1").bind(paused)
}

pub(in super::super) fn maintenance_policy_paused<'q>(policy: &'q str) -> Scalar<'q, bool> {
    sqlx::query_scalar("SELECT CASE WHEN c.policy='gc' THEN m.gc_paused ELSE c.paused END FROM maintenance_controls c CROSS JOIN mokyu_meta m WHERE c.policy=$1").bind(policy)
}

pub(in super::super) fn maintenance_controls() -> Scalar<'static, Value> {
    sqlx::query_scalar(
        "SELECT to_jsonb(c)||jsonb_build_object('paused',CASE WHEN c.policy='gc' THEN m.gc_paused ELSE c.paused END,'latest',latest.value,'active',active.value) FROM maintenance_controls c CROSS JOIN mokyu_meta m LEFT JOIN LATERAL (SELECT to_jsonb(t) value FROM tasks t WHERE coalesce(t.detail->>'policy',t.task_type)=c.policy ORDER BY created_at DESC,id DESC LIMIT 1) latest ON true LEFT JOIN LATERAL (SELECT to_jsonb(t) value FROM tasks t WHERE coalesce(t.detail->>'policy',t.task_type)=c.policy AND t.state IN ('queued','running','paused') ORDER BY created_at,id LIMIT 1) active ON true ORDER BY c.policy",
    )
}

pub(in super::super) fn preparing_pack_count() -> Scalar<'static, String> {
    sqlx::query_scalar("SELECT count(*)::text FROM packs WHERE state='preparing'")
}

pub(in super::super) fn pause_gc(paused: bool) -> Statement<'static> {
    sqlx::query("UPDATE mokyu_meta SET gc_paused=$1").bind(paused)
}

pub(in super::super) fn pause_maintenance_policy<'q>(
    policy: &'q str,
    paused: bool,
) -> Statement<'q> {
    sqlx::query("UPDATE maintenance_controls SET paused=$2,updated_at=now() WHERE policy=$1")
        .bind(policy)
        .bind(paused)
}

pub(in super::super) fn active_lifecycle_task<'q>(policy: &'q str) -> Scalar<'q, Uuid> {
    sqlx::query_scalar("SELECT id FROM tasks WHERE task_type=$1 AND state IN ('queued','running','paused') ORDER BY created_at LIMIT 1").bind(policy)
}

pub(in super::super) fn lifecycle_storages() -> Rows<'static, (Uuid, String, i64)> {
    sqlx::query_as(
        "SELECT s.id,s.type,sp.revision FROM storages s JOIN storage_profiles sp ON sp.storage_id=s.id WHERE s.enabled ORDER BY s.created_at,s.id",
    )
}

pub(in super::super) fn scope_history_task(child: Uuid, parent: Uuid) -> Statement<'static> {
    sqlx::query(
        "UPDATE tasks SET parent_task_id=$2,batch_total=1,execution_scope='cluster' WHERE id=$1",
    )
    .bind(child)
    .bind(parent)
}

pub(in super::super) fn scheduled_maintenance<'q>(policy: &'q str) -> Statement<'q> {
    sqlx::query("UPDATE maintenance_controls SET last_scheduled_at=now() WHERE policy=$1")
        .bind(policy)
}

pub(in super::super) fn finish_lifecycle_task(
    id: Execution,
    count: i64,
    again: bool,
    report: Value,
) -> Statement<'static> {
    sqlx::query("UPDATE tasks SET processed=processed+$2,state=CASE WHEN $3 THEN state ELSE 'completed' END,detail=detail||jsonb_build_object('report',$4::jsonb),updated_at=now() WHERE id=$1 AND state='running' AND lease_token=$5 AND ((lease_node_id IS NULL AND lease_expires_at IS NULL) OR lease_expires_at>clock_timestamp())").bind(id.id).bind(count).bind(again).bind(report)
        .bind(id.token)
}

pub(in super::super) fn maintenance_schedule() -> Rows<'static, (String, bool, Option<f64>)> {
    sqlx::query_as(
        "SELECT c.policy,CASE WHEN c.policy='gc' THEN m.gc_paused ELSE c.paused END,extract(epoch FROM now()-c.last_scheduled_at)::double precision FROM maintenance_controls c CROSS JOIN mokyu_meta m",
    )
}

pub(in super::super) fn due_pack_reuse() -> Scalar<'static, bool> {
    sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM pack_changes WHERE reason='reuse' AND next_check_at<=now())",
    )
}

impl<'r> sqlx::FromRow<'r, sqlx::postgres::PgRow> for PackSummary {
    fn from_row(row: &'r sqlx::postgres::PgRow) -> sqlx::Result<Self> {
        Ok(Self {
            id: row.try_get("id")?,
            state: row.try_get("state")?,
            compressed: row.try_get("compressed")?,
            raw_size: row.try_get("raw_size")?,
            stored_size: row.try_get("stored_size")?,
            member_count: row.try_get("member_count")?,
            created_at: row.try_get("created_at")?,
        })
    }
}

impl<'r> sqlx::FromRow<'r, sqlx::postgres::PgRow> for PackMember {
    fn from_row(row: &'r sqlx::postgres::PgRow) -> sqlx::Result<Self> {
        Ok(Self {
            chunk_id: row.try_get("chunk_id")?,
            ordinal: row.try_get("ordinal")?,
            raw_size: row.try_get("raw_size")?,
            visible_bytes: row.try_get("visible_bytes")?,
            current_source: row.try_get("current_source")?,
        })
    }
}

impl<'r> sqlx::FromRow<'r, sqlx::postgres::PgRow> for PackObject {
    fn from_row(row: &'r sqlx::postgres::PgRow) -> sqlx::Result<Self> {
        Ok(Self {
            bucket_id: row.try_get("bucket_id")?,
            bucket_name: row.try_get("bucket_name")?,
            key: row.try_get("key")?,
            version: row.try_get("version")?,
            size: row.try_get("size")?,
            content_type: row.try_get("content_type")?,
            public_read: row.try_get("public_read")?,
        })
    }
}

pub(in super::super) fn scoped_pack_limits() -> sqlx::RawSql {
    sqlx::raw_sql(
        "SET LOCAL statement_timeout='5s';SET LOCAL lock_timeout='1s';SET LOCAL work_mem='4MB'",
    )
}

pub(in super::super) fn scoped_packs<'q>(
    after: Id,
    ids: &'q [Uuid],
    admin: bool,
    all: bool,
    state: Option<&'q str>,
    limit: i64,
) -> Rows<'q, PackSummary> {
    sqlx::query_as("SELECT p.id::text,p.state,p.pack_compressed AS compressed,CASE WHEN $3 THEN p.raw_size::text END raw_size,CASE WHEN $3 THEN p.stored_size::text END stored_size,CASE WHEN $3 THEN p.member_count END member_count,p.created_at FROM packs p WHERE p.id>$1 AND ($5::text IS NULL OR p.state=$5) AND ($3 OR p.state='ready') AND ($4 OR EXISTS(SELECT 1 FROM pack_members m JOIN chunks c ON c.id=m.chunk_id JOIN extents e ON e.chunk_id=c.id JOIN streams s ON s.id=e.stream_id JOIN objects o ON o.bucket_id=s.bucket_id AND o.object_key=s.object_key AND o.stream_id=s.id WHERE m.pack_id=p.id AND c.pack_id=p.id AND o.bucket_id=ANY($2))) ORDER BY p.id LIMIT $6").bind(after).bind(ids).bind(admin).bind(all).bind(state).bind(limit)
}

pub(in super::super) fn scoped_pack<'q>(
    id: Id,
    ids: &'q [Uuid],
    admin: bool,
    all: bool,
) -> Rows<'q, PackSummary> {
    sqlx::query_as("SELECT p.id::text,p.state,p.pack_compressed AS compressed,CASE WHEN $3 THEN p.raw_size::text END raw_size,CASE WHEN $3 THEN p.stored_size::text END stored_size,CASE WHEN $3 THEN p.member_count END member_count,p.created_at FROM packs p WHERE p.id=$1 AND ($3 OR p.state='ready') AND ($4 OR EXISTS(SELECT 1 FROM pack_members m JOIN chunks c ON c.id=m.chunk_id JOIN extents e ON e.chunk_id=c.id JOIN streams s ON s.id=e.stream_id JOIN objects o ON o.bucket_id=s.bucket_id AND o.object_key=s.object_key AND o.stream_id=s.id WHERE m.pack_id=p.id AND c.pack_id=p.id AND o.bucket_id=ANY($2)))").bind(id).bind(ids).bind(admin).bind(all)
}

pub(in super::super) fn scoped_pack_members<'q>(
    id: Id,
    ids: &'q [Uuid],
    admin: bool,
    all: bool,
    after: Id,
    limit: i64,
) -> Rows<'q, PackMember> {
    sqlx::query_as("SELECT c.id::text chunk_id,CASE WHEN $3 THEN m.ordinal END ordinal,CASE WHEN $3 THEN c.raw_size END raw_size,coalesce((SELECT sum(upper(span)-lower(span))::text FROM unnest(v.spans) span),'0') AS visible_bytes,coalesce(c.pack_id=m.pack_id AND NOT EXISTS(SELECT 1 FROM chunk_encodings WHERE chunk_id=c.id AND state='ready'),false) AS current_source FROM pack_members m JOIN chunks c ON c.id=m.chunk_id LEFT JOIN LATERAL (SELECT range_agg(int8range(e.source_offset::bigint,e.source_offset::bigint+e.length)) spans FROM extents e JOIN streams s ON s.id=e.stream_id JOIN objects o ON o.bucket_id=s.bucket_id AND o.object_key=s.object_key AND o.stream_id=s.id WHERE e.chunk_id=c.id AND o.bucket_id=ANY($2)) v ON true WHERE m.pack_id=$1 AND c.id>$5 AND ($4 OR (v.spans IS NOT NULL AND c.pack_id=m.pack_id)) ORDER BY c.id LIMIT $6").bind(id).bind(ids).bind(admin).bind(all).bind(after).bind(limit)
}

pub(in super::super) fn scoped_pack_objects<'q>(
    id: Id,
    ids: Vec<Uuid>,
    user: Option<Uuid>,
    token: Option<Uuid>,
    admin: bool,
    last_bucket: Option<Uuid>,
    last_key: Option<&'q String>,
) -> Rows<'q, PackObject> {
    sqlx::query_as("SELECT DISTINCT o.bucket_id,b.name AS bucket_name,o.object_key AS key,s.id AS version,s.size::text,coalesce(s.metadata->>'content_type','application/octet-stream') AS content_type,s.public_read FROM pack_members m JOIN chunks c ON c.id=m.chunk_id JOIN extents e ON e.chunk_id=c.id JOIN streams s ON s.id=e.stream_id JOIN objects o ON o.bucket_id=s.bucket_id AND o.object_key=s.object_key AND o.stream_id=s.id JOIN buckets b ON b.id=o.bucket_id LEFT JOIN user_bucket_access a ON a.bucket_id=b.id AND a.user_id=$3 LEFT JOIN token_bucket_access t ON t.bucket_id=b.id AND t.token_id=$4 WHERE m.pack_id=$1 AND o.bucket_id=ANY($2) AND ($5 OR (c.pack_id=$1 AND 'bucket.list'=ANY(CASE WHEN $4::uuid IS NOT NULL THEN t.actions ELSE a.actions END))) AND ($6::uuid IS NULL OR (o.bucket_id,o.object_key)>($6,$7)) ORDER BY o.bucket_id,o.object_key LIMIT 101").bind(id).bind(ids).bind(user).bind(token).bind(admin).bind(last_bucket).bind(last_key)
}
