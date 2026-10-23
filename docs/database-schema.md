# 数据库结构

数据库保存对象目录、权限、存储映射、任务和运行状态；载荷位于 storage 或受保护的暂存目录。本文只列出当前公开语义，完整类型、约束、索引和触发器以 [`migrations/`](../migrations) 为准。

全新安装使用 `migrations/schema/{postgres,mysql,sqlite}.sql` 的对应最新结构；已初始化数据库执行所选后端的增量迁移：PostgreSQL 使用 `migrations/*.sql`，MySQL 使用 `migrations/mysql/*.sql`，SQLite 使用 `migrations/sqlite/*.sql`。已发布迁移不可修改。PostgreSQL 的单容器和群集使用同一套表；MySQL 和 SQLite 只有单容器表，不保存节点、lease、子任务或共享 staging。群集的 control、worker、data 共享 PostgreSQL，各自保留独立 `instance.data_path` 和 `node.json`。

下表描述共有数据含义，并列出 PostgreSQL 群集字段；原生类型和约束以各后端迁移为准。MySQL/MariaDB 共用 `migrations/mysql/`，使用二进制 UUID、自增 BIGINT 内容 ID、UTC `DATETIME(6)` 和二进制对象键。名称比较区分大小写和尾空格；MySQL 使用原生 JSON，MariaDB 使用带 JSON 有效性约束的文本表示。引用、配额和任务更新在同一事务中显式完成；容量 scope 的桶集合使用关联表。SQLite 使用 STRICT 表、16 字节 UUID、整数自增内容 ID、UTC 微秒整数时间、JSON 文本和二进制对象键；写事务使用 `BEGIN IMMEDIATE`，引用与配额同样在事务内显式维护。原生类型不影响对外 ID、分页、时间和字节数的接口语义。

PostgreSQL 的区块、物理编码、区块包、审计事件和巡检问题采用 `UUID` 主键，默认由 `uuidv7()` 生成，所有关联列使用同类型；自然键、复合键、偏移和计数保持原意。MySQL/MariaDB 和 SQLite 的对应主键、外键继续使用整数，仅 `chunks` 增加非空的 16 字节 `uuid`；物理编码和区块包复用既有 `location_id`，streams、raw_sources、fragments、quota_reservations 及其引用也使用原生整数；raw_sources.location_id 和 fragments.location_id 保存非空唯一的文件 UUID。multipart_files 与 Multipart UploadId 保留稳定 UUID，用于暂存文件和协议身份。PostgreSQL 的物理编码和区块包直接以主键作为文件 UUID，无单独 `location_id` 列。API 将身份作为不透明字符串，不要求各数据库部署的 ID 形态相同。

旧文件身份格式的升级只允许没有对象或来源数据的数据库；存在旧数据时在结构修改前终止，须通过 S3 逻辑迁移。新装结构与空库迁移得到相同结构。

容量分摊在 SQLite 中使用浮点比例并按接口精度输出；配额与实际对象字节账本始终使用整数。数据库空间来自本后端原生统计：SQLite 汇总表和索引页，无法提供的 PG 专属行估算与 vacuum 时间返回 null。

`stream_leases` 只用于群集的跨节点读取与写入保护。单容器通过进程内活动引用协调读写和 GC，不创建或续期 stream lease；请求取消释放引用，重启后按持久状态恢复未完成写入。`storage_permits` 也只供群集协调 storage 并发；单容器使用按 storage 和方向共享的进程内队列。

## 部署、运行设置与身份

| 表 | 关键字段 | 用途 |
| --- | --- | --- |
| `mokyu_meta` | `schema_version`、`deployment_id`、`deployment_mode`、`deployment_mode_revision`、`deployment_key_fingerprint`、`gc_paused`、`maintenance`、`project_management`、`pack_creation_paused` | 部署身份、single/cluster 模式和持久开关；不保存 deployment key 明文 |
| `runtime_settings` | `section`、`value`、`revision`、`updated_at`、`updated_by_user_id` | 类型化的 `web`、`requests`、`uploads`、`metrics`、`maintenance` 分组；Web/CLI 按 revision 更新 |
| `managed_keys` | `id`、`name`、`purpose`、`key_algorithm`、`secret_encrypted`、`fingerprint`、`created_at`、`revoked_at` | Mokyu 生成的 access/signing/block key；密文由 deployment key 保护，fingerprint 全局唯一并在吊销后保留；block 支持 AES-256-GCM、ChaCha20-Poly1305，存储也可关闭加密 |
| `nonce_counters` | `block_key_id`、`type_tag`、`reserved_id` | `(block_key_id,type_tag)` 复合主键；引用托管密钥，类型标记为 0–31，当前 0 为独立区块编码、1 为区块包；非负 `BIGINT` 高水位默认 0，SQLite 为 `INTEGER`，提交后不回退 |
| `nonce_observations` | `block_key_id`、`data`、`revision` | 密钥外键主键；非空 JSON 趋势摘要（SQLite 为合法 JSON 文本）及正整数 CAS 修订，无默认值；每个密钥最多保留 289 个采样点和持久预警，不用于分配或恢复序号 |
| `key_replacements` | `id`、`actor`、`plan`、`result`、`expires_at` | 关联存储换 key 的预览和幂等结果；预览十分钟、完成结果七天，过期由现有清理流程分批回收；不保存密钥材料 |
| `setup_tokens` | `token_hash`、`created_at`、`expires_at` | Web 首次管理员设置的一次性 token；CLI 初始化不依赖它 |
| `web_users`、`sessions`、`api_tokens` | 用户、会话和个人 API token 字段 | Web 认证、CSRF、授权修订和会话撤销 |
| `nodes` | `id`、`name`、`role`、`credential_encrypted`、`enabled`、`status`、`recovery_required`、`lease_expires_at` | control/worker/data 节点注册、凭据和状态；节点直接连接 PostgreSQL |

单容器不创建 `nodes` 记录或 `node.json`。群集节点凭据只写入本节点 `node.json`。释放节点时会清除其凭据、回收任务 lease 并允许同名节点重新注册；部署密钥丢失或改变不能作为普通升级处理。

## storage、桶与对象

| 表 | 关键字段 | 约束 |
| --- | --- | --- |
| `storages` | `id`、`name`、`type`、`path`、`endpoint_url`、`remote_bucket_name`、`region`、`prefix`、加密凭据、`identity_marker`、`block_key_id`、`enabled` | `type` 为 `local` 或 `s3`；保存连接信息时核对已有 `meta.json`，不会把空目标隐式初始化 |
| `storage_profiles` | `storage_id`、`revision`、`cdc_enabled`、`deduplication_enabled`、`write_processing`、`write_durability`（非空，默认 durable，可选 buffered/volatile）、`write_flush_interval`（bigint/SQLite integer，秒，默认 0）、`write_backlog_max_age`（相同类型，秒，默认 300）、`background_scheduling`（非空文本，默认 balanced，可选 foreground）、`public_cache_validation`（非空文本，默认 strict，可选 background）、`public_cache_refresh_interval` / `public_cache_max_age`（非空时长文本，默认 1s / 5s）、`upload_cache_overflow`（fallback/reject）、连接/重试、cache、compression、pack、integrity、migration、并发和字节预算字段 | 每个 storage 一行类型化策略；storage 级读写/control/维护及完整性预算在群集内共享 |
| `storage_permits` | `token`、`storage_id`、`direction`、`holder_id`、`acquired_at`、`expires_at` | 短租约，按 storage 和方向限制群集总并发 |
| `buckets` | `id`、`name`、`project_id`、`storage_id`、设置和状态字段 | `storage_id` 创建时绑定且不可修改；同一 storage 内去重，跨 storage 不去重 |
| `bucket_domains` | `host`、`bucket_id` | 公共 Host 到桶的唯一映射 |
| `objects`、`streams`、`extents`、`fragments` | `object_key`、`stream_type`、`payload_type`、`payload_revision`、`write_token`、`catalog_category`、范围映射、片段状态 | 对象目录和版本发布；完整 ready stream 才能发布；catalog_category 是文件分类，catalog_type 是 MIME type |

storage 连接和 profile 修改只改变后续写入及新请求的策略；已登记的后台去重继续按接收时的义务完成。存量载荷的加密、压缩或区块包转换必须显式启动 `storage rewrite` 或 `storage unpack` 任务。

`streams.http_compression` 是内部响应压缩摘要：PostgreSQL 使用 JSONB，MySQL/MariaDB 使用 JSON，SQLite 使用带 json_valid 检查的 TEXT，默认 `{}`，不通过对象 API 暴露。`estimated_size` 在最终发布时按 extent 引用累计，重复引用重复计数；完整独立区块采用已知存储长度，其余采用原始长度。`version` 是压缩策略修订；`zstd`、`br`、`gzip` 缺失表示未知，0 表示完整尝试收益不足，正数是实际编码长度。

各算法观测通过后端原子更新合并，同策略保留未知键，切换策略只保留有效预估并清除全部旧算法结果；旧节点不能覆盖新策略。观测只写 ready stream，遵守维护、运行模式与所有者边界，不改变对象时间、ETag 或目录 revision。内存队列有界且允许丢弃，数据库中没有额外观测任务或租约；读取不等待回写。

## 原始来源与范围

`streams.payload_type` 为 `chunks`（默认）或 `raw`，`payload_revision` 为正整数、默认 1。旧对象继续采用原表示；原始对象不创建 `extents` 或 `chunks` 行。

| 表 | 字段与约束 | 索引与生命周期 |
| --- | --- | --- |
| `raw_sources` | 原生 `id` 主键；MySQL/MariaDB/SQLite 另有非空唯一的文件 UUID `location_id`，PostgreSQL 复用主键；`storage_id` 非空外键，`owner_stream` 可空外键、删除 stream 时置空；`medium` 默认 local、`durability` 默认 durable、`boot_id` 可空；`state` 默认 writing；`size` 非负、默认 0；`digest` 可空 16 字节、ready 时必须存在；`content_hash` 可空 32 字节、`upload_id` 可空，`pending_upload` 默认 false；可空 `processing_task` 外键指向后台 CDC 的 upload 任务；正整数 `revision` 默认 1、`profile_revision` 非空；可空隔离时间/代码；创建和未引用时间默认当前时间 | 按 storage/state/unreferenced_at/id 查回收候选，按 owner 查未完成写入，以 raw_sources_sync 索引查询健康 ready 的 buffered/volatile 来源；非空内容摘要按 storage/content_hash/size 唯一；被 raw_ranges 引用时不可删除 |
| `raw_ranges` | `stream_id` 外键、删除 stream 时级联；`offset_bytes` 与 `length` 非负；`source_id` 非空来源外键；`source_offset` 非负、默认 0 | 主键 `(stream_id,offset_bytes)`，来源反查索引；发布事务检查有序连续、边界与来源健康，GC 分批释放引用 |
| `raw_processing` | `stream_id` 主键及级联外键，`source_id` 非空级联外键；`payload_revision` 非空；可空 32 字节 `content_hash`、`source_revision`、执行 `token` 和 `lease_expires_at`；`next_attempt_at` 默认当前时间、`attempts` 默认 0、`last_error` 可空 | 按 `(next_attempt_at,stream_id)` 扫描已发布 raw 对象；单容器直接执行，群集领取/续租及最终提交验证节点身份；读保护排空后原子切换来源，删除对象取消无用工作 |

| `raw_uploads` | `source_id` 主键及级联外键；可空执行 `token`、32 字节 `content_hash`、`last_error`；`deduplicate`、`background` 固定接收时策略；`next_attempt_at` 默认当前时间、`attempts` 默认 0 | S3 raw 的必须排空队列，群集另保存 `node_id` 和 `lease_expires_at`，只由所属节点处理；按来源保存，不依赖原对象名称；失败保留唯一缓存，完成后清除待上传标记并释放 pin |

UUID、时间、整数分别使用各数据库既有原生类型。原始来源使用 local、memory 或 shared_s3，确认策略为 durable、buffered 或 volatile；memory 必须带非空 boot_id，其余介质必须为空；完整读取校验长度和 MD5。逻辑用量按对象字节计量，物理用量按独立来源去重，并将共享收益归属到相应桶和项目；raw 没有编码节省。物理库存同时区分保留引用及待回收来源。

## 逻辑区块和物理来源

| 表 | 关键字段 | 说明 |
| --- | --- | --- |
| `chunks` | `id`、持久 UUID、`storage_id`、`hash`、`raw_size`、`standalone_size`、`encryption_algorithm`、`block_key_id`、`standalone_compressed`、`current_encoding_id`、`pack_id`、状态和回收时间 | 按 `storage_id + hash + raw_size` 去重；逻辑区块身份不因重写改变 |
| `chunk_encodings` | `encoding_id`、`chunk_id`、物理 UUID、`storage_id`、`standalone_size`、`standalone_compressed`、`encryption_algorithm`、`block_key_id`、`nonce_id`、`nonce`、`encoding_fingerprint`、`sync_pending`（非空、默认 false）、`boot_id`（可空，非空时必须 sync_pending）、状态和发布时间 | 独立区块的实际物理来源；同一逻辑区块最多一个 ready 独立来源；encodings_sync 索引枚举尚需文件同步的健康 ready/uploading 来源 |
| `packs`、`pack_members` | pack 的主键与物理 UUID、`storage_id`、`nonce_id`、加密/压缩字段、`pack_fingerprint` 及成员偏移 | 区块包来源和成员映射；只收纳同一 storage 的完整区块 |
| `pack_inputs`、`pack_maintenance`、`pack_changes` | 任务输入保护、游标、generation 和下一次检查时间 | 使打包、复用、回收和 Range 优化可恢复 |

`encoding_fingerprint` 和 `pack_fingerprint` 由 key、算法、压缩及对应 profile 计算。保存 profile 不自动重写存量；rewrite 根据 fingerprint 跳过已符合策略的来源。

物理来源的 `nonce_id` 是可空正整数 `BIGINT`（SQLite 为 `INTEGER`），默认空。未加密来源同时清空 key、nonce 和 `nonce_id`；加密来源在进入 ready 前必须具备三者。计数预留与业务主键独立，生命周期回收不删除或降低 `nonce_counters` 的高水位，见[存储格式](storage-format.md)。

## 上传、暂存和配额

| 表 | 关键字段 | 用途 |
| --- | --- | --- |
| `uploads`、`parts`、`web_uploads` | 上传状态、`write_token`、清单、`completion_token`、`completion_expires_at`、校验和 | S3 multipart 和 Web 上传；完成租约保护同一清单的执行和发布 |
| `multipart_files` | 文件 UUID、`upload_id`、`storage_id`、可空原生 `source_id`、open/sealed/deleting 状态、`reclaim_pending` | raw 分片的追加文件和单容器组装检查点；群集增加 `node_id`，按所属节点回收；fragments 记录文件 UUID 与偏移。检查点位于本地后端或 S3 的节点写缓存，分片仍引用时不可回收 |
| `pending_uploads` | `chunk_id`、`stream_id`、`staged_size`、`source_pack_id`、`node_id`、可空 `encoding_policy` JSON、重试和 lease 字段 | 节点本地待上传区块；新块保存接受时的压缩、组包和密钥设置，仅相同设置合并批次；最终确认后释放写占用并保留读缓存 |
| `staging_objects` | `storage_id`、`object_key`、`bytes`、`created_at` | 群集 CDC Multipart 尾块共享暂存的数据库字节账本 |
| `quota_scopes` | `scope`、`scope_id`、用量、`logical_bytes_limit`、`inflight_bytes_limit`、`bucket_count_limit`、`cached_bytes` | project/bucket 配额；NULL 表示不限，0 表示禁止新增 |
| `quota_reservations`、`quota_writes` | reservation、预留、inflight 和替换对象字段 | 接收、覆盖、发布、取消和恢复过程中的事务记账 |

群集 raw 原生 Multipart 复用 `uploads.raw_source_id` 保护最终候选，`remote_token`、`remote_expires_at` 管理初始化与清理，`remote_manifest_hash` 固定最终清单。`parts` 保存 `remote_node_id`、`remote_etag`、执行 token/租约、重试时间/错误和写缓存计费标志，确认必须匹配确切分片代次和所属节点。原生传输复用 `stream_leases` 保护读取；没有清理完最终候选引用的上传不删除历史记录。

配额按原始逻辑字节计量，与去重和压缩无关。数据库函数在同一事务中锁定 project 和 bucket 账本，保证并发上传不会超额。

## 任务、审计与统计

| 表 | 关键字段 | 用途 |
| --- | --- | --- |
| `tasks` | `task_type`、`detail.policy`、`cursor`、`parent_task_id`、`execution_scope`、`target_node_id`、`batch_total`、`batch_completed`、状态和执行令牌/lease 字段 | pack 任务在 detail.policy 中区分维护策略；single 在同一任务中按存储推进，群集拆分存储子任务；不依赖本机目录的群集任务由所有 worker 平等领取 |
| `maintenance_controls`、`maintenance_previews` | `policy`、策略暂停状态、预览参数、确认指纹和 task 回执 | GC、pack、rewrite、迁移、清查和破坏性操作的安全控制 |
| `integrity_issues`、`integrity_packs` | 巡检对象、位置、错误和共享 pack 记录 | 完整性任务只报告问题，不自动修复 |
| `storage_rate_state` | `storage_id`、`metric`、`tokens`、`replenished_at` | 群集巡检的共享 RPS 与字节预算；单容器使用内存状态 |
| `audit_events`、`media_operations` | actor、来源、请求、结果和幂等回执 | 管理操作审计与媒体操作幂等 |
| `usage_summary`、`usage_history`、`runtime_samples` | 范围摘要、历史采样和进程指标 | Web/CLI 统计展示，不参与权限或配额准入 |

single 任务不设置节点或过期 lease，重启后按持久状态恢复。群集任务在节点关闭或 lease 过期时回到可恢复状态。模式切换要求非终态任务已排空；保留的失败任务在 `detail.execution_mode` 记录原模式，不能在另一模式恢复。涉及某个 Data 本机目录的任务固定 `target_node_id`；共享 S3、数据库和 storage profile 的任务可由任意 worker 承担。

任务发布、游标更新和收尾校验本次领取的 lease token。重新领取后，旧执行不能提交结果或清理新执行的输入与临时来源。失败处理和临时来源回收在同一事务内完成；准备中的 pack、encoding 按 `owner_task` 建立部分索引。

single 的 pack/repack/reuse/reclaim/Range/unpack 将存储列表、profile revision、当前存储和快照上界保存在任务 detail 中。存储切换与游标清空在同一事务提交，processed 累计实际处理数量。暂停、失败和重启沿用原任务；profile 版本改变时暂停，显式恢复采用当前存储的新版本。

single 的 integrity 同样只创建一个任务，按各存储的对象和 chunk 上界执行 metadata/head/full 检查。问题、累计检查数、阶段和游标在同一事务保存，跨存储与重启后继续累计；任务的 issues 接口与 JSONL 报告覆盖整次巡检。群集保留按存储分派的子任务。

single 的 GC 和 cleanup 在一个任务中逐存储处理有界批次，cleanup 先执行 history 阶段。detail.report 保留 history 报告和按存储累计的清理计数，processed 保留总进度；存储切换与报告保存由同一执行代次保护。群集继续使用独立 history 和 storage 子任务。

single 的用量采样不创建 usage 任务：在一次刷新超时预算内依次读取部署与各存储统计，成功后一起更新本机快照，失败保留旧值并标记刷新错误。查询新权限范围仍唤醒采样，历史统计继续持久化；usage 子任务仅用于群集。

## 迁移和历史

空库新安装执行 `migrations/schema/` 中对应数据库的最新结构；已初始化数据库按 `mokyu_meta.schema_version` 执行增量迁移。非空未知结构拒绝启动，已发布迁移不可改写。兼容条件和操作步骤见[部署与恢复](deployment-and-recovery.md#升级与数据库迁移)。当前结构以迁移执行后的结果为准，历史文件中的旧名称不代表当前字段。

PostgreSQL 群集的 `node_reports` 按 node_id 保存一份最新报告，boot_id 与 sequence 防止旧进程和乱序覆盖。metadata 保存脱敏加载配置与修订，status 保存有限的运行快照；两者分别限制为 64 KiB 和 32 KiB。只由有效注册身份更新，不使用公共读取从库。单容器不写节点报告，MySQL/SQLite 不创建对应表。正在执行的节点任务通过 tasks_node_running 部分索引查询。

群集历史保存在 `node_runtime_samples`，按 node_id、boot_id、sequence 去重，记录数据库接收时间、进程单调运行时间及最多 16 KiB 的指标。各节点独立采样；control 仅协调汇总与清理。带 `scope=cluster` 的汇总进入 `runtime_samples`，保留当时的预期、报告和有效差分节点数；旧无归属记录不混入群集曲线。按节点/时间与群集 scope 建立索引，过期历史按每批 1000 项清理。

### 来源隔离字段

`chunk_encodings` 和 `packs` 的 `quarantined_at`、`quarantine_code` 与 `quarantine_revision` 独立于原生命周期状态。隔离时间和代码同时为空或同时有值；隔离/完整复检成功均递增代次。物理来源退休后保留隔离证据，逻辑 chunk 和 extents 不变。

引用与最终发布按固定顺序锁逻辑区块、独立编码、pack，当前写库重新判断合格来源。pack/rewrite/cache 批次在任务内部保存有界的来源 ID/隔离代次快照，发布时逐输入比较；仅重查当前“未隔离”不足以防止中间隔离又解除的竞争。群集完整复检通过有效执行任务保护目标物理位置，单容器使用既有本机 pin。

PostgreSQL 的逻辑区块资格检查使用 `FOR KEY SHARE`，新增引用及 extent 维护触发器使用 `FOR NO KEY UPDATE`，避免最终发布与普通引用互相串行等待。GC、pack/rewrite、缓存来源切换与恢复等改变区块状态或来源映射的入口仍先取 `FOR UPDATE`；物理来源资格检查使用 `FOR SHARE` 阻止同时隔离。引用已经有效时不重复写入相同的 `unreferenced_at=NULL`。

重传恢复在 `chunk_encodings.owner_stream` 登记 writing stream；它与 `owner_task` 互斥，外键禁止在释放候选前删除流。PostgreSQL/SQLite 用 CHECK，MySQL/MariaDB 用写入触发器保证互斥。`recovery_pending` 标记尚未确认发布结果的恢复候选，不赋予读取或引用资格。

候选登记与最终切换是两个短事务，中间的编码、写入和完整回读不持有新增来源行锁。最终事务比较来源快照并重新验证授权、写代次、模式、节点和存储策略，退休旧独立来源，发布新编码并建立当前写入的引用；pack 成员只解除逻辑 chunk 的当前 pack 绑定，不删除物理成员布局。并发胜者已提供合格来源时，退役本次候选并复用胜者。

流回收先退休并解绑其候选。`recovery_pending=true` 的候选即使已标记 deleted，也继续参与重复物理回收，并阻止 encoding/chunk 历史记录被提前清理；至少经过删除保留窗口后再次成功删除才清除此标记。已验证发布或确认未使用的完整候选沿用正常物理回收规则。
