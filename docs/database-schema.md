# 数据库结构

PostgreSQL 保存对象、权限、引用和任务；载荷位于后端或受保护的本地上传缓存。此页列出当前表、字段和关键约束，完整类型、默认值、外键及索引以 [migrations](../migrations) 的 SQL 为准。

全新安装顺序执行所有迁移，升级只执行未应用项。已发布迁移不可修改、改号或删除；schema_version 用于兼容检查，不可手改。每个服务独占数据库和 data。

## 约定

- UUID 用于对象、用户、上传及任务；bigint 用于区块、物理编码、区块包和审计。总字节及偏移为 bigint，单区块大小为 integer，时间为 timestamptz。
- key 使用 C 排序，长度 1～1024 UTF-8 字节，不折叠路径。身份、状态、动作由数据库约束和应用共同校验。
- 下表括号为主键。字段保留真实名称；JSONB 含义和发布规则见文末。

## 部署与身份

| 表 / 主键 | 字段 | 关键规则 |
| --- | --- | --- |
| `_sqlx_migrations` (version) | `version`、`description`、`installed_on`、`success`、`checksum`、`execution_time` | SQLx 历史与 SHA-384 校验和，不参与日常清理 |
| `mokyu_meta` (singleton) | `singleton`、`schema_version`、`deployment_id`、`backend_identity`、`backend_initialized`、`gc_paused`、`maintenance`、`created_at`、`access_coverage_since`、`access_flushed_at`、`project_management`、`pack_creation_paused` | 单例部署身份和持久开关 |
| `key_fingerprints` (key_id) | `key_id`、`algorithm`、`fingerprint` | 密钥指纹，禁止同 ID 换材料，不自动回收 |
| `manage_setup` (singleton) | `singleton`、`token_hash`、`created_at` | 首次初始化令牌哈希，成功后移除 |
| `web_users` (id) | `id`、`username`、`password_hash`、`enabled`、`created_at`、`display_name`、`locale`、`theme`、`avatar_email`、`avatar_enabled`、`auth_revision`、`role`、`authorization_revision`、`must_change_password` | username 唯一；admin/member；认证与授权修订号独立 |
| `sessions` (token_hash) | `token_hash`、`user_id`、`csrf_hash`、`expires_at`、`id`、`created_at`、`last_seen_at`、`reauthenticated_at`、`auth_revision`、`user_agent` | 只保存 Cookie/CSRF 哈希；id 不用于认证；随用户删除 |
| `api_tokens` (id) | `id`、`user_id`、`label`、`token_hash`、`prefix`、`system`、`auth_revision`、`authorization_revision`、`created_at`、`expires_at`、`last_used_at`、`revoked_at` | token_hash 唯一；权限与用户当前权限取交集 |
| `projects` (id) | `id`、`name`、`description`、`builtin`、`allow_bucket_create`、`created_at` | name 唯一，仅一个 builtin 默认项目 |
| `project_members` (user_id, project_id) | `user_id`、`project_id`、`role`、`scope` | reader/writer/maintainer；all/selected |
| `member_grants` (user_id, bucket_id) | `user_id`、`project_id`、`bucket_id`、`actions` | 复合外键保证成员与桶同项目 |
| `credentials` (access_key) | `access_key`、`secret_encrypted`、`enabled`、`created_at`、`project_id`、`authorization_revision`、`label`、`expires_at`、`last_used_at`、`created_by` | S3 应用密钥属于项目；创建者删除仅置空 created_by |
| `grants` (access_key, bucket_id) | `access_key`、`bucket_id`、`actions` | S3 逐桶动作，随凭据或桶删除 |
| `token_grants` (token_id, bucket_id) | `token_id`、`bucket_id`、`actions` | 个人 Token 逐桶动作，空授权无数据访问 |

`user_bucket_access` 和 `token_bucket_access` 视图计算有效动作，不复制权限状态。密码、角色、启用状态和授权变更推进相应修订号；写入发布前复核。S3 secret 由独立 credential-key 加密，个人 Token 不保存明文。

## 存储桶与对象

| 表 / 主键 | 字段 | 关键规则 |
| --- | --- | --- |
| `buckets` (id) | `id`、`name`、`state`、`cors`、`website_enabled`、`index_document`、`error_document`、`created_at`、`project_id`、`settings_revision`、`uploads_paused`、`public_base_url` | name 唯一；active/purging；设置变化推进修订号 |
| `domains` (host) | `host`、`bucket_id` | 小写 Host 映射，随桶删除，不自动抢占域名 |
| `objects` (bucket_id, key) | `bucket_id`、`key`、`stream_id`、`write_epoch`、`catalog_size`、`catalog_modified`、`catalog_type`、`catalog_kind`、`catalog_public` | stream_id 唯一；NULL 为未发布占位；write_epoch 防迟到覆盖 |
| `streams` (id) | `id`、`bucket_id`、`object_key`、`kind`、`state`、`size`、`etag`、`metadata`、`public_read`、`checksums`、`created_at`、`touched_at`、`upload_cache_bypass`、`write_authorization` | object/part 内容版本，包含无认证秘密的写入授权快照 |
| `extents` (stream_id, offset_bytes) | `stream_id`、`offset_bytes`、`length`、`chunk_id`、`fragment_id`、`source_offset` | chunk_id 与 fragment_id 恰一非空，source_offset 支持子区间 |
| `fragments` (id) | `id`、`owner_stream`、`sealed`、`size`、`hash`、`created_at` | multipart 原始片段；sealed 后才发布映射 |
| `catalog_build` (singleton) | `singleton`、`phase`、`cursor_bucket`、`cursor_key`、`scanned`、`current_index`、`last_error`、`updated_at` | indexes/backfill/ready，回填游标与批次同事务 |

objects.catalog_* 摘要与对象变更同事务更新，不重复计入配额。旧对象每批回填 500 行；桶内大小、日期、类型索引和 pg_trgm GIN 逐个并发构建。中断的无效索引重建，需要额外数据库磁盘空间。

## 区块与物理来源

| 表 / 主键 | 字段 | 关键规则 |
| --- | --- | --- |
| `chunks` (id) | `id`、`storage_id`、`owner_stream`、`hash`、`raw_size`、`stored_size`、`algorithm`、`key_id`、`compressed`、`nonce`、`format`、`state`、`created_at`、`unreferenced_at`、`deleted_at`、`encoding_id`、`pack_id`、`reference_changed_at`、`split_at`、`range_split_at`、`repack_after` | 明文 BLAKE3 去重身份；raw_size 为 1 B～4 MiB；保留初始独立压缩提示 |
| `chunk_locations` (id) | `id`、`chunk_id`、`storage_id`、`stored_size`、`compressed`、`nonce`、`state`、`created_at`、`unreferenced_at`、`deleted_at`、`owner_task`、`algorithm`、`key_id`、`stored_at` | storage_id 唯一；每个逻辑块最多一个 ready 独立来源 |
| `packs` (id) | `id`、`storage_id`、`algorithm`、`key_id`、`raw_size`、`stored_size`、`compressed`、`nonce`、`digest`、`member_count`、`state`、`created_at`、`unreferenced_at`、`deleted_at`、`owner_task`、`range_checked_at`、`stored_at` | storage_id 唯一；至少两个完整 CDC 成员，压缩标记独立于成员 |
| `pack_members` (pack_id, ordinal) | `pack_id`、`ordinal`、`chunk_id`、`offset_bytes` | 唯一 (pack_id,chunk_id)，偏移相对解压后的载荷 |
| `pack_inputs` (task_id, chunk_id) | `task_id`、`chunk_id`、`created_at` | chunk_id 唯一，保护重写输入并避免任务争用 |
| `pack_maintenance` (stream_id) | `stream_id`、`cursor`、`generation`、`next_check_at`、`updated_at`、`reason` | 待打包/合包候选，generation 防丢并发事件 |
| `pack_changes` (pack_id) | `pack_id`、`generation`、`next_check_at`、`reason` | reuse/reclaim 候选，同样按 generation 处理 |

读取实际独立载荷使用 chunk_locations，包使用 packs/pack_members；chunks.stored_size/compressed 只是独立压缩提示。重编码分配新物理 ID/storage_id，不复用 nonce 身份。stored_at 是远端成功发布时间，复用不重置；未知时在回收前查询 Last-Modified。

物理来源只有 ready 可选。独立来源为 uploading/ready/retired/deleting/deleted；区块包为 preparing/ready/retired/deleting/deleted。切换后的旧来源仍受 GC 宽限与最低存储期保护。

## 上传、缓存与额度

| 表 / 主键 | 字段 | 关键规则 |
| --- | --- | --- |
| `uploads` (id) | `id`、`bucket_id`、`object_key`、`access_key`、`state`、`metadata`、`public_read`、`checksum_algorithm`、`checksum_type`、`manifest_hash`、`result`、`output_stream`、`created_at`、`touched_at`、`durable_at` | active/completing/completed/aborted；Complete 清单及幂等结果 |
| `parts` (upload_id, part_number) | `upload_id`、`part_number`、`stream_id`、`write_epoch`、`quota_size` | 当前 part stream；write_epoch 防替换竞态，quota_size 记录接收量 |
| `web_uploads` (upload_id) | `upload_id`、`user_id`、`client_id`、`request_hash`、`file_name`、`expected_size`、`part_size`、`modified_at`、`expected_stream` | 唯一 (user_id,client_id)；用户删除不转移续传身份 |
| `pending_uploads` (chunk_id) | `chunk_id`、`stream_id`、`offset_bytes`、`cache_size`、`cache_compressed`、`source_pack`、`created_at`、`next_retry_at`、`attempts`、`last_error`、`owner_task` | 每逻辑块一份本地待上传来源，字节只计一次 |
| `cache_pins` (chunk_id, pin_type, owner_id) | `chunk_id`、`pin_type`、`owner_id`、`created_at` | upload/pack 共同保护 pending 文件，年龄不触发淘汰 |
| `quota_accounts` (kind, id) | `kind`、`id`、`used_bytes`、`reserved_bytes`、`inflight_bytes`、`object_count`、`bucket_count`、`byte_limit`、`inflight_limit`、`bucket_limit` | project/bucket 账本；非负用量，NULL 不限，0 为零 |
| `quota_reservations` (id) | `id`、`bucket_id`、`object_key`、`stream_id`、`upload_id`、`output_stream`、`closed`、`base_stream`、`credit_bytes`、`logical_bytes`、`inflight_bytes` | stream/upload 恰一非空；同旧版本只有一份覆盖抵扣 |
| `quota_writes` (stream_id) | `stream_id`、`reservation_id`、`allocated_bytes`、`contribution_bytes` | 已分配预留及贡献，随预留删除 |

配额先锁项目再锁桶，接收、发布、覆盖、删除与归还都在事务中记账。按当前对象原始大小计算，与去重和压缩无关。桶转移原子移动账本并撤销原桶专属授权。

pending 的 cache_size/cache_compressed 表示实际 .raw/.zst 文件。pin 随 pending 删除；降配、重启或超时不能删除唯一已确认副本。uploads.durable_at 记录首次观察到远端来源已全部就绪的时刻。

## 任务、审计与回执

| 表 / 主键 | 字段 | 关键规则 |
| --- | --- | --- |
| `maintenance_controls` (kind) | `kind`、`paused`、`last_scheduled_at`、`updated_at` | 七类持久策略和最近调度时间 |
| `maintenance_previews` (id) | `id`、`actor_id`、`token_id`、`action`、`parameters`、`fingerprint`、`expires_at`、`task_id` | 身份/Token 绑定的十分钟预览，task_id 保存执行回执 |
| `tasks` (id) | `id`、`kind`、`bucket_id`、`state`、`cursor`、`processed`、`detail`、`error`、`created_at`、`updated_at`、`created_by`、`source`、`started_at` | 持久游标、来源、发起者；queued/running/paused/completed/failed |
| `integrity_issues` (id) | `id`、`task_id`、`subject`、`code`、`chunk_id`、`storage_id`、`stream_id`、`bucket_id`、`object_key`、`detail`、`created_at` | 保留检查时身份，对象删除不抹除异常 |
| `integrity_packs` (task_id, pack_id) | `task_id`、`pack_id`、`error_code` | 避免一轮巡检重复验证共享包 |
| `audit_events` (id) | `id`、`created_at`、`finished_at`、`actor_id`、`actor_label`、`token_id`、`source`、`action`、`target`、`project_id`、`bucket_id`、`outcome`、`request_id`、`status`、`detail` | 持久意图与显式结果，用户删除不级联删除历史 |
| `media_operations` (id) | `id`、`user_id`、`token_id`、`client_id`、`request_hash`、`result`、`created_at` | 唯一 (user_id,token_id,client_id)，NULLS NOT DISTINCT；成功回执与变更同事务 |

## 访问与容量统计

| 表 / 主键 | 字段 | 关键规则 |
| --- | --- | --- |
| `chunk_access_stats` (chunk_id) | `chunk_id`、`reads`、`range_reads`、`bytes`、`last_read_at` | 累计读取量，不是逐请求审计 |
| `chunk_access_windows` (chunk_id, window_start) | `chunk_id`、`window_start`、`reads`、`range_reads`、`bytes`、`range_origin_reads` | UTC 小时窗口，含缓存和回源 Range |
| `pack_access_windows` (pack_id, window_start) | `pack_id`、`window_start`、`downloads`、`downloaded_bytes`、`partial_downloads`、`useful_bytes`、`partial_bytes`、`updated_at` | 实际包下载与局部有效字节，共享下载计一次 |
| `pack_member_access_windows` (window_start, pack_id, chunk_id) | `window_start`、`pack_id`、`chunk_id`、`downloads` | 局部回源涉及的成员，供收益估计 |
| `storage_insights` (id) | `id`、`bucket_ids`、`requested_at`、`as_of`、`data` | 按实时权限形成的范围摘要，global 的 bucket_ids 为 NULL |
| `storage_history` (scope_id, at) | `scope_id`、`at`、`data` | 同口径容量历史，随范围清理 |
| `runtime_history` (at) | `at`、`data` | 进程计数与缓存历史，重启处分段 |

访问先在有界内存计数，再批量落库；异常退出可丢失未刷盘观测。access_coverage_since/access_flushed_at 防止将重启或统计缺口视为零访问。范围数量、历史点数及保留期有上限，统计不参与权限或配额准入。

## 状态与清理

- streams：writing → ready → retired，未完成写入恢复为 abandoned。objects 只发布完整 ready 流；已接纳读取固定版本，不受后来覆盖影响。
- chunks：preparing → uploading → ready，不确定失败为 failed；逻辑退役后 deleting → deleted。最后引用消失设置 unreferenced_at，重新引用清空；删除认领与新引用互斥。
- part 可混合片段和区块，最终对象必须全部引用 ready 区块、映射连续且总长正确。先持久写文件，再提交 sealed 与引用。
- Complete 冻结清单、发布并保存结果；相同清单可重试。未提交的 completing 重启后回到 active。
- 重启重排 running 任务，paused 保持暂停；已完成历史按保留期清理。外键、owner_stream、pack_inputs 和活跃读写共同保护仍需要的记录。
- 逻辑行清理等待物理日志和包成员索引不再需要它；identity 序列与密钥指纹不回收。删除桶不会删除其他桶仍引用的区块。

保持 PostgreSQL autovacuum/ANALYZE 启用，程序不自动 VACUUM FULL。保留期见[配置](configuration.md#回收与历史清理)；不要手改状态、引用、序列或 nonce 绕过检查。

## JSONB 结构

| 字段 | 内容 |
| --- | --- |
| streams.metadata / uploads.metadata | content_type/cache_control/content_disposition/content_encoding/content_language/expires；user 为自定义 S3 元数据字典 |
| streams.checksums | S3 校验值及 checksum_type，与内部 BLAKE3 独立 |
| streams.write_authorization | 内部身份、修订号、所需动作，无认证秘密 |
| buckets.cors | origins/methods/headers/expose/max_age 规则数组 |
| uploads.result | Complete 的 ETag、时间等，与 manifest_hash 用于重试 |
| tasks.detail | 范围、批次进度及诊断，见[任务 API](manage-api-reference.md#后台任务) |
| media_operations.result | 逐项结果和新版本身份 |
| storage_insights.data / storage_history.data | 同口径 R/U/D/A 及物理来源快照 |
| audit_events.detail | 显式允许的结果，不含完整请求、密码或密钥 |

结构不是备份；恢复仍需[一致的数据库、密钥及实际载荷](deployment-and-recovery.md#备份材料)。
