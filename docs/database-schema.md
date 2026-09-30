# 数据库结构

本文列出当前表、字段和索引；完整 SQL 见 [migrations](../migrations)，迁移行为见[升级说明](deployment-and-recovery.md#升级与数据库迁移)。

- 时间使用 `timestamptz`，按 UTC 存储和传输；size、offset、length 的单位为字节。
- `—` 表示无默认值，可空列默认 NULL。`identity` 为 PostgreSQL 自增分配；JSONB 是内部结构，通过管理接口修改。
- 未特别注明的外键删除行为为 NO ACTION。主键和唯一约束的隐式索引不另列。
- 服务使用同步提交、fsync、数据库独占锁及 data 文件锁；运行计数和容量快照保存在进程内。

导航：[部署与密钥](#mokyu_meta) · [桶与权限](#buckets) · [对象与区块](#streams) · [分片上传](#uploads) · [管理用户](#web_users) · [任务与巡检](#tasks) · [状态与清理](#状态与清理)

## _sqlx_migrations

SQLx 管理的迁移历史，纳入数据库备份，不应手动修改。

| 字段 | 类型 | 含义 |
| --- | --- | --- |
| `version` | bigint PRIMARY KEY | 已应用的迁移编号 |
| `description` | text NOT NULL | 迁移描述 |
| `installed_on` | timestamptz NOT NULL DEFAULT now() | 登记时间 |
| `success` | boolean NOT NULL | 迁移是否成功 |
| `checksum` | bytea NOT NULL | SQL 文件的 SHA-384 校验和 |
| `execution_time` | bigint NOT NULL | 执行耗时（纳秒） |

## mokyu_meta

| 字段 | 类型 | 可空 | 默认值 | 含义 |
| --- | --- | --- | --- | --- |
| `singleton` | boolean | 否 | `true` | 固定 true，保证仅一行 |
| `schema_version` | integer | 否 | — | 当前结构编号，与最近迁移编号一致 |
| `deployment_id` | uuid | 否 | — | 部署 UUID |
| `backend_identity` | text | 否 | — | 后端 endpoint/bucket/prefix 身份 |
| `backend_initialized` | boolean | 否 | `false` | 后端 meta.json 已完成绑定；标识丢失时不自动重建 |
| `gc_paused` | boolean | 否 | `false` | 持久远端 GC 暂停标志 |
| `maintenance` | boolean | 否 | `false` | 持久维护标志 |
| `created_at` | timestamptz | 否 | `now()` | 部署初始化时间 |
| `access_coverage_since` / `access_flushed_at` | timestamptz | 否 | `now()` | 连续访问观测起点 / 最近成功落库时间；重启、丢失计数或落库中断后重新计算覆盖 |

主键：`singleton`，约束为 true，仅允许一行。

## key_fingerprints

| 字段 | 类型 | 可空 | 默认值 | 含义 |
| --- | --- | --- | --- | --- |
| `key_id` | text | 否 | — | 写入或历史密钥标识 |
| `algorithm` | text | 否 | — | 密钥算法或 credential 保护用途 |
| `fingerprint` | bytea | 否 | — | 密钥材料指纹；防止同 ID 换材料 |

主键：`key_id`；`fingerprint` 固定 32 字节。同 ID 不能替换实际密钥材料。

## buckets

| 字段 | 类型 | 可空 | 默认值 | 含义 |
| --- | --- | --- | --- | --- |
| `id` | uuid | 否 | — | 逻辑桶 UUID |
| `name` | text | 否 | — | S3 桶名，C 排序 |
| `state` | text | 否 | `'active'` | active / purging |
| `cors` | jsonb | 否 | `'[]'` | 项目 CORS 规则数组 |
| `website_enabled` | boolean | 否 | `false` | 公共 web 入口是否启用首页/404 路由 |
| `index_document` | text | 否 | `'index.html'` | 目录首页文件名，管理接口限制 1～255 UTF-8 字节、不含路径段 |
| `error_document` | text | 否 | `'404.html'` | 桶根相对对象键，最多 1024 UTF-8 字节；空字符串使用内置 404 |
| `created_at` | timestamptz | 否 | `now()` | 创建时间 |

主键：`id`；`name` 唯一、使用 C 排序。`state` 只允许表内枚举值。网站字段的 API 校验见[网站设置](manage-api-reference.md#网站设置)。

## credentials

| 字段 | 类型 | 可空 | 默认值 | 含义 |
| --- | --- | --- | --- | --- |
| `access_key` | text | 否 | — | S3 客户端 access key |
| `secret_encrypted` | bytea | 否 | — | AES-GCM 保护的 secret：随机 nonce + ciphertext + tag |
| `enabled` | boolean | 否 | `true` | 能否认证 |
| `created_at` | timestamptz | 否 | `now()` | 创建时间 |

主键：`access_key`。

credentials 另有：label text=''、可空 xpires_at/last_used_at timestamptz、可空 created_by uuid（用户删除时置空）。修改有效期推进授权 revision；应用密钥不随创建者删除。

## grants

| 字段 | 类型 | 可空 | 默认值 | 含义 |
| --- | --- | --- | --- | --- |
| `access_key` | text | 否 | — | 客户端凭据 |
| `bucket_id` | uuid | 否 | — | 可访问逻辑桶 |
| `actions` | text[] | 否 | — | 七种逐桶动作的子集 |

主键：`(access_key,bucket_id)`；两列分别引用 `credentials`、`buckets`，均随目标删除级联。

## domains

| 字段 | 类型 | 可空 | 默认值 | 含义 |
| --- | --- | --- | --- | --- |
| `host` | text | 否 | — | 小写 HTTP Host，可含端口 |
| `bucket_id` | uuid | 否 | — | 公共域名绑定桶 |

主键：`host`；`bucket_id` 引用 `buckets`，随桶删除级联。

## streams

| 字段 | 类型 | 可空 | 默认值 | 含义 |
| --- | --- | --- | --- | --- |
| `id` | uuid | 否 | — | 不可变对象 / part 版本 UUID |
| `bucket_id` | uuid | 否 | — | 所属桶 |
| `object_key` | text | 否 | — | 完整原始 key |
| `kind` | text | 否 | — | object / part |
| `state` | text | 否 | — | writing / ready / retired / abandoned |
| `size` | bigint | 否 | `0` | 原始字节总数，B |
| `etag` | text | 否 | `''` | 未带双引号的 ETag |
| `metadata` | jsonb | 否 | `'{}'` | HTTP 元数据及 user 字典 |
| `public_read` | boolean | 否 | `false` | 对象匿名读取标志 |
| `checksums` | jsonb | 否 | `'{}'` | S3 校验和值/类型 |
| `upload_cache_bypass` | boolean | 否 | `false` | 本次请求已因暂存不可用转同步，后续新块沿用 |
| `created_at` | timestamptz | 否 | `now()` | 该版本创建时间 |
| `touched_at` | timestamptz | 否 | `now()` | 发布、退役或写入进度时间 |

主键：`id`；`bucket_id` 引用 `buckets`；`object_key` 使用 C 排序。`kind/state` 受枚举约束，`size >= 0`。

| 索引 | 列与条件 |
| --- | --- |
| `streams_cleanup` | `(state,touched_at,id)` |
| `streams_writing` | `(bucket_id,object_key) WHERE state='writing'` |

## objects

| 字段 | 类型 | 可空 | 默认值 | 含义 |
| --- | --- | --- | --- | --- |
| `bucket_id` | uuid | 否 | — | 逻辑桶 |
| `key` | text | 否 | — | 对象 key，UTF-8 长度 1～1024 字节 |
| `stream_id` | uuid | 是 | — | 当前可见版本；NULL 为待写/删除占位 |
| `write_epoch` | uuid | 否 | — | 当前写入资格 UUID，迟到请求不能覆盖新版本 |

主键：`(bucket_id,key)`；`bucket_id` 引用 `buckets`；`stream_id` 唯一并引用 `streams`。`key` 使用 C 排序，UTF-8 长度为 1～1024 字节。

| 索引 | 列与条件 |
| --- | --- |
| `objects_empty` | `(bucket_id,key) WHERE stream_id IS NULL` |

## chunks

| 字段 | 类型 | 可空 | 默认值 | 含义 |
| --- | --- | --- | --- | --- |
| `id` | bigint | 否 | `GENERATED ALWAYS AS IDENTITY` | 正 bigint 自增分配；加密前提交 |
| `storage_id` | uuid | 否 | — | 初始物理 UUID，也是稳定的逻辑缓存身份 |
| `encoding_id` | bigint | 否 | `nextval('chunk_locations_id_seq')` | 初始独立编码身份；升级时沿用原 id |
| `pack_id` | bigint | 是 | — | 当前区块包来源，引用 packs |
| `owner_stream` | uuid | 是 | — | 尚未转交 extent 引用时的写入保护 |
| `hash` | bytea | 否 | — | 原始明文 BLAKE3，完整 32 字节 |
| `raw_size` | integer | 否 | — | 明文长度，B，1～4 MiB |
| `stored_size` | integer | 是 | — | 初始独立编码长度提示，B，含 tag；不随打包改变 |
| `algorithm` | text | 否 | — | none / aes-256-gcm / chacha20-poly1305 |
| `key_id` | text | 否 | — | 初始编码密钥和逻辑去重域；none 时为空串 |
| `compressed` | boolean | 否 | `false` | 独立压缩提示，不代表所在区块包的压缩状态 |
| `nonce` | bytea | 是 | — | AEAD 12 字节 nonce；未编码/none 时为 NULL |
| `format` | integer | 否 | `1` | 区块编码格式，固定为 1 |
| `state` | text | 否 | — | preparing / uploading / ready / failed / deleting / deleted |
| `created_at` | timestamptz | 否 | `now()` | 分配时间，UTC 日期用于 nonce |
| `unreferenced_at` | timestamptz | 是 | — | 最后引用消失的时间；有引用通常为 NULL |
| `deleted_at` | timestamptz | 是 | — | 逻辑区块完成退役的时间；物理删除由各来源表记录 |
| `reference_changed_at` | timestamptz | 否 | `now()` | 最近引用变化 |
| `split_at` / `range_split_at` / `repack_after` | timestamptz | 是 | — | 最近拆分 / Range 拆分 / 最早重新合并时间 |

主键：`id`；`storage_id` 唯一；`owner_stream` 引用 `streams`，目标删除时置 NULL。约束：hash 为 32 字节，raw_size 为 1～4194304，stored_size 非空时为 1～4194320，format=1，algorithm/state 受枚举约束。`none` 要求空 key_id、NULL nonce；加密模式要求非空 key_id，nonce 可在准备阶段为 NULL，否则长 12 字节。

| 索引 | 列与条件 |
| --- | --- |
| `chunks_dedup` | `UNIQUE (hash,raw_size,algorithm,key_id) WHERE state IN ('preparing','uploading','ready')` |
| `chunks_gc` | `(unreferenced_at,id) WHERE state IN ('ready','failed','deleting')` |
| `chunks_owner` | `(owner_stream) WHERE owner_stream IS NOT NULL` |
| `chunks_deleted` | `(deleted_at,id) WHERE state='deleted'` |
| `chunks_pack` | `(pack_id) WHERE pack_id IS NOT NULL` |

## 物理来源与区块包

逻辑块与物理编码分开计数。chunks 的初始编码字段保留为压缩提示；实际独立载荷读取 chunk_locations，打包载荷读取 packs/pack_members。每次重编码使用新物理 ID 和 storage_id。

| 表 | 字段（未注明可空者均 NOT NULL） |
| --- | --- |
| `chunk_locations` | id bigint BY DEFAULT identity PK；chunk_id bigint → chunks CASCADE；storage_id uuid UNIQUE；algorithm/key_id text；stored_size integer 可空；compressed bool；nonce bytea 可空；state text；created_at timestamptz；stored_at/unreferenced_at/deleted_at timestamptz 可空；owner_task uuid → tasks SET NULL 可空 |
| `packs` | id bigint ALWAYS identity PK；storage_id uuid UNIQUE；algorithm/key_id text；raw_size bigint >0；stored_size bigint 可空；compressed bool 默认 false；nonce bytea（12 B）/digest bytea（32 B）可空；member_count integer ≥2；state text；created_at timestamptz 默认 now()；stored_at/unreferenced_at/deleted_at/range_checked_at timestamptz 可空；owner_task uuid → tasks SET NULL 可空 |
| `pack_members` | pack_id bigint → packs CASCADE；ordinal integer ≥0；chunk_id bigint → chunks CASCADE；offset_bytes bigint ≥0；PK(pack_id,ordinal)、UNIQUE(pack_id,chunk_id) |
| `pack_inputs` | task_id uuid → tasks CASCADE；chunk_id bigint → chunks；created_at timestamptz 默认 now()；PK(task_id,chunk_id)、UNIQUE(chunk_id)，防止重写任务同时占有相同输入 |
| `pack_maintenance` | stream_id uuid PK → streams CASCADE；cursor bigint 默认 0；generation bigint 默认 1；reason text 默认 `pack`；next_check_at/updated_at timestamptz 默认 now() |
| `pack_changes` | pack_id bigint PK → packs CASCADE；generation bigint 默认 1；next_check_at timestamptz 默认 now()；reason text 默认 reuse，允许 reuse/reclaim |
| `integrity_packs` | task_id uuid → tasks CASCADE；pack_id bigint → packs CASCADE；error_code text 可空；PK(task_id,pack_id)，NULL 表示该任务已验证该物理包 |

独立来源状态为 uploading/ready/retired/deleting/deleted，区块包为 preparing/ready/retired/deleting/deleted。仅 ready 映射可发布；失败准备记录退役后仍受正常 GC 宽限。逻辑行清理必须等待它的物理日志删除及仍存活区块包的成员索引不再需要它。

`stored_at` 在远端写入成功并发布时记录；复用现有来源不重置。NULL 表示时间未知，启用最低存储期限后在物理回收前查询远端 `Last-Modified` 补齐，不使用分配记录时的 `created_at` 推算。

索引：chunk_locations_current 在 ready 上唯一约束 chunk_id；两类物理 GC 索引为 retired/deleting 的 (unreferenced_at,id)；pack_members_chunk 为 chunk_id；pack_maintenance_due 为 (next_check_at,stream_id)，pack_changes_due 为 (reason,next_check_at,pack_id)。引用变更的 statement trigger 和 stream 发布 trigger 在同一事务更新候选及 generation；任务只删除自己处理过的 generation，后续事件不会被覆盖。

## 访问统计

| 表 | 字段与主键 |
| --- | --- |
| `chunk_access_stats` | chunk_id bigint PK → chunks CASCADE；reads/range_reads/bytes bigint 默认 0；last_read_at timestamptz |
| `chunk_access_windows` | window_start timestamptz、chunk_id bigint → chunks CASCADE，构成 PK；reads/range_reads/bytes/range_origin_reads bigint 默认 0 |
| `pack_access_windows` | window_start timestamptz、pack_id bigint → packs CASCADE，构成 PK；downloads/downloaded_bytes/partial_downloads/partial_bytes/useful_bytes bigint 默认 0；updated_at timestamptz 默认 now() |
| `pack_member_access_windows` | window_start timestamptz、pack_id bigint → packs CASCADE、chunk_id bigint → chunks CASCADE，构成 PK；downloads bigint 默认 0，仅实际局部回源使用的成员 |

窗口按 UTC 小时聚合，另有 (chunk_id,window_start) 和 (pack_id,window_start) 查询索引。读取先更新有界进程计数，再批量落库；统计不是审计账本，进程异常退出可能丢失尚未落库的一小段。后台预热不增加逻辑访问；区块包统计记录实际回源及完整成员代价，缓存 Range 与远端局部区块包下载分开计数。

共享下载只登记一次；同次下载若服务完整读取或维护读取，不算局部 Range 压力。range_checked_at 避免在没有新观测时反复试压。Range 冷却保存在逻辑 chunks，另有 `(repack_after,id) WHERE range_split_at IS NOT NULL` 索引，不依赖旧包或任务历史。

## fragments

| 字段 | 类型 | 可空 | 默认值 | 含义 |
| --- | --- | --- | --- | --- |
| `id` | uuid | 否 | — | 本地原始片段 UUID / 文件名 |
| `owner_stream` | uuid | 是 | — | 写入未发布映射前的保护 |
| `sealed` | boolean | 否 | `false` | 本地写入已完成 |
| `size` | integer | 否 | — | 原始文件长度，B，1～4 MiB |
| `hash` | bytea | 否 | — | 原始片段 BLAKE3 |
| `created_at` | timestamptz | 否 | `now()` | 创建时间 |

主键：`id`；`owner_stream` 引用 `streams`，目标删除时置 NULL。size 为 1～4194304，hash 为 32 字节。

| 索引 | 列与条件 |
| --- | --- |
| `fragments_owner` | `(owner_stream) WHERE owner_stream IS NOT NULL` |
| `fragments_cleanup` | `(created_at,id)` |

## extents

| 字段 | 类型 | 可空 | 默认值 | 含义 |
| --- | --- | --- | --- | --- |
| `stream_id` | uuid | 否 | — | 所属对象 / part 版本 |
| `offset_bytes` | bigint | 否 | — | 在此版本中的起始偏移，B |
| `length` | integer | 否 | — | 映射长度，B，1～4 MiB |
| `chunk_id` | bigint | 是 | — | 远端区块来源，与 fragment 二选一 |
| `fragment_id` | uuid | 是 | — | 唯一原始本地来源，与 chunk 二选一 |
| `source_offset` | integer | 否 | `0` | 在来源中的偏移，B |

主键：`(stream_id,offset_bytes)`；随 stream 删除级联，另外引用 `chunks` 或 `fragments` 且必须二选一。offset_bytes ≥ 0，length 为 1～4194304，source_offset 为 0～4194303，source_offset + length ≤ 4194304。

| 索引 | 列与条件 |
| --- | --- |
| `extents_chunk` | `(chunk_id) WHERE chunk_id IS NOT NULL` |
| `extents_fragment` | `(fragment_id) WHERE fragment_id IS NOT NULL` |

## uploads

| 字段 | 类型 | 可空 | 默认值 | 含义 |
| --- | --- | --- | --- | --- |
| `id` | uuid | 否 | — | S3 UploadId UUID |
| `bucket_id` | uuid | 否 | — | 目标桶 |
| `object_key` | text | 否 | — | 目标对象 key |
| `access_key` | text | 否 | — | 创建上传的身份字符串 |
| `state` | text | 否 | `'active'` | active / completing / completed / aborted |
| `metadata` | jsonb | 否 | `'{}'` | 创建时对象元数据 |
| `public_read` | boolean | 否 | `false` | 创建时 ACL |
| `checksum_algorithm` | text | 是 | — | 请求的 S3 校验算法 |
| `checksum_type` | text | 是 | — | FULL_OBJECT / COMPOSITE |
| `manifest_hash` | text | 是 | — | 冻结的 Complete 请求清单摘要 |
| `result` | jsonb | 是 | — | 已提交的 Complete 返回数据，供幂等重试 |
| `output_stream` | uuid | 是 | — | 完成时构造的对象版本 |
| `created_at` | timestamptz | 否 | `now()` | 创建时间 |
| `touched_at` | timestamptz | 否 | `now()` | 有效上传/状态变更时间，决定过期 |

主键：`id`；`bucket_id` 引用 `buckets`；`output_stream` 引用 `streams`，目标删除时置 NULL。`object_key` 使用 C 排序，`state` 受枚举约束。

| 索引 | 列与条件 |
| --- | --- |
| `uploads_expiry` | `(state,touched_at,id)` |
| `uploads_list` | `(bucket_id,object_key,id)` |
| `uploads_finished` | `(touched_at,id) WHERE state IN ('completed','aborted')` |
| `uploads_output` | `(output_stream) WHERE output_stream IS NOT NULL` |

## parts

| 字段 | 类型 | 可空 | 默认值 | 含义 |
| --- | --- | --- | --- | --- |
| `upload_id` | uuid | 否 | — | 所属上传 |
| `part_number` | integer | 否 | — | 1～10000 |
| `stream_id` | uuid | 是 | — | 当前已确认 part 版本；首次在写时可为 NULL |
| `write_epoch` | uuid | 否 | — | part 替换资格 UUID；失败保持旧 stream |

主键：`(upload_id,part_number)`；随 upload 删除级联，`stream_id` 引用 `streams`；part_number 为 1～10000。

| 索引 | 列与条件 |
| --- | --- |
| `parts_stream` | `(stream_id) WHERE stream_id IS NOT NULL` |

## api_tokens 与 token_grants

`api_tokens`：`id uuid` 主键、`user_id uuid` 用户外键级联删除、`label text`（1～128 字节）、`prefix text`、唯一 `token_hash bytea`（32 字节）、`system boolean=false`、签发时的 `auth_revision bigint`、`authorization_revision bigint=0`、`created_at timestamptz=now()`，可空 `expires_at/last_used_at/revoked_at timestamptz`。索引 `(user_id,id)`。不存明文 Token。

`token_grants`：复合主键 `(token_id,bucket_id)`、有效动作 `actions text[]`，两外键级联删除，反向索引 `(bucket_id,token_id)`。`token_bucket_access` 视图计算 Token 范围与用户当前桶权限的交集；管理员仍受显式 Token 范围限制，完整 system Token 另行验证。

Token 范围、有效期及撤销变更推进授权 revision；用户安全 revision 改变使旧 Token 永久失效。权限检查同时锁定用户和 Token，防止撤权前的校验结果被用于稍后提交。

## audit_events

`id bigint identity` 主键；`created_at timestamptz=now()`、可空 `finished_at timestamptz`；可空 `actor_id/token_id uuid`，`actor_label text` 保留操作时账号名称；`source text` 为 web/token/cli。`action/target text`、可空 `project_id/bucket_id uuid` 表示动作与范围；`outcome text='unknown'` 为 unknown/succeeded/failed/partial；可空 `request_id text/status integer`，`detail jsonb='{}'` 仅含显式脱敏业务字段。

审计身份和范围不使用级联外键，删除业务记录不移除历史。索引 `(created_at,id)`、`(actor_id,id)`、`(project_id,id)`、`(bucket_id,id)` 与非空 request_id。关键授权变更与结果记录同一事务；未完成意图不自动推断成功。

## web_users

| 字段 | 类型 | 可空 | 默认值 | 含义 |
| --- | --- | --- | --- | --- |
| `id` | uuid | 否 | — | Web 管理员 UUID |
| `username` | text | 否 | — | 用户名 |
| `password_hash` | text | 否 | — | 带参数与 salt 的 Argon2 哈希 |
| `enabled` | boolean | 否 | `true` | 可否登录/使用会话 |
| `display_name` | text | 否 | `''` | 显示名，最多 240 UTF-8 字节 |
| `locale` | text | 是 | — | en/zh-CN/ja，NULL 跟随浏览器 |
| `theme` | text | 是 | — | auto/light/dark，NULL 跟随浏览器 |
| `avatar_email` | text | 否 | `''` | Gravatar 邮箱，最多 320 UTF-8 字节 |
| `avatar_enabled` | boolean | 否 | `false` | 用户主动开启外部头像 |
| `auth_revision` | bigint | 否 | `0` | 密码重置或禁用递增，防止旧验证结果生成有效会话 |
| `created_at` | timestamptz | 否 | `now()` | 创建时间 |

主键：`id`；`username` 唯一。

## sessions

| 字段 | 类型 | 可空 | 默认值 | 含义 |
| --- | --- | --- | --- | --- |
| `token_hash` | bytea | 否 | — | cookie token 的 BLAKE3，不存明文 token |
| `id` | uuid | 否 | `gen_random_uuid()` | 对外会话标识，唯一，不用于认证 |
| `user_id` | uuid | 否 | — | 所属 Web 用户 |
| `csrf_hash` | bytea | 否 | — | CSRF token 的 BLAKE3 |
| `expires_at` | timestamptz | 否 | — | 固定到期时间 |
| `created_at` / `last_seen_at` / `reauthenticated_at` | timestamptz | 否 | `now()` | 创建、最近活动、最近密码验证时间 |
| `auth_revision` | bigint | 否 | `0` | 必须匹配用户当前认证修订号 |
| `user_agent` | text | 否 | `''` | 浏览器描述，最多 512 UTF-8 字节，不用于鉴权 |

主键：`token_hash`；两个 hash 均固定 32 字节；`user_id` 引用 `web_users`，随用户删除级联。

| 索引 | 列与条件 |
| --- | --- |
| `sessions_expiry` | `(expires_at)` |
| `sessions_user` | `(user_id,created_at DESC,id)` |

## manage_setup

| 字段 | 类型 | 可空 | 默认值 | 含义 |
| --- | --- | --- | --- | --- |
| `singleton` | boolean | 否 | `true` | 固定 true，主键 |
| `token_hash` | bytea | 否 | — | 首次安装令牌的 BLAKE3，32 字节 |
| `created_at` | timestamptz | 否 | `now()` | 生成时间 |

首个用户创建后删除该行；令牌明文仅在管理 socket 旁的 0600 文件中保存。

## tasks

| 字段 | 类型 | 可空 | 默认值 | 含义 |
| --- | --- | --- | --- | --- |
| `id` | uuid | 否 | — | 维护任务 UUID |
| `kind` | text | 否 | — | purge / sweep / integrity / `pack` / unpack / upload / cache_flush |
| `bucket_id` | uuid | 是 | — | 目标桶；桶删除后为 NULL，巡检的原始范围另外保存在 detail |
| `state` | text | 否 | — | queued / running / paused / completed / failed |
| `cursor` | text | 是 | — | 最后处理对象 key、后端物理 key，或巡检的对象/范围/区块 JSON 游标 |
| `processed` | bigint | 否 | `0` | 累计处理条目数 |
| `detail` | jsonb | 否 | `'{}'` | 任务范围、预览参数、计数及少量样本 |
| `error` | text | 是 | — | 最近失败原因 |
| `created_at` | timestamptz | 否 | `now()` | 创建时间 |
| `updated_at` | timestamptz | 否 | `now()` | 最近批次/状态时间 |

主键：`id`；`bucket_id` 引用 `buckets`，目标删除时置 NULL。kind/state 受枚举约束。

| 索引 | 列与条件 |
| --- | --- |
| `tasks_list` | `(created_at DESC,id DESC)` |
| `tasks_state_list` | `(state,created_at DESC,id DESC)` |
| `tasks_work` | `(updated_at,id) WHERE state IN ('queued','running')` |
| `tasks_completed` | `(updated_at,id) WHERE state='completed'` |

## integrity_issues

仅保存异常；正常区块不逐条记录。异常与任务进度在同一事务提交。

| 字段 | 类型 | 可空 | 默认值 | 含义 |
| --- | --- | --- | --- | --- |
| `id` | bigint | 否 | `GENERATED ALWAYS AS IDENTITY` | 报告分页 ID，API 使用十进制字符串 |
| `task_id` | uuid | 否 | — | tasks 外键，ON DELETE CASCADE |
| `subject` | text | 否 | — | `chunk:ID` 或 `object:UUID:offset`，用于去重 |
| `code` | text | 否 | — | 稳定英文异常代码 |
| `chunk_id` | bigint | 是 | — | 异常区块 ID 快照 |
| `storage_id` | uuid | 是 | — | 实际异常物理区块或区块包的 UUID 快照 |
| `stream_id` | uuid | 是 | — | 异常对象版本快照 |
| `bucket_id` | uuid | 是 | — | 异常对象所属桶快照 |
| `object_key` | text | 是 | — | 原样保存的对象键 |
| `detail` | jsonb | 否 | `'{}'` | 期望值、实际值、范围位置等诊断信息 |
| `created_at` | timestamptz | 否 | `now()` | 异常记录时间 |

主键：`id`；`(task_id,subject,code)` 唯一，保证重试去重；`task_id` 引用 `tasks` 并级联删除。其他身份字段均为诊断快照，不设外键，主体删除后仍保留异常记录。应用先分批清理异常，再删除过期任务。

| 索引 | 列与条件 |
| --- | --- |
| `integrity_issues_page` | `(task_id,id)` |

## pending_uploads / cache_pins

| pending_uploads 字段 | 类型 / 默认 | 含义 |
| --- | --- | --- |
| chunk_id | bigint，主键 | 引用 chunks，逻辑块对应一份待处理缓存，物理字节只计一次 |
| stream_id / offset_bytes | uuid 可空 / bigint | 凑包顺序所属 stream 与位置；stream 删除时置空，后者非负 |
| cache_size / cache_compressed | bigint / boolean | 持久文件的实际字节和 .zst/.raw 后缀；大小必须为正 |
| source_pack | bigint 可空 | 引用原区块包；NULL 表示初次上传，非空表示暂存的复用拆包输入 |
| created_at / next_retry_at | timestamptz / now()、必填 | 最早暂存时间 / 下一次可执行时间，新增字节不重置前者 |
| attempts / last_error | integer / text 可空；0 / NULL | 跨轮重试次数和最近任务错误 |
| owner_task | uuid 可空 | 当前处理任务，任务删除时置空 |

索引：pending_uploads_due `(next_retry_at,chunk_id) WHERE owner_task IS NULL`；pending_uploads_stream `(stream_id,offset_bytes)`；非空 source_pack、owner_task 各有索引。

cache_pins 主键为 `(chunk_id,pin_type,owner_id)`：chunk_id 引用 pending_uploads 并级联删除；pin_type 为 upload/pack；owner_id 为上传 stream UUID；created_at 为 `timestamptz NOT NULL DEFAULT now()`。多个归属共同保护同一文件；实际来源提交或确认无需保留后一起解除。年龄不是删除唯一副本的依据。

## 状态与清理

| 对象 | 状态及发布规则 |
| --- | --- |
| streams / objects | writing → ready → retired；失败或启动恢复的 writing → abandoned。对象指针只指向完整 ready stream；每次写入或删除更换 write_epoch，迟到写入不能覆盖新对象；无写入版本后清理 NULL 占位 |
| chunks | preparing → uploading → ready；不确定失败转 failed。ready/failed → deleting → deleted 表示逻辑退役，物理来源另行按宽限删除；仍被未删除区块包索引需要的逻辑行继续保留 |
| uploads / parts | active → completing → completed，或 aborted。part 替换成功后才切换指针；Complete 冻结清单、发布并保存 result，相同清单重试复用结果。未提交的 completing 在重启后回到 active |
| tasks | queued → running → completed/failed；可暂停并从持久游标继续，重启将 running 重排 queued，paused 保持暂停 |

引用由 extents、owner_stream 和活跃读写保护共同决定。最后引用消失时设置 unreferenced_at，重新引用时清空；删除认领与新引用互斥，deleting 块不参与去重。已接纳的读取固定 stream，覆盖或删除对象不影响该读取。

最终对象的 extents 必须全部指向 ready 区块、偏移连续且总长正确；part 可混合区块和本地片段。片段先持久写入，再提交 sealed 与映射；缺失唯一来源会使受影响上传失效。发布和文件格式见[存储格式](storage-format.md#分片上传与发布)。

已结束记录按[清理配置](configuration.md#回收与历史清理)到期，仍被引用或活跃保护的记录暂缓。任务异常先于任务分批删除；identity 序列和密钥指纹不回收，物理区块身份不得复用。删除桶前需清除对象、上传和 stream；区块可能仍被其他桶引用。

chunks、extents、streams、objects、uploads、parts、fragments、sessions、tasks 的表级参数为 `autovacuum_vacuum_scale_factor=0.05`、`autovacuum_analyze_scale_factor=0.02`，其他阈值沿用 PostgreSQL 配置。

## JSONB 结构

| 字段 | 内容 |
| --- | --- |
| streams.metadata / uploads.metadata | 可空 HTTP 元数据：content_type、cache_control、content_disposition、content_encoding、content_language、expires；user 为 S3 自定义元数据字典 |
| streams.checksums | S3 校验算法对应值及 checksum_type，与内部 BLAKE3 去重哈希独立 |
| buckets.cors | origins/methods/headers/expose/max_age 规则数组，见[CORS 设置](manage-api-reference.md#cors-设置) |
| uploads.result | Complete 成功返回的 ETag、时间等，与 manifest_hash 一同用于幂等重试 |
| tasks.detail | purge：name/bucket_id；sweep：dry_run/prefix/older_than_seconds/min_storage_duration_seconds/cutoff/candidates/bytes/unrecognized/samples；integrity 见[巡检字段](manage-api-reference.md#完整性巡检) |

sweep 样本有界，不是可直接执行的删除清单。日常管理通过 CLI/Web 完成；不要手改状态、序列、引用或 nonce 来绕过检查。[数据库恢复](deployment-and-recovery.md#恢复步骤)还需核对历史密钥和后端身份。

## 项目授权

- `projects`：id UUID 主键、name 唯一、description、builtin、allow_bucket_create、created_at；仅一个内置项目。
- `mokyu_meta.project_management`：默认 false。
- `buckets.project_id`、`credentials.project_id`：引用项目，默认内置项目；有项目索引。
- `web_users.role`：admin/member，既有用户保留 admin，新行默认 member；`authorization_revision` 为 bigint。
- `project_members`：主键 (user_id,project_id)，role 为 reader/writer/maintainer，scope 为 all/selected。
- `member_grants`：主键 (user_id,bucket_id)，保存 project_id/actions；复合外键保证成员与桶属于同项目。
- `user_bucket_access` 视图：计算项目角色与指定桶动作交集，不复制权限状态。
- `credentials.authorization_revision`：bigint；成员/授权/身份变更通过触发器推进授权版本。
- `streams.write_authorization`：内部 JSONB，保存写入身份、授权版本及所需动作，发布前重新校验；不包含密码或令牌。
`web_users.must_change_password`：boolean NOT NULL DEFAULT false，限制账户先完成密码修改；目录索引为 `username COLLATE "C"`。用户身份和项目授权写入共享管理事务锁；数据流写入不占用此锁。
