# 数据库结构

PostgreSQL 保存对象目录、权限、存储映射、任务和运行状态；载荷位于 storage 或受保护的暂存目录。本文只列出当前公开语义，完整类型、约束、索引和触发器以 [`migrations/`](../migrations) 为准。

全新安装按编号执行全部迁移，已发布迁移不可修改。单容器模式和群集模式使用同一套表；群集中的 control、worker、data 共享数据库，但每个节点保留独立 `instance.data_path` 和 `node.json`。

`stream_leases` 只用于群集的跨节点读取与写入保护。单容器通过进程内活动引用协调读写和 GC，不创建或续期 stream lease；请求取消释放引用，重启后按持久状态恢复未完成写入。`storage_permits` 也只供群集协调 storage 并发；单容器使用按 storage 和方向共享的进程内队列。

## 部署、运行设置与身份

| 表 | 关键字段 | 用途 |
| --- | --- | --- |
| `mokyu_meta` | `schema_version`、`deployment_id`、`deployment_mode`、`deployment_mode_revision`、`deployment_key_fingerprint`、`gc_paused`、`maintenance`、`project_management`、`pack_creation_paused` | 部署身份、single/cluster 模式和持久开关；不保存 deployment key 明文 |
| `runtime_settings` | `section`、`value`、`revision`、`updated_at`、`updated_by_user_id` | 类型化的 `web`、`requests`、`uploads`、`metrics`、`maintenance` 分组；Web/CLI 按 revision 更新 |
| `managed_keys` | `id`、`name`、`purpose`、`key_algorithm`、`secret_encrypted`、`fingerprint`、`created_at`、`revoked_at` | 数据库生成的 access/signing/block key；密文由 deployment key 保护，block 支持 AES-256-GCM、ChaCha20-Poly1305 和无加密 |
| `setup_tokens` | `token_hash`、`created_at`、`expires_at` | Web 首次管理员设置的一次性 token；CLI 初始化不依赖它 |
| `web_users`、`sessions`、`api_tokens` | 用户、会话和个人 API token 字段 | Web 认证、CSRF、授权修订和会话撤销 |
| `nodes` | `id`、`name`、`role`、`credential_encrypted`、`enabled`、`status`、`recovery_required`、`lease_expires_at` | control/worker/data 节点注册、凭据和状态；节点直接连接 PostgreSQL |

单容器不创建 `nodes` 记录或 `node.json`。群集节点凭据只写入本节点 `node.json`。释放节点时会清除其凭据、回收任务 lease 并允许同名节点重新注册；部署密钥丢失或改变不能作为普通升级处理。

## storage、桶与对象

| 表 | 关键字段 | 约束 |
| --- | --- | --- |
| `storages` | `id`、`name`、`type`、`path`、`endpoint_url`、`remote_bucket_name`、`region`、`prefix`、加密凭据、`identity_marker`、`block_key_id`、`enabled` | `type` 为 `local` 或 `s3`；保存连接信息时核对已有 `meta.json`，不会把空目标隐式初始化 |
| `storage_profiles` | `storage_id`、`revision`、连接/重试、cache、compression、pack、integrity、migration、并发和字节预算字段 | 每个 storage 一行类型化策略；storage 级读写/control/维护及完整性预算在群集内共享 |
| `storage_permits` | `token`、`storage_id`、`direction`、`holder_id`、`acquired_at`、`expires_at` | 短租约，按 storage 和方向限制群集总并发 |
| `buckets` | `id`、`name`、`project_id`、`storage_id`、设置和状态字段 | `storage_id` 创建时绑定且不可修改；同一 storage 内去重，跨 storage 不去重 |
| `bucket_domains` | `host`、`bucket_id` | 公共 Host 到桶的唯一映射 |
| `objects`、`streams`、`extents`、`fragments` | `object_key`、`stream_type`、`write_token`、`catalog_category`、范围映射、片段状态 | 对象目录和版本发布；完整 ready stream 才能发布；catalog_category 是文件分类，catalog_type 是 MIME type |

storage 连接和 profile 修改只改变后续写入及新请求的策略。存量载荷的加密、压缩或区块包转换必须显式启动 `storage rewrite` 或 `storage unpack` 任务。

## 逻辑区块和物理来源

| 表 | 关键字段 | 说明 |
| --- | --- | --- |
| `chunks` | `id`、`storage_id`、`hash`、`raw_size`、`standalone_size`、`encryption_algorithm`、`block_key_id`、`standalone_compressed`、`current_encoding_id`、`pack_id`、状态和回收时间 | 按 `storage_id + hash + raw_size` 去重；逻辑区块身份不因重写改变 |
| `chunk_encodings` | `encoding_id`、`chunk_id`、`location_id`、`storage_id`、`standalone_size`、`standalone_compressed`、`encryption_algorithm`、`block_key_id`、`nonce`、`encoding_fingerprint`、状态和发布时间 | 独立区块的实际物理来源；同一逻辑区块最多一个 ready 独立来源 |
| `packs`、`pack_members` | pack 的 `location_id`、`storage_id`、加密/压缩字段、`pack_fingerprint` 及成员偏移 | 区块包来源和成员映射；只收纳同一 storage 的完整区块 |
| `pack_inputs`、`pack_maintenance`、`pack_changes` | 任务输入保护、游标、generation 和下一次检查时间 | 使打包、复用、回收和 Range 优化可恢复 |

`encoding_fingerprint` 和 `pack_fingerprint` 由 key、算法、压缩及对应 profile 计算。保存 profile 不自动重写存量；rewrite 根据 fingerprint 跳过已符合策略的来源。

## 上传、暂存和配额

| 表 | 关键字段 | 用途 |
| --- | --- | --- |
| `uploads`、`parts`、`web_uploads` | 上传状态、`write_token`、清单、校验和及暂存 storage/object key | S3 multipart 和 Web 上传的幂等、分片及共享暂存 |
| `pending_uploads` | `chunk_id`、`stream_id`、`staged_size`、`source_pack_id`、重试和 lease 字段 | 尚未发布的独立来源；群集暂存对象位于 `uploads` runtime 设置选择的 storage 的 `__mokyu/multipart/` 前缀 |
| `staging_objects` | `storage_id`、`object_key`、`bytes`、`created_at` | 群集共享暂存的数据库字节账本 |
| `quota_scopes` | `scope`、`scope_id`、用量、`logical_bytes_limit`、`inflight_bytes_limit`、`bucket_count_limit`、`cached_bytes` | project/bucket 配额；NULL 表示不限，0 表示禁止新增 |
| `quota_reservations`、`quota_writes` | reservation、预留、inflight 和替换对象字段 | 接收、覆盖、发布、取消和恢复过程中的事务记账 |

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

新安装执行完整迁移链，已发布迁移不可改写。兼容条件和操作步骤见[部署与恢复](deployment-and-recovery.md#升级与数据库迁移)。当前结构以迁移执行后的结果为准，历史文件中的旧名称不代表当前字段。
