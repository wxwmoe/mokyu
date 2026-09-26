# 数据库结构（0.0.2 / schema_version=4）

0.0.2 通过 `0002_cleanup.sql` 添加清理/引用索引及表级自动维护参数，`0003_task_listing.sql` 添加任务分页索引，`0004_integrity.sql` 添加完整性巡检任务与异常表，保留现有数据。0.0.1 发布的 `0001_baseline.sql` 保持不变；运行统计、容量快照和最近清理结果仍保存在进程内。

PostgreSQL 使用同步提交与 fsync；服务独占一个数据库级 advisory lock 和 data 文件锁

## _sqlx_migrations

SQLx 管理的迁移历史表，纳入数据库备份，不应手动修改

| 字段 | 类型 | 含义 |
| --- | --- | --- |
| `version` | bigint PRIMARY KEY | 迁移编号，0.0.1基线为1 |
| `description` | text NOT NULL | 迁移描述 |
| `installed_on` | timestamptz NOT NULL DEFAULT now() | 登记时间 |
| `success` | boolean NOT NULL | 迁移是否成功 |
| `checksum` | bytea NOT NULL | SQL文件的SHA-384校验和 |
| `execution_time` | bigint NOT NULL | 执行耗时（纳秒） |

所有时间为 `timestamptz`，以 UTC 存储/传输；所有 size、offset、length 单位为字节。部署UUID由数据库生成，其他UUID由服务生成；chunks.id 与 integrity_issues.id 使用 PostgreSQL identity。`—` 表示无默认值，调用者必须提供（可空列则默认 NULL）。JSONB 是内部结构，不是允许直接写库的管理接口。

## gateway_meta

| 字段 | PostgreSQL 类型 | 可空 | 默认 / identity | 含义 |
| --- | --- | --- | --- | --- |
| `singleton` | boolean | 否 | `true` | 固定 true，保证仅一行 |
| `schema_version` | integer | 否 | — | 当前数据库结构版本4，与最近一次迁移编号一致 |
| `deployment_id` | uuid | 否 | — | 部署 UUID |
| `backend_identity` | text | 否 | — | 后端 endpoint/bucket/prefix 身份 |
| `backend_initialized` | boolean | 否 | `false` | 后端 meta.json 已完成绑定；标识丢失时不自动重建 |
| `gc_paused` | boolean | 否 | `false` | 持久远端 GC 暂停标志 |
| `maintenance` | boolean | 否 | `false` | 持久维护标志 |
| `created_at` | timestamptz | 否 | `now()` | 部署初始化时间 |

约束和索引（实际 SQL）：

```sql
CREATE TABLE gateway_meta (
    singleton boolean PRIMARY KEY DEFAULT true CHECK (singleton),
    schema_version integer NOT NULL,
    deployment_id uuid NOT NULL,
    backend_identity text NOT NULL,
    backend_initialized boolean NOT NULL DEFAULT false,
    gc_paused boolean NOT NULL DEFAULT false,
    maintenance boolean NOT NULL DEFAULT false,
    created_at timestamptz NOT NULL DEFAULT now()
);
```

## key_fingerprints

| 字段 | PostgreSQL 类型 | 可空 | 默认 / identity | 含义 |
| --- | --- | --- | --- | --- |
| `key_id` | text | 否 | — | 写入或历史密钥标识 |
| `algorithm` | text | 否 | — | 密钥算法或 credential 保护用途 |
| `fingerprint` | bytea | 否 | — | 密钥材料指纹；防止同 ID 换材料 |

约束和索引（实际 SQL）：

```sql
CREATE TABLE key_fingerprints (
    key_id text PRIMARY KEY,
    algorithm text NOT NULL,
    fingerprint bytea NOT NULL CHECK (octet_length(fingerprint) = 32)
);
```

## buckets

| 字段 | PostgreSQL 类型 | 可空 | 默认 / identity | 含义 |
| --- | --- | --- | --- | --- |
| `id` | uuid | 否 | — | 逻辑桶 UUID |
| `name` | text | 否 | — | S3 桶名，C 排序 |
| `state` | text | 否 | `'active'` | active / purging |
| `cors` | jsonb | 否 | `'[]'` | 项目 CORS 规则数组 |
| `website_enabled` | boolean | 否 | `false` | 公共 web 入口是否启用首页/404 路由 |
| `index_document` | text | 否 | `'index.html'` | 目录首页文件名，管理接口限制1～255 UTF-8字节、不含路径段 |
| `error_document` | text | 否 | `'404.html'` | 桶根相对对象键，最多1024 UTF-8字节；空字符串使用内置404 |
| `created_at` | timestamptz | 否 | `now()` | 创建时间 |

约束和索引（实际 SQL）：

```sql
CREATE TABLE buckets (
    id uuid PRIMARY KEY,
    name text COLLATE "C" NOT NULL UNIQUE,
    state text NOT NULL DEFAULT 'active' CHECK (state IN ('active','purging')),
    cors jsonb NOT NULL DEFAULT '[]',
    website_enabled boolean NOT NULL DEFAULT false,
    index_document text NOT NULL DEFAULT 'index.html',
    error_document text NOT NULL DEFAULT '404.html',
    created_at timestamptz NOT NULL DEFAULT now()
);
```

## credentials

| 字段 | PostgreSQL 类型 | 可空 | 默认 / identity | 含义 |
| --- | --- | --- | --- | --- |
| `access_key` | text | 否 | — | S3 客户端 access key |
| `secret_encrypted` | bytea | 否 | — | AES-GCM 保护的 secret：随机 nonce + ciphertext + tag |
| `enabled` | boolean | 否 | `true` | 能否认证 |
| `created_at` | timestamptz | 否 | `now()` | 创建时间 |

约束和索引（实际 SQL）：

```sql
CREATE TABLE credentials (
    access_key text PRIMARY KEY,
    secret_encrypted bytea NOT NULL,
    enabled boolean NOT NULL DEFAULT true,
    created_at timestamptz NOT NULL DEFAULT now()
);
```

## grants

| 字段 | PostgreSQL 类型 | 可空 | 默认 / identity | 含义 |
| --- | --- | --- | --- | --- |
| `access_key` | text | 否 | — | 客户端凭据 |
| `bucket_id` | uuid | 否 | — | 可访问逻辑桶 |
| `writable` | boolean | 否 | — | false 只读，true 读写 |

约束和索引（实际 SQL）：

```sql
CREATE TABLE grants (
    access_key text NOT NULL REFERENCES credentials ON DELETE CASCADE,
    bucket_id uuid NOT NULL REFERENCES buckets ON DELETE CASCADE,
    writable boolean NOT NULL,
    PRIMARY KEY (access_key,bucket_id)
);
```

## domains

| 字段 | PostgreSQL 类型 | 可空 | 默认 / identity | 含义 |
| --- | --- | --- | --- | --- |
| `host` | text | 否 | — | 小写 HTTP Host，可含端口 |
| `bucket_id` | uuid | 否 | — | 公共域名绑定桶 |

约束和索引（实际 SQL）：

```sql
CREATE TABLE domains (
    host text PRIMARY KEY,
    bucket_id uuid NOT NULL REFERENCES buckets ON DELETE CASCADE
);
```

## streams

| 字段 | PostgreSQL 类型 | 可空 | 默认 / identity | 含义 |
| --- | --- | --- | --- | --- |
| `id` | uuid | 否 | — | 不可变对象/part 版本 UUID |
| `bucket_id` | uuid | 否 | — | 所属桶 |
| `object_key` | text | 否 | — | 完整原始 key |
| `kind` | text | 否 | — | object / part |
| `state` | text | 否 | — | writing / ready / retired / abandoned |
| `size` | bigint | 否 | `0` | 原始字节总数，B |
| `etag` | text | 否 | `''` | 未带双引号的 ETag |
| `metadata` | jsonb | 否 | `'{}'` | HTTP 元数据及 user 字典 |
| `public_read` | boolean | 否 | `false` | 对象匿名读取标志 |
| `checksums` | jsonb | 否 | `'{}'` | S3 校验和值/类型 |
| `created_at` | timestamptz | 否 | `now()` | 该版本创建时间 |
| `touched_at` | timestamptz | 否 | `now()` | 发布、退役或写入进度时间 |

约束和索引（实际 SQL）：

```sql
CREATE TABLE streams (
    id uuid PRIMARY KEY,
    bucket_id uuid NOT NULL REFERENCES buckets,
    object_key text COLLATE "C" NOT NULL,
    kind text NOT NULL CHECK (kind IN ('object','part')),
    state text NOT NULL CHECK (state IN ('writing','ready','retired','abandoned')),
    size bigint NOT NULL DEFAULT 0 CHECK (size >= 0),
    etag text NOT NULL DEFAULT '',
    metadata jsonb NOT NULL DEFAULT '{}',
    public_read boolean NOT NULL DEFAULT false,
    checksums jsonb NOT NULL DEFAULT '{}',
    created_at timestamptz NOT NULL DEFAULT now(),
    touched_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX streams_cleanup ON streams(state,touched_at,id);
```

## objects

| 字段 | PostgreSQL 类型 | 可空 | 默认 / identity | 含义 |
| --- | --- | --- | --- | --- |
| `bucket_id` | uuid | 否 | — | 逻辑桶 |
| `key` | text | 否 | — | 对象 key，UTF-8 长度1～1024 B |
| `stream_id` | uuid | 是 | — | 当前可见版本；NULL 为待写/删除占位 |
| `write_epoch` | uuid | 否 | — | 当前写入资格 UUID，迟到请求不能覆盖新版本 |

约束和索引（实际 SQL）：

```sql
CREATE TABLE objects (
    bucket_id uuid NOT NULL REFERENCES buckets,
    key text COLLATE "C" NOT NULL CHECK (octet_length(key) BETWEEN 1 AND 1024),
    stream_id uuid UNIQUE REFERENCES streams,
    write_epoch uuid NOT NULL,
    PRIMARY KEY(bucket_id,key)
);
```

## chunks

| 字段 | PostgreSQL 类型 | 可空 | 默认 / identity | 含义 |
| --- | --- | --- | --- | --- |
| `id` | bigint | 否 | `GENERATED ALWAYS AS IDENTITY` | 正 bigint 自增分配；加密前提交 |
| `storage_id` | uuid | 否 | — | 永不复用的物理 UUID |
| `owner_stream` | uuid | 是 | — | 尚未转交 extent 引用时的写入保护 |
| `hash` | bytea | 否 | — | 原始明文 BLAKE3 完整32字节 |
| `raw_size` | integer | 否 | — | 明文长度，B，1～4MiB |
| `stored_size` | integer | 是 | — | 后端长度，B，含AEAD tag；编码前NULL |
| `algorithm` | text | 否 | — | none / aes-256-gcm / chacha20-poly1305 |
| `key_id` | text | 否 | — | 历史解密密钥 ID；none 时空串 |
| `compressed` | boolean | 否 | `false` | 是否使用 zstd |
| `nonce` | bytea | 是 | — | AEAD 12字节 nonce；未编码/none 时NULL |
| `format` | integer | 否 | `1` | 区块格式版本1 |
| `state` | text | 否 | — | preparing / uploading / ready / failed / deleting / deleted |
| `created_at` | timestamptz | 否 | `now()` | 分配时间，UTC日期用于nonce |
| `unreferenced_at` | timestamptz | 是 | — | 最后引用消失的时间；有引用通常NULL |
| `deleted_at` | timestamptz | 是 | — | 后端删除确认时间 |

约束和索引（实际 SQL）：

```sql
CREATE TABLE chunks (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    storage_id uuid NOT NULL UNIQUE,
    owner_stream uuid REFERENCES streams ON DELETE SET NULL,
    hash bytea NOT NULL CHECK (octet_length(hash) = 32),
    raw_size integer NOT NULL CHECK (raw_size BETWEEN 1 AND 4194304),
    stored_size integer CHECK (stored_size BETWEEN 1 AND 4194320),
    algorithm text NOT NULL CHECK (algorithm IN ('none','aes-256-gcm','chacha20-poly1305')),
    key_id text NOT NULL,
    compressed boolean NOT NULL DEFAULT false,
    nonce bytea,
    format integer NOT NULL DEFAULT 1 CHECK (format = 1),
    state text NOT NULL CHECK (state IN ('preparing','uploading','ready','failed','deleting','deleted')),
    created_at timestamptz NOT NULL DEFAULT now(),
    unreferenced_at timestamptz,
    deleted_at timestamptz,
    CHECK ((algorithm = 'none' AND nonce IS NULL AND key_id = '') OR
           (algorithm <> 'none' AND (nonce IS NULL OR octet_length(nonce) = 12) AND key_id <> ''))
);
CREATE UNIQUE INDEX chunks_dedup ON chunks(hash,raw_size,algorithm,key_id) WHERE state IN ('preparing','uploading','ready');
CREATE INDEX chunks_gc ON chunks(unreferenced_at,id) WHERE state IN ('ready','failed','deleting');
CREATE INDEX chunks_owner ON chunks(owner_stream) WHERE owner_stream IS NOT NULL;
```

## fragments

| 字段 | PostgreSQL 类型 | 可空 | 默认 / identity | 含义 |
| --- | --- | --- | --- | --- |
| `id` | uuid | 否 | — | 本地原始片段 UUID / 文件名 |
| `owner_stream` | uuid | 是 | — | 写入未发布映射前的保护 |
| `sealed` | boolean | 否 | `false` | 本地写入已完成 |
| `size` | integer | 否 | — | 原始文件长度，B，1～4MiB |
| `hash` | bytea | 否 | — | 原始片段 BLAKE3 |
| `created_at` | timestamptz | 否 | `now()` | 创建时间 |

约束和索引（实际 SQL）：

```sql
CREATE TABLE fragments (
    id uuid PRIMARY KEY,
    owner_stream uuid REFERENCES streams ON DELETE SET NULL,
    sealed boolean NOT NULL DEFAULT false,
    size integer NOT NULL CHECK (size BETWEEN 1 AND 4194304),
    hash bytea NOT NULL CHECK (octet_length(hash) = 32),
    created_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX fragments_owner ON fragments(owner_stream) WHERE owner_stream IS NOT NULL;
```

## extents

| 字段 | PostgreSQL 类型 | 可空 | 默认 / identity | 含义 |
| --- | --- | --- | --- | --- |
| `stream_id` | uuid | 否 | — | 所属对象/part版本 |
| `offset_bytes` | bigint | 否 | — | 在此版本中的起始偏移，B |
| `length` | integer | 否 | — | 映射长度，B，1～4MiB |
| `chunk_id` | bigint | 是 | — | 远端区块来源，与fragment二选一 |
| `fragment_id` | uuid | 是 | — | 唯一原始本地来源，与chunk二选一 |
| `source_offset` | integer | 否 | `0` | 在来源中的偏移，B |

约束和索引（实际 SQL）：

```sql
CREATE TABLE extents (
    stream_id uuid NOT NULL REFERENCES streams ON DELETE CASCADE,
    offset_bytes bigint NOT NULL CHECK (offset_bytes >= 0),
    length integer NOT NULL CHECK (length BETWEEN 1 AND 4194304),
    chunk_id bigint REFERENCES chunks,
    fragment_id uuid REFERENCES fragments,
    source_offset integer NOT NULL DEFAULT 0 CHECK (source_offset >= 0 AND source_offset < 4194304),
    CHECK ((chunk_id IS NULL) <> (fragment_id IS NULL)),
    CHECK (source_offset + length <= 4194304),
    PRIMARY KEY(stream_id,offset_bytes)
);
CREATE INDEX extents_chunk ON extents(chunk_id) WHERE chunk_id IS NOT NULL;
CREATE INDEX extents_fragment ON extents(fragment_id) WHERE fragment_id IS NOT NULL;
```

## uploads

| 字段 | PostgreSQL 类型 | 可空 | 默认 / identity | 含义 |
| --- | --- | --- | --- | --- |
| `id` | uuid | 否 | — | S3 UploadId UUID |
| `bucket_id` | uuid | 否 | — | 目标桶 |
| `object_key` | text | 否 | — | 目标对象key |
| `access_key` | text | 否 | — | 创建上传的身份字符串 |
| `state` | text | 否 | `'active'` | active / completing / completed / aborted |
| `metadata` | jsonb | 否 | `'{}'` | 创建时对象元数据 |
| `public_read` | boolean | 否 | `false` | 创建时ACL |
| `checksum_algorithm` | text | 是 | — | 请求的S3校验算法 |
| `checksum_type` | text | 是 | — | FULL_OBJECT / COMPOSITE |
| `manifest_hash` | text | 是 | — | 冻结Complete请求清单摘要 |
| `result` | jsonb | 是 | — | 已提交Complete返回数据，供幂等重试 |
| `output_stream` | uuid | 是 | — | 完成时构造的对象版本 |
| `created_at` | timestamptz | 否 | `now()` | 创建时间 |
| `touched_at` | timestamptz | 否 | `now()` | 有效上传/状态变更时间，决定过期 |

约束和索引（实际 SQL）：

```sql
CREATE TABLE uploads (
    id uuid PRIMARY KEY,
    bucket_id uuid NOT NULL REFERENCES buckets,
    object_key text COLLATE "C" NOT NULL,
    access_key text NOT NULL,
    state text NOT NULL DEFAULT 'active' CHECK (state IN ('active','completing','completed','aborted')),
    metadata jsonb NOT NULL DEFAULT '{}',
    public_read boolean NOT NULL DEFAULT false,
    checksum_algorithm text,
    checksum_type text,
    manifest_hash text,
    result jsonb,
    output_stream uuid REFERENCES streams ON DELETE SET NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    touched_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX uploads_expiry ON uploads(state,touched_at,id);
CREATE INDEX uploads_list ON uploads(bucket_id,object_key,id);
```

## parts

| 字段 | PostgreSQL 类型 | 可空 | 默认 / identity | 含义 |
| --- | --- | --- | --- | --- |
| `upload_id` | uuid | 否 | — | 所属上传 |
| `part_number` | integer | 否 | — | 1～10000 |
| `stream_id` | uuid | 是 | — | 当前已确认part版本；首次在写时可NULL |
| `write_epoch` | uuid | 否 | — | part替换资格 UUID；失败保持旧stream |

约束和索引（实际 SQL）：

```sql
CREATE TABLE parts (
    upload_id uuid NOT NULL REFERENCES uploads ON DELETE CASCADE,
    part_number integer NOT NULL CHECK (part_number BETWEEN 1 AND 10000),
    stream_id uuid REFERENCES streams,
    write_epoch uuid NOT NULL,
    PRIMARY KEY(upload_id,part_number)
);
```

## web_users

| 字段 | PostgreSQL 类型 | 可空 | 默认 / identity | 含义 |
| --- | --- | --- | --- | --- |
| `id` | uuid | 否 | — | Web管理员 UUID |
| `username` | text | 否 | — | 用户名 |
| `password_hash` | text | 否 | — | 带参数与salt的Argon2哈希 |
| `enabled` | boolean | 否 | `true` | 可否登录/使用会话 |
| `created_at` | timestamptz | 否 | `now()` | 创建时间 |

约束和索引（实际 SQL）：

```sql
CREATE TABLE web_users (
    id uuid PRIMARY KEY,
    username text NOT NULL UNIQUE,
    password_hash text NOT NULL,
    enabled boolean NOT NULL DEFAULT true,
    created_at timestamptz NOT NULL DEFAULT now()
);
```

## sessions

| 字段 | PostgreSQL 类型 | 可空 | 默认 / identity | 含义 |
| --- | --- | --- | --- | --- |
| `token_hash` | bytea | 否 | — | cookie token 的 BLAKE3，不存明文token |
| `user_id` | uuid | 否 | — | 所属Web用户 |
| `csrf_hash` | bytea | 否 | — | CSRF token 的BLAKE3 |
| `expires_at` | timestamptz | 否 | — | 固定到期时间 |

约束和索引（实际 SQL）：

```sql
CREATE TABLE sessions (
    token_hash bytea PRIMARY KEY CHECK (octet_length(token_hash) = 32),
    user_id uuid NOT NULL REFERENCES web_users ON DELETE CASCADE,
    csrf_hash bytea NOT NULL CHECK (octet_length(csrf_hash) = 32),
    expires_at timestamptz NOT NULL
);
CREATE INDEX sessions_expiry ON sessions(expires_at);
```

## tasks

| 字段 | PostgreSQL 类型 | 可空 | 默认 / identity | 含义 |
| --- | --- | --- | --- | --- |
| `id` | uuid | 否 | — | 维护任务 UUID |
| `kind` | text | 否 | — | purge / sweep / integrity |
| `bucket_id` | uuid | 是 | — | 目标桶；桶删除后NULL，巡检的原始范围另外保存在 detail |
| `state` | text | 否 | — | queued / running / paused / completed / failed |
| `cursor` | text | 是 | — | 最后处理对象key、后端物理key，或巡检的对象/范围/区块 JSON 游标 |
| `processed` | bigint | 否 | `0` | 累计处理条目数 |
| `detail` | jsonb | 否 | `'{}'` | 任务范围、预览参数、计数及少量样本 |
| `error` | text | 是 | — | 最近失败原因 |
| `created_at` | timestamptz | 否 | `now()` | 创建时间 |
| `updated_at` | timestamptz | 否 | `now()` | 最近批次/状态时间 |

约束和索引（实际 SQL）：

```sql
CREATE TABLE tasks (
    id uuid PRIMARY KEY,
    kind text NOT NULL CHECK (kind IN ('purge','sweep','integrity')),
    bucket_id uuid REFERENCES buckets ON DELETE SET NULL,
    state text NOT NULL CHECK (state IN ('queued','running','paused','completed','failed')),
    cursor text,
    processed bigint NOT NULL DEFAULT 0,
    detail jsonb NOT NULL DEFAULT '{}',
    error text,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX tasks_list ON tasks(created_at DESC,id DESC);
CREATE INDEX tasks_state_list ON tasks(state,created_at DESC,id DESC);
CREATE INDEX tasks_work ON tasks(updated_at,id) WHERE state IN ('queued','running');
```

## integrity_issues

仅保存巡检异常，正常区块不逐条保存检查记录。进度及异常在同一事务提交，`(task_id,subject,code)` 保证重试去重。主体身份为检查时的快照；只有 task_id 使用外键，避免正常对象/区块清理使历史异常失去依据。应用按任务保留期分批清理此表，然后删除任务。

| 字段 | PostgreSQL 类型 | 可空 | 默认 / identity | 含义 |
| --- | --- | --- | --- | --- |
| `id` | bigint | 否 | GENERATED ALWAYS AS IDENTITY | 报告分页 ID，API 使用十进制字符串 |
| `task_id` | uuid | 否 | — | tasks 外键，ON DELETE CASCADE |
| `subject` | text | 否 | — | `chunk:ID` 或 `object:UUID:offset`，用于去重 |
| `code` | text | 否 | — | 稳定英文异常代码 |
| `chunk_id` | bigint | 是 | — | 异常区块 ID 快照 |
| `storage_id` | uuid | 是 | — | 异常物理区块 UUID 快照 |
| `stream_id` | uuid | 是 | — | 异常对象版本快照 |
| `bucket_id` | uuid | 是 | — | 异常对象所属桶快照 |
| `object_key` | text | 是 | — | 原样保存的对象键 |
| `detail` | jsonb | 否 | `'{}'` | 期望值、实际值、范围位置等诊断信息 |
| `created_at` | timestamptz | 否 | `now()` | 异常记录时间 |

```sql
CREATE TABLE integrity_issues (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    task_id uuid NOT NULL REFERENCES tasks ON DELETE CASCADE,
    subject text NOT NULL,
    code text NOT NULL,
    chunk_id bigint,
    storage_id uuid,
    stream_id uuid,
    bucket_id uuid,
    object_key text,
    detail jsonb NOT NULL DEFAULT '{}',
    created_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE(task_id,subject,code)
);
CREATE INDEX integrity_issues_page ON integrity_issues(task_id,id);
```

## 状态、引用与删除

- **streams**：writing → ready（原子发布）→ retired；失败或启动恢复中的 writing → abandoned。对象当前指针只指向完整 ready 版本。GET 固定版本并持有内存读取保护，覆盖/删除不影响已接纳的流。
- **objects**：主键为桶+key，stream_id 指向可见 generation。write_epoch 每次写/删除更换，旧任务迟到不能发布。NULL 占位在没有写入版本后清理。
- **chunks**：preparing（ID已提交）→ uploading（编码元数据已落库）→ ready（后端成功并建立来源引用）；不确定失败转 failed，后续加密分配新ID/UUID。ready/failed → deleting（数据库先认领）→ deleted（后端确认删除）。deleted 日志从 deleted_at 起默认保留7天，仍有extent时不删除；不再参与去重，不得重用其物理key或identity ID。
- **extents**：每行二选一引用 chunk 或 fragment，不保存全对象大字节串。最终对象只能引用 ready chunk，offset 连续且总长正确才发布。part 可混合来源和局部偏移。
- **uploads/parts**：active → completing → completed，或 aborted。part替换只有新版本成功后切换指针。Complete 冻结清单、生成正式CDC结果，并在同一发布事务保存 result；相同清单重试返回该结果。重启时未提交 completing 恢复 active；completed 不重复发布。ListParts 不修改活动时间。
- **引用保护**：所有 extents、owner_stream、活跃读取/完成/写入保护共同决定生命周期。失去最后引用时设置 unreferenced_at；再次引用清空资格。删除认领与新引用在行锁/短协调区内核对；删除中的块不参与新去重。
- **本地片段**：先登记写入所有者，再持久写文件和目录，最后提交 sealed/范围映射。启动时核对引用片段完整性；缺失唯一来源使对应未完成上传失效，不会让残缺对象发布。
- **tasks**：queued → running → completed/failed，可暂停并从持久cursor继续；重启把 running 重排 queued。completed 从 updated_at 起默认保留30天，包括sweep预览；其他状态不按年龄清理。sweep不把缓存或某种区块状态当作不存在，删除前重新查询整个索引。

completed/aborted uploads 从 touched_at 起默认保留24小时，且仍有part或活跃保护时暂缓。过期后Complete重试返回NoSuchUpload，不影响已发布对象。过期sessions分批移除。清理不重置chunks identity序列，也不删除key_fingerprints。

## 0002 新增索引与维护参数

| 索引 | 列与条件 |
| --- | --- |
| `chunks_deleted` | `(deleted_at,id)` WHERE state='deleted' |
| `uploads_finished` | `(touched_at,id)` WHERE state IN ('completed','aborted') |
| `tasks_completed` | `(updated_at,id)` WHERE state='completed' |
| `objects_empty` | `(bucket_id,key)` WHERE stream_id IS NULL |
| `streams_writing` | `(bucket_id,object_key)` WHERE state='writing' |
| `parts_stream` | `(stream_id)` WHERE stream_id IS NOT NULL |
| `uploads_output` | `(output_stream)` WHERE output_stream IS NOT NULL |
| `fragments_cleanup` | `(created_at,id)` |

chunks、extents、streams、objects、uploads、parts、fragments、sessions、tasks 的表级 `autovacuum_vacuum_scale_factor=0.05`、`autovacuum_analyze_scale_factor=0.02`，其他阈值沿用 PostgreSQL 配置。无新增字段或业务表。

FK 默认 NO ACTION，例外均明确写在上面的 SQL：授权/域名跟桶级联、会话跟用户级联、part跟上传级联、extent跟stream级联；owner/output/task目标在对应来源移除时置NULL。删除桶前先清掉依赖对象/上传/版本，区块可被其他桶共享。

## 0003 任务分页索引

`0003_task_listing.sql` 增加 `tasks_list(created_at DESC,id DESC)` 和 `tasks_state_list(state,created_at DESC,id DESC)`，替换原 `tasks_active` 索引。任务按创建时间和 UUID 确定顺序，通过游标继续；同一时间创建的任务也不会因分页遗漏。无新增表或字段。

## JSONB 结构

- `streams.metadata` / `uploads.metadata`：content_type、cache_control、content_disposition、content_encoding、content_language、expires（可空字符串），user（S3自定义元数据字典）。
- `checksums`：S3校验算法对应值和checksum_type；不是内部BLAKE3去重哈希。
- `buckets.cors`：origins/methods/headers/expose/max_age 规则数组，见 CLI 文档。
- `uploads.result`：成功完成的 ETag、时间等返回元数据，必须与 manifest_hash 一起保留供重试。
- `tasks.detail`：purge保存name/bucket_id；sweep保存dry_run/prefix/older_than_seconds/cutoff/candidates/bytes/unrecognized/samples。样本有界，不能用作不经复核的删除清单。

数据库日常管理通过CLI完成，不建议直接改状态、序列、引用或 nonce。恢复数据库时必须连同历史密钥和后端身份核对，见部署与恢复文档。
