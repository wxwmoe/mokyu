# 配置参考（0.0.2）

主配置默认为 `/config/config.toml`

可以通过 `media-gateway --config PATH` 改路径，配置在重启后生效，未知字段和错误类型会拒绝启动

镜像健康检查和 `cli` 也默认读取 `/config/config.toml`，使用其他配置路径时，CLI 调用 `media-gateway --config PATH cli ...`，并将 Compose 的 healthcheck 命令同步设为 `["CMD", "media-gateway", "--config", "PATH", "cli", "status"]`

资源限制不使用 `null` 或空字符串表示自动，**省略可选资源项**即自动预算或无该项业务配额

大小必须是正整数加 `B/KB/MB/GB/TB/KiB/MiB/GiB/TiB`，时间必须是正整数加 `s/m/h/d`。`2GB` 是 2,000,000,000 B，`2GiB` 是 2,147,483,648 B；不接受小数、`0`、`1G` 或 `auto`

## 完整字段

| 字段 | 类型 / 默认 | 含义与边界 |
| --- | --- | --- |
| `listen.s3` | string / `0.0.0.0:9000` | S3 监听 SocketAddr |
| `listen.web` | string / `0.0.0.0:9001` | 给反代的公共读和可选网站入口 |
| `listen.manage` | string / `0.0.0.0:9002` | 管理入口；三个监听地址必须不同 |
| `listen.admin_socket` | path / `/run/media-gateway/admin.sock` | CLI 的 Unix socket；权限 0600 |
| `listen.s3_domain` | optional string / 无 | 启用此域名的 virtual-hosted 寻址；否则用 path-style |
| `listen.region` | string / `us-east-1` | 客户端签名区域；与后端区域相互独立 |
| `listen.aws_chunk_limit` | size / `8MiB` | 单个 AWS 签名传输 chunk 上限；不是对象或 UploadPart 大小上限 |
| `database.host` | string / 必填 | PostgreSQL 主机或容器服务名 |
| `database.port` | u16 / 5432 | PostgreSQL 端口，不能为0 |
| `database.name` | string / 必填 | 数据库名 |
| `database.user` | string / 必填 | 数据库用户名 |
| `database.password` / `database.password_file` | string / path，二选一必填 | 直接密码或密码文本文件；不是数据库数据文件 |
| `database.ssl_mode` | string / `prefer` | disable/allow/prefer/require/verify-ca/verify-full；远程连接按部署需要验证证书 |
| `database.max_connections` | positive integer / 自动 | 业务连接池上限；另有一个独占锁连接 |
| `backend.endpoint` | string / 必填 | 带 scheme 的 S3 endpoint，默认要求 HTTPS |
| `backend.region` | string / 必填 | 后端签名区域 |
| `backend.bucket` | string / 必填 | 已存在的私有后端桶 |
| `backend.prefix` | string / `""` | 可省略；省略或空字符串使用后端桶根目录，非空时使用部署专用前缀。不允许前导 `/` 或 `.` / `..` 路径段 |
| `backend.access_key` / `backend.access_key_file` | string / path，二选一必填 | 后端 access key 或其文本文件 |
| `backend.secret_key` / `backend.secret_key_file` | string / path，二选一必填 | 后端 secret key 或其文本文件 |
| `backend.allow_http` | bool / `false` | 可信网络内可显式允许 HTTP 后端 |
| `security.credential_key_file` | path / 必填 | 64 位十六进制字符串，保护数据库内 S3 客户端 secret；与区块密钥不同 |
| `manage.origin` | string / `http://localhost:9002` | 浏览器看到的完整 origin，无末尾 `/`；登录和写操作严格匹配 |
| `manage.secure_cookie` | bool / `true` | Cookie Secure；通过 HTTP 访问时需要 false |
| `manage.session_lifetime` | duration / `12h` | 会话固定有效期，不随查询无限续期 |
| `storage.data` | path / `/data` | 唯一可写数据目录，含 multipart/chunks/gateway.lock |
| `storage.free_space_floor` | size / `1GiB` | 新文件预留后仍须保留的文件系统空间 |
| `processing.cpu_jobs` | positive integer / 自动 | 并行压缩/哈希/加解密等阻塞计算预算 |
| `processing.inflight_bytes` | size / 自动 | 在途数据槽预算，非进程 RSS 硬限制 |
| `processing.upload_concurrency` | positive integer / 自动 | 同时接收 PUT/part/完成操作的上限 |
| `processing.read_concurrency` | positive integer / 自动 | 同时流式读取上限 |
| `processing.backend_concurrency` | positive integer / 自动 | 后端请求并发上限 |
| `processing.connections` | positive integer / 自动 | 三入口合计连接上限 |
| `multipart.local_limit` | optional size / 无单独配额 | 原始尾部及预留总量；显式值至少 4MiB；始终受文件系统空余空间限制 |
| `multipart.idle_timeout` | duration / `24h` | 无有效上传进展的过期时间；ListParts 不续期 |
| `multipart.sweep_interval` | duration / `5m` | 本地过期/无引用映射清理间隔，任务完成也会唤醒清理 |
| `multipart.client_idle_timeout` | duration / `120s` | 上传相邻有效数据之间的等待上限 |
| `multipart.max_active_uploads` | optional positive bigint / 无 | 所有逻辑桶的活跃 multipart 数量配额 |
| `cache.max_size` | optional size / 无单独字节配额 | chunks 缓存实际文件字节上限；包括写入预留 |
| `cache.max_entries` | positive integer / 自动 | 缓存文件索引条目预算，ghost 元数据同样有界 |
| `cache.min_compression_savings_percent` | integer 0～100 / `20` | 压缩至少节省此百分比才保留 `.zst`，否则保存 `.raw`；0 保留所有已压缩载荷，100 全部缓存原始字节；只影响新填充 |
| `encryption.algorithm` | enum / `aes-256-gcm` | `none` / `aes-256-gcm` / `chacha20-poly1305`；只决定新写入 |
| `encryption.keyring_file` | optional path / 加密时必填 | 当前写密钥与历史读密钥 |
| `gc.unreferenced_grace` | duration / `48h` | 失去最后引用/活跃保护后的远端回收宽限 |
| `gc.interval` | duration / `30m` | 日常远端 GC 空闲检查间隔 |
| `gc.batch_size` | integer / `128` | 每批工作量，1～10000；同样用于维护任务 |
| `cleanup.interval` | duration / `5m` | 已结束记录和过期会话的后台清理间隔，启动后也执行一次 |
| `cleanup.batch_size` | integer / `1000` | 每条历史清理 DELETE 的最多行数，1～10000 |
| `cleanup.max_duration` | duration / `5s` | 每轮历史清理总时限，1～30 秒；本地引用清理也在批次间检查此预算 |
| `cleanup.deleted_chunk_retention` | duration / `7d` | 从远端删除成功后的 `deleted_at` 起保留区块日志 |
| `cleanup.upload_retention` | duration / `24h` | completed/aborted 上传记录保留时间；与活动上传的 idle_timeout 无关 |
| `cleanup.task_retention` | duration / `30d` | 已完成任务保留时间，包括 backend sweep 预览 |
| `statistics.refresh_interval` | duration / `15m` | 后台容量汇总完成后的等待间隔；页面刷新只读取快照 |
| `statistics.query_timeout` | duration / `2m` | 一轮汇总的总时限及 SQL 语句时限 |

duration 上限为 `i64::MAX / 1000` 秒；`statistics.query_timeout` 另受 PostgreSQL 限制，最多 2,147,483 秒。数量乘法溢出会拒绝。显式资源参数若与可用内存预算矛盾，也拒绝启动。

省略 `[cleanup]` 使用以上默认值。到期记录分批提交，达到时限后在后续轮次继续；锁住的行可跳过，不需要额外 crontab。历史清理在 GC 暂停及维护模式下仍运行，且不会删除远端数据。`CompleteMultipartUpload` 的重复完成结果只在上传记录保留期内提供，已发布对象不受该期限影响；清空桶会提前移除该桶的上传记录。已完成的 sweep 预览过期后需要重新预览。

省略 `[statistics]` 时使用默认间隔和时限。汇总使用只读事务，不新增业务表，失败时保留上次结果；大库可增大刷新间隔，按数据库性能调整时限。详见[管理 API](manage-api-reference.md#容量快照)。

## 自动预算

`C` 取 `/proc/self/status` 的 CPU 亲和性与 cgroup v2 CPU quota 的较小值（quota 向上取整，至少 1），亲和性读取失败使用 Rust 可用 CPU 估计，仍失败回退 1

`M` 取 `/proc/meminfo` 的 MemAvailable 与可读 cgroup v2 / 父级 `memory.max` 的较小值，内存探测失败时回退 512 MiB

默认 `inflight=M/4`，槽大小 `S=32MiB + 2×max(aws_chunk_limit−8MiB,0)`

槽数 `N=floor(inflight/S)`。至少要有两个槽，且 inflight 不得超过 `M/2`

| 预算 | 省略时计算 |
| --- | --- |
| cpu_jobs | `min(C,N)` |
| upload_concurrency | `max(1,min(C,N/2))` |
| read_concurrency | `max(1,min(2C,N))` |
| backend_concurrency | `min(4C,2N)` |
| connections | `max(32,16N)` |
| database.max_connections | `min(2C+4,64)`，池最少 0 个，闲置 60 秒释放 |
| cache.max_entries | `M/64/256` |

显式 cpu_jobs ≤ N、upload_concurrency < N、read_concurrency ≤ N、cache.max_entries ≤ M/256/8

例如 4 CPU / 1GiB 容器、默认 8MiB AWS chunk 时：8 个数据槽、4 个上传、8 个读取、16 个后端请求、12 个业务 DB 连接

磁盘空间可通过 `multipart.local_limit` 和 `cache.max_size` 分别限制

## 密钥与环境变量

```toml
active = "write-1"
[keys.write-1]
algorithm = "aes-256-gcm"
key = "REPLACE_WITH_64_HEX_CHARACTERS"
```

key ID 非空、最多128字节；不同 ID 不得使用同一实际密钥，历史 key 留在文件中供旧区块读取

改变加密算法时同时配置匹配的新 active key，`none` 可以省略 keyring，若还有旧加密区块，仍须提供相应历史密钥
