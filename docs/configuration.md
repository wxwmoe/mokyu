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
| `compression.strategy` | enum / `always` | `always` 全部试压；`sample` 抽样筛选；`file_type` 按对象类型筛选 |
| `compression.level` | signed integer / `3` | 当前 Zstd 支持的等级，包含负等级；0 使用 Zstd 默认等级，不表示关闭压缩；高等级需要更多 CPU 和工作内存 |
| `compression.min_savings_percent` | integer 0～100 / `2` | 后端保存压缩载荷要求的最低节省比例，排除加密标签；与字节门槛同时满足 |
| `compression.min_savings_bytes` | nonnegative integer / `256` | 最低节省字节数；两个门槛均为0时仍必须严格缩小 |
| `compression.context_idle_timeout` | duration / `30s` | Zstd 压缩、解压上下文及试压缓冲区的闲置保留时间；允许 `0s` 表示任务结束即释放 |
| `compression.skip_mime_types` | string array / 内置名单 | 仅 `file_type` 使用；省略采用内置名单，显式数组替换名单，`[]` 不跳过任何类型；仅精确 MIME，忽略大小写，不接受参数或通配符 |
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

默认 `inflight=M/4`，槽大小 `S=32MiB + W + 2×max(aws_chunk_limit−8MiB,0)`

`W` 为编解码工作区预算：静态链接的 Zstd 给出的单线程一次性压缩上下文上界与解压上下文大小之和的两倍，加上最大4MiB区块的压缩输出上界及16字节标签空间。压缩上下文上界随等级变化；两倍上下文预算覆盖重分配时的临时重叠，输出预算同时覆盖闲置缓冲区。该估算对输入大小保守，高等级在小内存容器中可能因不足两个槽而拒绝启动；可降低等级或增加容器内存与在途预算。

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

启动日志及运行统计中的 `slot_bytes`、`data_slots` 等字段显示当前等级下实际采用的预算。

磁盘空间可通过 `multipart.local_limit` 和 `cache.max_size` 分别限制

## 压缩策略

默认 `always` 关闭筛选，对每个需要新编码的区块进行完整试压。三个策略最终都要求压缩结果严格小于原始数据，并同时满足比例及字节门槛。已去重命中的区块直接复用；调整策略、等级或门槛不重写旧区块，也不改变读取方式。`cache.min_compression_savings_percent` 独立决定本地缓存是否保留压缩。

`sample` 对小于256KiB的区块直接试压；其余在头尾及中间两处各独立试压一段。每段长度为区块大小的1/64，限制在16～64KiB；1MiB区块最多采样64KiB，4MiB区块最多采样256KiB。任一段变小就停止采样并完整试压，只有四段均无收益时跳过。抽样与完整试压使用相同等级。局部或远距离重复可能被漏判，样本节省率不能代表全块节省率；需要尽量保留压缩收益时使用 `always`。

`file_type` 只按类型筛选，不叠加抽样。优先使用去掉参数、统一大小写后的对象 `Content-Type`；缺失、无效或为 `application/octet-stream` 时用 key 的扩展名补充。未知类型仍完整试压。普通 PUT 使用对象类型；multipart 使用初始化时的类型，包括乱序分片、边界重切、重启后继续及 Complete 尾部，不要求 UploadPart 携带相同请求头。CopyObject 直接复用已有区块，不重新编码；UploadPartCopy 尚不支持。

内置跳过名单如下；这些类型仍可能有压缩收益，是否跳过由所选策略决定：

| 类别 | MIME |
| --- | --- |
| 图片 | `image/jpeg`, `image/png`, `image/apng`, `image/gif`, `image/webp`, `image/avif` |
| 音视频 | `video/mp4`, `video/webm`, `audio/mp4`, `audio/mpeg`, `audio/aac`, `audio/ogg`, `video/ogg`, `application/ogg`, `audio/flac`, `audio/x-flac` |
| 压缩文件 | `application/zip`, `application/gzip`, `application/x-gzip`, `application/x-7z-compressed`, `application/vnd.rar`, `application/x-rar-compressed`, `application/x-xz`, `application/x-bzip2`, `application/zstd` |

扩展名补充支持 jpg/jpeg/jpe、png、apng、gif、webp、avif、mp4/m4v、webm、m4a、mp3/mp2、aac、ogg/oga/opus、ogv、ogx、flac、zip、gz/tgz、7z、rar、xz、bz2、zst/zstd；另外识别 svg、txt、html/htm、css、js/mjs、json、xml，以便自定义名单。不存在 `image/*` 之类整类跳过规则，SVG 默认仍试压。

压缩与解压上下文按需创建，共用现有 CPU 并发额度，借用期间独占使用；两类上下文分别限制保留数量，均计入上述预算。同一任务的抽样与完整试压复用临时缓冲区。采用的压缩结果直接交给加密和上传，解压结果由读取响应持有，上下文不等待网络请求完成。每块仍是独立 Zstd frame，不共享压缩历史。空闲清理每秒检查，即使没有新请求也释放到期工作区；实际进程 RSS 的回落还取决于分配器。

## 密钥与环境变量

```toml
active = "write-1"
[keys.write-1]
algorithm = "aes-256-gcm"
key = "REPLACE_WITH_64_HEX_CHARACTERS"
```

key ID 非空、最多128字节；不同 ID 不得使用同一实际密钥，历史 key 留在文件中供旧区块读取

改变加密算法时同时配置匹配的新 active key，`none` 可以省略 keyring，若还有旧加密区块，仍须提供相应历史密钥
