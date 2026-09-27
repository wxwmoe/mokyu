# 配置参考

主配置为 [TOML](../config.example.toml)，默认路径 `/config/config.toml`，重启后生效。未知字段、错误类型、数值溢出或与内存预算冲突的限制会拒绝启动。

使用其他路径时，统一修改服务、CLI 和健康检查：

```sh
media-gateway --config PATH serve
media-gateway --config PATH cli status
```

Compose 健康检查对应 `["CMD","media-gateway","--config","PATH","cli","status"]`。

- **省略可选资源项**表示自动预算或无该项配额，不使用 `null`、空字符串或 `auto`。
- 大小使用正整数加 `B/KB/MB/GB/TB/KiB/MiB/GiB/TiB`；`2GB` 为 2,000,000,000 字节，`2GiB` 为 2,147,483,648 字节。不接受小数、`0` 或 `1G`。
- 时间使用正整数加 `s/m/h/d`；允许零值的例外在字段表注明。通用时限最多为 `i64::MAX / 1000` 秒，个别字段有更小上限。

## 监听与管理

| 字段 | 类型 / 默认值 | 说明 |
| --- | --- | --- |
| `listen.s3` | 字符串 / `0.0.0.0:9000` | S3 监听 SocketAddr |
| `listen.web` | 字符串 / `0.0.0.0:9001` | 给反代的公共读和可选网站入口 |
| `listen.manage` | 字符串 / `0.0.0.0:9002` | 管理入口；三个监听地址必须不同 |
| `listen.admin_socket` | 路径 / `/run/media-gateway/admin.sock` | CLI 的 Unix socket；权限 0600 |
| `listen.s3_domain` | 可选字符串 / 无 | 启用此域名的 virtual-hosted 寻址；否则使用 path-style |
| `listen.region` | 字符串 / `us-east-1` | 客户端签名区域；与后端区域相互独立 |
| `listen.aws_chunk_limit` | 大小 / `8MiB` | 单个 AWS 签名传输 chunk 上限；不是对象或 UploadPart 大小上限 |
| `manage.origin` | 字符串 / `http://localhost:9002` | 浏览器看到的完整 origin，无末尾 `/`；登录和写操作严格匹配 |
| `manage.secure_cookie` | bool / `true` | Cookie Secure；通过 HTTP 访问时需要 false |
| `manage.session_lifetime` | 时间 / `12h` | 会话固定有效期，不随查询无限续期 |

## 数据库

| 字段 | 类型 / 默认值 | 说明 |
| --- | --- | --- |
| `database.host` | 字符串 / 必填 | PostgreSQL 主机或容器服务名 |
| `database.port` | u16 / 5432 | PostgreSQL 端口，不能为 0 |
| `database.name` | 字符串 / 必填 | 数据库名 |
| `database.user` | 字符串 / 必填 | 数据库用户名 |
| `database.password` / `database.password_file` | 字符串 / 路径，二选一必填 | 直接密码或密码文本文件 |
| `database.ssl_mode` | 字符串 / `prefer` | disable/allow/prefer/require/verify-ca/verify-full；远程连接按部署需要验证证书 |
| `database.max_connections` | 正整数 / 自动 | 业务连接池上限；另有一个独占锁连接 |

直接密码由 SQLx 连接参数传递，无需 URL 转义。密码与密码文件二选一，文件内容是密码文本。

## S3 后端

| 字段 | 类型 / 默认值 | 说明 |
| --- | --- | --- |
| `backend.endpoint` | 字符串 / 必填 | 带 scheme 的 S3 endpoint，默认要求 HTTPS |
| `backend.region` | 字符串 / 必填 | 后端签名区域 |
| `backend.bucket` | 字符串 / 必填 | 已存在的私有后端桶 |
| `backend.prefix` | 字符串 / `""` | 可省略；省略或空字符串使用后端桶根目录，非空时使用部署专用前缀。不允许前导 `/` 或 `.` / `..` 路径段 |
| `backend.access_key` / `backend.access_key_file` | 字符串 / 路径，二选一必填 | 后端 access key 或其文本文件 |
| `backend.secret_key` / `backend.secret_key_file` | 字符串 / 路径，二选一必填 | 后端 secret key 或其文本文件 |
| `backend.allow_http` | bool / `false` | 可信网络内可显式允许 HTTP 后端 |

endpoint、bucket 和 prefix 共同绑定部署身份；已有部署不能直接修改它们来搬迁数据。根目录和空前缀规则见[后端布局](storage-format.md#后端布局)。

## 数据目录与上传

| 字段 | 类型 / 默认值 | 说明 |
| --- | --- | --- |
| `storage.data` | 路径 / `/data` | 唯一可写数据目录，含 multipart/chunks/gateway.lock |
| `storage.free_space_floor` | 大小 / `1GiB` | 新文件预留后仍须保留的文件系统空间 |
| `multipart.local_limit` | 可选大小 / 无单独配额 | 原始尾部及预留总量；显式值至少 4 MiB；始终受文件系统空余空间限制 |
| `multipart.idle_timeout` | 时间 / `24h` | 无有效上传进展的过期时间；ListParts 不续期 |
| `multipart.sweep_interval` | 时间 / `5m` | 本地过期/无引用映射清理间隔，任务完成也会唤醒清理 |
| `multipart.client_idle_timeout` | 时间 / `120s` | 上传相邻有效数据之间的等待上限 |
| `multipart.max_active_uploads` | 可选正 bigint / 无 | 所有逻辑桶的活跃 multipart 数量配额 |

multipart 保存仍被引用的原始片段，不是可任意淘汰的缓存。目录用途见[本地数据](storage-format.md#本地数据)。

## 区块缓存

| 字段 | 类型 / 默认值 | 说明 |
| --- | --- | --- |
| `cache.max_size` | 可选大小 / 无单独字节配额 | chunks 缓存实际文件字节上限；包括写入预留 |
| `cache.max_entries` | 正整数 / 自动 | 缓存文件索引条目预算，ghost 元数据同样有界 |
| `cache.min_compression_savings_percent` | 整数 0～100 / `20` | 压缩至少节省此百分比才保留 `.zst`，否则保存 `.raw`；0 保留所有已压缩载荷，100 全部缓存原始字节；只影响新填充 |

## 处理资源

| 字段 | 类型 / 默认值 | 说明 |
| --- | --- | --- |
| `processing.cpu_jobs` | 正整数 / 自动 | 并行压缩/哈希/加解密等阻塞计算预算 |
| `processing.inflight_bytes` | 大小 / 自动 | 在途数据槽预算，非进程 RSS 硬限制 |
| `processing.upload_concurrency` | 正整数 / 自动 | PUT/part/完成操作的并发上限；完成操作持续占用名额直到后台合并结束 |
| `processing.read_concurrency` | 正整数 / 自动 | 同时流式读取上限 |
| `processing.backend_concurrency` | 正整数 / 自动 | 后端请求并发上限 |
| `processing.connections` | 正整数 / 自动 | 三入口合计连接上限 |

### 自动预算

`C` 为 CPU 亲和性与 cgroup v2 quota 的较小值（quota 向上取整，至少 1）；读取失败时使用 Rust 可用 CPU 估计，再失败回退 1。`M` 为 MemAvailable 与可读 cgroup v2／父级 memory.max 的较小值，探测失败回退 512 MiB。

默认在途预算 `inflight=M/4`。每槽预算 `S=32MiB + W + 2×max(aws_chunk_limit−8MiB,0)`，槽数 `N=floor(inflight/S)`。至少需要两个槽，且 inflight ≤ M/2。

`W` 包含两倍的 Zstd 压缩／解压上下文上界，以及 4 MiB 区块的压缩输出上界和 16 字节标签，覆盖重分配重叠及闲置缓冲。`sample` 使用不同的采样等级时，额外计入采样上下文。提高完整或采样等级可能增大 W；预算不足时可降低等级或增加内存及在途预算。启动日志和状态字段 `slot_bytes/data_slots` 显示实际值。

| 预算 | 自动值 | 显式限制 |
| --- | --- | --- |
| processing.cpu_jobs | min(C,N) | ≤ N |
| processing.upload_concurrency | max(1,min(C,N/2)) | < N |
| processing.read_concurrency | max(1,min(2C,N)) | ≤ N |
| processing.backend_concurrency | min(4C,2N) | 正整数 |
| processing.connections | max(32,16N) | 正整数 |
| database.max_connections | min(2C+4,64) | 正整数；池最少 0 个连接，闲置 60 秒释放 |
| cache.max_entries | M/64/256 | ≤ M/256/8 |

这些是内部工作预算；容器 CPU／内存硬限制由部署配置设置。本地磁盘分别用 `multipart.local_limit` 和 `cache.max_size` 限制。

## 压缩策略

| 字段 | 类型 / 默认值 | 说明 |
| --- | --- | --- |
| `compression.strategy` | 枚举 / `always` | `always` 全部试压；`sample` 抽样筛选；`file_type` 按对象类型筛选 |
| `compression.level` | 有符号整数 / `6` | 完整试压等级；支持当前 Zstd 的等级范围，包含负等级；0 使用 Zstd 默认等级 3，不表示关闭压缩 |
| `compression.sample_level` | 有符号整数 / `3` | 仅 `sample` 的采样试压使用；等级范围及 0 的含义与 `level` 相同，独立于完整试压等级 |
| `compression.min_savings_percent` | 整数 0～100 / `2` | 后端保存压缩载荷要求的最低节省比例，排除加密标签；与字节门槛同时满足 |
| `compression.min_savings_bytes` | 非负整数 / `256` | 最低节省字节数；两个门槛均为 0 时仍必须严格缩小 |
| `compression.context_idle_timeout` | 时间 / `30s` | Zstd 压缩、解压上下文及试压缓冲区的闲置保留时间；允许 `0s` 表示任务结束即释放 |
| `compression.skip_mime_types` | 字符串数组 / 内置名单 | 仅 `file_type` 使用；省略采用内置名单，显式数组替换名单，`[]` 不跳过任何类型；仅精确 MIME，忽略大小写，不接受参数或通配符 |

| 策略 | 行为 |
| --- | --- |
| `always` | 默认关闭筛选，对所有需新编码的区块完整试压 |
| `sample` | 小于 256 KiB 直接试压；其他块在头尾及中间两处抽样，任一段变小就完整试压，否则跳过 |
| `file_type` | 按对象 Content-Type 筛选；缺失、无效或为 application/octet-stream 时按扩展名补充，未知类型仍试压，不叠加抽样 |

抽样每段为块大小的 1/64，限制在 16～64 KiB；1 MiB 块最多采样 64 KiB，4 MiB 块最多采样 256 KiB。抽样使用 `sample_level`，通过筛选后按 `level` 完整试压；低等级抽样及有限窗口可能漏掉压缩收益，需要尽量保留收益时使用 `always`。将两个等级设为相同值可让抽样沿用完整试压等级。

所有策略最终都要求结果严格变小，并同时满足比例、字节门槛。去重命中直接复用；调整策略、等级和门槛不重写已有区块。multipart 使用初始化时的对象类型，UploadPart 无需重复 Content-Type；CopyObject 复用已有区块。

`file_type` 的 MIME 去参数、忽略大小写，只接受精确类型。内置跳过名单：

| 类别 | MIME |
| --- | --- |
| 图片 | image/jpeg、image/png、image/apng、image/gif、image/webp、image/avif |
| 音视频 | video/mp4、video/webm、audio/mp4、audio/mpeg、audio/aac、audio/ogg、video/ogg、application/ogg、audio/flac、audio/x-flac |
| 压缩文件 | application/zip、application/gzip、application/x-gzip、application/x-7z-compressed、application/vnd.rar、application/x-rar-compressed、application/x-xz、application/x-bzip2、application/zstd |

扩展名补充支持 jpg/jpeg/jpe、png、apng、gif、webp、avif、mp4/m4v、webm、m4a、mp3/mp2、aac、ogg/oga/opus、ogv、ogx、flac、zip、gz/tgz、7z、rar、xz、bz2、zst/zstd；也识别 svg、txt、html/htm、css、js/mjs、json、xml，供自定义名单使用。没有 `image/*` 通配规则，SVG 默认试压。

压缩／解压上下文及试压缓冲按 CPU 并发额度复用，每秒清理到期的闲置工作区；RSS 回落还取决于分配器。采样与完整试压的有效等级相同时共用压缩上下文，否则按需创建独立采样上下文，仍共用输出缓冲。区块仍各自使用独立 Zstd frame，不共享压缩历史。

## 加密与密钥

| 字段 | 类型 / 默认值 | 说明 |
| --- | --- | --- |
| `security.credential_key_file` | 路径 / 必填 | 64 个十六进制字符，保护数据库内 S3 客户端 secret；与区块密钥不同 |
| `encryption.algorithm` | 枚举 / `aes-256-gcm` | `none` / `aes-256-gcm` / `chacha20-poly1305`；只决定新写入 |
| `encryption.keyring_file` | 可选路径 / 加密时必填 | 当前写密钥与历史读密钥 |

keyring 示例：

```toml
active = "write-1"
[keys.write-1]
algorithm = "aes-256-gcm"
key = "REPLACE_WITH_64_HEX_CHARACTERS"
```

key ID 为 1～128 字节，不同 ID 必须使用不同实际密钥。更换算法时配置匹配的新 active key，历史密钥保留供读取。使用 `none` 时可省略 keyring；仍有旧加密区块时必须提供其密钥。数据库恢复的换密钥要求见[恢复步骤](deployment-and-recovery.md#恢复步骤)。

## 回收与历史清理

| 字段 | 类型 / 默认值 | 说明 |
| --- | --- | --- |
| `gc.unreferenced_grace` | 时间 / `48h` | 失去最后引用/活跃保护后的远端回收宽限 |
| `gc.interval` | 时间 / `30m` | 日常远端 GC 空闲检查间隔 |
| `gc.batch_size` | 整数 / `128` | GC、清空桶及后端清查的每批工作量，1～10000 |
| `cleanup.interval` | 时间 / `5m` | 已结束记录和过期会话的后台清理间隔，启动后也执行一次 |
| `cleanup.batch_size` | 整数 / `1000` | 每条历史清理 DELETE 的最多行数，1～10000 |
| `cleanup.max_duration` | 时间 / `5s` | 每轮历史清理总时限，1～30 秒；本地引用清理也在批次间检查此预算 |
| `cleanup.deleted_chunk_retention` | 时间 / `7d` | 从远端删除成功后的 `deleted_at` 起保留区块日志 |
| `cleanup.upload_retention` | 时间 / `24h` | completed/aborted 上传记录保留时间；与活动上传的 idle_timeout 无关 |
| `cleanup.task_retention` | 时间 / `30d` | 已完成任务及巡检异常保留时间，包括 sweep 预览 |

远端 GC 与数据库历史清理独立调度，无需 crontab。历史清理在 GC 暂停及维护模式下仍运行，不删除远端数据；到期行分批提交，达到时限后下一轮继续，被锁或仍被引用／活跃保护的行暂缓。

仅已确认远端删除的区块、completed/aborted 上传、completed 任务及过期会话按年龄清理。Complete 的幂等结果在上传记录保留期内可用；清空桶可能提前移除记录，已发布对象不受影响。sweep 预览过期后需重新预览。

## 运行统计

| 字段 | 类型 / 默认值 | 说明 |
| --- | --- | --- |
| `statistics.refresh_interval` | 时间 / `15m` | 后台容量汇总完成后的等待间隔；页面刷新只读取快照 |
| `statistics.query_timeout` | 时间 / `2m` | 一轮汇总的总时限及 SQL 语句时限 |

query_timeout 最多 2,147,483 秒。后台汇总失败时保留上次成功结果；大库可增加间隔并按数据库能力调整时限。字段、统计口径和过期标识见[容量快照](manage-api-reference.md#容量快照)。

## 完整性巡检

| 字段 | 类型 / 默认值 | 说明 |
| --- | --- | --- |
| `integrity.concurrency` | 正整数 / `1` | 区块检查并发，实际值不超过读取并发、在途槽数减一及 64；共享 CPU/后端预算 |
| `integrity.requests_per_second` | 可选正整数 / 无额外限速 | 远端巡检逻辑请求的平均速率；SDK 内部重试仍受其重试策略约束 |
| `integrity.bandwidth` | 可选大小 / 无额外限速 | full 模式平均每秒读取的编码字节，如 `10MiB`；允许一次并发批次的突发 |
| `integrity.request_timeout` | 时间 / `30s` | 单批检查总时限，1～120 秒，包含数据库、共享资源等待、SDK 重试和解码；超时保留已提交进度 |

巡检由 CLI 或 Web 手动启动。请求和带宽限额为批次间的平均限速，允许一个并发批次的突发；等待期间其他维护任务可继续。重启后，已有任务使用新的配置限额。使用方法见[CLI](cli-reference.md#完整性巡检)。
