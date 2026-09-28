# 管理页面与 API

管理端口默认 9002。所有登录用户均为部署管理员，可访问全部桶和私有对象。管理员由 [CLI](cli-reference.md#凭据与用户) 创建，无默认账户。

导航：[会话](#会话) · [存储桶](#存储桶) · [对象](#对象) · [运行状态](#运行状态) · [后台任务](#后台任务) · [完整性巡检](#完整性巡检) · [页面](#管理页面)

## 通用约定

- 除登录外，API 均需有效会话 Cookie。登录要求 `Origin` 精确匹配 `manage.origin`；其他写请求还要求 `X-CSRF-Token`。桶 CORS 不作用于管理端口。
- Cookie 为 `mgw_session`，HttpOnly、SameSite=Strict；Secure 和固定有效期由[配置](configuration.md#监听与管理)决定。更改密码、禁用或删除用户会撤销会话。
- JSON 请求体上限 16 KiB，批量对象操作另有说明。GET 路由也接受 HEAD，HEAD 不返回响应体。
- 查询参数按 UTF-8 编码；key 原样保留，不规范化斜杠、空格或路径。分页 token 不应解析或跨范围复用；并发变更期间不提供跨请求快照。
- 响应使用 `Cache-Control: private, no-store`，下载另带 `Vary: Cookie`；页面 CSP 限制外部脚本、插件和被嵌入。失败可用响应头 `X-Request-ID` 排查，见[请求标识](s3-compatibility.md#请求标识)。

业务错误返回 `{"error":"标准 HTTP 原因"}`；格式错误由框架返回，调用方应以 HTTP 状态判断。

| 状态 | 常见原因 |
| --- | --- |
| 400 / 422 | 参数、查询或 JSON 无效 |
| 403 | 会话、Origin 或 CSRF 校验失败 |
| 404 | 桶、对象或任务不存在 |
| 409 | 状态冲突、桶正在清除或不允许的任务操作 |
| 412 | 对象版本已变更或条件不满足 |
| 413 | 请求体超限 |
| 503 / 500 | 繁忙、维护限制 / 内部错误 |

## 会话

| 方法与路径 | 输入 | 成功响应 |
| --- | --- | --- |
| `POST /api/login` | `{username,password}`，需 Origin | 200 `{csrf_token}`，设置 Cookie |
| `POST /api/logout` | 无 | 204，撤销会话并清除 Cookie |
| `GET /api/session` | 无 | 200 `{id,username,csrf_token}` |

新标签页和刷新后可通过 session 取得 CSRF token。登录最多同时进行两个密码哈希任务，失败不泄露用户名是否存在。

## 存储桶

| 方法与路径 | 输入 | 成功响应 |
| --- | --- | --- |
| `GET /api/buckets` | 无 | 200 桶数组，按 name 排序，最多 1000 个 |
| `GET /api/buckets/{bucket}/website` | bucket 为 UUID | 200 网站设置 |
| `PUT /api/buckets/{bucket}/website` | `{website_enabled,index_document,error_document}`，均必填 | 200 保存后的设置 |
| `GET /api/buckets/{bucket}/cors` | bucket 为 UUID | 200 CORS 规则数组 |
| `PUT /api/buckets/{bucket}/cors` | 规则数组，`[]` 关闭 | 200 保存后的规则 |

桶信息：`{id,name,state,cors,website_enabled,index_document,error_document,created_at}`。设置仅在桶 active 且非维护模式时可写；创建、删除和清空桶使用 CLI。

### 网站设置

默认关闭，首页为 `index.html`、错误页为 `404.html`。

| 字段 | 约束 |
| --- | --- |
| website_enabled | bool |
| index_document | 单个文件名，1～255 UTF-8 字节 |
| error_document | 桶根相对对象键，最多 1024 UTF-8 字节；空字符串使用内置 404 |

路径拒绝前导斜杠、反斜杠、控制字符和 `.`／`..` 路径段；无效值返回 400，未知 JSON 字段返回 422。保存立即生效，路由规则见[公共网站](s3-compatibility.md#公共网站)。

### CORS 设置

每桶最多 100 条规则，同时作用于 S3 和公共读入口，不改变 ACL。

| 字段 | 类型与默认值 | 约束 |
| --- | --- | --- |
| origins | 必填字符串数组 | 非空，HTTP(S) origin 或 `*` |
| methods | 必填字符串数组 | 非空，GET/HEAD/POST/PUT/DELETE/OPTIONS |
| headers | 字符串数组，默认 `[]` | HTTP 头名或 `*` |
| expose | 字符串数组，默认 `[]` | HTTP 头名或 `*` |
| max_age | u32，默认 0 | 预检缓存秒数 |

非法规则返回 400，请求仍受 16 KiB 限制。Web 支持添加、删除、清空和填入 Wasabi 风格预设，保存后生效；匹配行为和预设内容见[S3 CORS](s3-compatibility.md#cors)。

## 对象

| 方法与路径 | 输入 | 成功响应 |
| --- | --- | --- |
| `GET /api/objects` | bucket=UUID；prefix 默认空，recursive 默认 false，token 可选，limit 默认 100、范围 0～1000 | 200 `{objects,prefixes,next_token}` |
| `GET /api/object` | bucket=UUID、key | 200 `{object,bucket_grants}` |
| `GET /api/object/chunks` | bucket、key、version=对象 UUID；after 可选，limit 默认 100、范围 1～200 | 200 `{chunks,next_offset}`；版本变更 412 |
| `POST /api/objects/actions` | `{bucket,action,objects:[{key,version}]}`；action 为 delete/private/public-read | 200 `{results:[{key,version,status}]}` |
| `GET/HEAD /api/download` | bucket、key；preview 默认 false | 原始字节或响应头，支持 Range 和条件请求 |

列表按 C 排序，游标绑定 bucket/prefix/recursive。`prefixes` 按 `/` 分组，文件和目录共用 limit；recursive=true 平铺列出匹配对象。下一页保留筛选参数并传入 next_token。

对象字段：`{id,bucket_id,object_key,kind,state,size,etag,metadata,public_read,checksums,created_at,touched_at}`。size 为原始字节，etag 不含引号；metadata 结构见[数据库](database-schema.md#jsonb-结构)。`bucket_grants` 为 `{access_key,writable}` 数组，不含 secret；凭据授权与对象 public_read 分别生效。

### 批量写操作

- objects 包含 1～1000 个不同 key，version 取对象列表／详情的 id；覆盖或删除后返回该项 412，避免操作新版本。
- 每项独立提交，允许部分成功。成功项为 200，失败项可能为 404/409/412/500/503；HTTP 200 不表示所有项成功，中断后应刷新核对结果。
- 该路由允许 2 MiB 请求体、最多两个并发请求；认证后取得名额，60 秒内未收完请求体返回 408。超限 413、繁忙 503。
- 删除走正常引用退役和 GC 流程，不同步删除共享块；ACL 不改变 S3 凭据授权。维护模式或清除中的桶拒绝写操作。
- 日志包含管理员 UUID、目标、动作、结果和 RequestId，不记录会话令牌或密钥。批次内失败也计入运行统计的失败请求。

### 区块详情

每行：`{id,offset_bytes,length,source_offset,raw_size,stored_size,independent_size_hint,payload_size,compression,algorithm,key_id,source,pack_id,reads,range_reads}`。id、offset_bytes、next_offset、pack_id 用十进制字符串表示，末页 next_offset 为 null；只读数据库，不访问后端。

source 为 chunk/pack；stored_size 是当前独立副本长度，没有独立副本时为 null。independent_size_hint、compression 和 payload_size 分别为独立编码长度提示、压缩标记和扣标签后的长度，不代表 pack 中某成员的实际占用。reads/range_reads 为延迟落库的累计块读取次数。length/source_offset 描述引用区间，不能据此直接推算删除释放空间。

### 预览与下载

preview=false 使用 attachment 和 application/octet-stream；preview=true 仅对 JPEG/PNG/GIF/WebP/AVIF、MP4/WebM 及 MPEG/OGG/MP4 音频 MIME 内联，其余仍下载。HTML/SVG/XML 不在管理同源执行；响应带 nosniff 和 sandbox CSP，始终需要管理员会话。

下载可返回 200、单段 Range 206、条件命中 304、条件不符 412 或范围无效 416（含总长）。后续区块损坏可能中止已经开始的响应，不发送损坏块明文；客户端须确认响应完整。

## 运行状态

`GET /api/status` 返回 200，与 `cli status` 相同。运行计数在进程内累计、重启归零；容量统计异步采集，读取此接口不会扫描数据库。

| 字段 | 含义 |
| --- | --- |
| version / resources | 程序版本 / 生效的[资源预算](configuration.md#自动预算) |
| local_bytes | `[multipart,chunks]` 本地占用 |
| gc_paused / maintenance / gc_running | GC 暂停、维护模式、GC 是否运行 |
| active_streams / data_slots_available | 活跃 stream 数 / 可用在途数据槽 |
| upload_slots_available / read_slots_available | 可用读写并发名额 |
| db_pool_size / db_pool_idle | 连接池总数 / 闲置连接 |
| backend_gets/puts/deletes | 后端区块操作累计次数 |
| backend_read_bytes / backend_write_bytes | 后端成功读取／写入的编码字节 |
| cache_hits / cache_hit_bytes | 缓存命中数 / 实际读取的 .raw 或 .zst 文件字节 |
| runtime.started_at / uptime_seconds | 进程启动时间 / 运行秒数 |
| runtime.http.s3/web/manage | 各 HTTP 入口的请求计数和耗时 |
| runtime.gc_deleted / gc_failures | 完成逻辑退役的区块数 / 本地清理、后台回收或物理删除失败次数 |
| io.backend.get/put/delete/head | 后端区块操作计数、成功传输字节和耗时，不含 marker、LIST 和 SDK 内部重试次数 |
| io.cache_lookups / cache_hit_rate | 区块读取次数 / 缓存命中比例；合并等待同次回源的请求各计一次 lookup |
| io.cpu_slots_available | 可用 CPU 名额 |
| io.backend_queues.read/upload/control | 各方向的 limit/running、queued（foreground/upload/maintenance）及 oldest_wait_seconds |
| io.cache_limit_bytes / multipart_limit_bytes | 配置的本地字节配额，未配置为 null |
| process_memory.rss_bytes / peak_rss_bytes | Linux RSS / 峰值，读取失败为 null；不是包含文件页缓存的容器内存 |
| cleanup | 历史清理状态，见下文 |
| storage | 后台容量快照，见下文 |

### 计数与耗时

每组 HTTP／后端统计包含 `started,completed,active,failed,canceled,client_errors,server_errors,bytes,failure_rate,duration_ms`。

- completed 包括成功、失败和取消；failed 包括 HTTP 4xx/5xx、流错误及 HTTP 200 中的延迟 XML 错误，与 canceled 互斥。`failure_rate=(failed+canceled)/completed`。
- client_errors/server_errors 按已产生的 HTTP 状态计数，后端组两项为 0。HTTP bytes 是交给 HTTP 层的响应体字节，不含请求、头部及网络开销，不保证客户端已收到。
- HTTP 耗时从收到完整请求头到响应结束／错误／取消；HEAD 和空响应在生成时结束。后端耗时从取得名额开始，GET 包含整个区块读取及 SDK 重试，不含解码或缓存写入。
- duration_ms 含 mean/p50/p95/p99。分位数为从 1 ms 起倍增的直方图近似上界；无样本或超过 2^23 ms 的溢出桶为 null。并发快照可能有瞬时差异，不保存历史时序。

### 清理结果

`cleanup`：`{running,interval,batch_size,max_duration,deleted_chunk_retention,upload_retention,task_retention,last_run}`，与 `cli cleanup status` 一致。

last_run 首次执行前为 null，此后为 `{started_at,finished_at,duration_ms,deleted,batches,budget_exhausted,last_error}`。deleted 统计 chunks/chunk_locations/packs/uploads/tasks/sessions/integrity_issues 已提交的删除行数；batches 包含删除零行的批次。时限耗尽后保留已提交结果、回滚未完成事务，后续轮次继续；失败时 last_error 为 cleanup_failed，详情见日志。该结果不包含本地 fragment 文件和 extent 清理。

### 容量快照

后台在启动后采集一次，之后按[统计配置](configuration.md#运行统计)调度。每轮使用只读、可重复读事务，最多一个汇总任务及连接，禁用查询并行，work_mem 为每节点 16 MiB，锁等待最多 1 秒；总时限及语句时限由 query_timeout 控制。大表汇总可能产生数据库 I/O 和临时文件。

`storage`：`{snapshot,collecting,last_attempt_at,last_error,age_seconds,stale,refresh_interval_seconds}`。首次成功前 snapshot 为 null；失败保留上次快照，last_error 为 refresh_failed。缺少快照、刷新失败或年龄超过两倍间隔时 stale=true，统计失败不阻止对象服务。

| snapshot 字段 | 口径 |
| --- | --- |
| as_of / collected_at | 数据库一致性快照时间 / 汇总完成时间 |
| objects / logical_bytes | 当前可见对象数量 / 原始字节总和，不含旧版本和未完成分片 |
| buckets / buckets_truncated | `{id,name,objects,logical_bytes}` 数组，含空桶，按 UUID 排序，最多 1000 桶；截断时全局总数仍包含全部桶 |
| chunks.states | 各状态区块行数，包括保留的 deleted 元数据 |
| chunks.stored_bytes | ready/retired/deleting 物理来源的编码大小，包含过渡副本与待回收数据 |
| chunks.unconfirmed_bytes | preparing/uploading 物理来源已记录的编码大小，远端是否存在尚不确定 |
| physical | 按 kind（chunk/pack）、state 分组的 objects/stored_bytes |
| live | 可见对象引用的唯一块数 chunks、引用区间总长 reference_bytes、唯一块原始大小 raw_bytes、编码大小 stored_bytes、扣标签后的 payload_bytes |
| unreferenced | chunks/eligible_chunks 为无 extent 引用的逻辑块数及过宽限、无 owner_stream 的数量；stored_bytes 为无引用独立来源及退役物理来源字节，eligible_bytes 按物理来源宽限筛选。部分闲置 pack 的剩余占用在 physical 中，实际回收还受引用和活跃保护约束 |
| tasks / uploads | 按状态计数的任务 / active、completing 上传 |
| cleanup | chunks/uploads/tasks/sessions/integrity_issues 到期历史的 `{eligible,oldest_at}`；时间分别为 deleted_at/touched_at/updated_at/expires_at，巡检异常使用所属任务 updated_at；排除仍有 extent 的块和仍有 part 的上传，可能含被锁或活跃保护暂缓的行 |
| database | `{table,total_bytes,index_bytes,live_rows_estimate,dead_rows_estimate,last_autovacuum,last_autoanalyze}`；大小含索引和 TOAST，行数为估计，维护时间可为 null |

去重节省量为 `live.reference_bytes-live.raw_bytes`；编码节省量为 `live.raw_bytes-live.payload_bytes`。物理大小按唯一可见来源计数：一个 pack 即使只剩部分成员仍在使用，也计入整个包；过渡副本另外计入 physical。每个加密物理载荷扣 16 字节标签，none 为 0；分母为 0 时比例为 null。部分引用、索引开销可使节省为负，不按桶分摊共享来源。

物理统计来自数据库，不遍历后端，不包含未索引对象、meta.json、提供商对象版本或账单规则；上传和删除期间可能短暂不一致。

## Pack

| 方法与路径 | 输入 | 成功响应 |
| --- | --- | --- |
| `GET /api/packs` | after 默认 0；limit 默认 100，1～200 | `{status,packs,next_after}`，按 ID 升序 |
| `GET /api/packs/{id}` | 正 bigint 十进制 ID | `{pack,members}`，成员含逻辑区块及当前映射标志 |
| `POST /api/packs/run` | `{kind}`：pack/reuse/reclaim/repack | `{task_id}`，已有同类任务可返回 existing=true |
| `POST /api/packs/unpack` | `{pack_id?:字符串,all?:bool,execute?:bool}` | 默认返回 preview/packs/raw_bytes/effect；execute=true 返回 task_id |

pack_id 与 all=true 必须二选一；全部拆包要求 pack.enabled=false。包及成员的大整数 ID 使用字符串，避免浏览器精度损失。Pack 页面可查看列表、成员、维护入口与拆包预览；写接口遵守会话、Origin、CSRF 和维护模式。操作、冷却和回收语义见[CLI](cli-reference.md#pack-维护)。

任务 detail.last_rewrite 保存最近一次成功切换的 before_bytes/output_bytes/temporary_added_bytes/transition_bytes，分别为旧布局、新布局、本批新增载荷及新旧并存大小；不包含更早批次仍在 GC 宽限内的副本，部署总占用以 physical 汇总为准。

## 后台任务

| 方法与路径 | 输入 | 成功响应 |
| --- | --- | --- |
| `GET /api/tasks` | state、token 可选；limit 默认 100，范围 1～200 | 200 `{tasks,next_token}`，按 created_at、id 降序 |
| `GET /api/tasks/{id}` | id 为任务 UUID | 200 任务详情 |
| `POST /api/tasks/{id}/actions` | `{action:"pause"或"resume"}` | 200 `{task_id,state}`，非法转换 409 |

任务字段：`{id,kind,bucket_id,state,cursor,processed,detail,error,created_at,updated_at}`。state 为 queued/running/paused/completed/failed，游标绑定筛选状态。API 状态值使用英语，界面负责翻译。

暂停／继续的适用状态及维护要求见[CLI 任务语义](cli-reference.md#后台任务)。整桶清除与后端清查通过 CLI 创建，Web 可控制已有任务；巡检可直接通过下列 API 创建。

## 完整性巡检

| 方法与路径 | 输入 | 成功响应 |
| --- | --- | --- |
| `POST /api/integrity` | `{mode?,bucket?,key?}`，mode 默认 metadata，可选 head/full；bucket 为名称，key 需 bucket | 200 `{task_id}` |
| `GET /api/tasks/{id}/issues` | after 默认 0，limit 默认 100，范围 1～200 | 200 `{issues,next_after}`；游标为十进制字符串或 null |
| `GET /api/tasks/{id}/issues/{issue}/objects` | after 可选，为 URL 编码的 JSON `[bucket UUID,key]` | 200 `{objects,next}`，每页 100 项 |
| `GET /api/tasks/{id}/report` | 仅已完成巡检 | JSONL 附件；其他状态 409，非巡检任务 404 |

创建请求拒绝未知字段／模式及长度不在 1～1024 UTF-8 字节内的 key；桶或精确对象不存在返回 404。最多一个 queued/running 巡检，冲突返回 409；暂停后可启动另一个，继续时仍检查此限制。模式区别见[巡检命令](cli-reference.md#完整性巡检)。

### 进度与覆盖范围

kind 为 integrity，detail 包含：

| 字段 | 含义 |
| --- | --- |
| mode / phase | 模式 / metadata、chunks、done |
| bucket_id / bucket / key | 保存的巡检范围 |
| upper_chunk_id / upper_object | 创建时的区块 ID 上界（十进制字符串）/ 对象上界 `[bucket UUID,key]` 或 null |
| objects_checked / chunks_checked / bytes_checked | 已提交批次的对象数 / 区块数 / 下载编码字节 |
| issues / skipped / finished_at | 异常数 / 因变更跳过数 / 完成时间 |

processed 为对象和区块检查数之和，不是百分比；cursor 是内部 JSON 字符串，可保存对象中途的进度。未提交批次及中断下载的流量不计入 bytes_checked，应结合运行统计判断。

巡检在线检查保存上界内、仍由范围内已发布对象引用的区块。它反映一段时间窗口，不是同一时刻快照；不重算完整对象 ETag，不检查活动上传、未引用块或本地缓存。新数据可能在扫描期间进入上界内的范围；晚于上界的新对象／区块不在覆盖范围。

当前批次持有读取保护，解除引用的数据可跳过。异常与进度原子保存，重启不重复登记；权限、连接及超时等执行错误停止任务并保留进度，不记作坏块。`completed` 且 issues>0 表示完成但有异常，`failed` 表示未完成。

### 异常与报告

异常字段：`{id,task_id,subject,code,chunk_id,storage_id,stream_id,bucket_id,object_key,detail,created_at}`；bigint ID 使用十进制字符串。

| code | 含义 |
| --- | --- |
| remote_missing / length_mismatch | 远端缺失 / 编码长度异常 |
| chunk_metadata / missing_key | 区块元数据异常 / 缺少历史密钥 |
| authentication_failed / decompression_failed / hash_mismatch | 认证 / 解压 / 原始长度或哈希校验失败 |
| object_metadata / mapping_gap / mapping_source / object_length | 对象身份或状态 / 映射缺口或重叠 / 来源 / 总长度异常 |

异常保留检查时的身份和诊断信息，不因对象删除而消失。关联对象返回当前引用的 `{bucket_id,bucket,key,version}`，可包含其他桶的共享引用；已替换或删除的历史版本不在结果中。

报告为 `application/x-ndjson`，首行 `{type:"task",task:...}`，随后逐条 `{type:"issue",issue:...}`，末行 `{type:"end",issues:N}`。每次读取 100 条、不持有长事务；过期清理或中断导致内容不全时不会输出结束行。异常随已完成任务按[保留期](configuration.md#回收与历史清理)分批清理，failed/paused 不自动到期。

## 管理页面

`GET/HEAD /` 提供对象、桶设置、状态和任务四个入口；静态资源为 `/app.js`、`/i18n.js`、`/app.css`，随二进制提供，无外部前端服务。

- 支持 zh-CN/en：首次按浏览器语言选择，中文以外回退英语；选择保存在 localStorage，切换保留未提交表单。名称、元数据和错误内容始终按文本显示。
- URL 保存 page、bucket、prefix、recursive、token、key、section、state、task、taskToken，支持刷新、前进后退和复制链接。section 为 cors/website，page 为 objects/settings/status/tasks；访问仍需登录。
- 对象每页 100 项，可按前缀／完整 key 定位；勾选仅限当前页，上一页使用本标签页历史。支持图片／视频预览和原文件下载；批量操作先列出目标，再显示逐项结果。
- 任务可每 5 秒刷新，页面隐藏或离开任务页时停止；用户可关闭。暂停或终止的巡检详情停止自动轮询，可手动刷新、查看关联对象和导出报告。
- 状态页刷新只读取运行计数和已缓存的容量快照。库存通过分页浏览，不设累计对象数量上限。
