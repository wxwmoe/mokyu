# 管理页面与 API（0.0.2）

管理端口 9002，当前所有登录用户均为部署管理员，可以浏览全部逻辑桶及私有对象

暂未提供多角色委托、Web 上传和批量编辑，删除任务由 CLI 创建，Web 查看任务及 GC 状态

## 页面和静态文件

| 方法 | 路径 | 内容 |
| --- | --- | --- |
| GET/HEAD | `/` | 单页管理：登录、桶选择、前缀目录/面包屑、分页、对象详情/ACL、媒体预览/下载、服务状态和任务 |
| GET/HEAD | `/app.js` | 原生 ES module，无前端运行时服务 |
| GET/HEAD | `/i18n.js` | zh-CN/en 字典和语言选择 |
| GET/HEAD | `/app.css` | 响应式样式 |

没有其他页面路由；目录和对象通过 query 参数传给 API，不映射成本地文件路径。静态文件随 Rust 二进制嵌入，不依赖 CDN。

首次按浏览器语言选择：中文语言使用 zh-CN，其余回退 en；右上角可切换，保存在 localStorage。日期、数字使用浏览器 Intl 格式化。对象名称、用户元数据、API字段名保持原样，不作为翻译文案或HTML执行。

## 会话与安全

登录体为JSON，限制16KiB。Cookie名 `mgw_session`，HttpOnly、SameSite=Strict，Secure由配置决定。数据库仅保存随机token哈希；会话默认12h。没有默认管理员，须先CLI创建。

POST登录要求 `Origin` 精确匹配 manage.origin；POST退出和PUT网站/CORS设置还要求 `X-CSRF-Token`。GET `/api/session` 给已登录标签页返回可用 CSRF token，支持新标签页和刷新。密码修改、禁用/删除用户会撤销旧会话。桶CORS不作用于管理端口。

所有管理 API 响应统一 `Cache-Control: private, no-store`；媒体下载另设 `Vary: Cookie`。登录最多同时2个哈希任务，失败不泄露用户名存在性。UI通过textContent/DOM API显示名称和元数据，CSP禁用外部脚本、插件和被嵌入框架。

## 方法和路径

除login外均需有效session cookie；CSRF列标“是”时同时需要Origin和X-CSRF-Token。

| 方法/路径 | query / JSON body | CSRF | 成功响应 |
| --- | --- | --- | --- |
| `POST /api/login` | body `{username,password}` | Origin | 200 `{csrf_token}`，Set-Cookie |
| `POST /api/logout` | 无 | 是 | 204，撤销会话并清cookie |
| `GET /api/session` | 无 | 否 | 200 `{id,username,csrf_token}` |
| `GET /api/status` | 无 | 否 | 200，见下方状态字段 |
| `GET /api/buckets` | 无 | 否 | 200 桶数组，按name排序，最多1000个 |
| `GET /api/buckets/{bucket}/website` | 路径bucket为UUID | 否 | 200 `{website_enabled,index_document,error_document}` |
| `PUT /api/buckets/{bucket}/website` | body `{website_enabled,index_document,error_document}`，三个字段均必填 | 是 | 200 保存后的设置；仅active桶、非维护模式可写 |
| `GET /api/buckets/{bucket}/cors` | 路径bucket为UUID | 否 | 200 CORS规则数组 |
| `PUT /api/buckets/{bucket}/cors` | body `[{origins,methods,headers,expose,max_age}]`；`[]`关闭 | 是 | 200 保存后的规则；仅active桶、非维护模式可写 |
| `GET /api/objects` | bucket=UUID 必填；prefix默认空；token可选；limit默认100，0～1000 | 否 | 200 `{objects,prefixes,next_token}` |
| `GET /api/object` | bucket=UUID、key必填 | 否 | 200 `{object,bucket_grants}` |
| `GET/HEAD /api/download` | bucket=UUID、key必填；preview默认false | 否 | 原始字节/HEAD头，支持Range与条件请求 |
| `GET /api/tasks` | 无 | 否 | 200，按created_at降序的最近100条任务 |

Axum 的 GET 路由也接受HEAD；JSON API的HEAD不返回body。前端只对实际下载使用HEAD语义。

所有query参数按UTF-8编码。`key` 保留完整字符串，不做路径规范化。分页以数据库 C 排序、桶/prefix绑定的游标继续；不使用深OFFSET。`prefixes` 是按 `/` 分组的完整目录前缀，文件与目录共用limit；下一页传入原prefix和返回的next_token。不要解析或跨桶复用token。

## 具体返回结构

桶：`{id,name,state,cors,website_enabled,index_document,error_document,created_at}`。

CORS每条规则需要非空origins/methods，headers/expose默认空数组，max_age为u32秒数、默认0；最多100条，管理请求仍受16KiB限制，非法规则返回400。methods支持GET/HEAD/POST/PUT/DELETE/OPTIONS；origins为HTTP(S)来源或`*`，headers/expose为HTTP头名或`*`。Web提供双语表单、添加/删除规则、清空和Wasabi风格预设；这些动作只修改表单，保存后生效。切换语言保留未保存内容，关闭CORS不修改对象ACL。数据库复用buckets.cors，CLI与Web共享校验和写入逻辑。

网站设置默认关闭，首页默认index.html，错误页默认404.html。index_document须为1～255 UTF-8字节的单个文件名；error_document为最多1024 UTF-8字节的桶根相对对象键，空字符串表示内置404。拒绝前导斜杠、反斜杠、控制字符和`.`/`..`路径段；无效字段返回400，未知JSON字段返回422。设置页支持zh-CN/en，切换语言保留尚未提交的表单值，保存后立即生效并持久化。

对象：`{id,bucket_id,object_key,kind,state,size,etag,metadata,public_read,checksums,created_at,touched_at}`。size为原始字节；etag不含引号；metadata的content_type/cache_control/content_disposition/content_encoding/content_language/expires/user可空。详情的bucket_grants数组为`{access_key,writable}`，仅查看授权，不暴露secret；public_read是匿名对象ACL，与桶凭据授权分别显示。

状态保留 `version`、`resources`（自动/显式生效预算）、`local_bytes:[multipart,chunks]`、`gc_paused`、`maintenance`、`active_streams`、`data_slots_available`、`db_pool_size`、`db_pool_idle`、`backend_gets/puts/deletes`、`backend_read_bytes/write_bytes`、`cache_hits/cache_hit_bytes`。

0.0.2 新增运行统计：

| 字段 | 含义 |
| --- | --- |
| `runtime.started_at/uptime_seconds` | 本次进程启动时间和运行秒数 |
| `runtime.http.s3/web/manage` | 三个 HTTP 接口的请求计数和耗时 |
| `runtime.gc_deleted/gc_failures` | 已回收区块数，以及本地清理、后台回收或单块远端删除失败次数 |
| `io.backend.get/put/delete` | 区块后端操作计数、成功传输字节数和耗时；不含 marker、清查 LIST 或 SDK 内部重试次数 |
| `io.cache_lookups/cache_hit_rate` | 区块读取次数和本地命中比例；合并等待同一次后端读取的请求仍各计一次 lookup |
| `io.cpu_slots_available/backend_slots_available` | 当前可用 CPU 处理和后端请求槽位 |
| `io.cache_limit_bytes/multipart_limit_bytes` | 显式配置的本地容量限制；未配置为 null |
| `process_memory.rss_bytes/peak_rss_bytes` | Linux 进程常驻内存和峰值；读取不可用时为 null，不等于容器含文件页缓存的内存占用 |
| `gc_running/upload_slots_available/read_slots_available` | 当前回收状态及可用读写并发槽位 |
| `storage` | 后台数据库容量快照，见下文 |

每组 HTTP/后端计数包含 `started,completed,active,failed,canceled,client_errors,server_errors,bytes,failure_rate,duration_ms`。`completed` 包括成功、失败和取消；`failed` 包括 HTTP 4xx/5xx、流错误或分片合并的延迟 XML 错误（这种错误可能使用 HTTP 200），`canceled` 为未完成便被丢弃的操作，两者互斥。`failure_rate=(failed+canceled)/completed`；`client_errors/server_errors` 按已产生的 HTTP 状态分别计数，后端组的这两项为 0。HTTP bytes 为交给 HTTP 层的响应数据字节，不含请求体、头部或网络开销，也不保证客户端已接收。

`duration_ms` 含 `mean,p50,p95,p99`。HTTP 从接收完整请求头计时，直到响应体结束、产生错误或被取消；HEAD/空响应在生成响应时结束。后端耗时从取得并发槽位开始，GET 包含读取整个区块，包含 SDK 重试等待，不含解码和缓存写入。分位数为固定直方图的近似上界（1ms 起按两倍递增）；无样本或分位数落入超出 2^23ms 的溢出桶时为 null。计数在进程内累计，重启归零；并发读取快照可能有瞬时差异，不提供历史时序。

管理页面以 zh-CN/en 展示概览、HTTP/后端操作、容量节省、各桶用量和任务数量；“刷新统计”只读取最新计数及已缓存的容量快照，不触发数据库扫描。

### 容量快照

后台启动后采集一次，此后每次采集结束等待 `statistics.refresh_interval`（默认 15m）。同一轮使用 PostgreSQL 只读、可重复读事务；最多一个汇总任务、一个查询连接，禁用查询并行，每个排序/哈希节点的 work_mem 为 16MiB。总时限和语句时限受 `statistics.query_timeout`（默认 2m）约束，锁等待最多 1s。大表汇总会消耗数据库 I/O，并可能写临时文件；可增大间隔，按数据库性能调整时限。

`storage` 包含 `snapshot,collecting,last_attempt_at,last_error,age_seconds,stale,refresh_interval_seconds`。首次完成前 snapshot 为 null。失败时保留上次成功快照，last_error 为 `refresh_failed`；缺少快照、最近刷新失败，或快照年龄超过两倍间隔时 stale 为 true。页面显示采集时间和过期状态，统计失败不阻止网关提供对象服务。

`snapshot` 的字段及口径：

| 字段 | 口径 |
| --- | --- |
| `as_of/collected_at` | 数据库一致性快照时间 / 汇总完成时间 |
| `objects/logical_bytes` | objects 当前可见版本的数量 / 原始字节总和，不含旧版本、未完成分片 |
| `buckets` | `{id,name,objects,logical_bytes}` 数组，包含空桶，按 UUID 排序，最多 1000 桶；`buckets_truncated` 表示截断，全局总数包含全部桶 |
| `chunks.states` | 各状态区块行数，包括保留的 deleted 元数据 |
| `chunks.stored_bytes` | ready/deleting 区块的已记录编码大小，含压缩及认证标签，包含等待回收的数据 |
| `chunks.unconfirmed_bytes` | uploading/failed 区块的已记录编码大小，远端是否存在尚不确定 |
| `live` | 当前可见对象引用的区块：`chunks` 为唯一块数，`reference_bytes` 为引用区间总长，`raw_bytes` 为唯一块原始大小，`stored_bytes` 为编码大小，`payload_bytes` 为编码大小减去 AEAD 标签（每块 16 字节，none 为 0） |
| `unreferenced` | 无 extent 引用且标记无引用的 ready/failed/deleting 区块数和 `stored_bytes`；`eligible_chunks/eligible_bytes` 进一步要求已过 GC 宽限且没有 owner_stream，不代表 GC 已执行或所有可回收对象的精确数量 |
| `tasks/uploads` | 按状态汇总的任务数量 / active、completing 分片上传数量 |

去重节省量为 `live.reference_bytes-live.raw_bytes`，比例以 reference_bytes 为分母；压缩节省量为 `live.raw_bytes-live.payload_bytes`，比例以 raw_bytes 为分母。分母为 0 时显示空值。共享区块只在全局计一次，不按桶分摊物理空间；部分区间引用整块时，去重节省量可能为负，保留实际计算结果。物理统计来自数据库，不遍历后端，不包含未索引对象、meta.json、提供商版本或账单规则；未确认上传和删除期间还可能存在短暂差异。

任务字段完整对应tasks表：`id,kind,bucket_id,state,cursor,processed,detail,error,created_at,updated_at`。状态翻译只发生在界面，API仍为固定英文状态值。错误详情只对管理员显示；任务管理用CLI。

## 预览、下载与错误

preview=false始终attachment、application/octet-stream。preview=true只允许明确的JPEG/PNG/GIF/WebP/AVIF、MP4/WebM及MPEG/OGG/MP4音频MIME内联；其他类型仍附件下载。HTML/SVG/XML等主动内容不在管理同源执行；下载响应带nosniff及sandbox CSP。预览始终经过管理员会话认证，公共ACL不参与授权。UI首版显示图片/视频预览。

媒体返回200或单段Range206；条件命中可304，前提不符412，范围不满足416（Content-Range含总长）。未知对象404、未认证/CSRF/Origin失败403，繁忙503，内部错误500。业务错误JSON为`{"error":"标准HTTP原因"}`；query/JSON格式错误由框架返回400/422，错误类型以HTTP状态为准。后端后续区块损坏可能使已开始的下载断流，但不会发送损坏块明文。

当前管理目录数量/任务视图有上述单次展示边界，对象库存本身通过分页浏览，无累计对象数量上限。
