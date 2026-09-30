# 管理页面与 API

管理端口默认 `9002`。页面使用同源 `/api/`，接口与 CLI 复用授权、发布和维护服务。日常使用见[管理指南](management.md)；请求与响应类型以 `GET /api/openapi.json` 为准，亦可运行 `mokyu api-schema` 离线导出。`GET /api/info` 返回产品、程序版本和契约标识。

## 通用约定

- 认证使用 `mokyu_session` Cookie 或 `Authorization: Bearer TOKEN`，不能混用，令牌不能放入 URL。Cookie 为 HttpOnly、SameSite=Strict，Secure 和固定寿命见[配置](configuration.md#监听与管理)。
- Cookie 写请求要求 `Origin` 精确匹配 `manage.origin`，并提交 `X-CSRF-Token`；登录、安装引导要求 Origin。`GET /api/session` 返回 CSRF token。Bearer 不使用 CSRF，桶 CORS 不作用于管理接口。
- 登录、首次安装状态、安装提交、info 和 OpenAPI 无需已有会话，其余入口默认拒绝未认证请求。账户与 Token 自助管理仅接受 Cookie；管理员明确签发的 system Token 可调用实例管理入口。
- 敏感管理写操作要求最近五分钟验证密码；收到 `ReauthenticationRequired` 后调用 `/api/me/reauth` 再重试。system Token 不使用交互式密码验证。
- JSON 默认上限 16 KiB；桶设置 128 KiB、授权与密钥 512 KiB、对象批量操作和上传完成 2 MiB。UploadPart 按预期分片大小校验。
- key 按原始 UTF-8 传递，不折叠斜杠、空格或路径段。bigint 身份、字节量及新接口计数使用十进制字符串。游标不可解析或跨筛选范围复用；分页不保证跨请求快照。
- 私有响应为 `Cache-Control: private, no-store`。GET 支持 HEAD，HEAD 无正文。错误为 `{error,code,request_id}`，与 `X-Request-ID` 对应；不回显密码、SQL 或请求正文。

| HTTP 状态 | 含义 |
| --- | --- |
| 400 / 422 | 无效参数、查询或 JSON |
| 403 | 身份、Origin、CSRF、权限或配额限制 |
| 404 | 资源不存在或不可见 |
| 409 | 状态冲突、维护前提不满足、幂等编号冲突 |
| 412 | 对象版本或设置修订号已变化 |
| 413 | 请求体超限 |
| 503 / 504 / 500 | 繁忙或索引未就绪 / 超时 / 内部失败 |

## 会话

| 方法与路径 | 用途 |
| --- | --- |
| `POST /api/login`、`POST /api/logout` | 登录、撤销当前会话 |
| `GET /api/session`、`GET /api/me` | 身份、个人资料及能力 |
| `PUT /api/me` | 保存显示名、语言、主题及头像偏好 |
| `POST /api/me/password` | 原密码验证后改密，保留当前会话、撤销其余会话及已有个人 Token |
| `POST /api/me/reauth` | 验证密码，为敏感操作建立五分钟窗口 |
| `GET /api/me/sessions` | 最多 100 个有效会话及 `more`，不返回认证凭据 |
| `DELETE /api/me/sessions/{id}`、`DELETE /api/me/sessions` | 撤销指定会话 / 其余会话 |
| `GET /api/bootstrap`、`POST /api/setup` | 首个管理员初始化 |

`locale` 为 en/zh-CN/ja/null，`theme` 为 auto/light/dark/null。null 使用浏览器偏好。显示名最多 240 UTF-8 字节，头像邮箱最多 320 字节；头像默认关闭，启用时用规范化邮箱的 SHA-256 构造 Gravatar URL。

密码为 12～1024 UTF-8 字节，哈希与验证共用两个并行名额。密码验证和会话创建同步检查用户版本，不能用重置前的验证结果创建新会话。`last_seen_at` 最多五分钟更新一次，不延长会话寿命。

首次安装仅在没有用户时开放。服务在管理 socket 旁写入 0600 的 `setup-token`；`POST /api/setup` 接收 `{token,username,password}`，成功后立即失效并移除文件。CLI 创建首个管理员也会关闭初始化入口。

## 项目与权限

默认项目管理关闭，内置 Default 项目承载全部桶。管理员始终访问全实例；成员按项目角色和范围授权。创建额外项目或普通成员会开启项目管理；关闭前须移除额外项目、普通成员和成员关系。

| 角色 | 动作上限 |
| --- | --- |
| reader | `bucket.list`、`object.read`、`storage.inspect` |
| writer | reader + `object.write`、`object.delete`、`object.acl` |
| maintainer | writer + `bucket.settings` |

范围为 all 或 selected；selected 的逐桶 actions 与角色取交集。桶列表的 `actions` 是当前身份的有效能力。S3 与 Web 在写入发布前复核授权；撤权阻止旧请求发布，已获授权的读取正文可完成。匿名 public-read 与成员权限独立。

| 方法与路径 | 用途 |
| --- | --- |
| `GET/POST /api/projects`、`PUT/DELETE /api/projects/{id}` | 项目目录、创建、修改及删除空项目 |
| `GET/PUT /api/settings/projects` | 项目管理开关 `{enabled}` |
| `GET /api/projects/{id}/members` | 项目成员及授权 |
| `PUT/DELETE /api/projects/{id}/members/{user}` | 完整替换 `{role,scope,grants}` / 移除成员 |
| `GET/POST /api/users`、`GET/PATCH/DELETE /api/users/{id}` | 用户目录、创建、角色与状态、删除 |
| `POST /api/users/{id}/reset-password` | 重置密码及下次改密要求 |

用户、项目及成员写操作限管理员。最后一个启用的管理员不可被删除、禁用或降级。角色/状态变更和重置密码撤销会话与旧 Token；删除用户不删除项目数据或 S3 应用密钥。`must_change_password` 只允许访问个人设置并完成改密。

用户目录支持 q/role/after/limit，每页最多 100；项目与成员列表最多 1000。成员授权最多 1000 个不同桶，all 的 grants 必须为空；项目名 1～128 字节，描述最多 2000 字节。

## 应用密钥与 API Token

| 方法与路径 | 用途 |
| --- | --- |
| `GET/POST /api/credentials` | 列举、创建项目 S3 应用密钥 |
| `PUT/DELETE /api/credentials/{key}` | 修改状态、有效期、授权 / 删除 |
| `PUT /api/credentials/{key}/grants` | 完整替换逐桶 actions |
| `POST /api/credentials/{key}/rotate` | 新密钥和可选旧密钥重叠期 |
| `GET/POST /api/tokens`、`PUT/DELETE /api/tokens/{id}` | 列举、创建、更新及撤销个人管理 Token |

S3 密钥由管理员管理，属于项目，不随创建者删除；只能授权同项目桶。Token 权限与用户当前权限取交集，空 grants 没有桶权限。`system=true` 仅管理员可签发，必须空 grants，允许全实例管理；仍不能调用 Cookie 专属入口。普通 Token 无全局任务、审计和用户管理权限。

secret 仅创建或轮换时返回。Token 只存哈希；S3 secret 由 credential-key 加密。label 1～128 字节，授权最多 1000 桶。`expires_in` 为正时长、最多 3650 天：密钥省略或 null 不到期；Token 省略为 90 天、null 不到期。更新的 `keep_expiry=true` 保留原时间；轮换 overlap 为 0～30 天。最近使用最多五分钟落库一次。

## 存储桶

| 方法与路径 | 用途 |
| --- | --- |
| `GET/POST /api/buckets` | 可见桶目录 / 创建桶，目录最多 1000 项 |
| `GET /api/bucket-projects` | 当前用户可创建桶的项目及额度 |
| `GET/PUT /api/buckets/{bucket}/settings` | 带 revision 的统一设置 |
| `GET/PUT /api/buckets/{bucket}/cors`、`website` | 单独读取或替换 CORS / 网站设置 |
| `POST /api/buckets/{bucket}/purge/preview` | 空桶删除或清桶的影响预览 |
| `DELETE /api/buckets/{bucket}` | 删除空桶 |
| `POST /api/buckets/{bucket}/purge` | 清空并删除，返回 task_id |
| `POST /api/buckets/{bucket}/transfer/preview`、`transfer` | 预览 / 执行项目转移 |

桶信息含 id、name、project_id、actions、state、revision、uploads_paused、public_base_url、CORS 和网站设置。统一保存携带读取时的 revision；并发改变返回 412。成员需要 bucket.settings，只能改 CORS 与网站；域名、公共 URL 和暂停新上传限管理员。

删除、清桶和转移限管理员，执行时提交预览返回的 confirmation 与精确 confirm_name。空桶删除也要求没有残留流或上传历史。转移先暂停新上传并排空写入、multipart、额度预留，再检查目标额度、转移用量、清除旧桶授权，按目标项目范围重新计算权限；公共域名和桶限额保留，成功后恢复新上传。已开始的 multipart 在暂停期间仍可完成。

### 网站设置

默认关闭，index_document 默认 `index.html`，为 1～255 字节文件名；error_document 默认 `404.html`，为最多 1024 字节的桶内路径，空值使用内置 404。页面必须已存在且 public-read，不改变 S3 路由。域名绑定不会抢占其他桶，public_base_url 只用于生成链接，不配置 DNS/TLS。

### CORS 设置

每桶最多 100 条，同时作用于 S3 与公共读端口，不改变 ACL。字段为 origins、methods、headers、expose、max_age；origin 支持 HTTP(S) 或 `*`，methods 支持 GET/HEAD/POST/PUT/DELETE/OPTIONS，headers/expose 为头名或 `*`。`[]` 关闭。Web 可填入预设；匹配规则见 [S3 CORS](s3-compatibility.md#cors)。

## 存储额度

`GET/PUT /api/quotas/{kind}/{id}` 读取 / 完整替换 project 或 bucket 额度。上限为 byte_limit、inflight_limit、bucket_limit，非负十进制字符串或 null；null 不限，`"0"` 为零。桶只设置 byte_limit，写操作限管理员。

逻辑用量按当前对象原始大小计算，去重和压缩不抵扣。接收前预留、覆盖抵扣旧版本、发布时转为已用；在途预算另含未完成分片及并发覆盖。降低额度不删文件，不增加用量的替换仍可执行。超额返回 403 QuotaExceeded。

桶账本需 storage.inspect。项目的完整汇总只向具有全项目范围的成员显示；受限成员仍可看项目共享上限，其他汇总为 null。受限 Token 只能读取自己的桶账本。

## 媒体目录与检索

`GET /api/buckets/{bucket}/objects` 需 bucket.list，返回 objects/prefixes/next/layout/search_mode/index_ready；对象版本为 id UUID。

| 参数 | 取值 |
| --- | --- |
| prefix / recursive | 路径前缀 / 是否展开子目录 |
| q / mode / search_in | 搜索词；contains/prefix/exact；name/path |
| kind / public | image/video/audio/document/archive/other；true/false |
| min_size / max_size | 原始字节范围，含边界 |
| since / until | RFC3339 更新时间范围，含边界 |
| sort / order | name/size/modified；asc/desc |
| after / limit | 上页 next；每页 1～200，默认 100 |

前缀和完整路径区分大小写，q 接在 prefix 后；包含搜索不区分大小写，通配符按普通字符处理。无法提取连续三个文字/数字时改为路径前缀，响应明确 search_mode。无筛选的名称升序使用虚拟目录，其余为平面文件列表，不推算文件夹大小。

`GET /api/buckets/{bucket}/catalog` 返回 indexes/backfill/ready。索引未就绪可浏览目录、按完整路径或前缀查找；其余查询返回 503 CatalogBuilding。查询预算 2 秒。目录摘要与对象变更同事务更新，旧对象后台分批回填，重启可继续。

### 预览与下载

| `GET /api/buckets/{bucket}/object` 后缀 | 用途与权限 |
| --- | --- |
| 无 | 详情和可用预览类型，bucket.list |
| `/content` | 原文件；preview 控制安全内联，object.read |
| `/thumbnail` | PNG 缩略图，每次验证 object.read |
| `/text` | 最多前 64 KiB 的安全文本，object.read |

均用 key/version 绑定当前对象，覆盖后旧版本返回 412。图片、音频和视频按允许的 MIME 明确加载；HTML/SVG 不嵌入，XML 等按文本显示。下载带 nosniff、sandbox CSP 和 private/no-store；支持 Range 与条件读取。服务端缩略图受[格式、像素和内存限制](configuration.md#监听与管理)，失败保留类型卡片，不自动下载原图。

## 对象操作

`POST /api/media/actions`：bucket、action、objects；每项 `{client_id,key,version,target_key?,target_version?}`。最多 1000 项、共享两个操作名额、60 秒请求期限。HTTP 200 返回 results，须逐项检查 status/code/output_version/replayed。

| action | 权限与语义 |
| --- | --- |
| delete | object.delete；解除引用，按正常 GC 回收 |
| private / public-read | object.acl |
| copy | 源 read、目标 write；默认私有，公开副本另需目标 acl |
| move | 源 read/delete、目标 write；保留 ACL，公开对象另需目标 acl |
| metadata | 源 read/write；公开对象另需 acl，产生新版本 |

复制/移动使用 target_bucket 和逐项 target_key；替换目标须提供 target_version。源/目标变化返回逐项 412。复制仅复用引用；同项目移动按逻辑净变化记账，跨项目受目标额度约束。

client_id 是调用方 UUID。成功回执与变更同事务提交；同身份、同参数重试返回回执，参数不同返回 409 IdempotencyConflict。已确认失败的新尝试使用新 ID，未知结果保留原 ID。重试仍检查实时权限，回执按 task_retention 清理。Web 每批 20 项、选择限当前页；关闭浏览器不保留未确认的操作编号。

## 上传与传输中心

| 方法与路径 | 用途 |
| --- | --- |
| `POST /api/buckets/{bucket}/uploads` | client_id、key、file_name、size、content_type、可选 modified_at/public_read/overwrite；幂等创建 |
| `GET /api/uploads`、`GET /api/uploads/{id}` | 分页目录 / 详情及后端落盘状态 |
| `GET /api/uploads/{id}/parts` | 已接收分片及 SHA-256，每页最多 1000 |
| `PUT /api/uploads/{id}/parts/{number}` | 原始字节和 Base64 `X-Content-SHA256` |
| `POST /api/uploads/{id}/complete` | 完整有序清单 `{parts:[{number,etag,sha256}]}` |
| `DELETE /api/uploads/{id}` | 终止上传 |

上传使用现有 multipart/配额/发布流程，不下发 S3 secret。默认每片 16 MiB，随文件大小增大以符合最多 10000 片、每片最多 5 GiB。列表支持 bucket/state/source/own/after/limit，每页最多 100；来源为 web/s3。

Web 发起者才能续传自己的上传；管理员可查看和终止其他上传。刷新后重新选择原文件，并核对已接收部分的完整校验和。完成仅表示对象已发布；remote_state 区分本地已保存与后端已就绪，上传缓存恢复要求见[备份材料](deployment-and-recovery.md#备份材料)。

## 存储洞察

| 方法与路径 | 内容 |
| --- | --- |
| `GET /api/insights` | bucket 或 project 范围快照与容量历史 |
| `GET /api/insights/runtime` | 管理员可见的运行历史、本地缓存、待上传及近期任务 |
| `GET /api/storage/packs`、`GET /api/storage/packs/{id}` | 授权范围的区块包及成员分页 |
| `GET /api/storage/packs/{id}/objects` | 当前引用文件，另需 bucket.list |
| `GET /api/object/chunks` | bucket/key/version 对象区块，需 storage.inspect；after/limit 分页 |

### 容量快照

按当前 storage.inspect 权限确定范围，省略时管理员看全局、成员看全部获授权桶。成员不获得未授权桶引用、完整共享包大小、全局成员序号或密钥标识。

空间口径：R 为原始逻辑量，U 为范围内引用区间并集，D 为先项目间、再桶间均分的内容归属，A 为所选物理来源编码占用的分摊。`R-A=(R-U)+(U-D)+(D-A)`，分别为范围内去重、共享内容和编码节省；U 跨桶不能相加，节省可能为负。归属用于说明空间，不用于收费或逻辑配额。

无远端来源时计入 pending，A 和编码节省为 null。全局 physical 区分当前来源、过渡副本、待回收与未确认来源；只统计数据库索引，不含提供商对象版本、未知后端对象或账单规则。

页面读取后台快照；collecting 表示首次收集中，as_of 是实际采集时间，失败保留旧结果并显示 stale。历史采样有上限和保留期，进程重启处分段，无采样不造趋势。

## 运行状态

`GET /api/status` 与 `cli status` 相同；`GET /api/service/status` 提供页面使用的进程、连接池、资源预算、读写名额、后端 read/upload/control 队列和目录索引状态。`GET /api/service/config` 提供脱敏配置、来源和重启要求，不读取秘密文件内容。

运行计数本进程累计、重启归零：HTTP started/completed/active/failed/canceled、4xx/5xx、字节及延时；缓存命中/查找、后端请求/字节、GC 成败。失败率为 `(failed+canceled)/completed`，耗时分位数是倍增直方图的近似上界。统计不是账单或逐请求日志；Linux RSS 也不等于含页缓存的容器内存。

## 整理工作台与服务诊断

以下入口限管理员或明确签发的 system Token。

| 方法与路径 | 用途 |
| --- | --- |
| `GET /api/maintenance` | 七类策略、周期、当前/最近任务、下次检查、打包开关和排空状态 |
| `POST /api/maintenance/{kind}/actions` | pause/resume/run；kind 为 pack/reuse/reclaim/repack/range/gc/cleanup |
| `POST /api/maintenance/mode`、`pack-creation` | `{enabled}`；维护模式 / 是否允许新包生成 |
| `POST /api/maintenance/flush` | 将已有上传缓存写入后端 |
| `POST /api/maintenance/sweep`、`sweep/preview` | 只读清查 / 根据已完成扫描准备删除确认 |
| `POST /api/maintenance/unpack/preview` | 单包 `{pack_id}` 或全部 `{all:true}` 拆包预览 |
| `POST /api/maintenance/{operation}/execute` | `{preview_id,confirmation}`，operation 为 unpack/sweep |
| `POST /api/service/key-material` | 生成新的 256 位随机密钥，仅返回一次，不读取/修改密钥文件 |

暂停在工作边界生效，已经发出的请求可完成。新包开关覆盖上传与所有维护路径；全部拆包需停止生成并等 preparing 包排空，未完成的全部拆包阻止恢复生成。旧载荷仍受 GC 宽限和最低存储期保护。

执行预览绑定账号/Token 和影响范围，十分钟有效，重复请求返回同一任务。破坏性 sweep 另需维护模式、排空操作及非空索引，每次删除前再次查询数据库。CLI 对应命令和恢复步骤见[维护参考](cli-reference.md#状态与维护)。

## 后台任务

`GET /api/tasks` 支持 state/kind/bucket/actor/token、分页游标和 limit（默认 50，最多 200），返回 tasks/next_token。`GET /api/tasks/{id}` 返回状态、来源、发起者、处理计数、阻止原因及 detail。`POST /api/tasks/{id}/actions` 提交 pause/resume。

状态为 queued/running/paused/completed/failed。暂停不回滚已提交批次；重启将 running 重排，paused 保持暂停。维护前提和策略可能阻止继续。任务创建成功只表示排队，不代表完成。

## 完整性巡检

| 方法与路径 | 用途 |
| --- | --- |
| `POST /api/integrity` | `{mode?,bucket?,key?}`；metadata/head/full，key 需 bucket |
| `GET /api/tasks/{id}/issues` | after/limit/code，异常及分组，每页最多 200 |
| `GET /api/tasks/{id}/issues/{issue}/objects` | 当前关联文件，每页 100；after 为返回的游标 |
| `GET /api/tasks/{id}/report` | 已完成巡检的 JSONL 下载 |

metadata 检查对象映射与区块身份，head 增加后端存在性和长度，full 下载并认证、解压、校验 BLAKE3。pending 的本地唯一副本在所有模式下检查。巡检绕过可丢弃缓存、不修复或删除数据；不重算整文件 ETag，不覆盖活动上传或无引用块。在线扫描保存上界，期间变化可被跳过，不是瞬时一致性快照。

异常代码包含 remote_missing、length_mismatch、pending_unavailable、chunk_metadata、missing_key、authentication_failed、decompression_failed、hash_mismatch、object_metadata、mapping_gap、mapping_source、object_length。网络/权限/超时让任务 failed 并保留进度，不记作坏块。completed 仍须查看异常数。

报告首行为 `{type:"task",task:...}`，随后为 issue，完整末行为 `{type:"end",issues:N}`。中断或清理造成缺失时没有结束行。异常随已完成任务到期；failed/paused 不自动删除。

## 兼容入口

以下接口保留共同权限检查；新客户端使用上文的带类型接口。

| 路径 | 行为 |
| --- | --- |
| `GET /api/objects`、`GET /api/object` | 旧目录和对象查询，bucket/key 查询参数 |
| `POST /api/objects/actions` | delete/private/public-read，逐项版本检查，无新操作回执 |
| `GET /api/download` | bucket/key/preview 下载 |
| `GET /api/packs`、`GET /api/packs/{id}` | 管理员物理包列表与成员 |
| `POST /api/cache/flush` | 与 maintenance/flush 相同的后端落盘任务 |

页面为 Vue 静态资源，运行无需 Node.js。带内容哈希的资源可长期缓存，入口 HTML 和品牌素材需重新验证，私有 API 不缓存。三语按需加载、明暗主题与账户同步规则见[管理指南](management.md)。
