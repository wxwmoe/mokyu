# 管理页面与 API

管理端口默认 `9002`。页面使用同源 `/api/v1/`，接口与 CLI 复用授权、发布和维护服务。日常使用见[管理指南](management.md)；请求与响应类型以 `GET /api/v1/openapi.json` 为准，亦可运行 `mokyu tool openapi` 离线导出。`GET /api/v1/info` 返回产品、程序版本和契约标识。

## 通用约定

- 认证使用 `mokyu_session` Cookie 或 `Authorization: Bearer TOKEN`，不能混用，令牌不能放入 URL。Cookie 为 HttpOnly、SameSite=Strict，Secure 和固定寿命见[配置](configuration.md#监听与管理)。
- Cookie 写请求要求 `Origin` 精确匹配 `control.origin`，并提交 `X-CSRF-Token`；登录、安装引导要求 Origin。`GET /api/v1/auth/session` 返回 CSRF token。Bearer 不使用 CSRF，桶 CORS 不作用于管理接口。
- 登录、首次安装状态、安装提交、info 和 OpenAPI 无需已有会话，其余入口默认拒绝未认证请求。账户与 Token 自助管理仅接受 Cookie；管理员明确签发的 system Token 可调用实例管理入口。
- 敏感管理写操作要求最近五分钟验证密码；收到 `ReauthenticationRequired` 后调用 `/api/v1/account/reauth` 再重试。system Token 不使用交互式密码验证。
- JSON 默认上限 16 KiB；桶设置 128 KiB、授权与密钥 512 KiB、对象批量操作和上传完成 2 MiB。UploadPart 按预期分片大小校验。
- key 按原始 UTF-8 传递，不折叠斜杠、空格或路径段。身份使用不透明字符串，可能是 UUID 或十进制整数，客户端不得作数值转换；字节量及新接口计数使用十进制字符串。游标不可解析或跨筛选范围复用；分页不保证跨请求快照。
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

`GET /api/v1/system/status` 保留本机状态语义，另返回权威 `deployment_mode`。仅群集包含 `current_node`（可空的注册 id、配置名称及角色）；单容器为 null。`GET /api/v1/system/config` 的环境行使用 `origin=environment`、`value_mode=read`，只读且不要求重启；缺失值以 `value_state=unavailable/not_read/not_applicable/unlimited` 区分，`value` 为空。数值零不属于缺失值。从库继承的非秘密配置使用 `value_mode=inherited`；环境观察不触发实时数据库探测。

| 方法与路径 | 用途 |
| --- | --- |
| `POST /api/v1/auth/login`、`POST /api/v1/auth/logout` | 登录、撤销当前会话 |
| `POST /api/v1/auth/refresh` | 使用请求体中的 refresh token 续期当前会话并轮换必要的凭据 |
| `GET /api/v1/auth/session`、`GET /api/v1/account` | 身份、个人资料及能力 |
| `PUT /api/v1/account` | 保存显示名、语言、主题及头像偏好 |
| `POST /api/v1/account/password` | 原密码验证后改密，保留当前会话、撤销其余会话及已有个人 Token |
| `POST /api/v1/account/reauth` | 验证密码，为敏感操作建立五分钟窗口 |
| `GET /api/v1/account/sessions` | 最多 100 个有效会话及 `more`，不返回认证凭据 |
| `DELETE /api/v1/account/sessions/{id}`、`DELETE /api/v1/account/sessions` | 撤销指定会话 / 其余会话 |
| `GET /api/v1/auth/bootstrap`、`POST /api/v1/auth/setup` | 首个管理员初始化 |

`locale` 为 en/zh-CN/ja/null，`theme` 为 auto/light/dark/null。null 使用浏览器偏好。显示名最多 240 UTF-8 字节，头像邮箱最多 320 字节；头像默认关闭，启用时用规范化邮箱的 SHA-256 构造 Gravatar URL。

密码为 12～1024 UTF-8 字节，哈希与验证共用两个并行名额。密码验证和会话创建同步检查用户版本，不能用重置前的验证结果创建新会话。`last_seen_at` 最多五分钟更新一次。登录响应和 refresh 响应返回一次性 refresh token；浏览器只在认证管理器调用 refresh 时从本地存储读取并放入 `{refresh_token}`，普通请求不会携带它。服务端只保存 refresh token 的哈希，并在轮换时撤销旧值。刷新失败不会恢复被撤销、禁用或已过期的身份。

首次安装仅在没有用户时开放。使用 `mokyu cli setup token` 生成一次性 token；数据库只保存哈希和一小时有效期，`POST /api/v1/auth/setup` 接收 `{token,username,password}`，成功后立即失效。CLI 创建首个管理员也会关闭初始化入口。

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
| `GET/POST /api/v1/projects`、`PUT/DELETE /api/v1/projects/{id}` | 项目目录、创建、修改及删除空项目 |
| `GET/PUT /api/v1/runtime-settings/projects` | 项目管理开关 `{enabled}` |
| `GET /api/v1/projects/{id}/members` | 项目成员及授权 |
| `PUT/DELETE /api/v1/projects/{id}/members/{user}` | 完整替换 `{role,scope,grants}` / 移除成员 |
| `GET/POST /api/v1/users`、`GET/PATCH/DELETE /api/v1/users/{id}` | 用户目录、创建、角色与状态、删除 |
| `POST /api/v1/users/{id}/reset-password` | 重置密码及下次改密要求 |

用户、项目及成员写操作限管理员。最后一个启用的管理员不可被删除、禁用或降级。角色/状态变更和重置密码撤销会话与旧 Token；删除用户不删除项目数据或 S3 应用密钥。`must_change_password` 只允许访问个人设置并完成改密。

用户目录支持 q/role/after/limit，每页最多 100；项目与成员列表最多 1000。成员授权最多 1000 个不同桶，all 的 grants 必须为空；项目名 1～128 字节，描述最多 2000 字节。

## 应用密钥与 API Token

| 方法与路径 | 用途 |
| --- | --- |
| `GET/POST /api/v1/s3-credentials` | 列举、创建项目 S3 应用密钥 |
| `PUT/DELETE /api/v1/s3-credentials/{key}` | 修改状态、有效期、授权 / 删除 |
| `PUT /api/v1/s3-credentials/{key}/grants` | 完整替换逐桶 actions |
| `POST /api/v1/s3-credentials/{key}/rotate` | 新密钥和可选旧密钥重叠期 |
| `GET/POST /api/v1/api-tokens`、`PUT/DELETE /api/v1/api-tokens/{id}` | 列举、创建、更新及撤销个人管理 Token |

S3 密钥由管理员管理，属于项目，不随创建者删除；只能授权同项目桶。Token 权限与用户当前权限取交集，空 grants 没有桶权限。`system=true` 仅管理员可签发，必须空 grants，允许全实例管理；仍不能调用 Cookie 专属入口。普通 Token 无全局任务、审计和用户管理权限。

secret 仅创建或轮换时返回。Token 只存哈希；S3 secret 由 deployment key 加密。label 1～128 字节，授权最多 1000 桶。`expires_in` 为正时长、最多 3650 天：密钥省略或 null 不到期；Token 省略为 90 天、null 不到期。更新的 `keep_expiry=true` 保留原时间；轮换 overlap 为 0～30 天。最近使用最多五分钟落库一次。S3 密钥使用时间在签名验证后进入有界后台队列，同一密钥合并回写；排队满、数据库超时或进程退出时允许丢弃本次统计，请求不等待此项回写。

## 存储桶

| 方法与路径 | 用途 |
| --- | --- |
| `GET/POST /api/v1/buckets` | 可见桶目录 / 创建桶；请求可含 `name`、`project_id`、`storage_id`，目录最多 1000 项 |
| `GET /api/v1/bucket-projects` | 当前用户可创建桶的项目及额度 |
| `GET/PUT /api/v1/buckets/{bucket}/settings` | 带 revision 的统一设置 |
| `GET /api/v1/buckets/{bucket}/cors`、`PUT /api/v1/buckets/{bucket}/cors` | 读取或替换 CORS 设置 |
| `GET /api/v1/buckets/{bucket}/website`、`PUT /api/v1/buckets/{bucket}/website` | 读取或替换网站设置 |
| `POST /api/v1/buckets/{bucket}/purge/preview` | 空桶删除或清桶的影响预览 |
| `DELETE /api/v1/buckets/{bucket}` | 删除空桶 |
| `POST /api/v1/buckets/{bucket}/purge` | 清空并删除，返回 task_id |
| `POST /api/v1/buckets/{bucket}/transfer/preview`、`POST /api/v1/buckets/{bucket}/transfer` | 预览 / 执行项目转移 |

域名绑定由管理员统一维护：`GET/POST /api/v1/domains` 列举或创建 `{host,bucket_id}` 绑定，`DELETE /api/v1/domains/{host}` 删除绑定。一个 host 只能指向一个桶；删除桶或转移项目不会静默抢占域名。

桶信息含 id、name、project_id、actions、status、revision、uploads_paused、public_base_url、CORS 和网站设置。统一保存携带读取时的 revision；并发改变返回 412。成员需要 bucket.settings，只能改 CORS 与网站；域名、公共 URL 和暂停新上传限管理员。

删除、清桶和转移限管理员，执行时提交预览返回的 confirmation 与精确 confirm_name。空桶删除也要求没有残留流或上传历史。转移先暂停新上传并排空写入、multipart、额度预留，再检查目标额度、转移用量、清除旧桶授权，按目标项目范围重新计算权限；公共域名和桶限额保留，成功后恢复新上传。已开始的 multipart 在暂停期间仍可完成。

### 网站设置

默认关闭，index_document 默认 `index.html`，为 1～255 字节文件名；error_document 默认 `404.html`，为最多 1024 字节的桶内路径，空值使用内置 404。页面必须已存在且 public-read，不改变 S3 路由。域名绑定不会抢占其他桶，public_base_url 只用于生成链接，不配置 DNS/TLS。

### CORS 设置

每桶最多 100 条，同时作用于 S3 与公共读端口，不改变 ACL。字段为 origins、methods、headers、expose、max_age；origin 支持 HTTP(S) 或 `*`，methods 支持 GET/HEAD/POST/PUT/DELETE/OPTIONS，headers/expose 为头名或 `*`。`[]` 关闭。Web 可填入预设；匹配规则见 [S3 CORS](s3-compatibility.md#cors)。

## 存储额度

`GET/PUT /api/v1/quotas/{scope_type}/{scope_id}` 读取 / 完整替换 project 或 bucket 额度。上限为 logical_bytes_limit、inflight_bytes_limit、bucket_count_limit，非负十进制字符串或 null；null 不限，`"0"` 为零。桶只设置 logical_bytes_limit，写操作限管理员。

逻辑用量按当前对象原始大小计算，去重和压缩不抵扣。接收前预留、覆盖抵扣旧版本、发布时转为已用；在途预算另含未完成分片及并发覆盖。降低额度不删文件，不增加用量的替换仍可执行。超额返回 403 QuotaExceeded。

桶账本需 storage.inspect。项目的完整汇总只向具有全项目范围的成员显示；受限成员仍可看项目共享上限，其他汇总为 null。受限 Token 只能读取自己的桶账本。

## 媒体目录与检索

`GET /api/v1/buckets/{bucket}/objects` 需 bucket.list，返回 items/prefixes/next_cursor/layout/search_mode；对象版本为 id UUID。

| 参数 | 取值 |
| --- | --- |
| prefix / recursive | 路径前缀 / 是否展开子目录 |
| q / mode / search_in | 搜索词；contains/prefix/exact；name/path |
| category / public | image/video/audio/document/archive/other；true/false |
| min_size / max_size | 原始字节范围，含边界 |
| since / until | RFC3339 更新时间范围，含边界 |
| sort / order | name/size/modified；asc/desc |
| after / limit | 上页 next；每页 1～200，默认 100 |

前缀和完整路径区分大小写，q 接在 prefix 后；包含搜索不区分大小写，通配符按普通字符处理。无法提取连续三个文字/数字时改为路径前缀，响应明确 search_mode。无筛选的名称升序使用虚拟目录，其余为平面文件列表，不推算文件夹大小。

图库索引在数据库迁移中建立，服务启动后即可使用搜索、筛选和排序。查询预算 2 秒，目录摘要与对象变更同事务更新。

### 预览与下载

| `GET /api/v1/buckets/{bucket}/object` 后缀 | 用途与权限 |
| --- | --- |
| 无 | 详情和可用预览类型，bucket.list |
| `/content` | 原文件；preview 控制安全内联，object.read |
| `/thumbnail` | PNG 缩略图，每次验证 object.read |
| `/text` | 最多前 64 KiB 的安全文本，object.read |

均用 key/version 绑定当前对象，覆盖后旧版本返回 412。图片、音频和视频按允许的 MIME 明确加载；HTML/SVG 不嵌入，XML 等按文本显示。下载带 nosniff、sandbox CSP 和 private/no-store；支持 Range 与条件读取。服务端缩略图受[格式、像素和内存限制](configuration.md#监听与管理)，失败保留类型卡片，不自动下载原图。

## 对象操作

`POST /api/v1/media/actions`：bucket、action、objects；每项 `{client_id,key,version,target_key?,target_version?}`。跨存储复制或移动还要显式提交 `confirm_storage_migration:true`。最多 1000 项、共享两个操作名额、60 秒请求期限。HTTP 200 返回 results，须逐项检查 status/code/output_version/task_id/replayed。

| action | 权限与语义 |
| --- | --- |
| delete | object.delete；解除引用，按正常 GC 回收 |
| private / public-read | object.acl |
| copy | 源 read、目标 write；默认私有，公开副本另需目标 acl |
| move | 源 read/delete、目标 write；保留 ACL，公开对象另需目标 acl |
| metadata | 源 read/write；公开对象另需 acl，产生新版本 |

复制/移动使用 target_bucket 和逐项 target_key；替换目标须提供 target_version。源/目标变化返回逐项 412。复制仅复用引用；同项目移动按逻辑净变化记账，跨项目受目标额度约束。源桶与目标桶绑定不同 storage 时，未确认返回逐项 409 `StorageMigrationConfirmationRequired`；确认后返回逐项 status 202 和 task_id，创建 `storage_migration` 后台任务，将区块重新编码到目标存储。跨存储复制不共享去重，移动只在目标发布成功后退休源对象；任务失败会保留源对象。

client_id 是调用方 UUID。成功回执与变更同事务提交；同身份、同参数重试返回回执，参数不同返回 409 IdempotencyConflict。已确认失败的新尝试使用新 ID，未知结果保留原 ID。重试仍检查实时权限，回执按 task_retention 清理。Web 每批 20 项、选择限当前页；关闭浏览器不保留未确认的操作编号。

## 上传与传输中心

| 方法与路径 | 用途 |
| --- | --- |
| `POST /api/v1/buckets/{bucket}/uploads` | client_id、key、file_name、size、content_type、可选 modified_at/public_read/overwrite；幂等创建 |
| `GET /api/v1/uploads`、`GET /api/v1/uploads/{id}` | 分页目录 / 详情及存储落盘状态 |
| `GET /api/v1/uploads/{id}/parts` | 已接收分片及 SHA-256，每页最多 1000 |
| `PUT /api/v1/uploads/{id}/parts/{number}` | 原始字节和 Base64 `X-Content-SHA256` |
| `POST /api/v1/uploads/{id}/complete` | 完整有序清单 `{parts:[{number,etag,sha256}]}` |
| `DELETE /api/v1/uploads/{id}` | 终止上传 |

上传使用现有 multipart/配额/发布流程，不下发 S3 secret。默认每片 16 MiB，随文件大小增大以符合最多 10000 片、每片最多 5 GiB。列表支持 bucket/state/source/own/after/limit，每页最多 100；来源为 web/s3。

Web 发起者才能续传自己的上传；管理员可查看和终止其他上传。刷新后重新选择原文件，并核对已接收部分的完整校验和。完成仅表示对象已发布；remote_state 区分本地已保存与存储已就绪，上传缓存恢复要求见[备份材料](deployment-and-recovery.md#备份材料)。

## 存储

| 方法与路径 | 用途 |
| --- | --- |
| `GET /api/v1/storages` | 列出存储；返回 id、name、type、连接范围、prefix、block_key_id、三项并发上限、profile_revision、enabled、created_at、updated_at，不返回凭据 |
| `POST /api/v1/storages` | 创建存储；请求包含 `{name,type,initialize,path?,endpoint?,bucket?,region?,prefix?,block_key_id?,access_key?,secret_key?,profile?}`，type 为 `local` 或 `s3`；`initialize` 必须明确为 `true`，profile 使用完整 typed storage profile |
| `PUT /api/v1/storages/{name}` | 修改连接字段、prefix、block_key_id 或 enabled；name 和 id 不可修改；切换 type 需单容器维护状态并排空上传、任务及待写来源，核对目标 meta 后保存，成功后须重启；并发、压缩、区块包、缓存和维护预算通过 profile 保存 |
| `POST /api/v1/storages/{name}/check` | 读取并核对已有 `meta.json`，无请求体 |
| `GET /api/v1/storages/{name}/profile` | 读取该 storage 的 typed profile；CDC 联动开关、单容器 write_durability、合并等待 write_flush_interval、积压阈值 write_backlog_max_age、后台调度 background_scheduling、公开快照 public_cache_validation / public_cache_refresh_interval / public_cache_max_age、来源快照 read_source_validation / source_validation_max_age、额外摘要 read_integrity、连接超时、重试、压缩、pack、cache、integrity、配额与生命周期字段均在 profile 内 |
| `PUT /api/v1/storages/{name}/key` | 设置或清除 `{key?}` 形式的 block key 绑定；返回新的 profile revision |
| `POST /api/v1/storages/{name}/rewrite` | 对存量物理编码执行 `preview`、`start`、`status`、`pause`、`resume` 或 `retry`；`start` 必须提交预览 confirmation |
| `POST /api/v1/storages/{name}/unpack` | 对指定 pack 执行 `preview` 或 `start`；请求 `{action,pack_id,confirmation?}`，start 必须提交预览 confirmation |

`local` 请求使用 `path`（省略时为 `/data/storage`），`s3` 请求需要 endpoint、bucket、region、access_key 和 secret_key；prefix 默认为空。创建前目标命名空间必须为空并写入身份标识。更新连接信息只接受与已有身份标识匹配的目标，不会为缺失的 `meta.json` 初始化目标；凭据只用于写入，响应不会返回明文。存储名称为 1～128 字节且不能含 `/` 或控制字符，endpoint 必须是无查询串或片段的 HTTP(S) URL。

创建桶时，零个启用存储会失败，一个启用存储会自动绑定，多个启用存储必须在请求中提供 `storage_id`；桶绑定后不可更改。同一存储内的桶共享去重，不同 storage 之间不共享。群集模式拒绝 local 存储。存储管理限管理员或 system Token，并按通用约定要求最近验证密码。

`storage_id` 表示逻辑存储后端；物理对象位置使用 `location_id`，不会把物理位置 UUID 当作存储名称或桶绑定。区块编码和 pack 的 block key 字段统一为 `block_key_id`。

存储类型切换未满足单容器维护状态或排空条件时返回 `409 StorageNotDrained`，不会改变连接；成功后当前进程要求重启。请求中的 type、字段组合或存储名称无效时不会写入数据库；存储不存在、命名空间非空、`meta.json` 缺失或身份不匹配也会拒绝操作并返回错误。确认具体错误时使用响应中的 `code` 和 `request_id`；不要通过重复创建来初始化已有存储。

## 存储洞察

| 方法与路径 | 内容 |
| --- | --- |
| `GET /api/v1/usage/summary` | bucket 或 project 范围快照与容量历史 |
| `GET /api/v1/usage/runtime` | 管理员可见的运行历史、本地缓存、待上传及近期任务 |
| `GET /api/v1/storage/packs`、`GET /api/v1/storage/packs/{id}` | 授权范围的区块包及成员分页 |
| `GET /api/v1/storage/packs/{id}/objects` | 当前引用文件，另需 bucket.list |
| `GET /api/v1/buckets/{bucket}/object/chunks` | bucket/key/version 对象区块，需 storage.inspect；after/limit 分页 |

### 容量快照

按当前 storage.inspect 权限确定范围，省略时管理员看全局、成员看全部获授权桶。成员不获得未授权桶引用、完整共享包大小、全局成员序号或密钥标识。

空间口径：R 为原始逻辑量，U 为范围内引用区间并集，D 为先项目间、再桶间均分的内容归属，A 为所选物理来源编码占用的分摊。`R-A=(R-U)+(U-D)+(D-A)`，分别为范围内去重、共享内容和编码节省；U 跨桶不能相加，节省可能为负。归属用于说明空间，不用于收费或逻辑配额。

无远端来源时计入 pending，A 和编码节省为 null。全局 physical 区分当前来源、过渡副本、待回收与未确认来源；只统计数据库索引，不含提供商对象版本、未知存储对象或账单规则。

页面读取后台快照；collecting 表示首次收集中，as_of 是实际采集时间，失败保留旧结果并显示 stale。历史采样有上限和保留期，进程重启处分段，无采样不造趋势。

## 运行状态

`GET /api/v1/system/status` 与 `cli status` 相同，提供进程、连接池、资源预算、读写名额、存储 read/upload/control 队列和目录索引状态；`cache_jobs_active` 为当前缓存填充/清理任务数；`credential_activity` 返回凭据使用时间队列的 `entries`、`pending`、`completed`、`dropped`、`failed`，计数按进程累计，completed 包含已检查但尚未到五分钟回写间隔的记录；`public_read_cache` 返回独立公开快照的 `limit_bytes`、`used_bytes`、`entries`、`hits`、`refreshed`、`failed` 和 `skipped`，失败不延长成功验证期限，已淘汰但仍被请求或队列持有的条目持续计费；存储状态的 `source_cache` 返回来源健康快照的 `limit_bytes`、`used_bytes`、`entries`、`hits`、`refreshed`、`failed` 和 `skipped`，额度包含在途读取持有的条目，失败不续期；`cors_cache` 返回规则缓存的 `entries`、`hits`、`parses`、`invalidated`、`evicted`、`bypassed` 和字节额度，`live_bytes` 包含已淘汰但仍被请求或响应头持有的规则额度；`upload_cache.raw_processing` 提供后台 raw 去重的 `pending`、`failed` 和 `bytes`，失败记录保留并重试，不影响已有独立来源；`upload_cache.raw_uploads` 用同样三个字段表示必须排空的 raw 来源，既有待上传工作不因关闭缓存而丢弃；`upload_cache.buffered_files` 返回待同步 raw 的 `pending`、`bytes` 和 `oldest`，`upload_cache.buffered_chunks` 用相同字段返回待同步区块（bytes 为原始大小），同步失败保留待办；`upload_cache.fallbacks` 和 `upload_cache.rejections` 分别统计本进程缓存不可用时的同步回退与策略拒绝次数，重启清零。`GET /api/v1/system/config` 提供脱敏配置、来源、值模式和重启要求，不读取秘密文件内容；来源由 `origin` 标识，值模式由 `value_mode` 标识。

`upload_cache.write_memory` 返回 `capacity` / `used` 字节额度，含发送中尚未释放的 RAM 正文；`buffered_files` 与 `buffered_chunks` 各自的 `volatile` 是待排空 RAM 来源数；已丢失来源通过 `/api/v1/maintenance/quarantines` 查询，启动日志同时报告数量，不会被计作已排空的健康数据。

运行计数本进程累计、重启归零：HTTP started/completed/active/failed/canceled、4xx/5xx、字节及延时；缓存命中/查找、存储请求/字节、GC 成败。失败率为 `(failed+canceled)/completed`，耗时分位数是倍增直方图的近似上界。统计不是账单或逐请求日志；Linux RSS 也不等于含页缓存的容器内存。

存储诊断区分后端 permission/timeout/missing/other、内容 length/authentication/decompression/hash/metadata/missing_key，以及索引缓存丢失、无效、回退和 pending_unavailable。共享来源读取按实际物理尝试计数，各 HTTP 请求分别计失败；后端传输成功后的内容校验失败不会计入后端传输失败。队列 `waiting` 记录获取许可的耗时、完成和取消；外层超时导致的取消也计入 canceled。普通读取只更新内存诊断，不写诊断数据库。服务状态 counters 使用 `storage.*` 键，群集节点状态在 `io.diagnostics` 中提供同一计数；缺报节点不能当成零。

## 控制设置

| 方法与路径 | 用途 |
| --- | --- |
| `GET /api/v1/runtime-settings` | 列出数据库中的 runtime settings 分组和 `restart_required` 状态 |
| `GET /api/v1/runtime-settings/{section}` | 读取一个 runtime settings 分组 |
| `PUT /api/v1/runtime-settings/{section}` | 管理员按 revision 替换一个分组；Web 设置也使用 `section=web`，值按当前 Rust schema 校验 |
| `POST /api/v1/runtime-settings/{section}/reset` | 恢复一个分组的当前代码默认值 |

runtime settings 首次从空数据库启动时由当前配置生成，之后由数据库统一保存。保存后响应只标记需要重启的字段；监听地址、数据库连接、部署密钥和资源预算不属于此 API，重启前请求继续使用旧值。

## 群集注册

| 方法与路径 | 用途 |
| --- | --- |
| `GET /api/v1/cluster/status` | 读取 deployment mode、enabled 节点数和 local storage 数 |
| `POST /api/v1/cluster/mode` | 在维护模式、工作已排空且独占运行权时切换 single/cluster，递增 mode revision；当前进程保持受限，必须重启 |
| `GET /api/v1/cluster/nodes` | 列出节点名称、角色、启用状态和最近 heartbeat |
| `POST /api/v1/cluster/nodes/{name}/release` | 释放节点、撤销凭据并重新排队其任务 |
| `POST /api/v1/cluster/recover` | 使用 deployment key 确认恢复模式并释放待恢复节点；仅 control 可调用 |

群集接口仅 PostgreSQL 可用；其他后端返回 `501 NotImplemented`，CLI 同样拒绝。`uploads.staging_storage_id` 和 `staging_bytes_limit` 仅 PostgreSQL 返回和接受，单容器的 PostgreSQL 可预先设置供模式切换使用。

节点丢失 `node.json` 时先释放旧节点，再重启对应角色完成本机注册。节点凭据不会出现在 URL、列表或普通 API 响应中。

## 整理工作台与服务诊断

以下入口限管理员或明确签发的 system Token。

| 方法与路径 | 用途 |
| --- | --- |
| `GET /api/v1/maintenance` | controls 中每项以 policy 标识策略，附带周期、当前/最近任务、下次检查、打包开关和排空状态 |
| `POST /api/v1/maintenance/{policy}/actions` | pause/resume/run；policy 为 pack/reuse/reclaim/repack/range/gc/cleanup |
| `POST /api/v1/maintenance/mode` | `{enabled}`；启用或退出维护模式 |
| `POST /api/v1/maintenance/pack-creation` | `{enabled}`；允许或停止新包生成 |
| `POST /api/v1/maintenance/flush` | 将已有上传缓存写入存储 |
| `POST /api/v1/maintenance/sweep` | 发起只读存储清查 |
| `POST /api/v1/maintenance/sweep/preview` | 根据已完成清查准备删除确认 |
| `POST /api/v1/maintenance/unpack/preview` | 单包 `{pack_id}` 或全部 `{all:true}` 拆包预览 |
| `POST /api/v1/maintenance/{operation}/execute` | `{preview_id,confirmation}`，operation 为 unpack/sweep |
| `GET /api/v1/keys` | 管理员密钥元数据；有效 block key 的 `nonce_capacity` 返回当前可预留比例、两个用途的十进制计数、近期时间估算及采样状态；诊断不可用时为空，不返回密钥材料 |
| `POST /api/v1/keys` | 只按名称和用途生成数据库密钥 |
| `POST /api/v1/keys/{id}/replace-preview` | `{target?,name?}`；预览全部当前关联存储及 profile revision，二者不能同时指定；缺省新建同算法密钥，预览十分钟有效 |
| `GET /api/v1/key-replacements/{id}` | 当前操作者的持久预览和可空提交结果；浏览器账号、个人 Token、CLI 节点相互独立 |
| `POST /api/v1/key-replacements/{id}` | 近期验证后原子提交；范围或 revision 改变返回 409；结果保留七天供同次重试，返回 `pending` 本机刷新未完成的存储及 `cluster` 标记 |
| `DELETE /api/v1/keys/{id}` | 撤销并删除未被任何 storage 或物理来源使用的数据库密钥 |

暂停在工作边界生效，已经发出的请求可完成。新包开关覆盖上传与所有维护路径；全部拆包需停止生成并等 preparing 包排空，未完成的全部拆包阻止恢复生成。旧载荷仍受 GC 宽限和最低存储期保护。

执行预览绑定账号/Token 和影响范围，十分钟有效，重复请求返回同一任务。破坏性 sweep 另需维护模式、排空操作及非空索引，每次删除前再次查询数据库。CLI 对应命令和恢复步骤见[维护参考](cli-reference.md#状态与维护)。

## 后台任务

`GET /api/v1/tasks` 支持 state/policy/bucket/storage/actor/token、分页游标和 limit（默认 50，最多 200），返回 items/next_cursor；policy 匹配 pack 任务的维护策略（如 reuse、range），其他任务匹配 task_type；storage 会筛选直接关联该后端存储或其存储桶的任务。`GET /api/v1/tasks/{id}` 返回 task_type、policy、状态、来源、发起者、处理计数、阻止原因及 detail。`POST /api/v1/tasks/{id}/actions` 提交 pause/resume。

单容器任务的 CLI 结果和巡检报告不含群集父子任务、节点与过期租约字段；执行令牌不作为公开任务字段返回。

状态为 queued/running/paused/completed/failed。暂停不回滚已提交批次；重启将 running 重排，paused 保持暂停。维护前提和策略可能阻止继续。任务创建成功只表示排队，不代表完成。`storage_migration` 的 detail 保存源/目标桶、对象版本、目标键和 copy/move 动作；可用同一任务接口查看、暂停和恢复。

## 完整性巡检

| 方法与路径 | 用途 |
| --- | --- |
| `POST /api/v1/integrity` | `{mode?,bucket?,key?}`；metadata/head/full，key 需 bucket |
| `GET /api/v1/tasks/{id}/issues` | after/limit/code，异常及分组，每页最多 200 |
| `GET /api/v1/tasks/{id}/issues/{issue}/objects` | 当前关联文件，每页 100；after 为返回的游标 |
| `GET /api/v1/tasks/{id}/report` | 已完成巡检的 JSONL 下载 |

metadata 检查对象映射与区块身份，head 增加存储存在性和长度，full 下载并认证、解压、校验 BLAKE3。pending 的本地唯一副本在所有模式下检查。巡检绕过可丢弃缓存、不修复或删除数据；不重算整文件 ETag，不覆盖活动上传或无引用块。在线扫描保存上界，期间变化可被跳过，不是瞬时一致性快照。

异常代码包含 remote_missing、length_mismatch、pending_unavailable、chunk_metadata、missing_key、authentication_failed、decompression_failed、hash_mismatch、object_metadata、mapping_gap、mapping_source、object_length。网络/权限/超时让任务 failed 并保留进度，不记作坏块。completed 仍须查看异常数。

报告首行为 `{type:"task",task:...}`，随后为 issue，完整末行为 `{type:"end",issues:N}`。中断或清理造成缺失时没有结束行。异常随已完成任务到期；failed/paused 不自动删除。

## 手工管理入口

`GET/PUT /api/v1/buckets/{bucket}/cors`、`POST /api/v1/cli`、`GET /api/v1/packs`、`GET /api/v1/packs/{id}`、`POST /api/v1/cache/flush` 和 `GET /api/v1/tasks/{id}/report` 已纳入同一 OpenAPI 合约；它们使用现有 control service 和统一权限边界。

页面为 Vue 静态资源，运行无需 Node.js。带内容哈希的资源可长期缓存，入口 HTML 和品牌素材需重新验证，私有 API 不缓存。三语按需加载、明暗主题与账户同步规则见[管理指南](management.md)。

### 节点报告与配置生效

群集只读查询要求管理员，但不要求最近五分钟重新验证密码；mode/release/recover 的危险操作规则保持不变。

| 方法与路径 | 返回内容 |
| --- | --- |
| `GET /api/v1/cluster/status` | 保留 mode/enabled_nodes/local_storages，增加按角色、联系、工作和配置状态统计的 nodes、分组生效汇总 settings 和 observed_at |
| `GET /api/v1/cluster/nodes` | `{items,next_cursor}`；支持 name/role/contact/work/configuration/section/after/limit，默认 50、最多 200，游标绑定筛选条件 |
| `GET /api/v1/cluster/nodes/{id}/status` | 节点身份和新鲜度、启动编号、资源与工作快照、最多 20 项当前任务及 more_tasks |
| `GET /api/v1/cluster/nodes/{id}/config` | 节点实际加载的脱敏 fields、加载时间 loaded_at、加载 revisions 和当前配置比较结果 |
| `GET /api/v1/cluster/settings` | sections 的生效计数和可分页 nodes；筛选方式与节点列表一致 |

节点查询用稳定 id；原释放接口仍以名称定位。`configuration` 为 applied/equivalent/restart_required/unknown；分组 settings 还可能为 not_applicable。equivalent 表示当前适用值相同但修订不同。`report_fresh=false` 的快照仍可查看，不能据此认定当前运行状态。`sampled_at` 是采集时刻，`received_at` 由数据库记录；任务清单查询当前数据库，不属于历史快照。

### 运行历史

`GET /api/v1/usage/runtime` 返回明确的 `scope`。single 的 `current/history` 延续进程指标；cluster 的本机字段为 null/空数组，`cluster.current/history` 返回后台汇总、窗口开始时间、预期/报告/有效差分节点数、gauges 和 rates，`stale` 标示汇总过期。`cluster.current` 为最新点，history 为其之前的点；两者合计受 limit 限制。缺少节点或差分基线时不外推为整个群集。

`GET /api/v1/cluster/nodes/{id}/runtime` 返回节点 history，包含 node_id、boot_id、sequence、数据库 received_at、单调 uptime_seconds、采样间隔和进程指标。两个接口都支持 `hours`（默认 24，1–168）和 `limit`（默认 2048，1–2048），只返回范围内最近的有限采样，按时间升序排列。群集查询使用主库、限制执行时间，只允许管理员读取，不要求近期密码重验。

## 来源隔离与复检

仅管理员可操作。`source` 为 `chunk`（独立编码）、`pack` 或 `raw`（原始文件）。`id` 是 encoding/pack 的正十进制字符串，raw 则使用原始来源 UUID，不是对象或逻辑 chunk ID。

| 方法与路径 | 用途 |
| --- | --- |
| `GET /api/v1/maintenance/quarantines` | 必须提供 `storage_id`、`source`；`after` 为可省略的字符串游标、`limit` 默认 50，范围 1～200；返回 `{items,next}`，next 为下一页游标或 null |
| `POST /api/v1/maintenance/quarantine/preview` | `{storage_id,source,id}`；完整核查精确物理来源及命名空间，返回带确认词的维护预览 |
| `POST /api/v1/maintenance/recheck/preview` | 相同范围，目标须已隔离；预览不解除隔离 |
| `POST /api/v1/maintenance/{quarantine,recheck}/execute` | `{preview_id,confirmation}`；要求近期密码验证和对应会话 CSRF，返回 `{task_id}`；重复执行同一预览返回同一任务 |

预览绑定位置、profile、身份和隔离代次；过期或已变化返回冲突。任务复核当前授权和执行 token，事务内提交隔离状态、代次、结果和审计。复检须完整回源校验；接受或 HEAD 成功均不代表恢复完成。任务详情不返回内部授权依据或维护输入快照。隔离列表的 retired/deleting/deleted 项只供查看，不能复检为 ready。

没有单独的正文修复 API。普通 S3/Web 上传持有同存储、同长度及同 BLAKE3 的完整区块时，可以为仅有隔离来源的逻辑区块生成并完整验证新编码。沿用原上传权限和最终发布条件，不能通过 Copy 或解除隔离接口跳过回读。恢复审计使用 `source.recover`，记录逻辑区块、新编码及旧来源摘要，不包含正文或密钥。
